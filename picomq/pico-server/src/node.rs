//! Node assembly: metadata plane + s3stream engine + topics over logs.
//!
//! Intentionally not here: the metadata layer is a
//! [`picomq_metadata::CommandSink`] the host constructs (`LocalSink` or
//! `picomq-sql`'s `SqlSink`) and owns, including its shutdown and any lease
//! keeper.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use picomq_auth::Authorizer;
use picomq_metadata::{
    CommandSink, MetadataError, MetadataNodeHandle, Registration, ViewPublisher,
};
use s3stream::{
    Config, Error as StreamError, ObjectStorageTrait, S3StreamBuilder, S3StreamEngine, WalError,
};
use s3stream_core::StorageFailureHandler;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::auth::TokenService;
use crate::error::ServiceError;
use crate::failover::{Factory, Failover};
use crate::group::GroupCoordinator;
use crate::log::Log;
use crate::ownership::MetadataOwnershipService;
use crate::service::S3StreamService;
use crate::streams::Streams;
use crate::topics::Topics;
use crate::transfer::TransferWatcher;
use crate::waiter::StreamWaiterRegistry;
use crate::wal::{ObjectWal, Wal};

const DELETED_CLEANUP_TICK: Duration = Duration::from_secs(1);

/// Node identity and engine tuning the host passes in.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: i32,
    /// The requested epoch. The node runs at the epoch its registration is
    /// assigned, at least one past every earlier incarnation.
    pub node_epoch: i64,
    /// Advertised address, used for redirects.
    pub http_address: String,
    /// Placement weight.
    pub slots: u32,
    /// Advertised addresses of additional wire protocols, keyed by protocol
    /// name (e.g. `"kafka"`).
    pub protocol_addresses: std::collections::BTreeMap<String, String>,
    pub cluster_id: String,
    pub heartbeat_interval: Duration,
    /// How long a node may go without a heartbeat before the cluster fences
    /// it, and before it stops serving on its own.
    pub node_timeout: Duration,
    pub engine: Config,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            node_epoch: 1,
            http_address: "http://127.0.0.1:4437".to_owned(),
            slots: 1,
            protocol_addresses: Default::default(),
            cluster_id: "picomq".to_owned(),
            heartbeat_interval: Duration::from_secs(5),
            node_timeout: Duration::from_secs(30),
            engine: Config::default(),
        }
    }
}

pub struct PicoNode {
    config: NodeConfig,
    handle: MetadataNodeHandle,
    views: Arc<ViewPublisher>,
    engine: Arc<S3StreamEngine>,
    service: Arc<S3StreamService>,
    topics: Arc<Topics>,
    ownership: Arc<MetadataOwnershipService>,
    groups: Arc<GroupCoordinator>,
    tokens: Arc<TokenService>,
    transfer_watcher: tokio::task::JoinHandle<()>,
    deleted_cleanup: tokio::task::JoinHandle<()>,
    liveness: tokio::task::JoinHandle<()>,
    fenced: watch::Sender<bool>,
}

impl PicoNode {
    /// Starts a node whose WAL lives in object storage.
    pub async fn start(
        config: NodeConfig,
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
        object_storage: Arc<dyn ObjectStorageTrait>,
        wal_storage: Arc<dyn ObjectStorageTrait>,
        schema_registry: Option<Arc<dyn picomq_schema::SchemaStore>>,
    ) -> Result<Self, ServiceError> {
        let wal = ObjectWal::new(&config.cluster_id, &config.engine.wal_config, wal_storage);
        Self::start_with_wal(
            config,
            sink,
            views,
            object_storage,
            Arc::new(wal),
            schema_registry,
        )
        .await
    }

    /// Registers the node, starts its heartbeat, then opens its WAL at the
    /// assigned epoch and recovers it, so a slow recovery never looks like a
    /// silent node.
    pub async fn start_with_wal(
        config: NodeConfig,
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
        object_storage: Arc<dyn ObjectStorageTrait>,
        wal: Arc<dyn Wal>,
        schema_registry: Option<Arc<dyn picomq_schema::SchemaStore>>,
    ) -> Result<Self, ServiceError> {
        let handle = MetadataNodeHandle::register(
            sink,
            views.clone(),
            Registration {
                node_id: config.node_id,
                node_epoch: config.node_epoch,
                http_address: config.http_address.clone(),
                slots: config.slots,
                protocol_addresses: config.protocol_addresses.clone(),
                wal: wal.location(),
            },
        )
        .await
        .map_err(|e| e.to_stream_error())?;

        let fenced = watch::Sender::new(false);
        let liveness = spawn_liveness(
            handle.clone(),
            config.heartbeat_interval,
            config.node_timeout,
            fenced.clone(),
        );
        let engine = match Self::open_engine(&config, &handle, object_storage, wal, &fenced).await {
            Ok(engine) => Arc::new(engine),
            Err(error) => {
                liveness.abort();
                return Err(error);
            }
        };

        let kv_client = handle.kv_client();
        let streams = Arc::new(Streams::new(
            engine.stream_client(),
            views.clone(),
            handle.clone(),
        ));
        let service = Arc::new(S3StreamService::new(
            streams,
            kv_client.clone(),
            views.clone(),
            Arc::new(StreamWaiterRegistry::new()),
        ));

        let mut topics = Topics::new(service.clone(), kv_client.clone(), views.clone());
        if let Some(registry) = schema_registry {
            topics = topics.with_schema_registry(registry);
        }
        let topics = Arc::new(topics);

        let ownership = Arc::new(MetadataOwnershipService::new(
            views.clone(),
            config.node_id,
            config.http_address.clone(),
            service.clone(),
        ));
        let groups = GroupCoordinator::new(
            config.node_id,
            service.clone(),
            topics.clone(),
            ownership.clone(),
        );
        let tokens = Arc::new(TokenService::new(kv_client));
        let transfer_watcher =
            TransferWatcher::spawn(service.clone(), views.clone(), config.node_id);
        let deleted_cleanup = service.spawn_deleted_cleanup(DELETED_CLEANUP_TICK);

        Ok(Self {
            config,
            handle,
            views,
            engine,
            service,
            topics,
            ownership,
            groups,
            tokens,
            transfer_watcher,
            deleted_cleanup,
            liveness,
            fenced,
        })
    }

    async fn open_engine(
        config: &NodeConfig,
        handle: &MetadataNodeHandle,
        object_storage: Arc<dyn ObjectStorageTrait>,
        wal: Arc<dyn Wal>,
        fenced: &watch::Sender<bool>,
    ) -> Result<S3StreamEngine, ServiceError> {
        let own = wal.open(handle.node_id(), handle.node_epoch()).await?;
        let engine = Config {
            node_id: config.node_id as u32,
            node_epoch: handle.node_epoch() as u64,
            wal_config: wal.location(),
            ..config.engine.clone()
        };

        Ok(S3StreamBuilder::new(engine)
            .object_storage(object_storage)
            .write_ahead_log(own)
            .stream_manager(Arc::new(handle.stream_manager()))
            .object_manager(Arc::new(handle.object_manager()))
            .failover_factory(Arc::new(Factory::new(handle.clone(), wal)))
            .storage_failure_handler(Arc::new(Fenced(fenced.clone())))
            .build()
            .await?)
    }

    pub fn service(&self) -> Arc<S3StreamService> {
        self.service.clone()
    }

    pub fn topics(&self) -> Arc<Topics> {
        self.topics.clone()
    }

    pub fn ownership(&self) -> Arc<MetadataOwnershipService> {
        self.ownership.clone()
    }

    pub fn groups(&self) -> Arc<GroupCoordinator> {
        self.groups.clone()
    }

    pub fn tokens(&self) -> Arc<TokenService> {
        self.tokens.clone()
    }

    pub fn authorizer(&self) -> Arc<Authorizer> {
        self.tokens.authorizer()
    }

    pub fn advertised_address(&self) -> &str {
        &self.config.http_address
    }

    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    pub fn metadata(&self) -> &MetadataNodeHandle {
        &self.handle
    }

    /// Request a live ownership move of a log to another node.
    pub async fn transfer(&self, log: &Log, to_node: i32) -> Result<u64, ServiceError> {
        let Some(stream_id) = self.service.stream_id(log)? else {
            return Err(ServiceError::kind(crate::ErrorKind::NotFound));
        };
        let view = self.views.load();
        let Some(row) = view.state.streams.get(&stream_id).copied() else {
            return Err(ServiceError::kind(crate::ErrorKind::NotFound));
        };
        if !view
            .state
            .nodes
            .get(&to_node)
            .is_some_and(|node| !node.fenced)
        {
            return Err(ServiceError::with_message(
                crate::ErrorKind::BadRequest,
                None,
                false,
                format!("node {to_node} is not a live node"),
            ));
        }

        self.handle
            .propose_transfer(stream_id, row.node_id, to_node)
            .await
            .map_err(|e| e.to_stream_error())?;
        Ok(stream_id)
    }

    pub fn spawn_failover(
        &self,
        leadership: watch::Receiver<bool>,
        tick: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let failover = Failover {
            handle: self.handle.clone(),
            views: self.views.clone(),
            engine: self.engine.clone(),
            streams: self.service.streams().clone(),
            heartbeat_interval: self.config.heartbeat_interval,
            node_timeout: self.config.node_timeout,
        };

        tokio::spawn(failover.run(leadership, tick))
    }

    pub fn fenced(&self) -> watch::Receiver<bool> {
        self.fenced.subscribe()
    }

    pub fn views(&self) -> Arc<ViewPublisher> {
        self.views.clone()
    }

    pub fn engine(&self) -> &S3StreamEngine {
        &self.engine
    }

    pub async fn close(&self) {
        self.transfer_watcher.abort();
        self.deleted_cleanup.abort();
        self.liveness.abort();
        self.service.shutdown().await;
        self.engine.shutdown().await;

        let node = self.handle.proposer();
        let Some(heartbeats) = self
            .views
            .load()
            .state
            .nodes
            .get(&node.node_id)
            .filter(|_| !*self.fenced.borrow())
            .map(|row| row.heartbeats)
        else {
            return;
        };
        if let Err(error) = self
            .handle
            .fence_node(node.node_id, node.node_epoch, heartbeats)
            .await
        {
            tracing::warn!(%error, "could not hand off this node's streams");
        }
    }
}

/// Keeps this node's membership: heartbeats every interval and stops the
/// node once its registration is fenced or replaced, or once no heartbeat
/// landed for the node timeout, about when the cluster fences it.
fn spawn_liveness(
    handle: MetadataNodeHandle,
    interval: Duration,
    timeout: Duration,
    fenced: watch::Sender<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let node = handle.proposer();
        let views = handle.views().clone();
        let mut beat = tokio::time::interval_at(Instant::now() + interval, interval);
        let mut landed = Instant::now();

        loop {
            let view = views.load();
            if !view.state.is_live(node) {
                tracing::error!("this node was fenced by the cluster, stopping");
                fenced.send_replace(true);
                return;
            }

            tokio::select! {
                _ = beat.tick() => {
                    match tokio::time::timeout(interval, handle.heartbeat()).await {
                        Ok(Ok(())) => landed = Instant::now(),
                        Ok(Err(MetadataError::NodeEpochMismatch { .. })) => continue,
                        Ok(Err(error)) => tracing::warn!(%error, "heartbeat failed"),
                        Err(_) => tracing::warn!("heartbeat timed out"),
                    }
                    if landed.elapsed() >= timeout {
                        tracing::error!("no heartbeat landed within the node timeout, stopping");
                        fenced.send_replace(true);
                        return;
                    }
                }
                () = views.wait_past(view.applied_index, interval) => {}
            }
        }
    })
}

/// Stops the node once its WAL is fenced: another node took it over.
struct Fenced(watch::Sender<bool>);

#[async_trait]
impl StorageFailureHandler for Fenced {
    async fn handle(&self, error: &StreamError) {
        if let StreamError::Wal(WalError::Fenced { .. }) = error {
            tracing::error!(%error, "this node's WAL was taken over, stopping");
            self.0.send_replace(true);
        }
    }
}
