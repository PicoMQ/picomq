//! Node assembly: metadata plane + s3stream engine + topics over logs.
//!
//! Intentionally not here: the metadata layer is a
//! [`picomq_metadata::CommandSink`] the host constructs (`LocalSink` or
//! `picomq-sql`'s `SqlSink`) and owns, including its shutdown and any lease
//! keeper.

use std::sync::Arc;

use picomq_auth::Authorizer;
use picomq_metadata::{CommandSink, MetadataNodeHandle, ViewPublisher};
use s3stream::{
    Config, ObjectStorageTrait, ObjectWalConfig, ObjectWalService, S3StreamBuilder, S3StreamEngine,
    WriteAheadLogTrait,
};

use crate::auth::TokenService;
use crate::error::ServiceError;
use crate::group::GroupCoordinator;
use crate::log::Log;
use crate::ownership::MetadataOwnershipService;
use crate::service::S3StreamService;
use crate::streams::Streams;
use crate::topics::Topics;
use crate::transfer::TransferWatcher;
use crate::waiter::StreamWaiterRegistry;

/// Node identity and engine tuning the host passes in.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: i32,
    pub node_epoch: i64,
    /// Advertised address, used for redirects.
    pub http_address: String,
    /// Placement weight.
    pub slots: u32,
    /// Advertised addresses of additional wire protocols, keyed by protocol
    /// name (e.g. `"kafka"`).
    pub protocol_addresses: std::collections::BTreeMap<String, String>,
    pub cluster_id: String,
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
            engine: Config::default(),
        }
    }
}

pub struct PicoNode {
    config: NodeConfig,
    handle: MetadataNodeHandle,
    views: Arc<ViewPublisher>,
    engine: S3StreamEngine,
    service: Arc<S3StreamService>,
    topics: Arc<Topics>,
    ownership: Arc<MetadataOwnershipService>,
    groups: Arc<GroupCoordinator>,
    tokens: Arc<TokenService>,
    transfer_watcher: tokio::task::JoinHandle<()>,
}

impl PicoNode {
    pub async fn start(
        config: NodeConfig,
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
        object_storage: Arc<dyn ObjectStorageTrait>,
        wal_storage: Arc<dyn ObjectStorageTrait>,
        schema_registry: Option<Arc<dyn picomq_schema::SchemaStore>>,
    ) -> Result<Self, ServiceError> {
        let mut wal_config = ObjectWalConfig::from_uri_or_defaults(&config.engine.wal_config)
            .map_err(|e| {
                ServiceError::with_message(crate::ErrorKind::BadRequest, None, false, e.to_string())
            })?;
        wal_config.cluster_id = config.cluster_id.clone();
        wal_config.node_id = config.node_id as u32;
        wal_config.epoch = config.node_epoch as u64;
        let wal = Arc::new(ObjectWalService::new(wal_storage, wal_config));
        Self::start_with_wal(config, sink, views, object_storage, wal, schema_registry).await
    }

    pub async fn start_with_wal(
        config: NodeConfig,
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
        object_storage: Arc<dyn ObjectStorageTrait>,
        wal: Arc<dyn WriteAheadLogTrait>,
        schema_registry: Option<Arc<dyn picomq_schema::SchemaStore>>,
    ) -> Result<Self, ServiceError> {
        let handle =
            MetadataNodeHandle::new(config.node_id, config.node_epoch, sink, views.clone());
        handle
            .register_with_slots(
                &config.http_address,
                config.slots,
                config.protocol_addresses.clone(),
            )
            .await
            .map_err(|e| e.to_stream_error())?;

        let engine = S3StreamBuilder::new(config.engine.clone())
            .object_storage(object_storage)
            .write_ahead_log(wal)
            .stream_manager(Arc::new(handle.stream_manager()))
            .object_manager(Arc::new(handle.object_manager()))
            .build()
            .await?;

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
            views.clone(),
        );
        let tokens = Arc::new(TokenService::new(kv_client));
        let transfer_watcher =
            TransferWatcher::spawn(service.clone(), views.clone(), config.node_id);

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
        })
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
        self.handle
            .propose_transfer(stream_id, row.node_id, to_node)
            .await
            .map_err(|e| e.to_stream_error())?;
        Ok(stream_id)
    }

    pub fn views(&self) -> Arc<ViewPublisher> {
        self.views.clone()
    }

    pub fn engine(&self) -> &S3StreamEngine {
        &self.engine
    }

    pub async fn close(&self) {
        self.transfer_watcher.abort();
        self.service.shutdown().await;
        self.engine.shutdown().await;
    }
}
