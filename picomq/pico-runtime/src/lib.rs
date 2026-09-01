//! Assemble a `ServerConfig` into a running node: metadata store, storage, engine, listeners.

pub mod config;

use std::sync::Arc;
use std::time::Duration;

use picomq_auth::{AccessToken, Scope, TokenRecord, TokenStore, Verifier};
use picomq_http::{RunningServer, ServeOptions};
use picomq_metadata::{CommandSink, MetadataLifecycle, ObjectCleaner};
use picomq_server::{KvTokenStore, NodeConfig, PicoNode};
use picomq_sql::{
    LeaseConfig, LeaseKeeper, MetaStore, PgStore, SqlSink, SqlSinkConfig, SqliteStore,
};
use s3stream::{IdUri, ObjectStorageTrait, ObjectStoreAdapter};

pub use config::{AuthMode, KafkaConfig, MetaBackend, ServerConfig};

/// (`MetadataLifecycle`).
const LIFECYCLE_TICK: Duration = Duration::from_secs(1);

const SCHEMA_CACHE_EXPIRY: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("metadata store: {0}")]
    Store(#[from] picomq_sql::StoreError),
    #[error("metadata log: {0}")]
    MetadataLog(#[from] picomq_sql::SqlSinkError),
    #[error("object storage: {0}")]
    Storage(#[from] s3stream::ObjectError),
    #[error("node startup: {0}")]
    Node(#[from] picomq_server::ServiceError),
    #[error("bind {addr}: {source}")]
    Bind {
        addr: std::net::SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("create data directory {path}: {source}")]
    DataDir {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "refusing non-loopback bind {addr} on an unauthenticated listener: run with auth required, bind loopback, or pass --insecure-allow-remote"
    )]
    InsecureBind { addr: std::net::SocketAddr },
    #[error("bootstrap token: {0}")]
    BootstrapToken(#[from] picomq_auth::AuthError),
    #[error("bootstrap token {id:?} conflicts with a stored token of the same id")]
    BootstrapConflict { id: String },
}

/// A running PicoMQ process: metadata log, node, background maintenance and
/// the HTTP listeners.
pub struct PicoServer {
    server: RunningServer,
    kafka: Option<(std::net::SocketAddr, tokio::task::JoinHandle<()>)>,
    lease: LeaseKeeper,
    lifecycle: tokio::task::JoinHandle<()>,
    token_expiry: tokio::task::JoinHandle<()>,
    ttl_sweep: tokio::task::JoinHandle<()>,
    compaction_check: tokio::task::JoinHandle<()>,
    /// Kept alive for the process lifetime: dropping it aborts the log's
    /// flusher/tailer tasks, so it must outlive the node.
    sink: Arc<SqlSink>,
    schema_registry: Option<Arc<dyn picomq_schema::SchemaStore>>,
}

/// Open the metadata log, start the node, and serve the configured listeners,
/// in that order: metadata first (a node must be registered before it can
/// own streams), then storage and the engine, then listeners.
pub async fn start(config: ServerConfig) -> Result<PicoServer, RuntimeError> {
    if !config.insecure_allow_remote {
        // The Kafka listener carries no authentication, so a non-loopback
        // bind needs the explicit opt-out regardless of auth mode.
        let auth_off = config.auth_mode == AuthMode::Off;
        for addr in [
            auth_off.then_some(config.addr),
            auth_off.then_some(config.admin_addr).flatten(),
            config.kafka.as_ref().map(|kafka| kafka.listen),
        ]
        .into_iter()
        .flatten()
        {
            if !addr.ip().is_loopback() {
                return Err(RuntimeError::InsecureBind { addr });
            }
        }
    }

    let schema_registry = match &config.schema_registry {
        None => None,
        Some(uri) => {
            let registry = picomq_schema::Builder::from(open_adapter(uri)?.object_store())
                .with_cache_expiry_after(Some(SCHEMA_CACHE_EXPIRY))
                .build();
            tracing::info!(%uri, "schema registry enabled");
            Some(Arc::new(registry) as Arc<dyn picomq_schema::SchemaStore>)
        }
    };

    let store = open_store(&config.meta_backend).await?;
    let (sink, views) = SqlSink::open(store.clone(), SqlSinkConfig::default()).await?;
    let sink = Arc::new(sink);

    let storage_uri = config.storage_uri.clone();
    let object_storage = open_bucket(&storage_uri)?;
    let wal_storage = open_bucket(&config.wal_uri())?;

    let engine = s3stream::Config {
        cluster_id: config.cluster_id.clone(),
        node_id: u32::try_from(config.node_id).unwrap_or_default(),
        node_epoch: u64::try_from(config.node_epoch).unwrap_or_default(),
        data_buckets: vec![storage_uri],
        wal_config: config.wal_uri(),
        ..config.engine.clone()
    };
    // Bind before node registration so a default advertise (port 0 configs)
    // carries the real bound port.
    let kafka_listener = match &config.kafka {
        Some(kafka) => Some(bind_kafka(kafka.listen).await?),
        None => None,
    };
    let protocol_addresses = match (&config.kafka, &kafka_listener) {
        (Some(kafka), Some((_, bound))) => {
            let advertise = kafka.advertise.clone().unwrap_or_else(|| bound.to_string());
            std::collections::BTreeMap::from([(picomq_kafka::PROTOCOL_NAME.to_owned(), advertise)])
        }
        _ => Default::default(),
    };
    let node = Arc::new(
        PicoNode::start(
            NodeConfig {
                node_id: config.node_id,
                node_epoch: config.node_epoch,
                http_address: config.advertised_url(),
                slots: config.slots,
                protocol_addresses,
                cluster_id: config.cluster_id.clone(),
                engine,
            },
            sink.clone() as Arc<dyn CommandSink>,
            views,
            object_storage.clone(),
            wal_storage,
            schema_registry.clone(),
        )
        .await?,
    );

    if let Some(wire) = &config.bootstrap_token {
        bootstrap_token(node.tokens().store().as_ref(), wire).await?;
    }

    let lease = LeaseKeeper::spawn(
        store,
        format!("node-{}-{}", config.node_id, config.node_epoch),
        LeaseConfig::default(),
    );
    let lifecycle = Arc::new(MetadataLifecycle::new(
        sink.clone() as Arc<dyn CommandSink>,
        Arc::new(ObjectCleaner::new(
            sink.clone() as Arc<dyn CommandSink>,
            node.views(),
            Some(object_storage),
        )),
        LIFECYCLE_TICK,
    ))
    .drive(lease.leadership());
    let token_expiry = node
        .tokens()
        .spawn_expiry_loop(lease.leadership(), LIFECYCLE_TICK);
    let ttl_sweep = node
        .service()
        .spawn_ttl_sweep(lease.leadership(), LIFECYCLE_TICK);
    let compaction_check = node.service().spawn_compaction_check(LIFECYCLE_TICK);

    let kafka = kafka_listener
        .map(|(listener, bound)| (bound, spawn_kafka(&config, &node, listener, bound)));

    let addr = config.addr;
    let authorizer = match config.auth_mode {
        AuthMode::Required => Some(node.authorizer()),
        AuthMode::Off => None,
    };
    let server = picomq_http::serve(
        node,
        ServeOptions {
            protocol: config.http_protocol,
            addr,
            admin_addr: config.admin_addr,
            routing_mode: config.routing_mode,
            long_poll_timeout: config.long_poll_timeout,
            sse_max_duration: config.sse_max_duration,
            max_chunk_size: config.max_chunk_size,
            max_request_size: config.max_request_size,
            shutdown_drain: config.shutdown_drain,
            backlog: config.backlog,
            leadership: Some(lease.leadership()),
            authorizer,
        },
    )
    .await
    .map_err(|source| RuntimeError::Bind { addr, source })?;

    Ok(PicoServer {
        server,
        kafka,
        lease,
        lifecycle,
        token_expiry,
        ttl_sweep,
        compaction_check,
        sink,
        schema_registry,
    })
}

async fn bind_kafka(
    addr: std::net::SocketAddr,
) -> Result<(tokio::net::TcpListener, std::net::SocketAddr), RuntimeError> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|source| RuntimeError::Bind { addr, source })?;
    let bound = listener
        .local_addr()
        .map_err(|source| RuntimeError::Bind { addr, source })?;
    Ok((listener, bound))
}

fn spawn_kafka(
    config: &ServerConfig,
    node: &Arc<PicoNode>,
    listener: tokio::net::TcpListener,
    bound: std::net::SocketAddr,
) -> tokio::task::JoinHandle<()> {
    let broker = Arc::new(picomq_kafka::BrokerContext::new(
        config.node_id,
        config.cluster_id.clone(),
        node.service(),
        node.ownership(),
        node.views(),
        node.metadata().clone(),
    ));
    let listener_config = picomq_kafka::ListenerConfig {
        addr: bound,
        max_request_bytes: config.max_request_size,
        ..Default::default()
    };
    tracing::info!(%bound, "kafka listener started");
    tokio::spawn(async move {
        if let Err(error) = picomq_kafka::KafkaListener::new(listener_config, broker)
            .serve(listener)
            .await
        {
            tracing::error!(%error, "kafka listener stopped");
        }
    })
}

/// Seed the operator's root token. Idempotent: a restart with the same token
/// is a no-op. A different token under the same id fails startup instead of
/// silently rotating a live credential.
async fn bootstrap_token(store: &KvTokenStore, wire: &str) -> Result<(), RuntimeError> {
    let token = AccessToken::parse(wire)?;
    let verifier = Verifier::from_secret(&token.secret);
    let record = TokenRecord {
        id: token.id.clone(),
        verifier,
        scope: Scope::root(),
        created_at_ms: picomq_common::now_ms(),
        issued_by: String::new(),
    };
    if store.put_if_absent(record).await? {
        tracing::info!(id = %token.id, "bootstrap token stored");
        return Ok(());
    }
    let existing = store
        .get(&token.id)
        .await?
        .ok_or_else(|| RuntimeError::BootstrapConflict {
            id: token.id.clone(),
        })?;
    if existing.verifier == verifier && existing.scope == Scope::root() {
        return Ok(());
    }
    Err(RuntimeError::BootstrapConflict { id: token.id })
}

fn open_adapter(uri: &str) -> Result<ObjectStoreAdapter, RuntimeError> {
    let parsed = IdUri::parse(uri)?;
    if parsed.protocol == "file" {
        let path = std::path::PathBuf::from(&parsed.path);
        std::fs::create_dir_all(&path).map_err(|source| RuntimeError::DataDir { path, source })?;
    }
    Ok(ObjectStoreAdapter::from_bucket_uri(uri)?)
}

fn open_bucket(uri: &str) -> Result<Arc<dyn ObjectStorageTrait>, RuntimeError> {
    Ok(Arc::new(open_adapter(uri)?))
}

async fn open_store(backend: &MetaBackend) -> Result<Arc<dyn MetaStore>, RuntimeError> {
    Ok(match backend {
        MetaBackend::Sqlite(None) => Arc::new(SqliteStore::memory().await?),
        MetaBackend::Sqlite(Some(path)) => {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent).map_err(|source| RuntimeError::DataDir {
                    path: parent.to_path_buf(),
                    source,
                })?;
            }
            Arc::new(SqliteStore::open(path).await?)
        }
        MetaBackend::Postgres(url) => Arc::new(PgStore::connect(url).await?),
    })
}

impl PicoServer {
    /// The bound protocol address (resolves a configured port 0).
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.server.local_addr()
    }

    /// The bound admin address, when the admin listener is enabled.
    pub fn admin_addr(&self) -> Option<std::net::SocketAddr> {
        self.server.admin_addr()
    }

    pub fn base_url(&self) -> &str {
        self.server.base_url()
    }

    pub fn node(&self) -> Arc<PicoNode> {
        self.server.node()
    }

    /// The bound Kafka address, when the Kafka listener is enabled.
    pub fn kafka_addr(&self) -> Option<std::net::SocketAddr> {
        self.kafka.as_ref().map(|(addr, _)| *addr)
    }

    pub fn schema_registry(&self) -> Option<&Arc<dyn picomq_schema::SchemaStore>> {
        self.schema_registry.as_ref()
    }

    pub async fn shutdown(self) {
        if let Some((_, task)) = &self.kafka {
            task.abort();
        }
        self.server.shutdown().await;
        self.lifecycle.abort();
        self.token_expiry.abort();
        self.ttl_sweep.abort();
        self.compaction_check.abort();
        self.lease.shutdown().await;
        drop(self.sink);
    }
}
