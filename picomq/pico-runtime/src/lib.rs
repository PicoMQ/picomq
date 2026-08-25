//! Assemble a `ServerConfig` into a running node: metadata store, storage, engine, listeners.

pub mod config;

use std::sync::Arc;
use std::time::Duration;

use pico_auth::{AccessToken, Scope, TokenRecord, TokenStore, Verifier};
use pico_frontend::{RunningServer, ServeOptions};
use pico_metadata::{CommandSink, MetadataLifecycle, ObjectCleaner};
use pico_server::{KvTokenStore, NodeConfig, PicoNode};
use pico_sql::{LeaseConfig, LeaseKeeper, MetaStore, PgStore, SqlSink, SqlSinkConfig, SqliteStore};
use s3stream::{IdUri, ObjectStorageTrait, ObjectStoreAdapter};

pub use config::{AuthMode, MetaBackend, ServerConfig};

/// (`MetadataLifecycle`).
const LIFECYCLE_TICK: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("metadata store: {0}")]
    Store(#[from] pico_sql::StoreError),
    #[error("metadata log: {0}")]
    MetadataLog(#[from] pico_sql::SqlSinkError),
    #[error("object storage: {0}")]
    Storage(#[from] s3stream::ObjectError),
    #[error("node startup: {0}")]
    Node(#[from] pico_server::ServiceError),
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
        "auth is off: refusing non-loopback bind {addr}, run with auth required, bind loopback, or pass --insecure-allow-remote"
    )]
    InsecureBind { addr: std::net::SocketAddr },
    #[error("bootstrap token: {0}")]
    BootstrapToken(#[from] pico_auth::AuthError),
    #[error("bootstrap token {id:?} conflicts with a stored token of the same id")]
    BootstrapConflict { id: String },
}

/// A running PicoMQ process: metadata log, node, background maintenance and
/// the HTTP listeners.
pub struct PicoServer {
    server: RunningServer,
    lease: LeaseKeeper,
    lifecycle: tokio::task::JoinHandle<()>,
    token_expiry: tokio::task::JoinHandle<()>,
    /// Kept alive for the process lifetime: dropping it aborts the log's
    /// flusher/tailer tasks, so it must outlive the node.
    sink: Arc<SqlSink>,
}

/// Open the metadata log, start the node, and serve the configured protocol,
/// in that order: metadata first (a node must be registered before it can
/// own streams), then storage and the engine, then listeners.
pub async fn start(config: ServerConfig) -> Result<PicoServer, RuntimeError> {
    if config.auth_mode == AuthMode::Off && !config.insecure_allow_remote {
        for addr in [Some(config.addr), config.admin_addr].into_iter().flatten() {
            if !addr.ip().is_loopback() {
                return Err(RuntimeError::InsecureBind { addr });
            }
        }
    }
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
    let node = Arc::new(
        PicoNode::start(
            NodeConfig {
                node_id: config.node_id,
                node_epoch: config.node_epoch,
                http_address: config.advertised_url(),
                slots: config.slots,
                cluster_id: config.cluster_id.clone(),
                engine,
            },
            sink.clone() as Arc<dyn CommandSink>,
            views,
            object_storage.clone(),
            wal_storage,
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

    let addr = config.addr;
    let authorizer = match config.auth_mode {
        AuthMode::Required => Some(node.authorizer()),
        AuthMode::Off => None,
    };
    let server = pico_frontend::serve(
        node,
        ServeOptions {
            protocol: config.protocol,
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
        lease,
        lifecycle,
        token_expiry,
        sink,
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
        created_at_ms: pico_common::now_ms(),
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

fn open_bucket(uri: &str) -> Result<Arc<dyn ObjectStorageTrait>, RuntimeError> {
    let parsed = IdUri::parse(uri)?;
    if parsed.protocol == "file" {
        let path = std::path::PathBuf::from(&parsed.path);
        std::fs::create_dir_all(&path).map_err(|source| RuntimeError::DataDir { path, source })?;
    }
    Ok(Arc::new(ObjectStoreAdapter::from_bucket_uri(uri)?))
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

    pub async fn shutdown(self) {
        self.server.shutdown().await;
        self.lifecycle.abort();
        self.token_expiry.abort();
        self.lease.shutdown().await;
        drop(self.sink);
    }
}
