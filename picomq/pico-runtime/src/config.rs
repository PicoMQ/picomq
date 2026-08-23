//! What a PicoMQ process needs to know to start.
//!
//! One [`ServerConfig::meta_url`] points at the SQL metadata log (see
//! `pico_sql`). Argument parsing belongs to the binary (`pico serve`), which
//! builds this struct.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use pico_common::now_ms;
use pico_frontend::{Protocol, RoutingMode};

/// Where the metadata command log lives.
///
/// (`lite`) deployment. Postgres is the clustered one.
#[derive(Debug, Clone)]
pub enum MetaBackend {
    /// A SQLite file. `None` is an in-memory database (single process, wiped
    /// in a temp dir.
    Sqlite(Option<PathBuf>),
    /// A Postgres connection URL (`postgres://...`).
    Postgres(String),
}

impl MetaBackend {
    /// Parse the `--meta-url` form: `sqlite::memory:`, `sqlite:<path>` (also
    /// `sqlite://<path>`), or any `postgres://` / `postgresql://` URL.
    pub fn parse(url: &str) -> Result<Self, InvalidMetaUrl> {
        if url == "sqlite::memory:" || url == "sqlite://:memory:" {
            return Ok(Self::Sqlite(None));
        }
        if let Some(path) = url
            .strip_prefix("sqlite://")
            .or_else(|| url.strip_prefix("sqlite:"))
        {
            if path.is_empty() {
                return Err(InvalidMetaUrl {
                    url: url.to_owned(),
                });
            }
            return Ok(Self::Sqlite(Some(PathBuf::from(path))));
        }
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            return Ok(Self::Postgres(url.to_owned()));
        }
        Err(InvalidMetaUrl {
            url: url.to_owned(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unsupported --meta-url {url:?}: expected sqlite::memory:, sqlite:<path> or postgres://…")]
pub struct InvalidMetaUrl {
    pub url: String,
}

/// Whether the frontends and the admin plane require bearer tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthMode {
    /// No enforcement. Non-loopback binds are refused in this mode.
    #[default]
    Off,
    /// Every classified request needs a token that passes scope checks.
    Required,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub node_id: i32,
    pub node_epoch: i64,
    pub addr: SocketAddr,
    pub admin_addr: Option<SocketAddr>,
    pub advertised_url: Option<String>,
    pub protocol: Protocol,
    pub meta_backend: MetaBackend,
    pub storage_uri: String,
    /// WAL bucket URI. Defaults to the data bucket (with the next bucket id)
    /// when unset.
    pub wal_uri: Option<String>,
    pub cluster_id: String,
    pub routing_mode: RoutingMode,
    pub slots: u32,
    pub long_poll_timeout: Duration,
    pub sse_max_duration: Duration,
    pub max_chunk_size: usize,
    pub shutdown_drain: Duration,
    /// `acceptQueueSize`). The kernel clamps it to `somaxconn`.
    pub backlog: i32,
    pub auth_mode: AuthMode,
    /// Permits non-loopback binds with auth off.
    pub insecure_allow_remote: bool,
    /// Root token (wire form) seeded at startup with [`Scope::root`]
    /// (`pico_auth::Scope::root`). Idempotent across restarts. A different
    /// stored token under the same id fails startup.
    pub bootstrap_token: Option<String>,
    /// `ServerConfig#applyTo`.
    pub engine: s3stream::Config,
}

/// Defaults: 127.0.0.1:4437, admin 9090, redirect routing, 25s long poll,
/// 55s SSE, 64 KiB chunks, no drain, storage under `./objects`.
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            node_epoch: now_ms(),
            addr: SocketAddr::from(([127, 0, 0, 1], 4437)),
            admin_addr: Some(SocketAddr::from(([127, 0, 0, 1], 9090))),
            advertised_url: None,
            protocol: Protocol::Pico,
            meta_backend: MetaBackend::Sqlite(Some(PathBuf::from("./data/meta.db"))),
            storage_uri: "-2@file://./objects".to_owned(),
            wal_uri: None,
            cluster_id: "picomq".to_owned(),
            routing_mode: RoutingMode::Redirect,
            slots: 1,
            long_poll_timeout: Duration::from_secs(25),
            sse_max_duration: Duration::from_secs(55),
            max_chunk_size: 64 * 1024,
            shutdown_drain: Duration::ZERO,
            backlog: 1024,
            auth_mode: AuthMode::Off,
            insecure_allow_remote: false,
            bootstrap_token: None,
            engine: s3stream::Config::default(),
        }
    }
}

impl ServerConfig {
    pub fn advertised_url(&self) -> String {
        self.advertised_url
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.addr))
    }

    /// WAL when `--wal` is absent. The bucket id is the engine's per-bucket
    /// handle, so the WAL gets its own: sharing one id would make WAL and data
    /// objects collide in the same namespace.
    pub fn wal_uri(&self) -> String {
        self.wal_uri
            .clone()
            .unwrap_or_else(|| derive_wal_uri(&self.storage_uri))
    }
}

/// `-2@file://./objects` → `-3@file://./objects`: same backend, next bucket id.
fn derive_wal_uri(storage_uri: &str) -> String {
    match storage_uri.split_once('@') {
        Some((id, rest)) => match id.trim().parse::<i16>() {
            Ok(id) => format!("{}@{rest}", id.saturating_sub(1)),
            Err(_) => storage_uri.to_owned(),
        },
        None => storage_uri.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_meta_urls() {
        assert!(matches!(
            MetaBackend::parse("sqlite::memory:").unwrap(),
            MetaBackend::Sqlite(None)
        ));
        assert!(matches!(
            MetaBackend::parse("sqlite:/tmp/meta.db").unwrap(),
            MetaBackend::Sqlite(Some(path)) if path == std::path::Path::new("/tmp/meta.db")
        ));
        assert!(matches!(
            MetaBackend::parse("postgres://user@host/pico").unwrap(),
            MetaBackend::Postgres(_)
        ));
        assert!(MetaBackend::parse("mysql://host/pico").is_err());
        assert!(MetaBackend::parse("sqlite:").is_err());
    }

    #[test]
    fn wal_uri_defaults_to_the_next_bucket_id() {
        let config = ServerConfig {
            storage_uri: "-2@file://./objects".to_owned(),
            ..Default::default()
        };
        assert_eq!(config.wal_uri(), "-3@file://./objects");
    }

    #[test]
    fn advertised_url_defaults_to_the_bound_address() {
        let config = ServerConfig::default();
        assert_eq!(config.advertised_url(), "http://127.0.0.1:4437");
    }
}
