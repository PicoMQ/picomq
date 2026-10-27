//! `pico serve` arguments.
//!
//! One `--meta-url` points at the SQL metadata log, and `--protocol` picks
//! which HTTP frontend is mounted. The Kafka listener always runs alongside it.
//!
//! This module only *parses*: every field maps to
//! [`picomq_runtime::ServerConfig`], which owns what starting a server means.
//! `--topo` / `ClusterConfig` files are not ported.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, ValueEnum};
use picomq_http::{HttpProtocol, RoutingMode};
use picomq_runtime::{AuthMode, KafkaConfig, MetaBackend, ServerConfig, TlsConfig};

use crate::stream::ProtocolArg;

#[derive(Debug, Args)]
pub struct ServeArgs {
    #[arg(long, env = "PICO_NODE_ID", default_value_t = 1)]
    node_id: i32,

    /// `--http-port`, collapsed into one flag).
    #[arg(long, env = "PICO_LISTEN", default_value = "127.0.0.1:4437")]
    listen: SocketAddr,

    #[arg(long, env = "PICO_ADMIN_LISTEN", default_value = "127.0.0.1:9090")]
    admin_listen: SocketAddr,

    /// Kafka listener bind address.
    #[arg(long, env = "PICO_KAFKA_LISTEN", default_value = "127.0.0.1:9092")]
    kafka_listen: SocketAddr,

    /// Kafka address advertised to clients. Defaults to `--kafka-listen`.
    #[arg(long, env = "PICO_KAFKA_ADVERTISE")]
    kafka_advertise: Option<String>,

    #[arg(long, env = "PICO_NO_ADMIN")]
    no_admin: bool,

    /// Public URL of this node, used for cross-node redirects and registered
    /// in metadata. Defaults to `http://{listen}`, or `https://{listen}` with
    /// TLS, which only works single-node on localhost.
    #[arg(long, env = "PICO_HTTP_ADDRESS")]
    http_address: Option<String>,

    /// Public URL of this node's admin listener, registered so other nodes
    /// can forward console requests to it. Defaults to `http://{admin-listen}`,
    /// or `https://{admin-listen}` with TLS.
    #[arg(long, env = "PICO_ADMIN_ADDRESS")]
    admin_address: Option<String>,

    /// Metadata log: `sqlite::memory:`, `sqlite:<path>` or `postgres://…`
    #[arg(long, env = "PICO_META_URL", default_value = "sqlite:./data/meta.db")]
    meta_url: String,

    #[arg(
        long,
        env = "PICO_STORAGE",
        default_value = "-2@file://./objects",
        allow_hyphen_values = true
    )]
    storage: String,

    /// WAL location: a bucket URI, or a postgres:// URL to keep the WAL in
    /// Postgres. Defaults to the data bucket with its own bucket id
    #[arg(long, env = "PICO_WAL", allow_hyphen_values = true)]
    wal: Option<String>,

    #[arg(long, env = "PICO_CLUSTER_ID", default_value = "picomq")]
    cluster_id: String,

    /// `--routing`).
    #[arg(long, value_enum, env = "PICO_ROUTING", default_value_t = RoutingArg::Redirect)]
    routing: RoutingArg,

    /// Placement weight: how many streams this node takes relative to its
    #[arg(long, env = "PICO_SLOTS", default_value_t = 1)]
    slots: u32,

    /// `--long-poll-timeout-sec`).
    #[arg(long, env = "PICO_LONG_POLL_TIMEOUT_SEC", default_value_t = 25)]
    long_poll_timeout_sec: u64,

    #[arg(long, env = "PICO_SSE_MAX_DURATION_SEC", default_value_t = 55)]
    sse_max_duration_sec: u64,

    /// Seconds between this node's heartbeats.
    #[arg(long, env = "PICO_HEARTBEAT_INTERVAL_SEC", default_value_t = 5)]
    heartbeat_interval_sec: u64,

    /// Seconds a node may miss heartbeats before its partitions fail over.
    #[arg(long, env = "PICO_NODE_TIMEOUT_SEC", default_value_t = 30)]
    node_timeout_sec: u64,

    /// Consumer groups this node holds in memory at once. Groups without
    /// members are unloaded first.
    #[arg(long, env = "PICO_MAX_GROUPS", default_value_t = 10_000)]
    max_groups: usize,

    #[arg(long, env = "PICO_MAX_CHUNK_SIZE", default_value_t = 64 * 1024)]
    max_chunk_size: usize,

    /// Cap on a single request body. Oversized bodies get `413`.
    #[arg(long, env = "PICO_MAX_REQUEST_SIZE", default_value_t = 32 * 1024 * 1024)]
    max_request_size: usize,

    /// Seconds to fail readiness before closing listeners, so a load balancer
    #[arg(long, env = "PICO_SHUTDOWN_DRAIN_SEC", default_value_t = 0)]
    shutdown_drain_sec: u64,

    /// Seconds to wait for open connections once the listeners stop
    /// accepting. After that the node stops waiting and hands off its
    /// streams, so a stalled client cannot hold up the handoff.
    #[arg(long, env = "PICO_SHUTDOWN_TIMEOUT_SEC", default_value_t = 10)]
    shutdown_timeout_sec: u64,

    /// Accept queue depth for the listeners. The kernel clamps it to
    /// `somaxconn` (128 on macOS, 4096 on current Linux).
    #[arg(long, env = "PICO_BACKLOG", default_value_t = 1024)]
    backlog: i32,

    #[arg(long, env = "PICO_WAL_CACHE_SIZE")]
    wal_cache_size: Option<u64>,

    #[arg(long, env = "PICO_BLOCK_CACHE_SIZE")]
    block_cache_size: Option<u64>,

    /// `--wal-upload-threshold`).
    #[arg(long, env = "PICO_WAL_UPLOAD_THRESHOLD")]
    wal_upload_threshold: Option<u64>,

    /// Upload buffered WAL this often even below the threshold. 0 disables it.
    #[arg(long, env = "PICO_WAL_UPLOAD_INTERVAL_MS")]
    wal_upload_interval_ms: Option<u64>,

    /// `off` leaves requests open and refuses non-loopback binds.
    #[arg(long, value_enum, env = "PICO_AUTH", default_value_t = AuthArg::Off)]
    auth: AuthArg,

    /// Permit non-loopback binds with auth off, when something else guards
    /// the network.
    #[arg(long, env = "PICO_INSECURE_ALLOW_REMOTE")]
    insecure_allow_remote: bool,

    /// Root token (wire form) seeded at startup. Idempotent across restarts,
    /// a different stored token under the same id fails startup.
    #[arg(long, env = "PICO_AUTH_BOOTSTRAP_TOKEN")]
    auth_bootstrap_token: Option<String>,

    /// Read the bootstrap token from this file instead.
    #[arg(
        long,
        env = "PICO_AUTH_BOOTSTRAP_TOKEN_FILE",
        conflicts_with = "auth_bootstrap_token"
    )]
    auth_bootstrap_token_file: Option<PathBuf>,

    /// PEM certificate chain served by every listener. Turns on TLS: HTTPS
    /// for the stream and admin listeners, SASL_SSL or SSL for Kafka. The
    /// file is reread every minute, so a renewed certificate needs no
    /// restart.
    #[arg(long, env = "PICO_TLS_CERTIFICATE", requires = "tls_key")]
    tls_certificate: Option<PathBuf>,

    /// PEM private key for `--tls-certificate`.
    #[arg(long, env = "PICO_TLS_KEY", requires = "tls_certificate")]
    tls_key: Option<PathBuf>,

    /// PEM file of CA certificates this node trusts, on top of the system's,
    /// when it calls another node's admin listener. Needed when node
    /// certificates come from a private CA.
    #[arg(long, env = "PICO_TLS_CA_CERTIFICATE")]
    tls_ca_certificate: Option<PathBuf>,

    /// Seconds a client has to finish its TLS handshake.
    #[arg(long, env = "PICO_TLS_HANDSHAKE_TIMEOUT_SEC", default_value_t = 10)]
    tls_handshake_timeout_sec: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AuthArg {
    Off,
    Required,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RoutingArg {
    Redirect,
    Local,
}

impl ServeArgs {
    /// `protocol` is the global `--protocol` flag: which HTTP frontend to
    /// serve. Kafka is a listener, not a frontend choice, so `--protocol
    /// kafka` is rejected here.
    pub fn into_config(
        self,
        protocol: ProtocolArg,
    ) -> Result<ServerConfig, Box<dyn std::error::Error>> {
        let http_protocol = match protocol {
            ProtocolArg::Pico => HttpProtocol::Pico,
            ProtocolArg::Ds => HttpProtocol::Ds,
            ProtocolArg::Kafka => {
                return Err(
                    "`serve` takes --protocol pico or ds; the Kafka listener is always on".into(),
                );
            }
        };
        let defaults = ServerConfig::default();
        let mut engine = s3stream::Config::default();
        if let Some(size) = self.wal_cache_size {
            engine.wal_cache_size = size;
        }
        if let Some(size) = self.block_cache_size {
            engine.block_cache_size = size;
        }
        if let Some(threshold) = self.wal_upload_threshold {
            engine.wal_upload_threshold = threshold;
        }
        if let Some(interval) = self.wal_upload_interval_ms {
            engine.wal_upload_interval_ms = interval;
        }

        let bootstrap_token = match (self.auth_bootstrap_token, self.auth_bootstrap_token_file) {
            (token @ Some(_), None) => token,
            (None, Some(path)) => Some(
                std::fs::read_to_string(&path)
                    .map_err(|e| format!("read bootstrap token file {}: {e}", path.display()))?
                    .trim()
                    .to_owned(),
            ),
            (None, None) => None,
            (Some(_), Some(_)) => unreachable!("clap conflicts_with"),
        };

        Ok(ServerConfig {
            node_id: self.node_id,
            node_epoch: defaults.node_epoch,
            addr: self.listen,
            admin_addr: (!self.no_admin).then_some(self.admin_listen),
            advertised_url: self.http_address,
            admin_url: self.admin_address,
            http_protocol,
            kafka: KafkaConfig {
                listen: self.kafka_listen,
                advertise: self.kafka_advertise,
            },
            meta_backend: MetaBackend::parse(&self.meta_url)?,
            storage_uri: self.storage,
            wal_uri: self.wal,
            cluster_id: self.cluster_id,
            routing_mode: match self.routing {
                RoutingArg::Redirect => RoutingMode::Redirect,
                RoutingArg::Local => RoutingMode::LocalAlways,
            },
            slots: self.slots,
            long_poll_timeout: Duration::from_secs(self.long_poll_timeout_sec),
            sse_max_duration: Duration::from_secs(self.sse_max_duration_sec),
            heartbeat_interval: Duration::from_secs(self.heartbeat_interval_sec),
            node_timeout: Duration::from_secs(self.node_timeout_sec),
            max_groups: self.max_groups,
            max_chunk_size: self.max_chunk_size,
            max_request_size: self.max_request_size,
            shutdown_drain: Duration::from_secs(self.shutdown_drain_sec),
            shutdown_timeout: Duration::from_secs(self.shutdown_timeout_sec),
            backlog: self.backlog,
            auth_mode: match self.auth {
                AuthArg::Off => AuthMode::Off,
                AuthArg::Required => AuthMode::Required,
            },
            insecure_allow_remote: self.insecure_allow_remote,
            bootstrap_token,
            tls: self
                .tls_certificate
                .zip(self.tls_key)
                .map(|(certificate, key)| TlsConfig {
                    handshake_timeout: Duration::from_secs(self.tls_handshake_timeout_sec),
                    ..TlsConfig::new(certificate, key)
                }),
            tls_ca_certificate: self.tls_ca_certificate,
            engine,
        })
    }
}

#[cfg(test)]
mod tests {
    use clap::Command;

    use super::*;

    #[test]
    fn every_flag_has_an_environment_variable() {
        let command = ServeArgs::augment_args(Command::new("serve"));

        let missing: Vec<_> = command
            .get_arguments()
            .filter(|arg| arg.get_env().is_none())
            .map(|arg| arg.get_id().to_string())
            .collect();
        assert!(missing.is_empty(), "flags without PICO_*: {missing:?}");
    }
}
