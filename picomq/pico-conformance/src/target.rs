use std::net::{SocketAddr, TcpListener};

use anyhow::Context;
use picomq_http::HttpProtocol;
use picomq_runtime::{KafkaConfig, MetaBackend, PicoServer, ServerConfig};
use tempfile::TempDir;

use crate::suite::Suite;

pub struct Target {
    url: Option<String>,
    bootstrap: Option<String>,
    host_address: Option<String>,
    node: Option<Node>,
}

struct Node {
    server: PicoServer,
    _dir: TempDir,
}

impl Target {
    pub async fn start(suite: Suite) -> anyhow::Result<Self> {
        let dir = tempfile::tempdir()?;

        let kafka = match suite {
            Suite::Kafka => container_reachable()?,
            Suite::Ds => KafkaConfig::new(loopback()),
        };
        let bootstrap = kafka.advertise.clone();
        let host_address = bootstrap
            .as_ref()
            .map(|_| format!("127.0.0.1:{}", kafka.listen.port()));

        let server = picomq_runtime::start(config(&dir, suite.protocol(), kafka)?).await?;

        Ok(Self {
            url: Some(format!("http://{}", server.local_addr())),
            bootstrap,
            host_address,
            node: Some(Node { server, _dir: dir }),
        })
    }

    pub fn remote(
        url: Option<String>,
        bootstrap: Option<String>,
        host_address: Option<String>,
    ) -> Self {
        Self {
            url,
            bootstrap,
            host_address,
            node: None,
        }
    }

    pub fn url(&self) -> anyhow::Result<&str> {
        self.url.as_deref().context("target has no http listener")
    }

    pub fn bootstrap(&self) -> Option<&str> {
        self.bootstrap.as_deref()
    }

    pub fn host_address(&self) -> Option<&str> {
        self.host_address.as_deref()
    }

    pub async fn stop(self) {
        if let Some(node) = self.node {
            node.server.shutdown().await;
        }
    }
}

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn container_reachable() -> anyhow::Result<KafkaConfig> {
    let port = TcpListener::bind(("0.0.0.0", 0))?.local_addr()?.port();

    Ok(KafkaConfig {
        listen: SocketAddr::from(([0, 0, 0, 0], port)),
        advertise: Some(format!("host.docker.internal:{port}")),
    })
}

fn config(
    dir: &TempDir,
    protocol: HttpProtocol,
    kafka: KafkaConfig,
) -> anyhow::Result<ServerConfig> {
    let path = dir.path();

    Ok(ServerConfig {
        addr: loopback(),
        admin_addr: Some(loopback()),
        http_protocol: protocol,
        insecure_allow_remote: !kafka.listen.ip().is_loopback(),
        kafka,
        meta_backend: MetaBackend::parse(&format!("sqlite:{}", path.join("meta.db").display()))?,
        storage_uri: format!("1@file://{}", path.join("objects").display()),
        wal_uri: Some(format!("2@file://{}", path.join("wal").display())),
        engine: s3stream::Config {
            wal_upload_interval_ms: 200,
            ..Default::default()
        },
        ..Default::default()
    })
}
