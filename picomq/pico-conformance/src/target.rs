use std::net::{SocketAddr, TcpListener};

use anyhow::Context;
use picomq_http::HttpProtocol;
use picomq_runtime::{KafkaConfig, MetaBackend, PicoServer, ServerConfig};
use tempfile::TempDir;

pub struct Listeners {
    pub protocol: HttpProtocol,
    pub kafka: bool,
}

pub struct Target {
    url: Option<String>,
    bootstrap: Option<String>,
    kafka_address: Option<String>,
    node: Option<Node>,
}

struct Node {
    server: PicoServer,
    _dir: TempDir,
}

impl Target {
    pub async fn start(listeners: Listeners) -> anyhow::Result<Self> {
        let dir = tempfile::tempdir()?;

        let kafka = match listeners.kafka {
            true => Some(container_reachable()?),
            false => None,
        };

        let bootstrap = kafka.as_ref().and_then(|k| k.advertise.clone());
        let kafka_address = kafka
            .as_ref()
            .map(|k| format!("127.0.0.1:{}", k.listen.port()));

        let server = picomq_runtime::start(config(&dir, listeners.protocol, kafka)?).await?;

        Ok(Self {
            url: Some(format!("http://{}", server.local_addr())),
            bootstrap,
            kafka_address,
            node: Some(Node { server, _dir: dir }),
        })
    }

    pub fn remote(url: Option<String>, bootstrap: Option<String>) -> Self {
        Self {
            url,
            kafka_address: bootstrap.clone(),
            bootstrap,
            node: None,
        }
    }

    pub fn url(&self) -> anyhow::Result<&str> {
        self.url.as_deref().context("target has no http listener")
    }

    pub fn bootstrap(&self) -> Option<&str> {
        self.bootstrap.as_deref()
    }

    pub fn kafka_address(&self) -> Option<&str> {
        self.kafka_address.as_deref()
    }

    pub async fn stop(self) {
        if let Some(node) = self.node {
            node.server.shutdown().await;
        }
    }
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
    kafka: Option<KafkaConfig>,
) -> anyhow::Result<ServerConfig> {
    let path = dir.path();
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));

    Ok(ServerConfig {
        addr: loopback,
        admin_addr: Some(loopback),
        http_protocol: protocol,
        insecure_allow_remote: kafka.is_some(),
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
