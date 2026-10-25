use std::net::{SocketAddr, TcpListener};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use picomq_auth::{
    AccessToken, Audience, OperationGroups, ReadWrite, ResourceSet, Scope, TokenRecord, TokenStore,
};
use picomq_http::HttpProtocol;
use picomq_runtime::{AuthMode, KafkaConfig, MetaBackend, PicoServer, ServerConfig, TlsConfig};
use tempfile::TempDir;

use crate::certificates::Certificates;
use crate::kafka::sasl::Credentials;
use crate::suite::Suite;

const ROOT_ID: &str = "conformance/root";
const READER_ID: &str = "conformance/reader";
const WRITER_ID: &str = "conformance/writer";

pub struct Target {
    url: Option<String>,
    bootstrap: Option<String>,
    host_address: Option<String>,
    node: Option<Node>,
    sasl_plaintext: Option<Secured>,
    sasl_ssl: Option<Secured>,
    certificates: Option<Certificates>,
}

struct Node {
    server: PicoServer,
    _dir: TempDir,
}

pub struct Secured {
    bootstrap: String,
    host_address: String,
    root: Credentials,
    reader: Credentials,
    writer: Credentials,
    node: Node,
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

        let (sasl_plaintext, sasl_ssl, certificates) = match suite {
            Suite::Kafka => {
                let certificates = Certificates::mint()?;
                let tls = TlsConfig::new(certificates.certificate(), certificates.key());
                (
                    Some(Secured::start(None).await?),
                    Some(Secured::start(Some(tls)).await?),
                    Some(certificates),
                )
            }
            Suite::Ds => (None, None, None),
        };

        Ok(Self {
            url: Some(format!("http://{}", server.local_addr())),
            bootstrap,
            host_address,
            node: Some(Node { server, _dir: dir }),
            sasl_plaintext,
            sasl_ssl,
            certificates,
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
            sasl_plaintext: None,
            sasl_ssl: None,
            certificates: None,
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

    pub fn sasl_plaintext(&self) -> Option<&Secured> {
        self.sasl_plaintext.as_ref()
    }

    pub fn sasl_ssl(&self) -> Option<&Secured> {
        self.sasl_ssl.as_ref()
    }

    pub fn certificates(&self) -> Option<&Certificates> {
        self.certificates.as_ref()
    }

    pub async fn stop(self) {
        let nodes = [
            self.node,
            self.sasl_plaintext.map(|secured| secured.node),
            self.sasl_ssl.map(|secured| secured.node),
        ];
        for node in nodes.into_iter().flatten() {
            node.server.shutdown().await;
        }
    }
}

impl Secured {
    async fn start(tls: Option<TlsConfig>) -> anyhow::Result<Self> {
        let dir = tempfile::tempdir()?;
        let kafka = container_reachable()?;
        let bootstrap = kafka
            .advertise
            .clone()
            .context("kafka listener advertises")?;
        let host_address = format!("127.0.0.1:{}", kafka.listen.port());

        let (root, _) = AccessToken::issue(ROOT_ID)?;
        let server = picomq_runtime::start(ServerConfig {
            auth_mode: AuthMode::Required,
            bootstrap_token: Some(root.render()),
            insecure_allow_remote: false,
            tls,
            ..config(&dir, HttpProtocol::Pico, kafka)?
        })
        .await?;

        let reader = issue(&server, READER_ID, stream_scope(ReadWrite::read_only())).await?;
        let writer = issue(
            &server,
            WRITER_ID,
            stream_scope(ReadWrite {
                read: false,
                write: true,
            }),
        )
        .await?;

        Ok(Self {
            bootstrap,
            host_address,
            root: credentials(&root),
            reader: credentials(&reader),
            writer: credentials(&writer),
            node: Node { server, _dir: dir },
        })
    }

    pub fn bootstrap(&self) -> &str {
        &self.bootstrap
    }

    pub fn host_address(&self) -> &str {
        &self.host_address
    }

    pub fn root(&self) -> &Credentials {
        &self.root
    }

    pub fn reader(&self) -> &Credentials {
        &self.reader
    }

    pub fn writer(&self) -> &Credentials {
        &self.writer
    }
}

fn credentials(token: &AccessToken) -> Credentials {
    Credentials {
        username: token.id.clone(),
        password: token.render(),
    }
}

fn stream_scope(stream: ReadWrite) -> Scope {
    Scope {
        streams: ResourceSet::prefix(""),
        groups: OperationGroups {
            stream,
            ..OperationGroups::default()
        },
        audiences: [Audience::Kafka].into(),
        ..Scope::default()
    }
}

async fn issue(server: &PicoServer, id: &str, scope: Scope) -> anyhow::Result<AccessToken> {
    let (token, verifier) = AccessToken::issue(id)?;
    let created_at_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

    let stored = server
        .node()
        .tokens()
        .store()
        .put_if_absent(TokenRecord {
            id: token.id.clone(),
            verifier,
            scope,
            created_at_ms,
            issued_by: ROOT_ID.to_owned(),
        })
        .await?;
    anyhow::ensure!(stored, "token {id} already exists");

    Ok(token)
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
