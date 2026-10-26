//! Loopback test harness: a `PicoNode` on `LocalSink` + memory object
//! storage, served over a real TCP socket by `picomq_http::serve`. The same
//! bind path `pico serve` uses. Timeouts are shortened for tests (short long
//! poll, 2s SSE cap).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use picomq_auth::{AccessToken, Scope, TokenRecord, TokenStore};
use picomq_http::{HttpProtocol, RoutingMode, RunningServer, ServeOptions, serve};
use picomq_listener::Listener;
use picomq_metadata::LocalSink;
use picomq_schema::{Registry, SchemaStore};
use picomq_server::CreateCommand;
use picomq_server::wal::ObjectWal;
use picomq_server::{NodeConfig, PicoNode};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};
use serde_json::Value;

pub struct TestServer {
    #[allow(dead_code)]
    pub base_url: String,
    #[allow(dead_code)]
    pub admin_url: String,
    #[allow(dead_code)]
    pub node: Arc<PicoNode>,
    #[allow(dead_code)]
    server: RunningServer,
}

impl TestServer {
    #[allow(dead_code)]
    pub async fn shutdown(self) {
        self.server.shutdown().await;
    }
}

pub async fn start_node() -> Arc<PicoNode> {
    start_node_inner(None).await
}

pub async fn start_node_with_schema(registry: Registry) -> Arc<PicoNode> {
    start_node_inner(Some(Arc::new(registry))).await
}

async fn start_node_inner(schema_registry: Option<Arc<dyn SchemaStore>>) -> Arc<PicoNode> {
    let (sink, views) = LocalSink::new();
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(2));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(3));
    let engine = s3stream::Config {
        wal_upload_interval_ms: 200,
        wal_config: "3@mem://wal?batchInterval=5".into(),
        ..Default::default()
    };
    let config = NodeConfig {
        node_id: 1,
        node_epoch: 1,
        engine,
        ..Default::default()
    };
    let wal = ObjectWal::new(&config.cluster_id, &config.engine.wal_config, wal_storage);
    Arc::new(
        PicoNode::start(
            config,
            Arc::new(sink),
            views,
            object_storage,
            Arc::new(wal),
            schema_registry,
        )
        .await
        .unwrap(),
    )
}

async fn start(protocol: HttpProtocol) -> TestServer {
    start_with_node(protocol, start_node().await).await
}

async fn start_with_node(protocol: HttpProtocol, node: Arc<PicoNode>) -> TestServer {
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = serve(
        node.clone(),
        Listener::bind(picomq_metrics::ListenerKind::Stream, loopback, 1024, None).unwrap(),
        Some(Listener::bind(picomq_metrics::ListenerKind::Admin, loopback, 1024, None).unwrap()),
        ServeOptions {
            protocol,
            routing_mode: RoutingMode::LocalAlways,
            long_poll_timeout: Duration::from_secs(1),
            sse_max_duration: Duration::from_secs(2),
            max_chunk_size: 64 * 1024,
            max_request_size: 32 * 1024 * 1024,
            ..Default::default()
        },
    )
    .await;
    TestServer {
        base_url: format!("http://{}", server.local_addr()),
        admin_url: format!("http://{}", server.admin_addr().unwrap()),
        node,
        server,
    }
}

#[allow(dead_code)]
pub async fn picomq_server() -> TestServer {
    start(HttpProtocol::Pico).await
}

#[allow(dead_code)]
pub async fn picomq_server_with_schema(registry: Registry) -> TestServer {
    start_with_node(HttpProtocol::Pico, start_node_with_schema(registry).await).await
}

#[allow(dead_code)]
pub async fn ds_server() -> TestServer {
    start(HttpProtocol::Ds).await
}

#[allow(dead_code)]
pub struct Console {
    pub server: RunningServer,
    pub node: Arc<PicoNode>,
    pub root: String,
    pub http: reqwest::Client,
}

#[allow(dead_code)]
impl Console {
    pub async fn start() -> Self {
        let node = start_node().await;
        let root = issue(&node, "ops/root", Scope::root()).await;
        let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = serve(
            node.clone(),
            Listener::bind(picomq_metrics::ListenerKind::Stream, loopback, 1024, None).unwrap(),
            Some(
                Listener::bind(picomq_metrics::ListenerKind::Admin, loopback, 1024, None).unwrap(),
            ),
            ServeOptions {
                protocol: HttpProtocol::Pico,
                routing_mode: RoutingMode::LocalAlways,
                authorizer: Some(node.authorizer()),
                ..Default::default()
            },
        )
        .await;

        Self {
            server,
            node,
            root,
            http: reqwest::Client::new(),
        }
    }

    pub fn admin(&self, path: &str) -> String {
        format!("http://{}{path}", self.server.admin_addr().unwrap())
    }

    pub fn data(&self, path: &str) -> String {
        format!("http://{}{path}", self.server.local_addr())
    }

    pub async fn get(&self, path: &str, token: &str) -> (u16, Value) {
        let response = self
            .http
            .get(self.admin(path))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    pub async fn every(&self, path: &str, token: &str, key: &str) -> (Vec<Value>, usize) {
        let mut items = Vec::new();
        let mut pages = 0;
        let mut cursor: Option<String> = None;

        loop {
            let separator = if path.contains('?') { '&' } else { '?' };
            let url = match &cursor {
                Some(cursor) => format!("{path}{separator}cursor={cursor}"),
                None => path.to_owned(),
            };
            let (status, page) = self.get(&url, token).await;
            assert_eq!(status, 200, "{page}");
            pages += 1;
            items.extend(
                page["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|item| item[key].clone()),
            );

            match page["next"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => return (items, pages),
            }
        }
    }

    pub async fn append(&self, topic: &str, partition: u32, count: usize) {
        for n in 0..count {
            let appended = self
                .http
                .post(self.data(&format!("{topic}?partition={partition}")))
                .bearer_auth(&self.root)
                .header("Content-Type", "text/plain")
                .body(format!("record {n}"))
                .send()
                .await
                .unwrap();
            assert_eq!(appended.status(), 200);
        }
    }
}

#[allow(dead_code)]
pub async fn issue(node: &PicoNode, id: &str, scope: Scope) -> String {
    let (token, verifier) = AccessToken::issue(id).unwrap();
    node.tokens()
        .store()
        .put_if_absent(TokenRecord {
            id: token.id.clone(),
            verifier,
            scope,
            created_at_ms: 1,
            issued_by: String::new(),
        })
        .await
        .unwrap();
    token.render()
}

#[allow(dead_code)]
pub async fn create(node: &PicoNode, name: &str, partitions: u32) {
    let mut command = CreateCommand::new(name, "text/plain");
    command.partitions = partitions;
    node.topics().create(command).await.unwrap();
}
