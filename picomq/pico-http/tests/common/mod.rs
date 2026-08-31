//! Loopback test harness: a `PicoNode` on `LocalSink` + memory object
//! storage, served over a real TCP socket by `picomq_http::serve`. The same
//! bind path `pico serve` uses. Timeouts are shortened for tests (short long
//! poll, 2s SSE cap).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use picomq_http::{serve, Protocol, RoutingMode, RunningServer, ServeOptions};
use picomq_metadata::LocalSink;
use picomq_schema::{Registry, SchemaStore};
use picomq_server::{NodeConfig, PicoNode};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};

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
    Arc::new(
        PicoNode::start(
            NodeConfig {
                node_id: 1,
                node_epoch: 1,
                engine,
                ..Default::default()
            },
            Arc::new(sink),
            views,
            object_storage,
            wal_storage,
            schema_registry,
        )
        .await
        .unwrap(),
    )
}

async fn start(protocol: Protocol) -> TestServer {
    start_with_node(protocol, start_node().await).await
}

async fn start_with_node(protocol: Protocol, node: Arc<PicoNode>) -> TestServer {
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = serve(
        node.clone(),
        ServeOptions {
            protocol,
            addr: loopback,
            admin_addr: Some(loopback),
            routing_mode: RoutingMode::LocalAlways,
            long_poll_timeout: Duration::from_secs(1),
            sse_max_duration: Duration::from_secs(2),
            max_chunk_size: 64 * 1024,
            max_request_size: 32 * 1024 * 1024,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    TestServer {
        base_url: format!("http://{}", server.local_addr()),
        admin_url: format!("http://{}", server.admin_addr().unwrap()),
        node,
        server,
    }
}

#[allow(dead_code)]
pub async fn picomq_server() -> TestServer {
    start(Protocol::Pico).await
}

#[allow(dead_code)]
pub async fn picomq_server_with_schema(registry: Registry) -> TestServer {
    start_with_node(Protocol::Pico, start_node_with_schema(registry).await).await
}

#[allow(dead_code)]
pub async fn kafka_http_with_schema(registry: Registry) -> TestServer {
    start_with_node(Protocol::Kafka, start_node_with_schema(registry).await).await
}

#[allow(dead_code)]
pub async fn ds_server() -> TestServer {
    start(Protocol::Ds).await
}
