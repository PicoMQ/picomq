//! Catalog changelog: create/delete appear on `/_sys/catalog`, list omits
//! it, client writes are rejected, and a projector restart does not
//! duplicate events.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use pico_metadata::{CommandSink, ViewPublisher};
use pico_server::{
    AppendCommand, CatalogSource, CreateCommand, ErrorKind, NodeConfig, OffsetToken, PicoNode,
    CATALOG_STREAM,
};
use pico_sql::{SqlSink, SqlSinkConfig, SqliteStore};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};
use serde_json::Value;

struct SqlCatalog(Arc<SqlSink>);

#[async_trait]
impl CatalogSource for SqlCatalog {
    async fn fetch_after(&self, after: u64, limit: u32) -> Result<Vec<(u64, Vec<u8>)>, String> {
        self.0
            .fetch_after(after, limit)
            .await
            .map_err(|e| e.to_string())
    }

    fn set_flushable_idx(&self, idx: u64) {
        self.0.set_flushable_idx(idx);
    }
}

fn create(name: &str) -> CreateCommand {
    CreateCommand {
        name: name.into(),
        content_type: "text/plain".into(),
        ttl_seconds: None,
        expires_at_ms: None,
        closed: false,
        initial_payload: Bytes::new(),
        external_id: None,
        internal: false,
    }
}

async fn start_node(sink: Arc<dyn CommandSink>, views: Arc<ViewPublisher>) -> PicoNode {
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(2));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(3));
    PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 1,
            http_address: "http://127.0.0.1:4001".into(),
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
    )
    .await
    .unwrap()
}

async fn catalog_events(service: &pico_server::S3StreamService) -> Vec<Value> {
    let Ok(read) = service
        .read(CATALOG_STREAM, OffsetToken::beginning(), 0, 0)
        .await
    else {
        return Vec::new();
    };
    read.records
        .iter()
        .filter_map(|r| serde_json::from_slice::<Value>(&r.payload).ok())
        .collect()
}

async fn wait_catalog(service: &pico_server::S3StreamService) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if service.head(CATALOG_STREAM).await.unwrap().is_some() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "catalog stream never appeared"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_event(service: &pico_server::S3StreamService, op: &str, name: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(event) = catalog_events(service)
            .await
            .into_iter()
            .find(|e| e["op"] == op && e["name"] == name)
        {
            return event;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "catalog never recorded {op} {name}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn catalog_projects_create_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteStore::open(&dir.path().join("meta.db"))
            .await
            .unwrap(),
    );
    let (sink, views) = SqlSink::open(
        store,
        SqlSinkConfig {
            poll_interval: Duration::from_millis(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let sink = Arc::new(sink);
    let node = start_node(sink.clone() as Arc<dyn CommandSink>, views).await;
    let service = node.service();
    let (tx, leadership) = tokio::sync::watch::channel(true);
    let projector = service.spawn_catalog_loop(Arc::new(SqlCatalog(sink.clone())), leadership);

    wait_catalog(&service).await;

    let listed = service.list("/", None, 100).await.unwrap();
    assert!(listed.streams.iter().all(|s| s.name != CATALOG_STREAM));

    let client_append = service
        .append(AppendCommand {
            name: CATALOG_STREAM.into(),
            payloads: vec![Bytes::from_static(b"{\"op\":\"nope\"}")],
            content_type: Some("application/json".into()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(client_append.kind, ErrorKind::BadRequest);
    assert_eq!(
        service.delete(CATALOG_STREAM).await.unwrap_err().kind,
        ErrorKind::BadRequest
    );

    let created = service.create(create("/orders")).await.unwrap();
    assert!(created.created);
    let create_event = wait_event(&service, "create", "/orders").await;
    assert_eq!(
        create_event["stream_id"].as_u64(),
        Some(created.meta.stream_id)
    );
    assert!(create_event["applied_idx"].as_u64().unwrap() > 0);

    assert!(service.delete("/orders").await.unwrap());
    let delete_event = wait_event(&service, "delete", "/orders").await;
    assert_eq!(
        delete_event["stream_id"].as_u64(),
        Some(created.meta.stream_id)
    );

    let before = catalog_events(&service).await;
    projector.abort();
    drop(tx);
    let (tx, leadership) = tokio::sync::watch::channel(true);
    let projector = service.spawn_catalog_loop(Arc::new(SqlCatalog(sink)), leadership);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = catalog_events(&service).await;
    assert_eq!(after, before, "restart must not rewrite applied_idx events");

    service.create(create("/other")).await.unwrap();
    wait_event(&service, "create", "/other").await;

    projector.abort();
    drop(tx);
    node.close().await;
}
