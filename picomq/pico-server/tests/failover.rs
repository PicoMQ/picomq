//! Catalog failover e2e against real Postgres and S3 (RustFS): a hard-killed
//! leader (lease expires by TTL, stream recovered after its restart) and a
//! graceful shutdown (closed stream revived by the next leader), with every
//! event landing exactly once.
//!
//! ```text
//! docker compose -f harness/aio/compose.cluster.yml up -d postgres rustfs createbucket
//! PICOMQ_PG_URL=postgres://picomq:picomq@localhost:5432/picomq \
//! PICOMQ_S3_ENDPOINT=http://localhost:9000 \
//! AWS_ACCESS_KEY_ID=picomq AWS_SECRET_ACCESS_KEY=picomqpicomq AWS_REGION=us-east-1 \
//!     cargo test -p pico-server --test failover -- --nocapture
//! ```
//!
//! Skipped (pass, with a note) when the variables are absent. WIPES the
//! metadata tables; point it at a dedicated test database.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use pico_metadata::CommandSink;
use pico_server::{
    CatalogSource, CreateCommand, NodeConfig, OffsetToken, PicoNode, S3StreamService,
    CATALOG_STREAM,
};
use pico_sql::{LeaseConfig, LeaseKeeper, MetaStore, PgStore, SqlSink, SqlSinkConfig};
use s3stream::{ObjectStorageTrait, ObjectStoreAdapter};
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

struct Env {
    pg_url: String,
    s3_endpoint: String,
    cluster_id: String,
}

impl Env {
    fn bucket_uri(&self, id: i16) -> String {
        format!(
            "{id}@s3://picomq?region=us-east-1&endpoint={}&pathStyle=true&batchInterval=5",
            self.s3_endpoint
        )
    }
}

struct Node {
    node: PicoNode,
    sink: Arc<SqlSink>,
    store: Arc<dyn MetaStore>,
}

async fn start_node(env: &Env, node_id: i32, node_epoch: i64) -> Node {
    let store: Arc<dyn MetaStore> = Arc::new(PgStore::connect(&env.pg_url).await.unwrap());
    let (sink, views) = SqlSink::open(
        store.clone(),
        SqlSinkConfig {
            poll_interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let sink = Arc::new(sink);
    let data_uri = env.bucket_uri(-2);
    let wal_uri = env.bucket_uri(-3);
    let object_storage: Arc<dyn ObjectStorageTrait> =
        Arc::new(ObjectStoreAdapter::from_bucket_uri(&data_uri).unwrap());
    let wal_storage: Arc<dyn ObjectStorageTrait> =
        Arc::new(ObjectStoreAdapter::from_bucket_uri(&wal_uri).unwrap());
    let engine = s3stream::Config {
        cluster_id: env.cluster_id.clone(),
        node_id: u32::try_from(node_id).unwrap(),
        node_epoch: u64::try_from(node_epoch).unwrap(),
        data_buckets: vec![data_uri],
        wal_config: wal_uri,
        wal_upload_interval_ms: 100,
        ..Default::default()
    };
    let node = PicoNode::start(
        NodeConfig {
            node_id,
            node_epoch,
            http_address: format!("http://127.0.0.1:44{node_id:02}"),
            cluster_id: env.cluster_id.clone(),
            engine,
            ..Default::default()
        },
        sink.clone() as Arc<dyn CommandSink>,
        views,
        object_storage,
        wal_storage,
    )
    .await
    .unwrap();
    Node { node, sink, store }
}

fn lease_config() -> LeaseConfig {
    LeaseConfig {
        ttl_ms: 1_000,
        check_interval: Duration::from_millis(50),
    }
}

async fn wait_leader(keeper: &LeaseKeeper) {
    let mut rx = keeper.leadership();
    tokio::time::timeout(Duration::from_secs(10), rx.wait_for(|v| *v))
        .await
        .expect("leadership never arrived")
        .unwrap();
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

async fn catalog_events(service: &S3StreamService) -> Vec<Value> {
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

async fn wait_event(service: &S3StreamService, op: &str, name: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if catalog_events(service)
            .await
            .iter()
            .any(|e| e["op"] == op && e["name"] == name)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "catalog never recorded {op} {name}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn count(events: &[Value], op: &str, name: &str) -> usize {
    events
        .iter()
        .filter(|e| e["op"] == op && e["name"] == name)
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_survives_hard_kill_and_graceful_close() {
    let (Ok(pg_url), Ok(s3_endpoint)) = (
        std::env::var("PICOMQ_PG_URL"),
        std::env::var("PICOMQ_S3_ENDPOINT"),
    ) else {
        eprintln!("PICOMQ_PG_URL or PICOMQ_S3_ENDPOINT not set; skipping failover e2e");
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let admin = sqlx::PgPool::connect(&pg_url).await.expect("connect");
    sqlx::query("DROP TABLE IF EXISTS meta_log, meta_snapshot, meta_lease")
        .execute(&admin)
        .await
        .expect("drop tables");
    admin.close().await;
    let env = Env {
        pg_url,
        s3_endpoint,
        cluster_id: format!("catalog-e2e-{}", pico_common::now_ms()),
    };

    // Era 1: node 1 leads and projects.
    let n1 = start_node(&env, 1, 1).await;
    let keeper1 = LeaseKeeper::spawn(n1.store.clone(), "node-1".into(), lease_config());
    wait_leader(&keeper1).await;
    let projector1 = n1
        .node
        .service()
        .spawn_catalog_loop(Arc::new(SqlCatalog(n1.sink.clone())), keeper1.leadership());
    n1.node.service().create(create("/one")).await.unwrap();
    wait_event(&n1.node.service(), "create", "/one").await;

    // Hard kill: nothing closed, lease expires by TTL, leftover engine
    // tasks are a zombie for epoch fencing to handle.
    projector1.abort();
    drop(keeper1);
    drop(n1);

    // Era 2: node 2 leads. It stalls on the still-Opened catalog until the
    // crashed node restarts and its WAL recovery closes it.
    let n2 = start_node(&env, 2, 1).await;
    let keeper2 = LeaseKeeper::spawn(n2.store.clone(), "node-2".into(), lease_config());
    wait_leader(&keeper2).await;
    let projector2 = n2
        .node
        .service()
        .spawn_catalog_loop(Arc::new(SqlCatalog(n2.sink.clone())), keeper2.leadership());
    let n1_restarted = start_node(&env, 1, 2).await;
    wait_event(&n2.node.service(), "create", "/one").await;
    n2.node.service().create(create("/two")).await.unwrap();
    wait_event(&n2.node.service(), "create", "/two").await;

    // Graceful close: lease released, catalog left Closed on node 2.
    projector2.abort();
    keeper2.shutdown().await;
    n2.node.close().await;

    // Era 3: node 3 revives the closed catalog stream locally.
    let n3 = start_node(&env, 3, 1).await;
    let keeper3 = LeaseKeeper::spawn(n3.store.clone(), "node-3".into(), lease_config());
    wait_leader(&keeper3).await;
    let projector3 = n3
        .node
        .service()
        .spawn_catalog_loop(Arc::new(SqlCatalog(n3.sink.clone())), keeper3.leadership());
    n3.node.service().create(create("/three")).await.unwrap();
    wait_event(&n3.node.service(), "create", "/three").await;

    // Every create landed exactly once across both failovers.
    let events = catalog_events(&n3.node.service()).await;
    for name in ["/one", "/two", "/three"] {
        assert_eq!(count(&events, "create", name), 1, "duplicate create {name}");
    }
    assert!(
        events
            .iter()
            .all(|e| e["op"] != "delete" && e["op"] != "update"),
        "unexpected events: {events:?}"
    );

    projector3.abort();
    keeper3.shutdown().await;
    n3.node.close().await;
    n1_restarted.node.close().await;
}
