//! End-to-end: the real s3stream engine on the SQL-backed metadata plane.
//!
//! Mirrors `pico-metadata/tests/engine_e2e.rs` (which runs over
//! `LocalSink`) with the three things only this crate adds:
//!
//! - the durable log: the engine's control-plane traffic flows through
//!   SQLite via `SqlSink`, with the snapshot cycle enabled and running
//!   *underneath* live engine traffic.
//! - leader-gated lifecycle: a `LeaseKeeper` election drives the ported
//!   `MetadataLifecycle` (prepared-object expiry) against the same sink.
//! - restart: a fresh `SqlSink` over the same database restores the full
//!   control-plane state (snapshot + log tail). Streams, epochs, KV.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pico_metadata::lifecycle::{MetadataLifecycle, ObjectCleaner};
use pico_metadata::{CommandSink, MetadataCommand, MetadataNodeHandle};
use pico_sql::{LeaseConfig, LeaseKeeper, MetaStore, SqlSink, SqlSinkConfig, SqliteStore};
use s3stream::{
    AppendContext, Config, CreateStreamOptions, FetchContext, KVClient as _, KeyValue,
    MemoryObjectStorage, ObjectStorageTrait, ObjectWalConfig, ObjectWalService, OpenStreamOptions,
    RecordBatch, S3StreamBuilder,
};

const NODE_ID: i32 = 1;
const NODE_EPOCH: i64 = 1;

fn sink_config() -> SqlSinkConfig {
    SqlSinkConfig {
        poll_interval: Duration::from_millis(1),
        // Low threshold so the cycle actually runs under this test's traffic.
        snapshot_every: 8,
        snapshot_min_interval: Duration::ZERO,
        ..SqlSinkConfig::default()
    }
}

#[tokio::test]
async fn engine_end_to_end_on_sql_metadata_plane() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("meta.db");

    let store: Arc<dyn MetaStore> = Arc::new(SqliteStore::open(&db_path).await.unwrap());
    let (sink, views) = SqlSink::open(store.clone(), sink_config()).await.unwrap();
    sink.set_flushable_idx(u64::MAX);
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let handle = MetadataNodeHandle::new(NODE_ID, NODE_EPOCH, sink.clone(), views.clone());
    handle.register("http://node-1:9090").await.unwrap();

    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let mut wal_config = ObjectWalConfig::defaults();
    wal_config.cluster_id = "sql-e2e".into();
    wal_config.node_id = NODE_ID as u32;
    wal_config.epoch = NODE_EPOCH as u64;

    let engine = S3StreamBuilder::new(Config::default())
        .object_storage(object_storage.clone())
        .write_ahead_log(Arc::new(ObjectWalService::new(wal_storage, wal_config)))
        .stream_manager(Arc::new(handle.stream_manager()))
        .object_manager(Arc::new(handle.object_manager()))
        .kv_client(Arc::new(handle.kv_client()))
        .build()
        .await
        .unwrap();

    // Same scenario as the LocalSink e2e: two streams so the close-time
    // upload commits a real stream-set object.
    let client = engine.stream_client();
    let stream = client
        .create_and_open_stream(CreateStreamOptions {
            epoch: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let stream_id = stream.stream_id();
    for i in 0..10u64 {
        let result = stream
            .append(
                AppendContext::default(),
                RecordBatch::new(1, 0, Bytes::from(vec![i as u8; 128])),
            )
            .await
            .unwrap();
        assert_eq!(result.base_offset, i);
    }
    let second = client
        .create_and_open_stream(CreateStreamOptions {
            epoch: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    second
        .append(
            AppendContext::default(),
            RecordBatch::new(1, 0, Bytes::from(vec![9u8; 64])),
        )
        .await
        .unwrap();
    stream.close().await.unwrap();
    let index = engine.range_index_cache();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while index.search_object_id(stream_id, 0).is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "range index never saw the commit"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    second.close().await.unwrap();

    // Commit landed in the SQL-backed plane.
    {
        let view = views.load();
        assert_eq!(view.state.get_stream(stream_id).unwrap().end_offset, 10);
        assert!(view.state.objects_count() >= 1);
        assert!(view.state.get_opening_streams(NODE_ID).is_empty());
    }

    // Reopen at a higher epoch: committed data readable, epoch replicated.
    let reopened = client
        .open_stream(
            stream_id,
            OpenStreamOptions {
                epoch: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(reopened.next_offset(), 10);
    let fetched = reopened
        .fetch(FetchContext::default(), 0, 10, usize::MAX)
        .await
        .unwrap();
    let total: u64 = fetched
        .records
        .iter()
        .map(|r| r.last_offset - r.base_offset)
        .sum();
    assert_eq!(total, 10);
    reopened.close().await.unwrap();

    // KV through the replicated plane.
    let kv = handle.kv_client();
    kv.put_kv(KeyValue {
        key: "topic/0".into(),
        value: Bytes::from_static(b"42"),
    })
    .await
    .unwrap();
    assert_eq!(
        kv.get_kv("topic/0").await.unwrap(),
        Some(Bytes::from_static(b"42"))
    );

    engine.shutdown().await;

    // Lease-driven lifecycle: win the election, expire a lapsed prepare.
    sink.propose(MetadataCommand::PrepareObject {
        node_id: NODE_ID,
        node_epoch: NODE_EPOCH,
        count: 1,
        ttl_ms: 1, // long lapsed against wall-clock now
        now_ms: 0,
    })
    .await
    .unwrap();
    assert!(views.load().state.prepared_objects_count() >= 1);

    let keeper = LeaseKeeper::spawn(
        store.clone(),
        format!("node-{NODE_ID}"),
        LeaseConfig {
            ttl_ms: 500,
            check_interval: Duration::from_millis(20),
        },
    );
    let cleaner = Arc::new(ObjectCleaner::new(
        sink.clone(),
        views.clone(),
        Some(object_storage),
    ));
    let lifecycle = Arc::new(MetadataLifecycle::new(
        sink.clone(),
        cleaner,
        Duration::from_millis(5),
    ));
    let driver = lifecycle.clone().drive(keeper.leadership());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while views.load().state.prepared_objects_count() != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "lifecycle never expired the prepare"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    driver.abort();
    keeper.shutdown().await;

    // Restart: a fresh sink over the same DB restores everything.
    let snapshot_state = views.load().state.clone();
    drop(handle);
    drop(sink);

    let store: Arc<dyn MetaStore> = Arc::new(SqliteStore::open(&db_path).await.unwrap());
    let (sink, views) = SqlSink::open(store, sink_config()).await.unwrap();
    let restored = views.load();
    assert_eq!(
        restored.state, snapshot_state,
        "cold start must reproduce the exact state"
    );
    assert_eq!(restored.state.get_stream(stream_id).unwrap().end_offset, 10);
    assert_eq!(restored.state.get_stream(stream_id).unwrap().epoch, 2);
    assert_eq!(
        restored.state.get_kv("topic/0"),
        Some(Bytes::from_static(b"42"))
    );

    // The restored plane is live: stale epochs still fenced.
    let handle = MetadataNodeHandle::new(NODE_ID, NODE_EPOCH, Arc::new(sink), views);
    use s3stream::StreamManagerTrait as _;
    handle
        .stream_manager()
        .close_stream(stream_id, 1)
        .await
        .expect_err("stale epoch must be fenced after restart");
}
