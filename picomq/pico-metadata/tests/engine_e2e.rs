//! End-to-end: the real s3stream engine running on the REAL metadata plane
//! (LocalSink + MetadataStreamManager/MetadataObjectManager/MetadataKvClient)
//! instead of the engine's in-memory test managers, collapsed onto a 1-node
//! sink. The scenario mirrors the facade's `end_to_end_append_fetch_reopen`
//! test so any divergence between the memory managers and this metadata plane
//! shows up as a test difference.

use std::sync::Arc;

use bytes::Bytes;
use picomq_metadata::{LocalSink, MetadataNodeHandle};
use s3stream::{
    AppendContext, Config, CreateStreamOptions, FetchContext, KVClient as _, KeyValue,
    MemoryObjectStorage, ObjectStorageTrait, ObjectWalConfig, ObjectWalService, OpenStreamOptions,
    RecordBatch, S3StreamBuilder, StreamManagerTrait as _,
};

const NODE_ID: i32 = 1;
const NODE_EPOCH: i64 = 1;

#[tokio::test]
async fn engine_end_to_end_on_real_metadata_plane() {
    let (sink, views) = LocalSink::new();
    let handle = MetadataNodeHandle::new(NODE_ID, NODE_EPOCH, Arc::new(sink), views.clone());
    // MetadataLifecycle registers the node before serving. Every write
    // command below is fenced on this registration.
    handle.register("http://node-1:9090").await.unwrap();

    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let mut wal_config = ObjectWalConfig::defaults();
    wal_config.cluster_id = "metadata-e2e".into();
    wal_config.node_id = NODE_ID as u32;
    wal_config.epoch = NODE_EPOCH as u64;

    let engine = S3StreamBuilder::new(Config::default())
        .object_storage(object_storage)
        .write_ahead_log(Arc::new(ObjectWalService::new(wal_storage, wal_config)))
        .stream_manager(Arc::new(handle.stream_manager()))
        .object_manager(Arc::new(handle.object_manager()))
        .kv_client(Arc::new(handle.kv_client()))
        .build()
        .await
        .unwrap();

    let client = engine.stream_client();
    let stream = client
        .create_and_open_stream(CreateStreamOptions {
            epoch: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let stream_id = stream.stream_id();

    // The create+open went through the replicated state machine.
    {
        let view = views.load();
        let meta = view
            .state
            .get_stream(stream_id)
            .expect("stream in metadata plane");
        assert_eq!(meta.epoch, 1);
        assert_eq!(meta.node_id, NODE_ID);
        assert_eq!(view.state.get_opening_streams(NODE_ID).len(), 1);
    }

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
    let fetched = stream
        .fetch(FetchContext::default(), 2, 7, usize::MAX)
        .await
        .unwrap();
    assert_eq!(fetched.records.first().unwrap().base_offset, 2);
    assert_eq!(fetched.records[0].payload, Bytes::from(vec![2u8; 128]));

    // Second stream so the close-time upload commits a real stream-SET object
    // (single-stream blocks split into stream objects, which skip the range index).
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
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while index.search_object_id(stream_id, 0).is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "range index never saw the commit"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    second.close().await.unwrap();

    // The commit landed in the metadata plane: end offset advanced, the object
    // registered under this node, the prepare lease consumed.
    {
        let view = views.load();
        let meta = view.state.get_stream(stream_id).unwrap();
        assert_eq!(meta.end_offset, 10);
        assert!(view.state.objects_count() >= 1);
        assert!(!view.state.get_server_objects(NODE_ID).is_empty());
        assert!(
            view.state.get_opening_streams(NODE_ID).is_empty(),
            "closes released ownership"
        );
    }

    // Reopen at a higher epoch through the real StreamManager: committed data
    // must still be readable, and the metadata plane must show the new epoch.
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
    assert_eq!(views.load().state.get_stream(stream_id).unwrap().epoch, 2);

    // KV through the real MetadataKvClient (replicated, not MemoryKvClient).
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
    assert_eq!(kv.list_kv("topic/").await.unwrap().len(), 1);
    assert_eq!(
        views.load().state.get_kv("topic/0"),
        Some(Bytes::from_static(b"42"))
    );

    engine.shutdown().await;

    // Stale-epoch writes are fenced by the plane: epoch 2 owns the stream, so a
    // straggler still acting at epoch 1 is rejected.
    handle
        .stream_manager()
        .close_stream(stream_id, 1)
        .await
        .expect_err("stale epoch must be fenced");
}
