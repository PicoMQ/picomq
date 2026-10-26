use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use picomq_metadata::sink::Proposed;
use picomq_metadata::{
    CommandSink, LocalSink, MetadataCommand, MetadataError, MetadataState, ViewPublisher, apply,
};
use picomq_server::ownership::OwnershipService as _;
use picomq_server::record::encode_batch;
use picomq_server::wal::ObjectWal;
use picomq_server::{
    AppendBatchResult, AppendCommand, CreateCommand, ErrorKind, Log, LogRecord, MAX_NEW_PARTITIONS,
    NodeConfig, OffsetToken, PicoNode, Retention, ServiceError, Topic, Topics, partition_log,
};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait, StreamState};

async fn start_node(
    node_id: i32,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
) -> PicoNode {
    start_engine(node_id, sink, views, s3stream::Config::default()).await
}

async fn start_engine(
    node_id: i32,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
    engine: s3stream::Config,
) -> PicoNode {
    let object_storage: Arc<dyn ObjectStorageTrait> =
        Arc::new(MemoryObjectStorage::new((node_id * 2) as i16));
    let wal_storage: Arc<dyn ObjectStorageTrait> =
        Arc::new(MemoryObjectStorage::new((node_id * 2 + 1) as i16));
    let config = NodeConfig {
        node_id,
        node_epoch: 1,
        http_address: format!("http://127.0.0.1:{}", 4000 + node_id),
        heartbeat_interval: Duration::from_millis(100),
        engine,
        ..Default::default()
    };
    let wal = ObjectWal::new(&config.cluster_id, &config.engine.wal_config, wal_storage);
    PicoNode::start(config, sink, views, object_storage, Arc::new(wal))
        .await
        .unwrap()
}

async fn local_node() -> PicoNode {
    let (sink, views) = LocalSink::new();
    start_node(1, Arc::new(sink), views).await
}

fn create(name: &str, content_type: &str) -> CreateCommand {
    CreateCommand::new(name, content_type)
}

fn append(values: &[&[u8]], content_type: &str) -> AppendCommand {
    AppendCommand {
        records: values
            .iter()
            .map(|v| LogRecord::value(Bytes::copy_from_slice(v)))
            .collect(),
        content_type: Some(content_type.into()),
        ..Default::default()
    }
}

async fn log_of(topics: &Topics, name: &str) -> Log {
    let topic = topics.get(name).await.unwrap().unwrap();
    partition_log(&topic, 0).unwrap()
}

#[tokio::test]
async fn create_append_read_close_via_topics() {
    let node = local_node().await;
    let topics = node.topics();

    let created = topics
        .create(create("/streams/demo", "text/plain"))
        .await
        .unwrap();
    assert!(created.created);

    let appended = topics
        .append("/streams/demo", 0, append(&[b"hello"], "text/plain"))
        .await
        .unwrap();
    assert!(appended.applied);
    assert!(!appended.closed);

    let batch = topics
        .read("/streams/demo", 0, OffsetToken::beginning(), 1024, 0)
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(
        batch.records[0].record.value.as_deref(),
        Some(&b"hello"[..])
    );
    assert!(batch.up_to_date);

    let closed = topics.close("/streams/demo").await.unwrap();
    assert!(closed.next_offset.record_offset() >= 1);
    let closed = topics.get("/streams/demo").await.unwrap().unwrap();
    assert!(closed.closed);

    topics
        .create(create("/streams/batch", "application/octet-stream"))
        .await
        .unwrap();
    let appended = topics
        .append(
            "/streams/batch",
            0,
            append(&[b"a", b"bb", b"ccc"], "application/octet-stream"),
        )
        .await
        .unwrap();
    assert!(appended.applied);
    assert_eq!(appended.next_offset.record_offset(), 3);

    let all = topics
        .read("/streams/batch", 0, OffsetToken::beginning(), 1024, 0)
        .await
        .unwrap();
    let values: Vec<&[u8]> = all
        .records
        .iter()
        .map(|r| r.record.value.as_deref().unwrap())
        .collect();
    assert_eq!(values, [b"a".as_slice(), b"bb", b"ccc"]);
    let offsets: Vec<u64> = all
        .records
        .iter()
        .map(|r| r.offset.record_offset())
        .collect();
    assert_eq!(offsets, [0, 1, 2]);
    assert!(all.up_to_date);

    let tail = topics
        .read(
            "/streams/batch",
            0,
            OffsetToken::of_record_offset(1),
            1024,
            0,
        )
        .await
        .unwrap();
    assert_eq!(tail.records.len(), 2);
    assert_eq!(tail.records[0].record.value.as_deref(), Some(&b"bb"[..]));

    let limited = topics
        .read("/streams/batch", 0, OffsetToken::beginning(), 1024, 2)
        .await
        .unwrap();
    assert_eq!(limited.records.len(), 2);
    assert_eq!(limited.next_offset.record_offset(), 2);
    assert!(!limited.up_to_date);

    let effective = topics.trim("/streams/batch", 0, 2).await.unwrap();
    assert!(effective <= 2, "effective trim {effective}");
    let topic = topics.get("/streams/batch").await.unwrap().unwrap();
    let offsets = topics.live_offsets(&topic, 0).await.unwrap();
    assert_eq!(offsets.start_offset.record_offset(), effective);

    let trimmed = topics
        .read(
            "/streams/batch",
            0,
            OffsetToken::of_record_offset(2),
            1024,
            0,
        )
        .await
        .unwrap();
    assert_eq!(trimmed.records.len(), 1);
    assert_eq!(
        trimmed.records[0].record.value.as_deref(),
        Some(&b"ccc"[..])
    );

    node.close().await;
}

#[tokio::test]
async fn concurrent_producers_on_one_topic_pipeline() {
    let node = local_node().await;
    let topics = node.topics();
    topics
        .create(create("/streams/hot", "application/octet-stream"))
        .await
        .unwrap();

    let mut tasks = tokio::task::JoinSet::new();
    for producer in 0..8u8 {
        let topics = Arc::clone(&topics);
        tasks.spawn(async move {
            for record in 0..16u8 {
                topics
                    .append(
                        "/streams/hot",
                        0,
                        append(&[&[producer, record]], "application/octet-stream"),
                    )
                    .await
                    .unwrap();
            }
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.unwrap();
    }

    let all = topics
        .read("/streams/hot", 0, OffsetToken::beginning(), 1 << 20, 0)
        .await
        .unwrap();
    assert_eq!(all.records.len(), 128);
    assert!(all.up_to_date);
    for (i, record) in all.records.iter().enumerate() {
        assert_eq!(record.offset.record_offset(), i as u64, "offsets dense");
    }
    for producer in 0..8u8 {
        let seen: Vec<u8> = all
            .records
            .iter()
            .filter(|r| r.record.value.as_ref().unwrap()[0] == producer)
            .map(|r| r.record.value.as_ref().unwrap()[1])
            .collect();
        assert_eq!(
            seen,
            (0..16u8).collect::<Vec<_>>(),
            "producer {producer} order"
        );
    }

    node.close().await;
}

#[tokio::test]
async fn list_and_match_seq_via_topics() {
    let node = local_node().await;
    let topics = node.topics();

    for name in ["/list/a", "/list/b", "/list/c", "/other/x"] {
        topics.create(create(name, "text/plain")).await.unwrap();
    }

    let names = |page: &picomq_server::Page<String, picomq_server::Topic>| -> Vec<String> {
        page.items
            .iter()
            .map(|(_, topic)| topic.name.spelling().to_owned())
            .collect()
    };

    let all = topics.list("/list/", None, 100, |_| true);
    assert_eq!(names(&all), ["/list/a", "/list/b", "/list/c"]);
    assert_eq!(all.next, None);

    let page = topics.list("/list/", None, 2, |_| true);
    assert_eq!(names(&page), ["/list/a", "/list/b"]);
    assert_eq!(page.next.as_deref(), Some("list.b"));

    let rest = topics.list("/list/", page.next.as_deref(), 100, |_| true);
    assert_eq!(names(&rest), ["/list/c"]);
    assert_eq!(rest.next, None);

    let at = |payload: &'static [u8], match_seq: u64| AppendCommand {
        match_seq: Some(match_seq),
        ..append(&[payload], "text/plain")
    };
    let first = topics.append("/list/a", 0, at(b"one", 0)).await.unwrap();
    assert!(first.applied);
    assert_eq!(first.next_offset.record_offset(), 1);

    let conflict = topics
        .append("/list/a", 0, at(b"stale", 0))
        .await
        .unwrap_err();
    assert_eq!(conflict.kind, ErrorKind::MatchFailed);
    assert_eq!(conflict.next_offset.unwrap().record_offset(), 1);

    assert!(
        topics
            .append("/list/a", 0, at(b"two", 1))
            .await
            .unwrap()
            .applied
    );

    node.close().await;
}

#[tokio::test]
async fn core_flow_on_sql_sink() {
    use picomq_sql::{MetaStore, SqlSink, SqlSinkConfig, SqliteStore};

    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn MetaStore> = Arc::new(
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
    let node = start_node(1, Arc::new(sink), views).await;
    let topics = node.topics();

    topics
        .create(create("/sql/demo", "application/json"))
        .await
        .unwrap();
    let appended = topics
        .append(
            "/sql/demo",
            0,
            append(&[br#"{"a":1}"#, br#"{"b":2}"#], "application/json"),
        )
        .await
        .unwrap();
    assert_eq!(appended.next_offset.record_offset(), 2);

    let read = topics
        .read("/sql/demo", 0, OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert_eq!(read.records.len(), 2);
    assert_eq!(
        read.records[0].record.value.as_deref(),
        Some(&br#"{"a":1}"#[..])
    );
    assert_eq!(
        read.records[1].record.value.as_deref(),
        Some(&br#"{"b":2}"#[..])
    );

    assert!(topics.delete("/sql/demo").await.unwrap());
    assert!(topics.get("/sql/demo").await.unwrap().is_none());
    assert!(!topics.delete("/sql/demo").await.unwrap());

    node.close().await;
}

#[tokio::test]
async fn wait_appended_long_poll() {
    let node = local_node().await;
    let topics = node.topics();
    topics
        .create(create("/poll/a", "text/plain"))
        .await
        .unwrap();

    let waiter = topics.clone();
    let parked = tokio::spawn(async move {
        waiter
            .wait_appended(
                "/poll/a",
                0,
                OffsetToken::beginning(),
                Duration::from_secs(10),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!parked.is_finished());

    topics
        .append("/poll/a", 0, append(&[b"x"], "text/plain"))
        .await
        .unwrap();
    assert!(parked.await.unwrap().unwrap());

    let ready = topics
        .wait_appended(
            "/poll/a",
            0,
            OffsetToken::beginning(),
            Duration::from_millis(1),
        )
        .await
        .unwrap();
    assert!(ready);

    let unknown = topics
        .wait_appended(
            "/poll/none",
            0,
            OffsetToken::beginning(),
            Duration::from_millis(1),
        )
        .await
        .unwrap();
    assert!(!unknown);

    topics.close("/poll/a").await.unwrap();
    let closed = topics
        .wait_appended(
            "/poll/a",
            0,
            OffsetToken::of_record_offset(99),
            Duration::from_millis(1),
        )
        .await
        .unwrap();
    assert!(closed);

    node.close().await;
}

#[tokio::test]
async fn producer_idempotency_and_closed_replay() {
    use picomq_server::types::Producer;

    let node = local_node().await;
    let topics = node.topics();
    topics
        .create(create("/prod/a", "text/plain"))
        .await
        .unwrap();

    let with_producer = |payload: &'static [u8], epoch: u64, seq: u64| AppendCommand {
        producer: Some(Producer::new("p1", epoch, seq).unwrap()),
        ..append(&[payload], "text/plain")
    };

    assert!(
        topics
            .append("/prod/a", 0, with_producer(b"one", 1, 0))
            .await
            .unwrap()
            .applied
    );
    let dup = topics
        .append("/prod/a", 0, with_producer(b"one", 1, 0))
        .await
        .unwrap();
    assert!(!dup.applied);
    assert_eq!(dup.producer_seq, Some(0));

    let gap = topics
        .append("/prod/a", 0, with_producer(b"three", 1, 2))
        .await
        .unwrap_err();
    assert_eq!(gap.kind, ErrorKind::SequenceGap);
    assert_eq!((gap.expected_seq, gap.received_seq), (Some(1), Some(2)));

    assert!(
        topics
            .append("/prod/a", 0, with_producer(b"two", 2, 0))
            .await
            .unwrap()
            .applied
    );
    let stale = topics
        .append("/prod/a", 0, with_producer(b"nope", 1, 1))
        .await
        .unwrap_err();
    assert_eq!(stale.kind, ErrorKind::Fenced);
    assert_eq!(stale.producer_epoch, Some(2));

    let close = AppendCommand {
        close_after: true,
        producer: Some(Producer::new("p1", 2, 1).unwrap()),
        ..append(&[b"last"], "text/plain")
    };
    assert!(
        topics
            .append("/prod/a", 0, close.clone())
            .await
            .unwrap()
            .closed
    );
    let replay = topics.append("/prod/a", 0, close).await.unwrap();
    assert!(replay.closed);
    assert!(!replay.applied);

    let other = AppendCommand {
        producer: Some(Producer::new("p2", 0, 0).unwrap()),
        ..append(&[b"x"], "text/plain")
    };
    let err = topics.append("/prod/a", 0, other).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::Closed);
    assert!(err.closed);

    node.close().await;
}

#[tokio::test]
async fn ownership_routes_to_open_owner() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;

    let unknown = node1
        .ownership()
        .owner_of(&Log::Group("absent".into()))
        .await
        .unwrap();
    assert!(unknown.local);
    assert_eq!(unknown.stream_id, None);

    node1
        .topics()
        .create(create("/own/a", "text/plain"))
        .await
        .unwrap();
    let log = log_of(&node1.topics(), "/own/a").await;

    let placed = node2.ownership().owner_of(&log).await.unwrap();
    assert!(!placed.local);
    assert_eq!(placed.owner_node_id, Some(1));

    node1
        .topics()
        .append("/own/a", 0, append(&[b"opened"], "text/plain"))
        .await
        .unwrap();
    let local = node1.ownership().owner_of(&log).await.unwrap();
    assert!(local.local);

    let remote = node2.ownership().owner_of(&log).await.unwrap();
    assert!(!remote.local);
    assert_eq!(remote.owner_node_id, Some(1));
    assert_eq!(remote.epoch, local.epoch);
    assert_eq!(
        remote.owner_advertised_address.as_deref(),
        Some("http://127.0.0.1:4001")
    );

    node1.close().await;
    let released = node2.ownership().owner_of(&log).await.unwrap();
    assert!(released.local);
    assert!(released.stream_id.is_some());

    node2.close().await;
}

async fn start_shared_node(
    node_id: i32,
    node_epoch: i64,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
    object_storage: Arc<dyn ObjectStorageTrait>,
    wal_storage: Arc<dyn ObjectStorageTrait>,
) -> PicoNode {
    let config = NodeConfig {
        node_id,
        node_epoch,
        http_address: format!("http://127.0.0.1:{}", 4000 + node_id),
        ..Default::default()
    };
    let wal = ObjectWal::new(&config.cluster_id, &config.engine.wal_config, wal_storage);
    PicoNode::start(config, sink, views, object_storage, Arc::new(wal))
        .await
        .unwrap()
}

async fn wait_for_view(
    views: &ViewPublisher,
    what: &str,
    satisfied: impl Fn(&picomq_metadata::MetadataView) -> bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if satisfied(&views.load()) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn transfer_moves_partition_to_target_node() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node1 = start_shared_node(
        1,
        1,
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;
    let node2 = start_shared_node(
        2,
        1,
        sink.clone(),
        views.clone(),
        object_storage,
        wal_storage,
    )
    .await;

    node1
        .topics()
        .create(create("/xfer/a", "text/plain"))
        .await
        .unwrap();
    node1
        .topics()
        .append("/xfer/a", 0, append(&[b"before"], "text/plain"))
        .await
        .unwrap();
    let log = log_of(&node1.topics(), "/xfer/a").await;
    let topic = node1.topics().get("/xfer/a").await.unwrap().unwrap();
    let before = node1.topics().watermarks(&topic, 0).await.unwrap().epoch;

    let racer = {
        let topics = node1.topics();
        tokio::spawn(async move {
            let mut applied = 0u64;
            for i in 0..20u8 {
                match topics
                    .append("/xfer/a", 0, append(&[&[i]], "text/plain"))
                    .await
                {
                    Ok(_) => applied += 1,
                    Err(e) => {
                        assert_eq!(e.kind, ErrorKind::Transferring, "{e:?}");
                        break;
                    }
                }
            }
            applied
        })
    };
    let stream_id = node1.transfer(&log, 2).await.unwrap();
    let raced = racer.await.unwrap();

    wait_for_view(&views, "transfer completion", |view| {
        !view.state.pending_transfers().contains_key(&stream_id)
            && view
                .state
                .streams()
                .get(&stream_id)
                .is_some_and(|row| row.node_id == 2)
    })
    .await;
    wait_for_view(&views, "target pre-warm open", |view| {
        view.state
            .streams()
            .get(&stream_id)
            .is_some_and(|row| row.state == s3stream::StreamState::Opened && row.node_id == 2)
    })
    .await;

    let owner = node1.ownership().owner_of(&log).await.unwrap();
    assert!(!owner.local);
    assert_eq!(owner.owner_node_id, Some(2));
    assert!(node2.ownership().owner_of(&log).await.unwrap().local);

    let appended = node2
        .topics()
        .append("/xfer/a", 0, append(&[b"after"], "text/plain"))
        .await
        .unwrap();
    assert!(appended.applied);
    let all = node2
        .topics()
        .read("/xfer/a", 0, OffsetToken::beginning(), 1 << 20, 0)
        .await
        .unwrap();
    assert_eq!(all.records.len() as u64, 2 + raced);
    assert_eq!(all.records[0].record.value.as_deref(), Some(&b"before"[..]));
    assert_eq!(
        all.records.last().unwrap().record.value.as_deref(),
        Some(&b"after"[..])
    );

    let after = node2.topics().watermarks(&topic, 0).await.unwrap().epoch;
    assert!(after > before, "epoch {before} -> {after}");
    assert_eq!(
        node2.topics().epochs(&topic, 0).unwrap(),
        [(before, 0), (after, 1 + raced)]
    );

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn stale_transfer_completes_when_source_restarts() {
    use picomq_metadata::{MetadataCommand, Proposer};

    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node2 = start_shared_node(
        2,
        1,
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;

    sink.propose(MetadataCommand::RegisterNode {
        wal: String::new(),
        node_id: 1,
        node_epoch: 1,
        http_address: "http://127.0.0.1:4001".into(),
        protocol_addresses: Default::default(),
        slots: 1,
    })
    .await
    .unwrap();
    sink.propose(MetadataCommand::CreateStream {
        proposer: Proposer::new(1, 1),
    })
    .await
    .unwrap();
    let stream_id = views.load().state.next_stream_id() - 1;
    sink.propose(MetadataCommand::OpenStream {
        proposer: Proposer::new(1, 1),

        stream_id,
        epoch: 5,
    })
    .await
    .unwrap();
    sink.propose(MetadataCommand::TransferStream {
        proposer: Proposer::new(1, 1),
        stream_id,
        from_node: 1,
        to_node: 2,
    })
    .await
    .unwrap();
    assert!(
        views
            .load()
            .state
            .pending_transfers()
            .contains_key(&stream_id)
    );

    let node1 = start_shared_node(1, 2, sink, views.clone(), object_storage, wal_storage).await;
    wait_for_view(&views, "stale transfer completion", |view| {
        !view.state.pending_transfers().contains_key(&stream_id)
            && view
                .state
                .streams()
                .get(&stream_id)
                .is_some_and(|row| row.node_id == 2)
    })
    .await;

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn topics_survive_restart() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));

    let node = start_shared_node(
        1,
        1,
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;
    node.topics()
        .create(create("/durable/a", "text/plain"))
        .await
        .unwrap();
    node.topics()
        .append("/durable/a", 0, append(&[b"kept"], "text/plain"))
        .await
        .unwrap();
    node.close().await;

    let node = start_shared_node(1, 2, sink, views, object_storage, wal_storage).await;

    let topic = node.topics().get("/durable/a").await.unwrap().unwrap();
    let offsets = node.topics().live_offsets(&topic, 0).await.unwrap();
    assert_eq!(offsets.next_offset.record_offset(), 1);
    let read = node
        .topics()
        .read("/durable/a", 0, OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].record.value.as_deref(), Some(&b"kept"[..]));

    node.close().await;
}

fn kafka_batch(producer: Option<(i64, i16, i32)>, values: &[&[u8]]) -> Bytes {
    let records: Vec<LogRecord> = values
        .iter()
        .map(|v| LogRecord::value(Bytes::copy_from_slice(v)))
        .collect();
    let mut batch = encode_batch(0, -1, 1, None, &records).to_vec();
    if let Some((id, epoch, base_seq)) = producer {
        batch[43..51].copy_from_slice(&id.to_be_bytes());
        batch[51..53].copy_from_slice(&epoch.to_be_bytes());
        batch[53..57].copy_from_slice(&base_seq.to_be_bytes());
        let crc = crc32c::crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
    }
    Bytes::from(batch)
}

async fn produce(
    topics: &Topics,
    name: &str,
    payload: Bytes,
) -> Result<AppendBatchResult, ServiceError> {
    let topic = topics.get(name).await.unwrap().unwrap();
    let submitted = topics.submit_batch(&topic, 0, payload).await?;
    topics.finish_batch(submitted).await
}

#[tokio::test]
async fn kafka_batch_append_read_and_idempotency() {
    let node = local_node().await;
    let topics = node.topics();
    let service = node.service();
    let created = topics
        .create(create("/topics/demo", "application/octet-stream"))
        .await
        .unwrap();
    let log = log_of(&topics, "/topics/demo").await;

    let sent = kafka_batch(None, &[b"k1", b"k2", b"k3"]);
    let appended = produce(&topics, "/topics/demo", sent.clone())
        .await
        .unwrap();
    assert!(!appended.duplicate);
    assert_eq!(appended.base_offset, 0);
    assert_eq!(appended.log_start_offset, 0);

    let watermarks = service.watermarks(&log).await.unwrap();
    assert_eq!(watermarks.log_start_offset, 0);
    assert_eq!(watermarks.high_watermark, 3);

    let from_start = service.read_batches(&log, 0, usize::MAX).await.unwrap();
    assert_eq!(from_start.batches.len(), 1);
    assert_eq!(from_start.batches[0].base_offset, 0);
    assert_eq!(from_start.batches[0].last_offset, 3);
    assert_eq!(from_start.batches[0].count, 3);
    assert_eq!(&from_start.batches[0].payload[..8], &0i64.to_be_bytes());
    assert_eq!(&from_start.batches[0].payload[8..12], &sent[8..12]);
    assert_eq!(
        &from_start.batches[0].payload[12..16],
        &(watermarks.epoch as i32).to_be_bytes()
    );
    assert_eq!(&from_start.batches[0].payload[16..], &sent[16..]);
    assert_eq!(from_start.next_offset, 3);

    let mid = service.read_batches(&log, 1, usize::MAX).await.unwrap();
    assert_eq!(mid.batches.len(), 1);
    assert_eq!(mid.batches[0].base_offset, 0);
    assert_eq!(mid.next_offset, 3);

    let records = topics
        .read("/topics/demo", 0, OffsetToken::of_record_offset(1), 0, 0)
        .await
        .unwrap();
    let values: Vec<&[u8]> = records
        .records
        .iter()
        .map(|r| r.record.value.as_deref().unwrap())
        .collect();
    assert_eq!(values, [b"k2", b"k3"]);
    assert_eq!(records.records[0].offset.record_offset(), 1);
    assert!(records.up_to_date);

    let id = created.meta.topic.id;
    let by_id = topics.by_id(&id).await.unwrap().unwrap();
    assert_eq!(by_id.name.spelling(), "/topics/demo");
    assert!(
        topics
            .by_id(&picomq_server::TopicId([7u8; 16]))
            .await
            .unwrap()
            .is_none()
    );

    let idem = produce(
        &topics,
        "/topics/demo",
        kafka_batch(Some((1, 0, 0)), &[b"i1", b"i2"]),
    )
    .await
    .unwrap();
    assert!(!idem.duplicate);
    assert_eq!(idem.base_offset, 3);

    let second = service.read_batches(&log, 3, usize::MAX).await.unwrap();
    assert_eq!(&second.batches[0].payload[..8], &3i64.to_be_bytes());

    let dup = produce(
        &topics,
        "/topics/demo",
        kafka_batch(Some((1, 0, 0)), &[b"i1", b"i2"]),
    )
    .await
    .unwrap();
    assert!(dup.duplicate);
    assert_eq!(dup.base_offset, 3);

    let gap = produce(
        &topics,
        "/topics/demo",
        kafka_batch(Some((1, 0, 3)), &[b"g"]),
    )
    .await
    .unwrap_err();
    assert_eq!(gap.kind, ErrorKind::SequenceGap);
    assert_eq!((gap.expected_seq, gap.received_seq), (Some(2), Some(3)));

    let bumped = produce(
        &topics,
        "/topics/demo",
        kafka_batch(Some((1, 1, 0)), &[b"e"]),
    )
    .await
    .unwrap();
    assert!(!bumped.duplicate);
    assert_eq!(bumped.base_offset, 5);

    let fenced = produce(
        &topics,
        "/topics/demo",
        kafka_batch(Some((1, 0, 1)), &[b"s"]),
    )
    .await
    .unwrap_err();
    assert_eq!(fenced.kind, ErrorKind::Fenced);
    assert_eq!(fenced.producer_epoch, Some(1));

    let junk = produce(
        &topics,
        "/topics/demo",
        Bytes::from_static(b"definitely not kafka"),
    )
    .await
    .unwrap_err();
    assert_eq!(junk.kind, ErrorKind::CorruptBatch);

    node.close().await;
}

#[tokio::test]
async fn http_and_kafka_writers_share_one_log() {
    let node = local_node().await;
    let topics = node.topics();
    topics
        .create(create("/mixed/log", "application/octet-stream"))
        .await
        .unwrap();
    let log = log_of(&topics, "/mixed/log").await;

    let http = topics
        .append(
            "/mixed/log",
            0,
            append(&[b"h1", b"h2"], "application/octet-stream"),
        )
        .await
        .unwrap();
    assert_eq!(http.next_offset.record_offset(), 2);
    let kafka = produce(&topics, "/mixed/log", kafka_batch(None, &[b"k1"]))
        .await
        .unwrap();
    assert_eq!(kafka.base_offset, 2);
    let http = topics
        .append(
            "/mixed/log",
            0,
            append(&[b"h3"], "application/octet-stream"),
        )
        .await
        .unwrap();
    assert_eq!(http.next_offset.record_offset(), 4);

    let all = topics
        .read("/mixed/log", 0, OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    let values: Vec<&[u8]> = all
        .records
        .iter()
        .map(|r| r.record.value.as_deref().unwrap())
        .collect();
    assert_eq!(values, [b"h1", b"h2", b"k1", b"h3"]);
    let offsets: Vec<u64> = all
        .records
        .iter()
        .map(|r| r.offset.record_offset())
        .collect();
    assert_eq!(offsets, [0, 1, 2, 3]);

    let ts: Vec<i64> = all.records.iter().map(|r| r.record.timestamp_ms).collect();
    assert_eq!(ts[0], ts[1]);
    assert!(ts[3] > ts[0]);
    assert_eq!(ts[2], 1);

    let batches = node
        .service()
        .read_batches(&log, 0, usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        batches
            .batches
            .iter()
            .map(|b| (b.base_offset, b.last_offset))
            .collect::<Vec<_>>(),
        [(0, 2), (2, 3), (3, 4)]
    );
    for batch in &batches.batches {
        let header = picomq_server::record::batch_header(&batch.payload).unwrap();
        assert_eq!(header.base_offset, batch.base_offset);
        assert_eq!(header.record_count, batch.count);
    }

    node.close().await;
}

#[tokio::test]
async fn kafka_producer_state_survives_restart_via_rescan() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node = start_shared_node(
        1,
        1,
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;

    let batch = |seq: i32, count: usize| {
        let values: Vec<Vec<u8>> = (0..count).map(|i| vec![i as u8]).collect();
        let values: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
        kafka_batch(Some((42, 0, seq)), &values)
    };

    node.topics()
        .create(create("/topics/replayed", "application/octet-stream"))
        .await
        .unwrap();
    let first = produce(&node.topics(), "/topics/replayed", batch(0, 3))
        .await
        .unwrap();
    let second = produce(&node.topics(), "/topics/replayed", batch(3, 2))
        .await
        .unwrap();
    assert_eq!((first.base_offset, second.base_offset), (0, 3));
    node.close().await;

    let node = start_shared_node(1, 2, sink, views, object_storage, wal_storage).await;

    let retry = produce(&node.topics(), "/topics/replayed", batch(3, 2))
        .await
        .unwrap();
    assert!(retry.duplicate);
    assert_eq!(retry.base_offset, 3);
    let next = produce(&node.topics(), "/topics/replayed", batch(5, 1))
        .await
        .unwrap();
    assert!(!next.duplicate);
    assert_eq!(next.base_offset, 5);
    let gap = produce(&node.topics(), "/topics/replayed", batch(9, 1))
        .await
        .unwrap_err();
    assert_eq!(gap.kind, ErrorKind::SequenceGap);

    node.close().await;
}

#[tokio::test]
async fn kafka_multi_batch_spans() {
    let node = local_node().await;
    let topics = node.topics();
    topics
        .create(create("/topics/multi", "application/octet-stream"))
        .await
        .unwrap();
    let log = log_of(&topics, "/topics/multi").await;

    let one = kafka_batch(None, &[b"a", b"b"]);
    let two = kafka_batch(None, &[b"c", b"d", b"e"]);
    let payload = Bytes::from([&one[..], &two[..]].concat());
    let appended = produce(&topics, "/topics/multi", payload).await.unwrap();
    assert_eq!(appended.base_offset, 0);
    let watermarks = node.service().watermarks(&log).await.unwrap();
    assert_eq!(watermarks.high_watermark, 5);

    let read = node
        .service()
        .read_batches(&log, 0, usize::MAX)
        .await
        .unwrap();
    let stored: Vec<u8> = read
        .batches
        .iter()
        .flat_map(|b| b.payload.iter().copied())
        .collect();
    assert_eq!(stored.len(), one.len() + two.len());
    assert_eq!(&stored[..8], &0i64.to_be_bytes());
    assert_eq!(&stored[one.len()..one.len() + 8], &2i64.to_be_bytes());
    assert_eq!(read.next_offset, 5);

    let records = topics
        .read("/topics/multi", 0, OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert_eq!(records.records.len(), 5);
    assert_eq!(records.records[4].offset.record_offset(), 4);

    let with_producer = kafka_batch(Some((9, 0, 0)), &[b"x"]);
    let rejected = produce(
        &topics,
        "/topics/multi",
        Bytes::from([&with_producer[..], &two[..]].concat()),
    )
    .await
    .unwrap_err();
    assert_eq!(rejected.kind, ErrorKind::BadRequest);

    node.close().await;
}

#[tokio::test]
async fn names_share_one_canonical_topic() {
    let node = local_node().await;
    let topics = node.topics();

    let created = topics
        .create(create("/orders/eu", "application/json"))
        .await
        .unwrap();
    assert!(created.created);
    assert_eq!(created.meta.topic.name.kafka(), Some("orders.eu"));

    let by_kafka = topics.get_kafka("orders.eu").await.unwrap().unwrap();
    assert_eq!(by_kafka.name.spelling(), "/orders/eu");
    let by_dots = topics.get("/orders.eu").await.unwrap().unwrap();
    assert_eq!(by_dots.id, created.meta.topic.id);

    let again = topics
        .create(create("/orders.eu", "application/json"))
        .await
        .unwrap();
    assert!(!again.created);
    assert_eq!(again.meta.topic.id, created.meta.topic.id);
    assert_eq!(again.meta.topic.name.spelling(), "/orders/eu");

    let conflict = topics
        .create(create("/orders.eu", "text/plain"))
        .await
        .unwrap_err();
    assert_eq!(conflict.kind, ErrorKind::Conflict);

    let spaced = topics
        .create(create("/has space", "application/json"))
        .await
        .unwrap();
    assert_eq!(spaced.meta.topic.name.kafka(), None);
    assert!(topics.get_kafka("has space").await.unwrap().is_none());

    let kafka: Vec<String> = topics
        .kafka_topics()
        .into_iter()
        .map(|topic| topic.name.canonical().to_owned())
        .collect();
    assert_eq!(kafka, ["orders.eu"]);

    node.close().await;
}

#[tokio::test]
async fn recreated_topic_starts_without_history() {
    let node = local_node().await;
    let topics = node.topics();

    let first = topics.create(create("/again", "text/plain")).await.unwrap();
    topics
        .append("/again", 0, append(&[b"old"], "text/plain"))
        .await
        .unwrap();
    assert!(topics.delete("/again").await.unwrap());

    let second = topics.create(create("/again", "text/plain")).await.unwrap();
    assert!(second.created);
    assert_ne!(second.meta.topic.id, first.meta.topic.id);
    assert!(topics.by_id(&first.meta.topic.id).await.unwrap().is_none());

    let read = topics
        .read("/again", 0, OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert!(read.records.is_empty());

    node.close().await;
}

#[tokio::test]
async fn topic_and_log_indexes_follow_create_and_delete() {
    let node = local_node().await;
    let topics = node.topics();

    let created = topics
        .create(create("/topics/indexed", "application/octet-stream"))
        .await
        .unwrap();
    let id = created.meta.topic.id;
    let log = log_of(&topics, "/topics/indexed").await;
    let stream_id = node.service().stream_id(&log).unwrap().unwrap();

    let view = node.views().load();
    assert!(view.state.get_kv("topic/topics.indexed").is_some());
    assert_eq!(
        view.state
            .get_kv(&format!("topic-id/{}", id.hex()))
            .as_deref(),
        Some(b"topics.indexed".as_slice())
    );
    assert_eq!(
        view.state
            .get_kv(&format!("idx/sid/{stream_id}"))
            .as_deref(),
        Some(log.key().as_bytes())
    );
    assert!(view.state.get_kv(&log.key()).is_some());

    assert!(topics.delete("/topics/indexed").await.unwrap());
    assert!(topics.by_id(&id).await.unwrap().is_none());

    let view = node.views().load();
    assert!(view.state.get_kv("topic/topics.indexed").is_none());
    assert!(
        view.state
            .get_kv(&format!("topic-id/{}", id.hex()))
            .is_none()
    );
    assert!(view.state.get_kv(&format!("idx/sid/{stream_id}")).is_none());
    assert!(view.state.get_kv(&log.key()).is_none());

    node.close().await;
}

#[tokio::test]
async fn ttl_sweep_expires_untouched_topics() {
    let node = local_node().await;
    let topics = node.topics();

    let mut command = create("/streams/ephemeral", "text/plain");
    command.ttl_seconds = Some(1);
    topics.create(command).await.unwrap();
    assert!(
        node.views()
            .load()
            .state
            .get_kv("topic/streams.ephemeral")
            .is_some()
    );

    let (tx, rx) = tokio::sync::watch::channel(true);
    let sweep = topics.spawn_ttl_sweep(rx, Duration::from_millis(20));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while node
        .views()
        .load()
        .state
        .get_kv("topic/streams.ephemeral")
        .is_some()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sweep never expired the topic"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    drop(tx);
    sweep.await.unwrap();
    node.close().await;
}

#[tokio::test]
async fn a_filtered_list_spends_its_budget_and_resumes() {
    let node = local_node().await;
    let topics = node.topics();
    for i in 0..30 {
        topics
            .create(create(&format!("/page/{i:02}"), "text/plain"))
            .await
            .unwrap();
    }
    let wanted =
        |topic: &picomq_server::Topic| ["/page/15", "/page/25"].contains(&topic.name.spelling());

    let first = topics.list("/page/", None, 1, wanted);
    assert!(first.items.is_empty());
    assert_eq!(first.next.as_deref(), Some("page.09"));

    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = topics.list("/page/", after.as_deref(), 1, wanted);
        found.extend(page.items.into_iter().map(|(name, _)| name));
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(found, ["page.15", "page.25"]);

    node.close().await;
}

#[tokio::test]
async fn partitions_grow_and_stay_independent() {
    let node = local_node().await;
    let topics = node.topics();

    let created = topics
        .create(CreateCommand {
            partitions: 2,
            ..create("/parts", "text/plain")
        })
        .await
        .unwrap();
    assert_eq!(created.meta.partitions.len(), 2);

    topics
        .append("/parts", 1, append(&[b"one"], "text/plain"))
        .await
        .unwrap();
    let first = topics
        .read("/parts", 0, OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    let second = topics
        .read("/parts", 1, OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert!(first.records.is_empty());
    assert_eq!(second.records[0].record.value.as_deref(), Some(&b"one"[..]));

    let grown = grow(&topics, "/parts", 4, &[]).await.unwrap();
    assert_eq!(grown.partitions, 4);
    topics
        .append("/parts", 3, append(&[b"three"], "text/plain"))
        .await
        .unwrap();
    assert_eq!(
        grow(&topics, "/parts", 3, &[]).await.unwrap_err().kind,
        ErrorKind::BadRequest
    );

    let missing = topics
        .append("/parts", 4, append(&[b"x"], "text/plain"))
        .await
        .unwrap_err();
    assert_eq!(missing.kind, ErrorKind::NotFound);

    let close = topics.close("/parts").await.unwrap_err();
    assert_eq!(close.kind, ErrorKind::BadRequest);

    node.close().await;
}

#[tokio::test]
async fn closed_topics_do_not_grow() {
    let node = local_node().await;
    let topics = node.topics();

    topics
        .create(create("/sealed", "text/plain"))
        .await
        .unwrap();
    topics.close("/sealed").await.unwrap();

    let grown = grow(&topics, "/sealed", 2, &[]).await.unwrap_err();
    assert_eq!(grown.kind, ErrorKind::Closed);
    assert_eq!(topics.get("/sealed").await.unwrap().unwrap().partitions, 1);

    node.close().await;
}

#[tokio::test]
async fn partition_counts_are_capped() {
    let node = local_node().await;
    let topics = node.topics();

    let oversized = topics
        .create(CreateCommand {
            partitions: MAX_NEW_PARTITIONS + 1,
            ..create("/huge", "text/plain")
        })
        .await
        .unwrap_err();
    assert_eq!(oversized.kind, ErrorKind::BadRequest);
    assert!(topics.get("/huge").await.unwrap().is_none());

    topics.create(create("/small", "text/plain")).await.unwrap();
    let grown = topics
        .update(picomq_server::UpdateTopicCommand {
            name: "/small".into(),
            partitions: Some(MAX_NEW_PARTITIONS + 2),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(grown.kind, ErrorKind::BadRequest);
    assert_eq!(topics.get("/small").await.unwrap().unwrap().partitions, 1);

    node.close().await;
}

fn stream_ids(node: &PicoNode, topic: &Topic) -> Vec<u64> {
    (0..topic.partitions)
        .map(|index| {
            let log = partition_log(topic, index).unwrap();
            node.service().stream_id(&log).unwrap().unwrap()
        })
        .collect()
}

fn placements(node: &PicoNode, topic: &Topic) -> Vec<(i32, StreamState)> {
    let view = node.views().load();

    stream_ids(node, topic)
        .iter()
        .map(|stream_id| {
            let row = view.state.streams()[stream_id];
            (row.node_id, row.state)
        })
        .collect()
}

#[tokio::test]
async fn partitions_are_placed_across_nodes() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;

    let topic = node1
        .topics()
        .create(CreateCommand {
            partitions: 4,
            ..create("/spread", "text/plain")
        })
        .await
        .unwrap()
        .meta
        .topic;

    let placed = placements(&node1, &topic);
    assert!(
        placed
            .iter()
            .all(|(_, state)| *state == StreamState::Closed)
    );
    for node_id in [1, 2] {
        assert!(
            placed.iter().any(|(owner, _)| *owner == node_id),
            "{placed:?}"
        );
    }

    let grown = grow(&node2.topics(), "/spread", 6, &[]).await.unwrap();
    let added = &placements(&node1, &grown)[4..];
    for node_id in [1, 2] {
        assert!(
            added.iter().any(|(owner, _)| *owner == node_id),
            "{added:?}"
        );
    }

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn initial_records_keep_the_first_partition_local() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;

    for name in ["/seeded/a", "/seeded/b", "/seeded/c"] {
        let topic = node2
            .topics()
            .create(CreateCommand {
                initial_records: vec![LogRecord::value(Bytes::from_static(b"seed"))],
                ..create(name, "text/plain")
            })
            .await
            .unwrap()
            .meta
            .topic;

        assert_eq!(placements(&node2, &topic), vec![(2, StreamState::Opened)]);
    }

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn deleted_topics_release_streams_held_by_other_nodes() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;

    let topic = node1
        .topics()
        .create(CreateCommand {
            partitions: 4,
            ..create("/doomed", "text/plain")
        })
        .await
        .unwrap()
        .meta
        .topic;
    let mut held = Vec::new();
    for (index, stream_id) in stream_ids(&node1, &topic).into_iter().enumerate() {
        if views.load().state.streams()[&stream_id].node_id != 2 {
            continue;
        }

        node2
            .topics()
            .append("/doomed", index as u32, append(&[b"x"], "text/plain"))
            .await
            .unwrap();
        held.push(stream_id);
    }
    assert!(!held.is_empty());
    assert!(
        held.iter()
            .all(|id| views.load().state.streams()[id].state == StreamState::Opened)
    );

    assert!(node1.topics().delete("/doomed").await.unwrap());
    wait_for_view(&views, "held streams deleted", |view| {
        held.iter().all(|id| !view.state.streams().contains_key(id))
    })
    .await;

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn a_clean_shutdown_hands_streams_to_the_survivors() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;
    let (_lease, leadership) = tokio::sync::watch::channel(true);
    let failover = node1.spawn_failover(leadership, Duration::from_millis(50));

    let topic = node1
        .topics()
        .create(CreateCommand {
            partitions: 2,
            ..create("/handoff", "text/plain")
        })
        .await
        .unwrap()
        .meta
        .topic;
    assert!(
        placements(&node1, &topic)
            .iter()
            .any(|(owner, _)| *owner == 2)
    );

    node2.close().await;

    let streams = stream_ids(&node1, &topic);
    wait_for_view(&views, "streams of node 2 placed on node 1", |view| {
        streams
            .iter()
            .all(|stream_id| view.state.streams()[stream_id].node_id == 1)
    })
    .await;

    failover.abort();
    node1.close().await;
}

#[tokio::test]
async fn a_fenced_node_notices() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;
    let mut fenced = node2.fenced();

    let heartbeats = views.load().state.nodes()[&2].heartbeats;
    node1
        .metadata()
        .fence_node(2, node2.metadata().node_epoch(), heartbeats)
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), fenced.wait_for(|fenced| *fenced))
        .await
        .expect("node 2 never noticed the fence")
        .unwrap();

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn delete_records_waits_for_the_upload() {
    let (sink, views) = LocalSink::new();
    let engine = s3stream::Config {
        wal_upload_interval_ms: 100,
        ..Default::default()
    };
    let node = start_engine(1, Arc::new(sink), views, engine).await;
    let topics = node.topics();

    topics
        .create(create("/trimmed", "text/plain"))
        .await
        .unwrap();
    topics
        .append("/trimmed", 0, append(&[b"a", b"b", b"c"], "text/plain"))
        .await
        .unwrap();
    let topic = topics.get("/trimmed").await.unwrap().unwrap();

    let start = topics
        .delete_records(&topic, 0, 2, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(start >= 2, "start {start}");
    assert_eq!(
        topics
            .live_offsets(&topic, 0)
            .await
            .unwrap()
            .start_offset
            .record_offset(),
        start
    );

    node.close().await;
}

#[tokio::test]
async fn delete_records_times_out_before_the_upload() {
    let node = local_node().await;
    let topics = node.topics();

    topics
        .create(create("/pending", "text/plain"))
        .await
        .unwrap();
    topics
        .append("/pending", 0, append(&[b"a", b"b"], "text/plain"))
        .await
        .unwrap();
    let topic = topics.get("/pending").await.unwrap().unwrap();

    let error = topics
        .delete_records(&topic, 0, 2, Duration::from_millis(200))
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);

    node.close().await;
}

#[tokio::test]
async fn a_retried_close_seals_after_an_interrupted_one() {
    use picomq_server::types::Producer;

    let node = local_node().await;
    let topics = node.topics();
    topics
        .create(create("/interrupted", "text/plain"))
        .await
        .unwrap();

    let last = |close_after| AppendCommand {
        producer: Some(Producer::new("p1", 1, 0).unwrap()),
        close_after,
        ..append(&[b"last"], "text/plain")
    };
    topics.append("/interrupted", 0, last(false)).await.unwrap();

    let retried = topics.append("/interrupted", 0, last(true)).await.unwrap();
    assert!(!retried.applied);
    assert!(retried.closed);
    assert!(topics.get("/interrupted").await.unwrap().unwrap().closed);

    let again = topics.append("/interrupted", 0, last(true)).await.unwrap();
    assert!(again.closed);
    assert_eq!(again.next_offset.record_offset(), 1);

    node.close().await;
}

async fn start_sqlite_node(
    path: &std::path::Path,
    node_id: i32,
    poll_interval: Duration,
    object_storage: Arc<dyn ObjectStorageTrait>,
    wal_storage: Arc<dyn ObjectStorageTrait>,
) -> (PicoNode, Arc<ViewPublisher>) {
    use picomq_sql::{MetaStore, SqlSink, SqlSinkConfig, SqliteStore};

    let store: Arc<dyn MetaStore> = Arc::new(SqliteStore::open(path).await.unwrap());
    let (sink, views) = SqlSink::open(
        store,
        SqlSinkConfig {
            poll_interval,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let node = start_shared_node(
        node_id,
        1,
        Arc::new(sink),
        views.clone(),
        object_storage,
        wal_storage,
    )
    .await;
    (node, views)
}

async fn start_lagging_pair(
    path: &std::path::Path,
) -> (PicoNode, Arc<ViewPublisher>, PicoNode, Arc<ViewPublisher>) {
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let (node1, views1) = start_sqlite_node(
        path,
        1,
        Duration::from_millis(1),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;
    let (node2, views2) = start_sqlite_node(
        path,
        2,
        Duration::from_secs(60),
        object_storage,
        wal_storage,
    )
    .await;
    (node1, views1, node2, views2)
}

#[tokio::test]
async fn a_lagging_node_syncs_before_reporting_a_topic_missing() {
    let dir = tempfile::tempdir().unwrap();
    let (node1, views1, node2, views2) = start_lagging_pair(&dir.path().join("meta.db")).await;

    node1
        .topics()
        .create(create("/fresh", "text/plain"))
        .await
        .unwrap();
    assert!(views2.load().applied_index < views1.load().applied_index);

    let found = tokio::time::timeout(Duration::from_secs(5), node2.topics().get("/fresh"))
        .await
        .expect("the lookup waited for the poll")
        .unwrap();
    assert!(found.is_some());
    assert!(node2.topics().get("/missing").await.unwrap().is_none());

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn deleted_topics_destroy_streams_that_were_never_opened() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views.clone()).await;
    let topics = node.topics();

    topics
        .create(create("/never-opened", "text/plain"))
        .await
        .unwrap();
    let log = log_of(&topics, "/never-opened").await;
    let stream_id = node.service().stream_id(&log).unwrap().unwrap();
    let row = views.load().state.streams()[&stream_id];
    assert_eq!(row.state, StreamState::Closed);
    assert_eq!(row.node_id, 1);

    assert!(topics.delete("/never-opened").await.unwrap());
    wait_for_view(&views, "the placed stream to be destroyed", |view| {
        !view.state.streams().contains_key(&stream_id)
    })
    .await;

    node.close().await;
}

#[tokio::test]
async fn partitions_go_to_the_requested_owners() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node1 = start_shared_node(
        1,
        1,
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;
    let node2 = start_shared_node(2, 1, sink, views.clone(), object_storage, wal_storage).await;
    let topics = node1.topics();
    let owners_of = |topic: &Topic| -> Vec<i32> {
        let state = &views.load().state;
        (0..topic.partitions)
            .map(|index| {
                let log = partition_log(topic, index).unwrap();
                let stream_id = node1.service().stream_id(&log).unwrap().unwrap();
                state.streams()[&stream_id].node_id
            })
            .collect()
    };

    topics
        .create(CreateCommand {
            partitions: 4,
            owners: vec![2, 1, 2, 1],
            ..create("/assigned", "text/plain")
        })
        .await
        .unwrap();
    let topic = topics.get("/assigned").await.unwrap().unwrap();
    assert_eq!(owners_of(&topic), [2, 1, 2, 1]);

    let grown = grow(&topics, "/assigned", 6, &[1, 2]).await.unwrap();
    assert_eq!(owners_of(&grown), [2, 1, 2, 1, 1, 2]);

    let missing = grow(&topics, "/assigned", 8, &[1]).await.unwrap_err();
    assert_eq!(missing.kind, ErrorKind::BadRequest);

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn a_restarted_coordinator_keeps_the_members_that_join_it() {
    use picomq_server::{JoinInput, Membership};

    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let join = || JoinInput {
        group_id: "g".to_owned(),
        member_id: String::new(),
        instance_id: None,
        client_id: "test".to_owned(),
        client_host: String::new(),
        membership: Membership::Subscribed(vec!["/a".to_owned()]),
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 2_000,
        require_known_member_id: false,
    };

    let node = start_shared_node(
        1,
        1,
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;
    assert!(node.groups().join(None, join()).await.result.is_ok());
    node.close().await;

    let restarted = start_shared_node(1, 2, sink, views, object_storage, wal_storage).await;
    assert!(restarted.groups().join(None, join()).await.result.is_ok());
    let described = restarted.groups().describe(None, "g").await.unwrap();
    assert_eq!(described.members.len(), 1);

    restarted.close().await;
}

struct Recorder {
    sink: LocalSink,
    applied: std::sync::Mutex<Vec<(u64, MetadataCommand)>>,
}

#[async_trait::async_trait]
impl CommandSink for Recorder {
    async fn propose(&self, command: MetadataCommand) -> Result<Proposed, MetadataError> {
        let proposed = self.sink.propose(command.clone()).await?;
        self.applied
            .lock()
            .unwrap()
            .push((proposed.applied_index, command));
        Ok(proposed)
    }

    async fn sync(&self) -> Result<(), MetadataError> {
        self.sink.sync().await
    }
}

#[tokio::test]
async fn new_logs_are_published_with_their_owner() {
    let (sink, views) = LocalSink::new();
    let recorder = Arc::new(Recorder {
        sink,
        applied: Default::default(),
    });
    let node = start_node(1, recorder.clone(), views).await;
    let service = node.service();
    let local = Log::Group("local".into());
    let placed = Log::Group("placed".into());

    service.create_local_log(&local).await.unwrap();
    service.create_placed_log(&placed, None).await.unwrap();

    let streams =
        [&local, &placed].map(|log| (log.key(), service.stream_id(log).unwrap().unwrap()));
    let mut applied = recorder.applied.lock().unwrap().clone();
    applied.sort_by_key(|(index, _)| *index);
    let mut state = MetadataState::new();
    for (_, command) in &applied {
        apply(&mut state, command).unwrap();
        for (key, stream_id) in &streams {
            if state.get_kv(key).is_some() {
                assert_ne!(
                    state.streams()[stream_id].node_id,
                    -1,
                    "{key} was published before it had an owner"
                );
            }
        }
    }

    node.close().await;
}

#[tokio::test]
async fn a_lost_claim_destroys_its_stream() {
    let dir = tempfile::tempdir().unwrap();
    let (node1, _, node2, views2) = start_lagging_pair(&dir.path().join("meta.db")).await;
    let log = Log::Group("claimed".into());

    node1.service().create_local_log(&log).await.unwrap();
    node2.service().create_local_log(&log).await.unwrap();

    let winner = node1.service().stream_id(&log).unwrap().unwrap();
    assert_eq!(node2.service().stream_id(&log).unwrap(), Some(winner));
    assert!(views2.load().state.streams().keys().eq([&winner]));

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn a_claim_that_cannot_own_its_stream_destroys_it() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views.clone()).await;
    let log = Log::Group("unowned".into());

    let placed = node.service().create_placed_log(&log, Some(99)).await;
    assert!(placed.is_err());
    assert_eq!(node.service().stream_id(&log).unwrap(), None);
    let state = &views.load().state;
    assert!(state.streams().is_empty());
    assert!(state.get_kv(&log.key()).is_none());

    node.close().await;
}

#[tokio::test]
async fn a_failed_append_reopens_the_stream_at_a_new_epoch() {
    let (sink, views) = LocalSink::new();
    let node = start_node(1, Arc::new(sink), views.clone()).await;
    let topics = node.topics();
    topics
        .create(create("/reopened", "text/plain"))
        .await
        .unwrap();
    topics
        .append("/reopened", 0, append(&[b"first"], "text/plain"))
        .await
        .unwrap();
    let log = log_of(&topics, "/reopened").await;
    let stream_id = node.service().stream_id(&log).unwrap().unwrap();
    let held = node.service().streams().held(stream_id).unwrap();

    held.close().await.unwrap();
    let failed = topics
        .append("/reopened", 0, append(&[b"lost"], "text/plain"))
        .await
        .unwrap_err();
    assert_eq!(failed.kind, ErrorKind::Durability);

    let appended = topics
        .append("/reopened", 0, append(&[b"second"], "text/plain"))
        .await
        .unwrap();
    assert_eq!(appended.next_offset.record_offset(), 2);
    let row = views.load().state.streams()[&stream_id];
    assert_eq!((row.state, row.epoch), (StreamState::Opened, 2));

    node.close().await;
}

/// A node WAL that can be told to fail its next append before writing it.
struct Faulty {
    wal: picomq_server::wal::ObjectWal,
    fail: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl picomq_server::wal::Wal for Faulty {
    fn location(&self) -> String {
        self.wal.location()
    }

    async fn open(
        &self,
        node_id: i32,
        epoch: i64,
    ) -> Result<Arc<dyn s3stream::WriteAheadLogTrait>, s3stream::Error> {
        Ok(Arc::new(FaultyLog {
            log: self.wal.open(node_id, epoch).await?,
            fail: self.fail.clone(),
        }))
    }

    async fn take_over(
        &self,
        request: &s3stream::FailoverRequest,
    ) -> Result<Arc<dyn s3stream::WriteAheadLogTrait>, s3stream::Error> {
        self.wal.take_over(request).await
    }
}

struct FaultyLog {
    log: Arc<dyn s3stream::WriteAheadLogTrait>,
    fail: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl s3stream::WriteAheadLogTrait for FaultyLog {
    async fn start(&self) -> Result<(), s3stream::WalError> {
        self.log.start().await
    }

    async fn shutdown_gracefully(&self) {
        self.log.shutdown_gracefully().await
    }

    fn metadata(&self) -> s3stream_wal::WalMetadata {
        self.log.metadata()
    }

    fn uri(&self) -> &str {
        self.log.uri()
    }

    fn submit(
        &self,
        record: s3stream::StreamRecordBatch,
    ) -> Result<s3stream_wal::PendingAppend, s3stream::WalError> {
        if self.fail.swap(false, Ordering::SeqCst) {
            let failed = std::io::Error::other("injected write failure");
            return Ok(s3stream_wal::PendingAppend {
                durable: Box::pin(async move { Err(s3stream::WalError::Io(failed)) }),
            });
        }
        self.log.submit(record)
    }

    fn set_append_listener(&self, listener: s3stream_wal::AppendListener) {
        self.log.set_append_listener(listener)
    }

    async fn get(
        &self,
        offset: s3stream_wal::RecordOffset,
    ) -> Result<s3stream::StreamRecordBatch, s3stream::WalError> {
        self.log.get(offset).await
    }

    async fn get_range(
        &self,
        start: s3stream_wal::RecordOffset,
        end: s3stream_wal::RecordOffset,
    ) -> Result<Vec<s3stream::StreamRecordBatch>, s3stream::WalError> {
        self.log.get_range(start, end).await
    }

    fn confirm_offset(&self) -> s3stream_wal::RecordOffset {
        self.log.confirm_offset()
    }

    fn recover(&self) -> s3stream_wal::RecoverStream {
        self.log.recover()
    }

    async fn reset(&self) -> Result<(), s3stream::WalError> {
        self.log.reset().await
    }

    async fn trim(&self, offset: s3stream_wal::RecordOffset) -> Result<(), s3stream::WalError> {
        self.log.trim(offset).await
    }
}

async fn faulty_node() -> (PicoNode, Arc<AtomicBool>) {
    let (sink, views) = LocalSink::new();
    let fail = Arc::new(AtomicBool::new(false));
    let wal = Faulty {
        wal: ObjectWal::new("picomq", "", Arc::new(MemoryObjectStorage::new(1))),
        fail: fail.clone(),
    };
    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 1,
            ..Default::default()
        },
        Arc::new(sink),
        views,
        Arc::new(MemoryObjectStorage::new(0)),
        Arc::new(wal),
    )
    .await
    .unwrap();
    (node, fail)
}

async fn values(topics: &Topics, name: &str) -> Vec<Bytes> {
    topics
        .read(name, 0, OffsetToken::beginning(), 1 << 20, 0)
        .await
        .unwrap()
        .records
        .into_iter()
        .map(|record| record.record.value.unwrap())
        .collect()
}

#[tokio::test]
async fn a_retry_after_a_failed_append_is_written_not_deduplicated() {
    use picomq_server::types::Producer;

    let (node, fail) = faulty_node().await;
    let topics = node.topics();
    topics
        .create(create("/retried", "text/plain"))
        .await
        .unwrap();
    let send = |payload: &'static [u8], seq: u64| AppendCommand {
        producer: Some(Producer::new("p", 0, seq).unwrap()),
        ..append(&[payload], "text/plain")
    };

    topics
        .append("/retried", 0, send(b"zero", 0))
        .await
        .unwrap();
    fail.store(true, Ordering::SeqCst);
    let failed = topics
        .append("/retried", 0, send(b"one", 1))
        .await
        .unwrap_err();
    assert_eq!(failed.kind, ErrorKind::Durability);

    let retried = topics.append("/retried", 0, send(b"one", 1)).await.unwrap();
    assert!(
        retried.applied,
        "the retry is written, not acked as a duplicate"
    );
    assert_eq!(values(&topics, "/retried").await, ["zero", "one"]);

    node.close().await;
}

#[tokio::test]
async fn a_kafka_retry_after_a_failed_append_is_written_not_deduplicated() {
    let (node, fail) = faulty_node().await;
    let topics = node.topics();
    topics
        .create(create("/produced", "application/octet-stream"))
        .await
        .unwrap();

    produce(
        &topics,
        "/produced",
        kafka_batch(Some((1, 0, 0)), &[b"zero"]),
    )
    .await
    .unwrap();
    fail.store(true, Ordering::SeqCst);
    let failed = produce(
        &topics,
        "/produced",
        kafka_batch(Some((1, 0, 1)), &[b"one"]),
    )
    .await
    .unwrap_err();
    assert_eq!(failed.kind, ErrorKind::Durability);

    let retried = produce(
        &topics,
        "/produced",
        kafka_batch(Some((1, 0, 1)), &[b"one"]),
    )
    .await
    .unwrap();
    assert!(
        !retried.duplicate,
        "the retry is written, not acked as a duplicate"
    );
    assert_eq!(values(&topics, "/produced").await, ["zero", "one"]);

    node.close().await;
}

struct Outage {
    sink: LocalSink,
    down: AtomicBool,
}

#[async_trait::async_trait]
impl CommandSink for Outage {
    async fn propose(&self, command: MetadataCommand) -> Result<Proposed, MetadataError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(MetadataError::Unavailable {
                message: "store down".into(),
            });
        }
        self.sink.propose(command).await
    }

    async fn sync(&self) -> Result<(), MetadataError> {
        self.sink.sync().await
    }
}

#[tokio::test]
async fn a_metadata_outage_is_reported_as_unavailable() {
    let (sink, views) = LocalSink::new();
    let outage = Arc::new(Outage {
        sink,
        down: Default::default(),
    });
    let node = start_node(1, outage.clone(), views).await;
    let topics = node.topics();

    outage.down.store(true, Ordering::SeqCst);
    let error = topics
        .create(CreateCommand::new("/during", "text/plain"))
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Unavailable, "{error}");

    outage.down.store(false, Ordering::SeqCst);
    topics
        .create(CreateCommand::new("/during", "text/plain"))
        .await
        .unwrap();

    node.close().await;
}

/// A Kafka batch whose records all carry `timestamp_ms` as their create time.
fn timed_batch(timestamp_ms: i64, values: &[&[u8]]) -> Vec<u8> {
    let records: Vec<LogRecord> = values
        .iter()
        .map(|v| LogRecord::value(Bytes::copy_from_slice(v)))
        .collect();
    let mut batch = encode_batch(0, -1, timestamp_ms, None, &records).to_vec();
    batch[21..23].copy_from_slice(&0i16.to_be_bytes());
    let crc = crc32c::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    batch
}

#[tokio::test]
async fn timestamp_lookups_skip_records_trimmed_inside_a_batch() {
    let (sink, views) = LocalSink::new();
    let engine = s3stream::Config {
        wal_upload_interval_ms: 100,
        ..Default::default()
    };
    let node = start_engine(1, Arc::new(sink), views, engine).await;
    let topics = node.topics();
    topics
        .create(create("/topics/timed", "application/octet-stream"))
        .await
        .unwrap();
    produce(
        &topics,
        "/topics/timed",
        Bytes::from(timed_batch(100, &[b"a", b"b"])),
    )
    .await
    .unwrap();
    let topic = topics.get("/topics/timed").await.unwrap().unwrap();
    let start = topics
        .delete_records(&topic, 0, 1, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(start, 1);

    let found = topics
        .record_at_or_after(&topic, 0, 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.offset.record_offset(), 1);
    let newest = topics
        .record_with_max_timestamp(&topic, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(newest.offset.record_offset(), 1);

    node.close().await;
}

#[tokio::test]
async fn timestamp_lookups_find_records_across_objects_and_the_wal() {
    let node = uploading_node().await;
    let topics = node.topics();
    topics
        .create(create("/stamped", "text/plain"))
        .await
        .unwrap();
    for objects in 1..=3 {
        append_object(&node, "/stamped", b"uploaded", objects).await;
    }
    topics
        .append("/stamped", 0, append(&[b"tail"], "text/plain"))
        .await
        .unwrap();
    let topic = topics.get("/stamped").await.unwrap().unwrap();
    let written = topics
        .read("/stamped", 0, OffsetToken::of_record_offset(0), 0, 0)
        .await
        .unwrap()
        .records;
    assert_eq!(written.len(), 4);

    for record in &written {
        let found = topics
            .record_at_or_after(&topic, 0, record.record.timestamp_ms)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.offset, record.offset);
    }
    let newest = written.last().unwrap();
    assert!(
        topics
            .record_at_or_after(&topic, 0, newest.record.timestamp_ms + 1)
            .await
            .unwrap()
            .is_none()
    );
    let found = topics
        .record_with_max_timestamp(&topic, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.offset, newest.offset);

    node.close().await;
}

#[tokio::test]
async fn the_max_timestamp_is_found_before_a_later_older_batch() {
    let node = uploading_node().await;
    let topics = node.topics();
    topics
        .create(create("/skewed", "application/octet-stream"))
        .await
        .unwrap();
    for (timestamp_ms, values) in [(100, &[&b"a"[..]][..]), (500, &[b"b", b"c"])] {
        produce(
            &topics,
            "/skewed",
            Bytes::from(timed_batch(timestamp_ms, values)),
        )
        .await
        .unwrap();
    }
    let log = log_of(&topics, "/skewed").await;
    let stream_id = node.service().stream_id(&log).unwrap().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while node.views().load().state.object_spans(stream_id).is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "no upload");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    produce(&topics, "/skewed", Bytes::from(timed_batch(300, &[b"d"])))
        .await
        .unwrap();
    let topic = topics.get("/skewed").await.unwrap().unwrap();

    let newest = topics
        .record_with_max_timestamp(&topic, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(newest.offset.record_offset(), 1);
    assert_eq!(newest.record.timestamp_ms, 500);

    node.close().await;
}

async fn uploading_node() -> PicoNode {
    let (sink, views) = LocalSink::new();
    let engine = s3stream::Config {
        wal_upload_interval_ms: 100,
        ..Default::default()
    };
    start_engine(1, Arc::new(sink), views, engine).await
}

async fn append_object(node: &PicoNode, name: &str, value: &[u8], objects: usize) -> u64 {
    let topics = node.topics();
    topics
        .append(name, 0, append(&[value], "text/plain"))
        .await
        .unwrap();
    let log = log_of(&topics, name).await;
    let stream_id = node.service().stream_id(&log).unwrap().unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while node.views().load().state.object_spans(stream_id).len() < objects {
        assert!(tokio::time::Instant::now() < deadline, "no upload");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stream_id
}

#[tokio::test]
async fn retention_trims_a_closed_partition_without_reopening_it() {
    let node = uploading_node().await;
    let retention = Retention {
        ms: Some(1_000),
        bytes: None,
    };
    node.topics()
        .create(CreateCommand {
            retention,
            ..create("/kept", "text/plain")
        })
        .await
        .unwrap();
    let stream_id = append_object(&node, "/kept", b"old", 1).await;
    node.service()
        .release_for_transfer(stream_id)
        .await
        .unwrap();
    let log = log_of(&node.topics(), "/kept").await;

    node.service()
        .apply_retention(&log, retention, picomq_common::now_ms() + 60_000)
        .await
        .unwrap();

    let row = node.views().load().state.streams()[&stream_id];
    assert_eq!(row.state, StreamState::Closed);
    assert_eq!(row.start_offset, row.end_offset);
    assert!(node.service().streams().held(stream_id).is_none());

    node.close().await;
}

#[tokio::test]
async fn retention_does_not_reopen_a_partition_released_while_it_waited() {
    let node = uploading_node().await;
    let retention = Retention {
        ms: Some(1_000),
        bytes: None,
    };
    node.topics()
        .create(CreateCommand {
            retention,
            ..create("/moving", "text/plain")
        })
        .await
        .unwrap();
    let stream_id = append_object(&node, "/moving", b"old", 1).await;
    let log = log_of(&node.topics(), "/moving").await;
    let service = node.service();

    let held = service.begin_append(&log).await.unwrap();
    let release = tokio::spawn({
        let service = service.clone();
        async move { service.release_for_transfer(stream_id).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let trim = tokio::spawn({
        let service = service.clone();
        let log = log.clone();
        async move {
            service
                .apply_retention(&log, retention, picomq_common::now_ms() + 60_000)
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(held);

    release.await.unwrap().unwrap();
    trim.await.unwrap().unwrap();

    let row = node.views().load().state.streams()[&stream_id];
    assert_eq!(row.state, StreamState::Closed);
    assert_eq!(row.start_offset, row.end_offset);
    assert!(service.streams().held(stream_id).is_none());

    node.close().await;
}

#[tokio::test]
async fn retention_with_nothing_expired_does_not_wait_on_appends() {
    let node = uploading_node().await;
    let retention = Retention {
        ms: Some(60_000),
        bytes: None,
    };
    node.topics()
        .create(CreateCommand {
            retention,
            ..create("/fresh", "text/plain")
        })
        .await
        .unwrap();
    append_object(&node, "/fresh", b"new", 1).await;
    let log = log_of(&node.topics(), "/fresh").await;
    let service = node.service();

    let held = service.begin_append(&log).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        service.apply_retention(&log, retention, picomq_common::now_ms()),
    )
    .await
    .expect("retention waited on the append gate")
    .unwrap();
    drop(held);

    node.close().await;
}

#[tokio::test]
async fn retention_bytes_keeps_the_newest_object_of_an_open_partition() {
    let node = uploading_node().await;
    node.topics()
        .create(create("/sized", "text/plain"))
        .await
        .unwrap();
    append_object(&node, "/sized", b"first", 1).await;
    let stream_id = append_object(&node, "/sized", b"second", 2).await;
    let spans = node.views().load().state.object_spans(stream_id);
    let retention = Retention {
        ms: None,
        bytes: Some(spans[1].bytes),
    };
    let log = log_of(&node.topics(), "/sized").await;

    node.service()
        .apply_retention(&log, retention, picomq_common::now_ms())
        .await
        .unwrap();

    let row = node.views().load().state.streams()[&stream_id];
    assert_eq!(row.state, StreamState::Opened);
    assert_eq!(row.start_offset, spans[0].end_offset);

    node.close().await;
}

async fn grow(
    topics: &Topics,
    name: &str,
    partitions: u32,
    owners: &[i32],
) -> Result<Topic, ServiceError> {
    topics
        .update(picomq_server::UpdateTopicCommand {
            name: name.to_owned(),
            partitions: Some(partitions),
            owners: owners.to_vec(),
            ..Default::default()
        })
        .await
}

async fn move_log(from: &PicoNode, views: &ViewPublisher, log: &Log, to: i32) {
    let stream_id = from.transfer(log, to).await.unwrap();
    wait_for_view(views, "transfer completion", |view| {
        !view.state.pending_transfers().contains_key(&stream_id)
            && view
                .state
                .streams()
                .get(&stream_id)
                .is_some_and(|row| row.node_id == to)
    })
    .await;
}

#[tokio::test]
async fn a_named_producer_retry_after_a_round_trip_transfer_is_deduplicated() {
    use picomq_server::types::Producer;

    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node1 = start_shared_node(
        1,
        1,
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;
    let node2 = start_shared_node(2, 1, sink, views.clone(), object_storage, wal_storage).await;
    let send = |payload: &'static [u8], seq: u64| AppendCommand {
        producer: Some(Producer::new("p", 1, seq).unwrap()),
        ..append(&[payload], "text/plain")
    };

    node1
        .topics()
        .create(create("/xfer/dedup", "text/plain"))
        .await
        .unwrap();
    let log = log_of(&node1.topics(), "/xfer/dedup").await;
    node1
        .topics()
        .append("/xfer/dedup", 0, send(b"zero", 0))
        .await
        .unwrap();

    move_log(&node1, &views, &log, 2).await;
    let moved = node2
        .topics()
        .append("/xfer/dedup", 0, send(b"one", 1))
        .await
        .unwrap();
    assert!(moved.applied);
    move_log(&node2, &views, &log, 1).await;

    let retried = node1
        .topics()
        .append("/xfer/dedup", 0, send(b"one", 1))
        .await
        .unwrap();
    assert!(
        !retried.applied,
        "the retry is deduplicated by the new owner"
    );
    assert_eq!(retried.producer_seq, Some(1));
    let next = node1
        .topics()
        .append("/xfer/dedup", 0, send(b"two", 2))
        .await
        .unwrap();
    assert!(next.applied);
    assert_eq!(
        values(&node1.topics(), "/xfer/dedup").await,
        ["zero", "one", "two"]
    );

    node1.close().await;
    node2.close().await;
}

#[tokio::test]
async fn kafka_producer_state_is_replayed_after_a_restart_even_when_the_first_append_has_no_producer()
 {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node = start_shared_node(
        1,
        1,
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;
    let batch = |seq: i32, count: usize| {
        let values: Vec<Vec<u8>> = (0..count).map(|i| vec![i as u8]).collect();
        let values: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
        kafka_batch(Some((42, 0, seq)), &values)
    };

    node.topics()
        .create(create("/topics/replayed-late", "application/octet-stream"))
        .await
        .unwrap();
    produce(&node.topics(), "/topics/replayed-late", batch(0, 3))
        .await
        .unwrap();
    produce(&node.topics(), "/topics/replayed-late", batch(3, 2))
        .await
        .unwrap();
    node.close().await;

    let node = start_shared_node(1, 2, sink, views, object_storage, wal_storage).await;
    let plain = produce(
        &node.topics(),
        "/topics/replayed-late",
        kafka_batch(None, &[b"x"]),
    )
    .await
    .unwrap();
    assert_eq!(plain.base_offset, 5);

    let retry = produce(&node.topics(), "/topics/replayed-late", batch(3, 2))
        .await
        .unwrap();
    assert!(retry.duplicate, "the retry is deduplicated after replay");
    assert_eq!(retry.base_offset, 3);
    let next = produce(&node.topics(), "/topics/replayed-late", batch(5, 1))
        .await
        .unwrap();
    assert!(!next.duplicate);
    assert_eq!(next.base_offset, 6);

    node.close().await;
}

/// A metadata sink that refuses or holds writes under a key prefix.
struct KvFaults {
    inner: LocalSink,
    refuse: std::sync::Mutex<Option<&'static str>>,
    hold: std::sync::Mutex<Option<&'static str>>,
    release: tokio::sync::Notify,
}

impl KvFaults {
    fn new() -> (Arc<Self>, Arc<ViewPublisher>) {
        let (inner, views) = LocalSink::new();
        let sink = Self {
            inner,
            refuse: std::sync::Mutex::new(None),
            hold: std::sync::Mutex::new(None),
            release: tokio::sync::Notify::new(),
        };
        (Arc::new(sink), views)
    }

    fn refuse(&self, prefix: Option<&'static str>) {
        *self.refuse.lock().unwrap() = prefix;
    }

    fn hold_next(&self, prefix: &'static str) {
        *self.hold.lock().unwrap() = Some(prefix);
    }
}

fn kv_key(command: &MetadataCommand) -> Option<&str> {
    match command {
        MetadataCommand::PutKv { key, .. }
        | MetadataCommand::PutKvIfAbsent { key, .. }
        | MetadataCommand::PutKvIfMatches { key, .. } => Some(key),
        _ => None,
    }
}

#[async_trait::async_trait]
impl CommandSink for KvFaults {
    async fn propose(&self, command: MetadataCommand) -> Result<Proposed, MetadataError> {
        if let Some(key) = kv_key(&command) {
            let refused = self
                .refuse
                .lock()
                .unwrap()
                .is_some_and(|prefix| key.starts_with(prefix));
            if refused {
                return Err(MetadataError::Unavailable {
                    message: "write refused".into(),
                });
            }
            let held = {
                let mut hold = self.hold.lock().unwrap();
                match *hold {
                    Some(prefix) if key.starts_with(prefix) => hold.take().is_some(),
                    _ => false,
                }
            };
            if held {
                self.release.notified().await;
            }
        }
        self.inner.propose(command).await
    }

    async fn sync(&self) -> Result<(), MetadataError> {
        self.inner.sync().await
    }
}

#[tokio::test]
async fn http_producers_are_acknowledged_while_checkpoints_cannot_be_written() {
    use picomq_server::types::Producer;

    let (sink, views) = KvFaults::new();
    let node = start_node(1, sink.clone() as Arc<dyn CommandSink>, views).await;
    let topics = node.topics();
    topics
        .create(create("/unsaved", "text/plain"))
        .await
        .unwrap();
    let send = |payloads: Vec<&'static [u8]>, seq: u64| AppendCommand {
        producer: Some(Producer::new("p", 0, seq).unwrap()),
        ..append(&payloads, "text/plain")
    };
    topics
        .append("/unsaved", 0, send(vec![b"zero"], 0))
        .await
        .unwrap();

    sink.refuse(Some("producers/"));
    let many: Vec<&'static [u8]> =
        vec![b"x"; picomq_server::producers::CHECKPOINT_INTERVAL as usize];
    let crossed = topics
        .append("/unsaved", 0, send(many.clone(), 1))
        .await
        .unwrap();
    assert!(
        crossed.applied,
        "a refused checkpoint never fails an append"
    );
    let retried = topics.append("/unsaved", 0, send(many, 1)).await.unwrap();
    assert!(!retried.applied);
    assert!(
        topics
            .append("/unsaved", 0, send(vec![b"two"], 2))
            .await
            .unwrap()
            .applied
    );
    assert_eq!(
        values(&topics, "/unsaved").await.len() as u64,
        2 + picomq_server::producers::CHECKPOINT_INTERVAL
    );

    node.close().await;
}

#[tokio::test]
async fn http_producer_state_is_rebuilt_from_the_log_after_a_restart() {
    use picomq_server::types::Producer;

    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node = start_shared_node(
        1,
        1,
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;
    let send = |payload: &'static [u8], seq: u64| AppendCommand {
        producer: Some(Producer::new("p", 2, seq).unwrap()),
        ..append(&[payload], "text/plain")
    };

    node.topics()
        .create(create("/rebuilt", "text/plain"))
        .await
        .unwrap();
    let payloads: [&'static [u8]; 3] = [b"zero", b"one", b"two"];
    for (seq, payload) in payloads.into_iter().enumerate() {
        node.topics()
            .append("/rebuilt", 0, send(payload, seq as u64))
            .await
            .unwrap();
    }
    node.close().await;

    let node = start_shared_node(1, 2, sink, views, object_storage, wal_storage).await;
    let retried = node
        .topics()
        .append("/rebuilt", 0, send(b"two", 2))
        .await
        .unwrap();
    assert!(
        !retried.applied,
        "the retry is deduplicated from the log alone"
    );
    assert_eq!(retried.producer_seq, Some(2));
    let stale = node
        .topics()
        .append("/rebuilt", 0, send(b"nope", 1))
        .await
        .unwrap();
    assert!(!stale.applied);
    assert!(
        node.topics()
            .append("/rebuilt", 0, send(b"three", 3))
            .await
            .unwrap()
            .applied
    );
    assert_eq!(
        values(&node.topics(), "/rebuilt").await,
        ["zero", "one", "two", "three"]
    );

    node.close().await;
}

#[tokio::test]
async fn a_new_session_is_refused_while_its_row_cannot_be_written() {
    use picomq_server::types::Producer;

    let (sink, views) = KvFaults::new();
    let node = start_node(1, sink.clone() as Arc<dyn CommandSink>, views).await;
    let topics = node.topics();
    topics
        .create(create("/sessions", "text/plain"))
        .await
        .unwrap();
    let send = |id: &'static str, seq: u64| AppendCommand {
        producer: Some(Producer::new(id, 0, seq).unwrap()),
        ..append(&[b"x"], "text/plain")
    };
    topics.append("/sessions", 0, send("p", 0)).await.unwrap();

    sink.refuse(Some("producer/"));
    assert!(
        topics
            .append("/sessions", 0, send("p", 1))
            .await
            .unwrap()
            .applied
    );
    let refused = topics
        .append("/sessions", 0, send("q", 0))
        .await
        .unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Unavailable);
    assert_eq!(values(&topics, "/sessions").await.len(), 2);

    sink.refuse(None);
    assert!(
        topics
            .append("/sessions", 0, send("q", 0))
            .await
            .unwrap()
            .applied
    );

    node.close().await;
}

#[tokio::test]
async fn a_former_owners_late_stream_seq_is_rejected() {
    let (sink, views) = KvFaults::new();
    let shared: Arc<dyn CommandSink> = sink.clone();
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node1 = start_shared_node(
        1,
        1,
        shared.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await;
    let node2 = start_shared_node(2, 1, shared, views.clone(), object_storage, wal_storage).await;
    let send = |payload: &'static [u8], seq: &str| AppendCommand {
        stream_seq: Some(seq.to_owned()),
        ..append(&[payload], "text/plain")
    };

    node1
        .topics()
        .create(create("/taken", "text/plain"))
        .await
        .unwrap();
    let log = log_of(&node1.topics(), "/taken").await;
    node1
        .topics()
        .append("/taken", 0, send(b"a", "a"))
        .await
        .unwrap();

    sink.hold_next("stream_seq/");
    let stalled = tokio::spawn({
        let topics = node1.topics();
        async move { topics.append("/taken", 0, send(b"b", "b")).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stream_id = node1.service().stream_id(&log).unwrap().unwrap();
    node1.service().streams().release(stream_id).await.unwrap();

    let taken = node2
        .topics()
        .append("/taken", 0, send(b"c", "c"))
        .await
        .unwrap();
    assert!(taken.applied);
    sink.release.notify_one();
    let late = stalled.await.unwrap().unwrap_err();
    assert_eq!(late.kind, ErrorKind::Transferring);

    let regressed = node2
        .topics()
        .append("/taken", 0, send(b"b", "b"))
        .await
        .unwrap_err();
    assert_eq!(regressed.kind, ErrorKind::Conflict);
    assert_eq!(values(&node2.topics(), "/taken").await, ["a", "c"]);

    node1.close().await;
    node2.close().await;
}
