use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use picomq_metadata::sink::Proposed;
use picomq_metadata::{
    CommandSink, LocalSink, MetadataCommand, MetadataError, MetadataState, ViewPublisher, apply,
};
use picomq_server::ownership::OwnershipService as _;
use picomq_server::record::encode_batch;
use picomq_server::{
    AppendBatchResult, AppendCommand, CreateCommand, ErrorKind, Log, LogRecord, MAX_NEW_PARTITIONS,
    NodeConfig, OffsetToken, PicoNode, ServiceError, Topic, Topics, partition_log,
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
    PicoNode::start(
        NodeConfig {
            node_id,
            node_epoch: 1,
            http_address: format!("http://127.0.0.1:{}", 4000 + node_id),
            heartbeat_interval: Duration::from_millis(100),
            engine,
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
        None,
    )
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
    assert_eq!(&batch.records[0].record.value[..], b"hello");
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
    let values: Vec<&[u8]> = all.records.iter().map(|r| &r.record.value[..]).collect();
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
    assert_eq!(&tail.records[0].record.value[..], b"bb");

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
    let offsets = topics.offsets(&topic, 0).await.unwrap();
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
    assert_eq!(&trimmed.records[0].record.value[..], b"ccc");

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
            .filter(|r| r.record.value[0] == producer)
            .map(|r| r.record.value[1])
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

    let names = |list: &picomq_server::TopicList| -> Vec<String> {
        list.topics
            .iter()
            .map(|m| m.topic.name.spelling().to_owned())
            .collect()
    };

    let all = topics.list("/list/", None, 0).unwrap();
    assert_eq!(names(&all), ["/list/a", "/list/b", "/list/c"]);
    assert!(!all.has_more);

    let page = topics.list("/list/", None, 2).unwrap();
    assert_eq!(names(&page), ["/list/a", "/list/b"]);
    assert!(page.has_more);

    let rest = topics.list("/list/", Some("/list/b"), 0).unwrap();
    assert_eq!(names(&rest), ["/list/c"]);
    assert!(!rest.has_more);

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
    assert_eq!(&read.records[0].record.value[..], br#"{"a":1}"#);
    assert_eq!(&read.records[1].record.value[..], br#"{"b":2}"#);

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
    PicoNode::start(
        NodeConfig {
            node_id,
            node_epoch,
            http_address: format!("http://127.0.0.1:{}", 4000 + node_id),
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
        None,
    )
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
        !view.state.pending_transfers.contains_key(&stream_id)
            && view
                .state
                .streams
                .get(&stream_id)
                .is_some_and(|row| row.node_id == 2)
    })
    .await;
    wait_for_view(&views, "target pre-warm open", |view| {
        view.state
            .streams
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
    assert_eq!(&all.records[0].record.value[..], b"before");
    assert_eq!(&all.records.last().unwrap().record.value[..], b"after");

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
    let stream_id = views.load().state.next_stream_id - 1;
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
            .pending_transfers
            .contains_key(&stream_id)
    );

    let node1 = start_shared_node(1, 2, sink, views.clone(), object_storage, wal_storage).await;
    wait_for_view(&views, "stale transfer completion", |view| {
        !view.state.pending_transfers.contains_key(&stream_id)
            && view
                .state
                .streams
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
    let offsets = node.topics().offsets(&topic, 0).await.unwrap();
    assert_eq!(offsets.next_offset.record_offset(), 1);
    let read = node
        .topics()
        .read("/durable/a", 0, OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert_eq!(read.records.len(), 1);
    assert_eq!(&read.records[0].record.value[..], b"kept");

    node.close().await;
}

fn kafka_batch(producer: Option<(i64, i16, i32)>, values: &[&[u8]]) -> Bytes {
    let records: Vec<LogRecord> = values
        .iter()
        .map(|v| LogRecord::value(Bytes::copy_from_slice(v)))
        .collect();
    let mut batch = encode_batch(0, -1, 1, &records).to_vec();
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
        .map(|r| &r.record.value[..])
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
    let values: Vec<&[u8]> = all.records.iter().map(|r| &r.record.value[..]).collect();
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
async fn list_paginates_with_start_after() {
    let node = local_node().await;
    let topics = node.topics();
    for i in 0..5 {
        topics
            .create(create(&format!("/page/{i}"), "text/plain"))
            .await
            .unwrap();
    }

    let names = |list: picomq_server::TopicList| -> (Vec<String>, bool) {
        let names = list
            .topics
            .iter()
            .map(|m| m.topic.name.spelling().to_owned())
            .collect();
        (names, list.has_more)
    };

    let (first, more) = names(topics.list("/page/", None, 2).unwrap());
    assert_eq!(first, ["/page/0", "/page/1"]);
    assert!(more);

    let (second, more) = names(topics.list("/page/", Some("/page/1"), 2).unwrap());
    assert_eq!(second, ["/page/2", "/page/3"]);
    assert!(more);

    let (last, more) = names(topics.list("/page/", Some("/page/3"), 2).unwrap());
    assert_eq!(last, ["/page/4"]);
    assert!(!more);

    node.close().await;
}

#[tokio::test]
async fn append_rejects_schema_invalid_records() {
    use object_store::memory::InMemory;
    use picomq_schema::{Registry, SchemaFormat};

    let schema = Bytes::from_static(
        br#"{
        "title": "Person",
        "type": "object",
        "properties": {
            "value": {
                "type": "object",
                "properties": {
                    "name": { "type": "string" }
                },
                "required": ["name"]
            }
        }
    }"#,
    );

    let (sink, views) = LocalSink::new();
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(2));
    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 1,
            http_address: "http://127.0.0.1:4010".into(),
            ..Default::default()
        },
        Arc::new(sink),
        views,
        object_storage,
        wal_storage,
        Some(Arc::new(Registry::new(InMemory::new()))),
    )
    .await
    .unwrap();
    let topics = node.topics();

    let registry = topics.schema_registry().unwrap();
    registry
        .put("person", SchemaFormat::Json, schema)
        .await
        .unwrap();
    assert_eq!(
        registry.get("person").await.unwrap().unwrap().0,
        SchemaFormat::Json
    );

    let mut cmd = create("/streams/orders", "application/json");
    cmd.schema_name = Some("person".into());
    cmd.schema_validate = true;
    topics.create(cmd).await.unwrap();

    let ok = topics
        .append(
            "/streams/orders",
            0,
            append(&[br#"{"name":"alice"}"#], "application/json"),
        )
        .await
        .unwrap();
    assert!(ok.applied);

    let err = topics
        .append(
            "/streams/orders",
            0,
            append(&[br#"{"name":1}"#], "application/json"),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SchemaViolation);

    topics
        .create(create("/streams/raw", "application/json"))
        .await
        .unwrap();
    topics
        .append(
            "/streams/raw",
            0,
            append(&[br#"{"name":1}"#], "application/json"),
        )
        .await
        .unwrap();

    topics
        .update(picomq_server::UpdateTopicCommand {
            name: "/streams/orders".into(),
            schema_validate: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    topics
        .append(
            "/streams/orders",
            0,
            append(&[br#"{"name":1}"#], "application/json"),
        )
        .await
        .unwrap();

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
    assert_eq!(&second.records[0].record.value[..], b"one");

    let grown = topics.grow("/parts", 4, &[]).await.unwrap();
    assert_eq!(grown.partitions, 4);
    topics
        .append("/parts", 3, append(&[b"three"], "text/plain"))
        .await
        .unwrap();
    assert_eq!(
        topics.grow("/parts", 3, &[]).await.unwrap_err().kind,
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

    let grown = topics.grow("/sealed", 2, &[]).await.unwrap_err();
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
        .grow("/small", MAX_NEW_PARTITIONS + 2, &[])
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
            let row = view.state.streams[stream_id];
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

    let grown = node2.topics().grow("/spread", 6, &[]).await.unwrap();
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
        if views.load().state.streams[&stream_id].node_id != 2 {
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
            .all(|id| views.load().state.streams[id].state == StreamState::Opened)
    );

    assert!(node1.topics().delete("/doomed").await.unwrap());
    wait_for_view(&views, "held streams deleted", |view| {
        held.iter().all(|id| !view.state.streams.contains_key(id))
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
            .all(|stream_id| view.state.streams[stream_id].node_id == 1)
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

    let heartbeats = views.load().state.nodes[&2].heartbeats;
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
            .offsets(&topic, 0)
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
    let row = views.load().state.streams[&stream_id];
    assert_eq!(row.state, StreamState::Closed);
    assert_eq!(row.node_id, 1);

    assert!(topics.delete("/never-opened").await.unwrap());
    wait_for_view(&views, "the placed stream to be destroyed", |view| {
        !view.state.streams.contains_key(&stream_id)
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
                state.streams[&stream_id].node_id
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

    let grown = topics.grow("/assigned", 6, &[1, 2]).await.unwrap();
    assert_eq!(owners_of(&grown), [2, 1, 2, 1, 1, 2]);

    let missing = topics.grow("/assigned", 8, &[1]).await.unwrap_err();
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
    assert!(node.groups().join(join()).await.result.is_ok());
    node.close().await;

    let restarted = start_shared_node(1, 2, sink, views, object_storage, wal_storage).await;
    assert!(restarted.groups().join(join()).await.result.is_ok());
    let described = restarted.groups().describe("g").await.unwrap();
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
                    state.streams[stream_id].node_id, -1,
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
    assert!(views2.load().state.streams.keys().eq([&winner]));

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
    assert!(state.streams.is_empty());
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
    let row = views.load().state.streams[&stream_id];
    assert_eq!((row.state, row.epoch), (StreamState::Opened, 2));

    node.close().await;
}
