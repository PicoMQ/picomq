//! Service-level end-to-end: a `PicoNode` on the metadata plane with
//! memory object storage.
//!
//! Covers create/append/read/close via services, with the atomic-batch and
//! trim tail. A third test repeats the core flow over the SQL-backed
//! sink (SQLite), and further tests cover long-poll waiters, producer
//! idempotency over the wire, delete, and ownership routing. Service-level
//! layer lands in a later phase.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pico_metadata::{CommandSink, LocalSink, ViewPublisher};
use pico_server::ownership::OwnershipService as _;
use pico_server::{AppendCommand, CreateCommand, ErrorKind, NodeConfig, OffsetToken, PicoNode};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};

async fn start_node(
    node_id: i32,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
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

async fn local_node() -> PicoNode {
    let (sink, views) = LocalSink::new();
    start_node(1, Arc::new(sink), views).await
}

fn create(name: &str, content_type: &str) -> CreateCommand {
    CreateCommand {
        name: name.into(),
        content_type: content_type.into(),
        ttl_seconds: None,
        expires_at_ms: None,
        closed: false,
        initial_payload: Bytes::new(),
        external_id: None,
        internal: false,
    }
}

fn append(name: &str, payloads: &[&[u8]], content_type: &str) -> AppendCommand {
    AppendCommand {
        name: name.into(),
        payloads: payloads.iter().map(|p| Bytes::copy_from_slice(p)).collect(),
        content_type: Some(content_type.into()),
        ..Default::default()
    }
}

/// (+ its `atomicBatchAppendReadAndTrim` tail).
#[tokio::test]
async fn create_append_read_close_via_services() {
    let node = local_node().await;
    let services = node.service();

    let created = services
        .create(create("/streams/demo", "text/plain"))
        .await
        .unwrap();
    assert!(created.created);

    let appended = services
        .append(append("/streams/demo", &[b"hello"], "text/plain"))
        .await
        .unwrap();
    assert!(appended.applied);
    assert!(!appended.closed);

    let batch = services
        .read("/streams/demo", OffsetToken::beginning(), 1024, 0)
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(&batch.records[0].payload[..], b"hello");
    assert!(batch.up_to_date);

    assert!(
        services
            .close("/streams/demo")
            .await
            .unwrap()
            .next_offset
            .record_offset()
            >= 1
    );
    assert!(
        services
            .head("/streams/demo")
            .await
            .unwrap()
            .unwrap()
            .closed
    );

    services
        .create(create("/streams/batch", "application/octet-stream"))
        .await
        .unwrap();
    let appended = services
        .append(AppendCommand {
            atomic: true,
            ..append(
                "/streams/batch",
                &[b"a", b"bb", b"ccc"],
                "application/octet-stream",
            )
        })
        .await
        .unwrap();
    assert!(appended.applied);
    assert_eq!(appended.next_offset.record_offset(), 3);

    let all = services
        .read("/streams/batch", OffsetToken::beginning(), 1024, 0)
        .await
        .unwrap();
    assert_eq!(all.records.len(), 3);
    assert_eq!(&all.records[0].payload[..], b"a");
    assert_eq!(&all.records[1].payload[..], b"bb");
    assert_eq!(&all.records[2].payload[..], b"ccc");
    assert_eq!(all.records[0].offset.record_offset(), 0);
    assert_eq!(all.records[1].offset.record_offset(), 1);
    assert_eq!(all.records[2].offset.record_offset(), 2);
    assert!(all.up_to_date);

    let tail = services
        .read("/streams/batch", OffsetToken::of_record_offset(1), 1024, 0)
        .await
        .unwrap();
    assert_eq!(tail.records.len(), 2);
    assert_eq!(&tail.records[0].payload[..], b"bb");

    let limited = services
        .read("/streams/batch", OffsetToken::beginning(), 1024, 2)
        .await
        .unwrap();
    assert_eq!(limited.records.len(), 2);
    assert_eq!(limited.next_offset.record_offset(), 2);
    assert!(!limited.up_to_date);

    let effective = services.trim("/streams/batch", 2).await.unwrap();
    assert!(effective <= 2, "effective trim {effective}");
    assert_eq!(
        services
            .head("/streams/batch")
            .await
            .unwrap()
            .unwrap()
            .start_offset
            .record_offset(),
        effective
    );

    let trimmed = services
        .read("/streams/batch", OffsetToken::of_record_offset(2), 1024, 0)
        .await
        .unwrap();
    assert_eq!(trimmed.records.len(), 1);
    assert_eq!(&trimmed.records[0].payload[..], b"ccc");

    node.close().await;
}

#[tokio::test]
async fn concurrent_producers_on_one_stream_pipeline() {
    let node = local_node().await;
    let services = node.service();
    services
        .create(create("/streams/hot", "application/octet-stream"))
        .await
        .unwrap();

    let mut tasks = tokio::task::JoinSet::new();
    for producer in 0..8u8 {
        let services = Arc::clone(&services);
        tasks.spawn(async move {
            for record in 0..16u8 {
                services
                    .append(append(
                        "/streams/hot",
                        &[&[producer, record]],
                        "application/octet-stream",
                    ))
                    .await
                    .unwrap();
            }
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.unwrap();
    }

    let all = services
        .read("/streams/hot", OffsetToken::beginning(), 1 << 20, 0)
        .await
        .unwrap();
    assert_eq!(all.records.len(), 128);
    assert!(all.up_to_date);
    for (i, record) in all.records.iter().enumerate() {
        assert_eq!(record.offset.record_offset(), i as u64, "offsets dense");
    }
    // Per-producer record order is preserved even though producers interleave.
    for producer in 0..8u8 {
        let seen: Vec<u8> = all
            .records
            .iter()
            .filter(|r| r.payload[0] == producer)
            .map(|r| r.payload[1])
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
async fn list_and_match_seq_via_services() {
    let node = local_node().await;
    let services = node.service();

    for name in ["/list/a", "/list/b", "/list/c", "/other/x"] {
        services.create(create(name, "text/plain")).await.unwrap();
    }

    let all = services.list("/list/", None, 0).await.unwrap();
    let names: Vec<&str> = all.streams.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["/list/a", "/list/b", "/list/c"]);
    assert!(!all.has_more);

    let page = services.list("/list/", None, 2).await.unwrap();
    let names: Vec<&str> = page.streams.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["/list/a", "/list/b"]);
    assert!(page.has_more);

    let rest = services.list("/list/", Some("/list/b"), 0).await.unwrap();
    let names: Vec<&str> = rest.streams.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["/list/c"]);
    assert!(!rest.has_more);

    let at = |payload: &[u8], match_seq: u64| AppendCommand {
        match_seq: Some(match_seq),
        ..append("/list/a", &[payload], "text/plain")
    };
    let first = services.append(at(b"one", 0)).await.unwrap();
    assert!(first.applied);
    assert_eq!(first.next_offset.record_offset(), 1);

    let conflict = services.append(at(b"stale", 0)).await.unwrap_err();
    assert_eq!(conflict.kind, ErrorKind::MatchFailed);
    assert_eq!(conflict.next_offset.unwrap().record_offset(), 1);

    assert!(services.append(at(b"two", 1)).await.unwrap().applied);

    node.close().await;
}

/// The core flow on the SQL-backed sink: same service, durable log underneath.
#[tokio::test]
async fn core_flow_on_sql_sink() {
    use pico_sql::{MetaStore, SqlSink, SqlSinkConfig, SqliteStore};

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
    let services = node.service();

    services
        .create(create("/sql/demo", "application/json"))
        .await
        .unwrap();
    // JSON array body splits into one record per element.
    let appended = services
        .append(append(
            "/sql/demo",
            &[br#"[{"a":1},{"b":2}]"#],
            "application/json",
        ))
        .await
        .unwrap();
    assert_eq!(appended.next_offset.record_offset(), 2);

    let read = services
        .read("/sql/demo", OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert_eq!(read.records.len(), 2);
    assert_eq!(&read.records[0].payload[..], br#"{"a":1}"#);
    assert_eq!(&read.records[1].payload[..], br#"{"b":2}"#);

    assert!(services.delete("/sql/demo").await.unwrap());
    assert!(services.head("/sql/demo").await.unwrap().is_none());
    assert!(!services.delete("/sql/demo").await.unwrap());

    node.close().await;
}

/// Long-poll: a parked reader wakes when an append passes its offset, and
/// `wait_appended` short-circuits on already-readable data and closed streams.
#[tokio::test]
async fn wait_appended_long_poll() {
    let node = local_node().await;
    let services = node.service();

    services
        .create(create("/poll/a", "text/plain"))
        .await
        .unwrap();

    let waiter = services.clone();
    let parked = tokio::spawn(async move {
        waiter
            .wait_appended("/poll/a", OffsetToken::beginning(), Duration::from_secs(10))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!parked.is_finished());

    services
        .append(append("/poll/a", &[b"x"], "text/plain"))
        .await
        .unwrap();
    assert!(parked.await.unwrap().unwrap());

    // Already readable: immediate true.
    assert!(services
        .wait_appended(
            "/poll/a",
            OffsetToken::beginning(),
            Duration::from_millis(1)
        )
        .await
        .unwrap());
    // Unknown stream: false.
    assert!(!services
        .wait_appended(
            "/poll/none",
            OffsetToken::beginning(),
            Duration::from_millis(1)
        )
        .await
        .unwrap());
    // Closed stream: true.
    services.close("/poll/a").await.unwrap();
    assert!(services
        .wait_appended(
            "/poll/a",
            OffsetToken::of_record_offset(99),
            Duration::from_millis(1)
        )
        .await
        .unwrap());

    node.close().await;
}

/// Producer idempotency over the service: duplicate suppressed, stale epoch
/// fenced, gap rejected. And the closed-stream replay path.
#[tokio::test]
async fn producer_idempotency_and_closed_replay() {
    use pico_server::types::Producer;

    let node = local_node().await;
    let services = node.service();
    services
        .create(create("/prod/a", "text/plain"))
        .await
        .unwrap();

    let with_producer = |payload: &[u8], epoch: u64, seq: u64| AppendCommand {
        producer: Some(Producer::new("p1", epoch, seq).unwrap()),
        ..append("/prod/a", &[payload], "text/plain")
    };

    assert!(
        services
            .append(with_producer(b"one", 1, 0))
            .await
            .unwrap()
            .applied
    );
    // Duplicate (same seq): not applied, last seq echoed.
    let dup = services.append(with_producer(b"one", 1, 0)).await.unwrap();
    assert!(!dup.applied);
    assert_eq!(dup.producer_seq, Some(0));
    // Gap.
    let gap = services
        .append(with_producer(b"three", 1, 2))
        .await
        .unwrap_err();
    assert_eq!(gap.kind, ErrorKind::SequenceGap);
    assert_eq!((gap.expected_seq, gap.received_seq), (Some(1), Some(2)));
    // Stale epoch after a bump.
    assert!(
        services
            .append(with_producer(b"two", 2, 0))
            .await
            .unwrap()
            .applied
    );
    let stale = services
        .append(with_producer(b"nope", 1, 1))
        .await
        .unwrap_err();
    assert_eq!(stale.kind, ErrorKind::Fenced);
    assert_eq!(stale.producer_epoch, Some(2));

    // Producer-attributed close, then idempotent close replay.
    let close = AppendCommand {
        close_after: true,
        producer: Some(Producer::new("p1", 2, 1).unwrap()),
        ..append("/prod/a", &[b"last"], "text/plain")
    };
    assert!(services.append(close.clone()).await.unwrap().closed);
    let replay = services.append(close).await.unwrap();
    assert!(replay.closed);
    assert!(!replay.applied);
    // A different producer appending to the closed stream is rejected.
    let other = AppendCommand {
        producer: Some(Producer::new("p2", 0, 0).unwrap()),
        ..append("/prod/a", &[b"x"], "text/plain")
    };
    let err = services.append(other).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::Closed);
    assert!(err.closed);

    node.close().await;
}

/// Ownership routing: a stream opened by another registered node resolves to
/// a remote owner with that node's advertised address. Unknown names and
/// closed streams stay local. Driven end-to-end through two nodes sharing
/// one metadata plane.
#[tokio::test]
async fn ownership_routes_to_open_owner() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let node1 = start_node(1, sink.clone(), views.clone()).await;
    let node2 = start_node(2, sink.clone(), views.clone()).await;

    // Unknown name: local (create may land here).
    let owner = node1.ownership().owner_of("/own/a").await.unwrap();
    assert!(owner.local);
    assert_eq!(owner.stream_id, None);

    node1
        .service()
        .create(create("/own/a", "text/plain"))
        .await
        .unwrap();
    assert!(node1.ownership().owner_of("/own/a").await.unwrap().local);
    let from_node2 = node2.ownership().owner_of("/own/a").await.unwrap();
    assert!(!from_node2.local);
    assert_eq!(from_node2.owner_node_id, Some(1));
    assert_eq!(
        from_node2.owner_advertised_address.as_deref(),
        Some("http://127.0.0.1:4001")
    );

    // Closing releases ownership: closed (not OPENED) streams are local anywhere.
    node1.close().await;
    let released = node2.ownership().owner_of("/own/a").await.unwrap();
    assert!(released.local);
    assert!(released.stream_id.is_some());

    node2.close().await;
}

/// Nodes sharing one object and WAL bucket, as a cluster shares S3.
/// The WAL prefixes by cluster/node/epoch, so one bucket is safe.
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
    )
    .await
    .unwrap()
}

async fn wait_for_view(
    views: &ViewPublisher,
    what: &str,
    satisfied: impl Fn(&pico_metadata::MetadataView) -> bool,
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

/// A live transfer: the source drains and closes, the completion re-points
/// the stream, the target pre-warms it, and appends continue on the target
/// with full data continuity. Appends racing the transfer either land
/// durably (and survive the move) or fail with a transfer conflict.
#[tokio::test]
async fn transfer_moves_stream_to_target_node() {
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
        .service()
        .create(create("/xfer/a", "text/plain"))
        .await
        .unwrap();
    node1
        .service()
        .append(append("/xfer/a", &[b"before"], "text/plain"))
        .await
        .unwrap();

    // Appends race the transfer proposal. Every Ok append is durable and must
    // survive the move. Acceptable failures are the transfer conflict while
    // the move is pending and the open refusal once the target holds the
    // stream. Both surface before any data is written.
    let racer = {
        let services = node1.service();
        tokio::spawn(async move {
            let mut applied = 0u64;
            for i in 0..20u8 {
                match services
                    .append(append("/xfer/a", &[&[i]], "text/plain"))
                    .await
                {
                    Ok(_) => applied += 1,
                    Err(e) => {
                        assert!(
                            matches!(e.kind, ErrorKind::Conflict | ErrorKind::BadRequest),
                            "unexpected failure {e:?}"
                        );
                        break;
                    }
                }
            }
            applied
        })
    };
    let stream_id = node1.transfer_stream("/xfer/a", 2).await.unwrap();
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
    // Pre-warm: the target opens the stream without any client request.
    wait_for_view(&views, "target pre-warm open", |view| {
        view.state
            .streams
            .get(&stream_id)
            .is_some_and(|row| row.state == s3stream::StreamState::Opened && row.node_id == 2)
    })
    .await;

    // Every node now routes the stream to node 2.
    let owner = node1.ownership().owner_of("/xfer/a").await.unwrap();
    assert!(!owner.local);
    assert_eq!(owner.owner_node_id, Some(2));
    assert!(node2.ownership().owner_of("/xfer/a").await.unwrap().local);

    // Continuity: the target serves the full history plus new appends.
    let appended = node2
        .service()
        .append(append("/xfer/a", &[b"after"], "text/plain"))
        .await
        .unwrap();
    assert!(appended.applied);
    let all = node2
        .service()
        .read("/xfer/a", OffsetToken::beginning(), 1 << 20, 0)
        .await
        .unwrap();
    assert_eq!(all.records.len() as u64, 2 + raced);
    assert_eq!(&all.records[0].payload[..], b"before");
    assert_eq!(&all.records.last().unwrap().payload[..], b"after");

    node1.close().await;
    node2.close().await;
}

/// Crash window: the source dies after the transfer was requested but before
/// it completed. When the source comes back (bumped epoch), its watcher seals
/// the stream at its last epoch and finishes the transfer.
#[tokio::test]
async fn stale_transfer_completes_when_source_restarts() {
    use pico_metadata::MetadataCommand;

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

    // A crashed node 1 left behind: registered, one stream opened, a transfer
    // requested but never completed. Raw proposals stand in for the process
    // that no longer exists.
    sink.propose(MetadataCommand::RegisterNode {
        node_id: 1,
        node_epoch: 1,
        http_address: "http://127.0.0.1:4001".into(),
        protocol_addresses: Default::default(),
        slots: 1,
    })
    .await
    .unwrap();
    sink.propose(MetadataCommand::CreateStream {
        node_id: 1,
        node_epoch: 1,
    })
    .await
    .unwrap();
    let stream_id = views.load().state.next_stream_id - 1;
    sink.propose(MetadataCommand::OpenStream {
        node_id: 1,
        node_epoch: 1,
        stream_id,
        epoch: 5,
    })
    .await
    .unwrap();
    sink.propose(MetadataCommand::TransferStream {
        stream_id,
        from_node: 1,
        to_node: 2,
    })
    .await
    .unwrap();
    assert!(views
        .load()
        .state
        .pending_transfers
        .contains_key(&stream_id));

    // The source restarts at a bumped epoch and its watcher converges the
    // stale transfer.
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

/// Registry entries and data survive a node restart: the KV plane holds the
/// registry (the entry cache is a cache, not the source of truth) and the
/// shared object storage holds the data.
#[tokio::test]
async fn named_streams_survive_restart() {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));

    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 1,
            ..Default::default()
        },
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await
    .unwrap();
    node.service()
        .create(create("/durable/a", "text/plain"))
        .await
        .unwrap();
    node.service()
        .append(append("/durable/a", &[b"kept"], "text/plain"))
        .await
        .unwrap();
    node.close().await;

    // Same metadata plane and storage, fresh node at a higher epoch.
    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 2,
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
    )
    .await
    .unwrap();

    let head = node.service().head("/durable/a").await.unwrap().unwrap();
    assert_eq!(head.next_offset.record_offset(), 1);
    let read = node
        .service()
        .read("/durable/a", OffsetToken::beginning(), 0, 0)
        .await
        .unwrap();
    assert_eq!(read.records.len(), 1);
    assert_eq!(&read.records[0].payload[..], b"kept");

    node.close().await;
}

/// A stand-in Kafka batch: 8-byte base-offset field (patched by the
/// service) followed by opaque body bytes.
fn fake_batch(body: &[u8]) -> Bytes {
    let mut payload = vec![0u8; 8];
    payload.extend_from_slice(body);
    Bytes::from(payload)
}

fn one_batch(record_count: u32) -> Vec<pico_server::BatchSpan> {
    vec![pico_server::BatchSpan {
        patch_at: 0,
        record_count,
    }]
}

/// Kafka batch append/read/watermarks and idempotent producer dedup at the
/// service layer.
#[tokio::test]
async fn kafka_batch_append_read_and_idempotency() {
    use pico_server::{AppendBatchCommand, NumericProducer};

    let node = local_node().await;
    let services = node.service();
    let external_id = *b"0123456789abcdef";
    services
        .create(CreateCommand {
            name: "/topics/demo".into(),
            content_type: "application/vnd.kafka.batch".into(),
            external_id: Some(external_id),
            ..create("/topics/demo", "application/vnd.kafka.batch")
        })
        .await
        .unwrap();

    let appended = services
        .append_batch(AppendBatchCommand {
            name: "/topics/demo".into(),
            payload: fake_batch(b"kafka-batch-bytes"),
            batches: one_batch(3),
            producer: None,
            base_timestamp_ms: 1,
        })
        .await
        .unwrap();
    assert!(!appended.duplicate);
    assert_eq!(appended.base_offset, 0);
    assert_eq!(appended.log_start_offset, 0);

    let watermarks = services.watermarks("/topics/demo").await.unwrap();
    assert_eq!(watermarks.log_start_offset, 0);
    assert_eq!(watermarks.high_watermark, 3);

    let from_start = services
        .read_batches("/topics/demo", 0, usize::MAX)
        .await
        .unwrap();
    assert_eq!(from_start.batches.len(), 1);
    assert_eq!(from_start.batches[0].base_offset, 0);
    assert_eq!(from_start.batches[0].last_offset, 3);
    assert_eq!(from_start.batches[0].count, 3);
    // Stored verbatim except the patched base-offset field.
    assert_eq!(&from_start.batches[0].payload[..8], &0i64.to_be_bytes());
    assert_eq!(&from_start.batches[0].payload[8..], b"kafka-batch-bytes");
    assert_eq!(from_start.next_offset, 3);

    // Mid-batch fetch returns the covering batch verbatim.
    let mid = services
        .read_batches("/topics/demo", 1, usize::MAX)
        .await
        .unwrap();
    assert_eq!(mid.batches.len(), 1);
    assert_eq!(mid.batches[0].base_offset, 0);
    assert_eq!(mid.next_offset, 3);

    // Topic UUID resolves back to the stream.
    assert_eq!(
        services.lookup_by_external_id(external_id).await.unwrap(),
        Some("/topics/demo".to_owned())
    );
    assert_eq!(
        services.lookup_by_external_id([7u8; 16]).await.unwrap(),
        None
    );

    let producer = NumericProducer {
        id: 1,
        epoch: 0,
        first_seq: 0,
    };
    let idem = services
        .append_batch(AppendBatchCommand {
            name: "/topics/demo".into(),
            payload: fake_batch(b"idem"),
            batches: one_batch(2),
            producer: Some(producer),
            base_timestamp_ms: 2,
        })
        .await
        .unwrap();
    assert!(!idem.duplicate);
    assert_eq!(idem.base_offset, 3);

    // The idempotent batch got its base offset patched to 3.
    let second = services
        .read_batches("/topics/demo", 3, usize::MAX)
        .await
        .unwrap();
    assert_eq!(&second.batches[0].payload[..8], &3i64.to_be_bytes());

    let dup = services
        .append_batch(AppendBatchCommand {
            name: "/topics/demo".into(),
            payload: fake_batch(b"idem-retry"),
            batches: one_batch(2),
            producer: Some(producer),
            base_timestamp_ms: 2,
        })
        .await
        .unwrap();
    assert!(dup.duplicate);
    assert_eq!(dup.base_offset, 3);

    let gap = services
        .append_batch(AppendBatchCommand {
            name: "/topics/demo".into(),
            payload: fake_batch(b"gap"),
            batches: one_batch(1),
            producer: Some(NumericProducer {
                id: 1,
                epoch: 0,
                first_seq: 3,
            }),
            base_timestamp_ms: 3,
        })
        .await
        .unwrap_err();
    assert_eq!(gap.kind, ErrorKind::SequenceGap);
    assert_eq!((gap.expected_seq, gap.received_seq), (Some(2), Some(3)));

    let bumped = services
        .append_batch(AppendBatchCommand {
            name: "/topics/demo".into(),
            payload: fake_batch(b"epoch2"),
            batches: one_batch(1),
            producer: Some(NumericProducer {
                id: 1,
                epoch: 1,
                first_seq: 0,
            }),
            base_timestamp_ms: 4,
        })
        .await
        .unwrap();
    assert!(!bumped.duplicate);
    assert_eq!(bumped.base_offset, 5);

    let fenced = services
        .append_batch(AppendBatchCommand {
            name: "/topics/demo".into(),
            payload: fake_batch(b"stale"),
            batches: one_batch(1),
            producer: Some(NumericProducer {
                id: 1,
                epoch: 0,
                first_seq: 1,
            }),
            base_timestamp_ms: 5,
        })
        .await
        .unwrap_err();
    assert_eq!(fenced.kind, ErrorKind::Fenced);
    assert_eq!(fenced.producer_epoch, Some(1));

    node.close().await;
}

/// A minimal but real Kafka v2 record batch header (61 bytes) plus an opaque
/// body, so producer identity survives in the stored bytes.
fn kafka_v2_batch(producer_id: i64, epoch: i16, base_seq: i32, count: u32) -> Bytes {
    let mut payload = Vec::with_capacity(64);
    payload.extend_from_slice(&0i64.to_be_bytes()); // base offset (patched)
    payload.extend_from_slice(&53i32.to_be_bytes()); // batch length
    payload.extend_from_slice(&(-1i32).to_be_bytes()); // partition leader epoch
    payload.push(2); // magic
    payload.extend_from_slice(&0i32.to_be_bytes()); // crc
    payload.extend_from_slice(&0i16.to_be_bytes()); // attributes
    payload.extend_from_slice(&(count as i32 - 1).to_be_bytes()); // last offset delta
    payload.extend_from_slice(&1i64.to_be_bytes()); // base timestamp
    payload.extend_from_slice(&1i64.to_be_bytes()); // max timestamp
    payload.extend_from_slice(&producer_id.to_be_bytes());
    payload.extend_from_slice(&epoch.to_be_bytes());
    payload.extend_from_slice(&base_seq.to_be_bytes());
    payload.extend_from_slice(&(count as i32).to_be_bytes());
    payload.extend_from_slice(b"body");
    Bytes::from(payload)
}

/// Producer spans are not durably written per append; after a restart they
/// are rebuilt from the stored batch headers, so a retry of a pre-restart
/// batch still replays as a duplicate with its original offset.
#[tokio::test]
async fn kafka_producer_state_survives_restart_via_rescan() {
    use pico_server::{AppendBatchCommand, NumericProducer};

    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 1,
            ..Default::default()
        },
        sink.clone(),
        views.clone(),
        object_storage.clone(),
        wal_storage.clone(),
    )
    .await
    .unwrap();

    let batch = |seq: i32, count: u32| AppendBatchCommand {
        name: "/topics/replayed".into(),
        payload: kafka_v2_batch(42, 0, seq, count),
        batches: vec![pico_server::BatchSpan {
            patch_at: 0,
            record_count: count,
        }],
        producer: Some(NumericProducer {
            id: 42,
            epoch: 0,
            first_seq: seq,
        }),
        base_timestamp_ms: 1,
    };

    node.service()
        .create(create("/topics/replayed", "application/vnd.kafka.batch"))
        .await
        .unwrap();
    let first = node.service().append_batch(batch(0, 3)).await.unwrap();
    let second = node.service().append_batch(batch(3, 2)).await.unwrap();
    assert_eq!((first.base_offset, second.base_offset), (0, 3));
    node.close().await;

    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 2,
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
    )
    .await
    .unwrap();

    // Retry of the second batch dedupes against rescanned state.
    let retry = node.service().append_batch(batch(3, 2)).await.unwrap();
    assert!(retry.duplicate);
    assert_eq!(retry.base_offset, 3);
    // The next sequence continues where the pre-restart session left off.
    let next = node.service().append_batch(batch(5, 1)).await.unwrap();
    assert!(!next.duplicate);
    assert_eq!(next.base_offset, 5);
    // A sequence gap is still rejected.
    let gap = node.service().append_batch(batch(9, 1)).await.unwrap_err();
    assert_eq!(gap.kind, ErrorKind::SequenceGap);

    node.close().await;
}

/// Multi-batch payloads span summed record counts, each batch header patched
/// with its own assigned base offset. Producers require single batches, and
/// client creates cannot claim the reserved subtree.
#[tokio::test]
async fn kafka_multi_batch_spans_and_reserved_names() {
    use pico_server::{AppendBatchCommand, BatchSpan, NumericProducer};

    let node = local_node().await;
    let services = node.service();
    services
        .create(create("/topics/multi", "application/vnd.kafka.batch"))
        .await
        .unwrap();

    // Two 12-byte fake batches back to back.
    let mut payload = Vec::new();
    payload.extend_from_slice(&fake_batch(b"one!"));
    payload.extend_from_slice(&fake_batch(b"two!"));
    let spans = vec![
        BatchSpan {
            patch_at: 0,
            record_count: 2,
        },
        BatchSpan {
            patch_at: 12,
            record_count: 3,
        },
    ];
    let appended = services
        .append_batch(AppendBatchCommand {
            name: "/topics/multi".into(),
            payload: Bytes::from(payload.clone()),
            batches: spans.clone(),
            producer: None,
            base_timestamp_ms: 1,
        })
        .await
        .unwrap();
    assert_eq!(appended.base_offset, 0);
    let watermarks = services.watermarks("/topics/multi").await.unwrap();
    assert_eq!(watermarks.high_watermark, 5);

    let read = services
        .read_batches("/topics/multi", 0, usize::MAX)
        .await
        .unwrap();
    assert_eq!(read.batches.len(), 1);
    let stored = &read.batches[0].payload;
    assert_eq!(&stored[..8], &0i64.to_be_bytes());
    assert_eq!(&stored[12..20], &2i64.to_be_bytes());

    // Idempotent producers must send exactly one batch.
    let rejected = services
        .append_batch(AppendBatchCommand {
            name: "/topics/multi".into(),
            payload: Bytes::from(payload),
            batches: spans,
            producer: Some(NumericProducer {
                id: 9,
                epoch: 0,
                first_seq: 0,
            }),
            base_timestamp_ms: 2,
        })
        .await
        .unwrap_err();
    assert_eq!(rejected.kind, ErrorKind::BadRequest);

    // Reserved namespace is closed to client creates.
    let reserved = services
        .create(create("/_sys/groups/g1", "application/json"))
        .await
        .unwrap_err();
    assert_eq!(reserved.kind, ErrorKind::BadRequest);
    let mut internal = create("/_sys/groups/g1", "application/json");
    internal.internal = true;
    services.create(internal).await.unwrap();

    node.close().await;
}

/// Index records exist after create and are gone after delete.
#[tokio::test]
async fn external_id_lookup_uses_replicated_index() {
    let node = local_node().await;
    let services = node.service();
    let external_id = *b"0123456789abcdef";

    let mut command = create("/topics/indexed", "application/octet-stream");
    command.external_id = Some(external_id);
    services.create(command).await.unwrap();

    assert_eq!(
        services.lookup_by_external_id(external_id).await.unwrap(),
        Some("/topics/indexed".to_owned())
    );
    assert_eq!(
        services
            .lookup_by_external_id(*b"ffffffffffffffff")
            .await
            .unwrap(),
        None
    );

    let stream_id = services
        .lookup_stream_id("/topics/indexed")
        .await
        .unwrap()
        .unwrap();
    let view = node.views().load();
    assert_eq!(
        view.state
            .get_kv(&format!("idx/sid/{stream_id}"))
            .as_deref(),
        Some(b"/topics/indexed".as_slice())
    );
    assert!(view
        .state
        .get_kv("idx/extid/30313233343536373839616263646566")
        .is_some());

    assert!(services.delete("/topics/indexed").await.unwrap());
    assert_eq!(
        services.lookup_by_external_id(external_id).await.unwrap(),
        None
    );
    let view = node.views().load();
    assert!(view.state.get_kv(&format!("idx/sid/{stream_id}")).is_none());
    assert!(view
        .state
        .get_kv("idx/extid/30313233343536373839616263646566")
        .is_none());

    node.close().await;
}

/// The sweep expires a TTL'd stream no request ever touches. Observed via
/// raw view reads so lazy expire-on-access cannot be doing the work.
#[tokio::test]
async fn ttl_sweep_expires_untouched_streams() {
    let node = local_node().await;
    let services = node.service();

    let mut command = create("/streams/ephemeral", "text/plain");
    command.ttl_seconds = Some(1);
    services.create(command).await.unwrap();
    assert!(node
        .views()
        .load()
        .state
        .get_kv("/streams/ephemeral")
        .is_some());

    let (tx, rx) = tokio::sync::watch::channel(true);
    let sweep = services.spawn_ttl_sweep(rx, Duration::from_millis(20));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while node
        .views()
        .load()
        .state
        .get_kv("/streams/ephemeral")
        .is_some()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sweep never expired the stream"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    drop(tx);
    sweep.await.unwrap();
    node.close().await;
}

/// Listing pages with `start_after`/`limit`.
#[tokio::test]
async fn list_paginates_with_start_after() {
    let node = local_node().await;
    let services = node.service();
    for i in 0..5 {
        services
            .create(create(&format!("/page/{i}"), "text/plain"))
            .await
            .unwrap();
    }

    let first = services.list("/page/", None, 2).await.unwrap();
    assert_eq!(
        first
            .streams
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        vec!["/page/0", "/page/1"]
    );
    assert!(first.has_more);

    let second = services.list("/page/", Some("/page/1"), 2).await.unwrap();
    assert_eq!(
        second
            .streams
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        vec!["/page/2", "/page/3"]
    );
    assert!(second.has_more);

    let last = services.list("/page/", Some("/page/3"), 2).await.unwrap();
    assert_eq!(
        last.streams
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        vec!["/page/4"]
    );
    assert!(!last.has_more);

    node.close().await;
}
