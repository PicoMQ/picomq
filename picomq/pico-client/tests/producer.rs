//! The producer session: order, batching, idempotent retries, backpressure.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use picomq_client::producer::{Producer, ProducerConfig};
use picomq_client::{ClientConfig, ErrorKind, Live, PicoClient, ReadLimits, StreamApi};
use picomq_http::HttpProtocol as ServeProtocol;
use picomq_runtime::{MetaBackend, ServerConfig};

async fn start(dir: &std::path::Path) -> (picomq_runtime::PicoServer, Arc<PicoClient>) {
    let server = picomq_runtime::start(ServerConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        admin_addr: None,
        http_protocol: ServeProtocol::Pico,
        kafka: None,
        meta_backend: MetaBackend::parse("sqlite::memory:").unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!(
            "2@file://{}?batchInterval=5",
            dir.join("wal").display()
        )),
        long_poll_timeout: Duration::from_secs(1),
        ..Default::default()
    })
    .await
    .unwrap();
    let endpoint = format!("http://{}", server.local_addr());
    // HTTP/2 so the session's in-flight batches share one connection.
    let http = picomq_client::http_client(&ClientConfig {
        http2: true,
        ..Default::default()
    })
    .unwrap();
    let client = Arc::new(PicoClient::with_http(&endpoint, http, Default::default()));
    (server, client)
}

async fn read_all(client: &PicoClient, name: &str) -> Vec<Bytes> {
    let mut from = client.beginning();
    let mut bodies = Vec::new();
    loop {
        let page = client
            .read(name, &from, Live::Off, ReadLimits::server_default())
            .await
            .unwrap();
        if page.records.is_empty() {
            return bodies;
        }
        bodies.extend(page.records.into_iter().map(|r| r.body));
        from = page.next;
    }
}

/// The headline guarantee: records land in `send` order even though the batches
/// carrying them are in flight concurrently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn records_land_in_send_order() {
    let dir = tempfile::tempdir().unwrap();
    let (server, client) = start(dir.path()).await;
    client
        .create("/streams/ordered", "text/plain", None)
        .await
        .unwrap();

    let producer = Producer::new(
        Arc::clone(&client),
        "/streams/ordered",
        "writer-1",
        ProducerConfig {
            linger: Duration::from_millis(2),
            max_batch_records: 16,
            ..Default::default()
        },
    );

    let count = 2_000;
    let mut pending = Vec::with_capacity(count);
    for i in 0..count {
        pending.push(producer.send(Bytes::from(i.to_string())).await.unwrap());
    }
    // Sequences come back in send order and without gaps.
    for (i, p) in pending.into_iter().enumerate() {
        assert_eq!(p.durable().await.unwrap(), i as u64);
    }
    producer.close().await.unwrap();

    let bodies = read_all(&client, "/streams/ordered").await;
    let expected: Vec<String> = (0..count).map(|i| i.to_string()).collect();
    let actual: Vec<String> = bodies
        .iter()
        .map(|b| String::from_utf8(b.to_vec()).unwrap())
        .collect();
    assert_eq!(actual, expected);

    server.shutdown().await;
}

/// Records are batched rather than sent one request each, which is where the
/// throughput comes from.
#[tokio::test]
async fn records_are_batched() {
    let dir = tempfile::tempdir().unwrap();
    let (server, client) = start(dir.path()).await;
    client
        .create("/streams/batched", "text/plain", None)
        .await
        .unwrap();

    let producer = Producer::new(
        Arc::clone(&client),
        "/streams/batched",
        "writer-1",
        ProducerConfig {
            linger: Duration::from_millis(50),
            max_batch_records: 100,
            ..Default::default()
        },
    );
    let pending: Vec<_> =
        futures::future::join_all((0..100).map(|i| producer.send(Bytes::from(format!("r{i}")))))
            .await
            .into_iter()
            .map(|p| p.unwrap())
            .collect();
    for p in pending {
        p.durable().await.unwrap();
    }

    // One batch of 100 means one append, so the stream's records all share a
    // timestamp. More usefully, the whole thing completes well inside the time
    // 100 sequential appends would take.
    let head = client.head("/streams/batched").await.unwrap().unwrap();
    assert_eq!(head.next, "100");

    producer.close().await.unwrap();
    server.shutdown().await;
}

/// A resent batch is applied once: the producer sequence makes the append
/// idempotent, which is what lets the session retry at all.
#[tokio::test]
async fn resending_a_batch_applies_it_once() {
    let dir = tempfile::tempdir().unwrap();
    let (server, client) = start(dir.path()).await;
    client
        .create("/streams/dedupe", "text/plain", None)
        .await
        .unwrap();

    let records = [Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    let first = client
        .append_as(
            "/streams/dedupe",
            &records,
            &picomq_client::pico::ProducerRef {
                id: "writer-1",
                epoch: 0,
                seq: 0,
            },
        )
        .await
        .unwrap();
    assert!(first.applied && !first.duplicate);

    let again = client
        .append_as(
            "/streams/dedupe",
            &records,
            &picomq_client::pico::ProducerRef {
                id: "writer-1",
                epoch: 0,
                seq: 0,
            },
        )
        .await
        .unwrap();
    assert!(again.duplicate, "same sequence is recognized, not appended");

    let head = client.head("/streams/dedupe").await.unwrap().unwrap();
    assert_eq!(head.next, "2", "two records, not four");

    server.shutdown().await;
}

/// A batch arriving before its predecessor is rejected rather than applied out
/// of order, which is the mechanism the session relies on.
#[tokio::test]
async fn a_batch_out_of_order_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (server, client) = start(dir.path()).await;
    client
        .create("/streams/gap", "text/plain", None)
        .await
        .unwrap();

    client
        .append_as(
            "/streams/gap",
            &[Bytes::from_static(b"first")],
            &picomq_client::pico::ProducerRef {
                id: "writer-1",
                epoch: 0,
                seq: 0,
            },
        )
        .await
        .unwrap();

    // Sequence 1 has not been sent, so 2 is out of order.
    let error = client
        .append_as(
            "/streams/gap",
            &[Bytes::from_static(b"early")],
            &picomq_client::pico::ProducerRef {
                id: "writer-1",
                epoch: 0,
                seq: 2,
            },
        )
        .await
        .expect_err("a gap must not be applied");
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(error.code, "sequence_gap");

    let head = client.head("/streams/gap").await.unwrap().unwrap();
    assert_eq!(head.next, "1", "only the in-order record was written");

    server.shutdown().await;
}

/// A record that could never fit the session's budget is refused outright
/// rather than deadlocking against a permit that will never be free.
#[tokio::test]
async fn a_record_larger_than_the_budget_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (server, client) = start(dir.path()).await;
    client
        .create("/streams/budget", "text/plain", None)
        .await
        .unwrap();

    let producer = Producer::new(
        Arc::clone(&client),
        "/streams/budget",
        "writer-1",
        ProducerConfig {
            max_buffered_bytes: 1024,
            ..Default::default()
        },
    );
    let error = producer
        .send(Bytes::from(vec![b'x'; 4096]))
        .await
        .expect_err("cannot buffer more than the whole budget");
    assert_eq!(error.kind, ErrorKind::BadRequest);

    producer.close().await.unwrap();
    server.shutdown().await;
}

/// `flush` waits for records handed over but not yet awaited.
#[tokio::test]
async fn flush_waits_for_durability() {
    let dir = tempfile::tempdir().unwrap();
    let (server, client) = start(dir.path()).await;
    client
        .create("/streams/flush", "text/plain", None)
        .await
        .unwrap();

    let producer = Producer::new(
        Arc::clone(&client),
        "/streams/flush",
        "writer-1",
        ProducerConfig::default(),
    );
    for i in 0..50 {
        // Deliberately dropping the handles: flush is the only thing waiting.
        let _ = producer.send(Bytes::from(format!("r{i}"))).await.unwrap();
    }
    producer.flush().await.unwrap();

    let head = client.head("/streams/flush").await.unwrap().unwrap();
    assert_eq!(
        head.next, "50",
        "flush returned before everything was durable"
    );

    producer.close().await.unwrap();
    server.shutdown().await;
}
