//! Rust client against a running node.
//!
//! ```bash
//! PICO_ENDPOINT=http://127.0.0.1:4437 \
//!   cargo test -p picomq-client --test docker_e2e -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use picomq_client::pico::ProducerRef;
use picomq_client::producer::{Producer, ProducerConfig};
use picomq_client::{
    ClientConfig, ErrorKind, Live, PicoClient, Protocol, ReadLimits, StreamApi, connect,
};

fn endpoint() -> String {
    std::env::var("PICO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4437".into())
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/e2e/rs-{prefix}-{nanos}")
}

fn pico() -> PicoClient {
    let http = picomq_client::http_client(&ClientConfig {
        http2: true,
        ..Default::default()
    })
    .unwrap();
    PicoClient::with_http(&endpoint(), http, Default::default())
}

async fn read_bodies(client: &PicoClient, name: &str) -> Vec<String> {
    let page = client
        .read(
            name,
            &client.beginning(),
            Live::Off,
            ReadLimits::server_default(),
        )
        .await
        .unwrap();
    page.records
        .into_iter()
        .map(|r| String::from_utf8(r.body.to_vec()).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_lifecycle() {
    let client = pico();
    let name = unique("life");
    assert!(client.create(&name, "text/plain", None).await.unwrap());
    assert!(!client.create(&name, "text/plain", None).await.unwrap());

    let ack = client
        .append(
            &name,
            &[Bytes::from_static(b"one"), Bytes::from_static(b"two")],
            "text/plain",
        )
        .await
        .unwrap();
    assert_eq!(ack.start, "0");
    assert_eq!(ack.next, "2");

    let head = client.head(&name).await.unwrap().unwrap();
    assert_eq!(head.next, "2");
    assert!(!head.closed);

    assert_eq!(read_bodies(&client, &name).await, ["one", "two"]);
    assert_eq!(client.close(&name).await.unwrap(), "2");
    let error = client
        .append(&name, &[Bytes::from_static(b"late")], "text/plain")
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Closed);
    assert!(client.delete(&name).await.unwrap());
    assert!(client.head(&name).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_ttl_and_head() {
    let client = pico();
    let name = unique("ttl");
    client.create(&name, "text/plain", Some(2)).await.unwrap();
    let head = client.head(&name).await.unwrap().unwrap();
    assert_eq!(head.ttl_seconds, Some(2));

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if client.head(&name).await.unwrap().is_none() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "ttl stream still present"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_gap_duplicate_and_fence() {
    let client = pico();
    let name = unique("gap");
    client.create(&name, "text/plain", None).await.unwrap();

    let first = client
        .append_as(
            &name,
            &[Bytes::from_static(b"a")],
            &ProducerRef {
                id: "w1",
                epoch: 1,
                seq: 0,
            },
        )
        .await
        .unwrap();
    assert!(first.applied);

    let again = client
        .append_as(
            &name,
            &[Bytes::from_static(b"a")],
            &ProducerRef {
                id: "w1",
                epoch: 1,
                seq: 0,
            },
        )
        .await
        .unwrap();
    assert!(again.duplicate);

    let gap = client
        .append_as(
            &name,
            &[Bytes::from_static(b"skip")],
            &ProducerRef {
                id: "w1",
                epoch: 1,
                seq: 2,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(gap.kind, ErrorKind::Conflict);
    assert_eq!(gap.code, "sequence_gap");

    let fence = client
        .append_as(
            &name,
            &[Bytes::from_static(b"stale")],
            &ProducerRef {
                id: "w1",
                epoch: 0,
                seq: 1,
            },
        )
        .await
        .unwrap_err();
    assert!(fence.kind == ErrorKind::StaleEpoch || fence.code == "fenced");

    client
        .append_as(
            &name,
            &[Bytes::from_static(b"b")],
            &ProducerRef {
                id: "w1",
                epoch: 1,
                seq: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(read_bodies(&client, &name).await, ["a", "b"]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_session_order() {
    let client = Arc::new(pico());
    let name = unique("order");
    client.create(&name, "text/plain", None).await.unwrap();
    let producer = Producer::new(
        Arc::clone(&client),
        &name,
        "session",
        ProducerConfig {
            linger: Duration::from_millis(2),
            max_batch_records: 16,
            ..Default::default()
        },
    );
    let count = 200;
    let mut pending = Vec::with_capacity(count);
    for i in 0..count {
        pending.push(producer.send(Bytes::from(i.to_string())).await.unwrap());
    }
    for (i, p) in pending.into_iter().enumerate() {
        assert_eq!(p.durable().await.unwrap(), i as u64);
    }
    producer.close().await.unwrap();
    let bodies = read_bodies(&client, &name).await;
    let expected: Vec<String> = (0..count).map(|i| i.to_string()).collect();
    assert_eq!(bodies, expected);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_client_concurrent_producers() {
    let name = unique("mp");
    pico().create(&name, "text/plain", None).await.unwrap();
    let writers = 6;
    let each = 30;
    let mut tasks = Vec::new();
    for w in 0..writers {
        let name = name.clone();
        tasks.push(tokio::spawn(async move {
            let client = pico();
            for i in 0..each {
                let id = format!("w{w}");
                client
                    .append_as(
                        &name,
                        &[Bytes::from(format!("w{w}-{i}"))],
                        &ProducerRef {
                            id: &id,
                            epoch: 0,
                            seq: i,
                        },
                    )
                    .await
                    .unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let bodies = read_bodies(&pico(), &name).await;
    assert_eq!(bodies.len(), (writers * each) as usize);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn rust_and_trait_clients_agree() {
    let boxed = connect(Protocol::Pico, &endpoint()).unwrap();
    let name = unique("trait");
    boxed.create(&name, "text/plain", None).await.unwrap();
    boxed
        .append(&name, &[Bytes::from_static(b"via-trait")], "text/plain")
        .await
        .unwrap();
    let page = boxed
        .read(
            &name,
            &boxed.beginning(),
            Live::Off,
            ReadLimits::server_default(),
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].body, Bytes::from_static(b"via-trait"));
}
