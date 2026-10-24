//! End-to-end Kafka compatibility with a real client (librdkafka): produce
//! and consume, consumer groups with commit/resume and rebalance, idempotent
//! producers, restart recovery, and an ignored load gate.
//!
//! Run the load gate explicitly:
//! `cargo test --release -p picomq-runtime --test kafka -- --ignored --nocapture`

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use picomq_auth::AccessToken;
use picomq_runtime::{AuthMode, KafkaConfig, MetaBackend, PicoServer, ServerConfig};
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn config(dir: &Path, node_epoch: i64) -> ServerConfig {
    ServerConfig {
        node_epoch,
        addr: loopback(),
        admin_addr: None,
        kafka: KafkaConfig::new(loopback()),
        meta_backend: MetaBackend::parse(&format!("sqlite:{}", dir.join("meta.db").display()))
            .unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        // Tight WAL group-commit window: this is a local-latency test, not an
        // S3-PUT-cost test (the 250ms default amortizes object writes).
        wal_uri: Some(format!(
            "2@file://{}?batchInterval=5",
            dir.join("wal").display()
        )),
        ..Default::default()
    }
}

async fn broker(dir: &Path, node_epoch: i64) -> (PicoServer, String) {
    let server = picomq_runtime::start(config(dir, node_epoch))
        .await
        .unwrap();
    let bootstrap = server.kafka_addr().to_string();
    (server, bootstrap)
}

fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "15000")
        .create()
        .unwrap()
}

fn consumer(bootstrap: &str, group: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "6000")
        .create()
        .unwrap()
}

async fn create_topic(bootstrap: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let results = admin
        .create_topics(
            [&NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    results[0].as_ref().unwrap();
}

async fn produce_n(producer: &FutureProducer, topic: &str, start: usize, count: usize) {
    for i in start..start + count {
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&format!("k{i}"))
                    .payload(&format!("v{i}")),
                Duration::from_secs(15),
            )
            .await
            .unwrap();
    }
}

async fn consume_n(consumer: &StreamConsumer, count: usize) -> Vec<(String, String, i64)> {
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("consume timed out")
            .unwrap();
        out.push((
            String::from_utf8(message.key().unwrap().to_vec()).unwrap(),
            String::from_utf8(message.payload().unwrap().to_vec()).unwrap(),
            message.offset(),
        ));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn produce_and_consume_with_assign() {
    let dir = tempfile::tempdir().unwrap();
    let (server, bootstrap) = broker(dir.path(), 1).await;
    create_topic(&bootstrap, "orders").await;

    produce_n(&producer(&bootstrap), "orders", 0, 100).await;

    let consumer = consumer(&bootstrap, "assign-reader");
    let mut assignment = rdkafka::TopicPartitionList::new();
    assignment
        .add_partition_offset("orders", 0, rdkafka::Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();
    let records = consume_n(&consumer, 100).await;
    for (i, (key, value, offset)) in records.iter().enumerate() {
        assert_eq!(key, &format!("k{i}"));
        assert_eq!(value, &format!("v{i}"));
        assert_eq!(*offset, i as i64);
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_commit_resume_and_rebalance() {
    let dir = tempfile::tempdir().unwrap();
    let (server, bootstrap) = broker(dir.path(), 1).await;
    create_topic(&bootstrap, "jobs").await;
    let producer = producer(&bootstrap);
    produce_n(&producer, "jobs", 0, 40).await;

    let first = consumer(&bootstrap, "workers");
    first.subscribe(&["jobs"]).unwrap();
    let records = consume_n(&first, 40).await;
    assert_eq!(records.last().unwrap().2, 39);
    first
        .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    drop(first);

    // A new member of the same group resumes at the committed offset and
    // takes over the partition (single-partition rebalance).
    produce_n(&producer, "jobs", 40, 20).await;
    let second = consumer(&bootstrap, "workers");
    second.subscribe(&["jobs"]).unwrap();
    let records = consume_n(&second, 20).await;
    assert_eq!(records[0].1, "v40");
    assert_eq!(records[0].2, 40);
    assert_eq!(records.last().unwrap().2, 59);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cooperative_sticky_members_split_topics_between_them() {
    let dir = tempfile::tempdir().unwrap();
    let (server, bootstrap) = broker(dir.path(), 1).await;
    create_topic(&bootstrap, "left").await;
    create_topic(&bootstrap, "right").await;
    let producer = producer(&bootstrap);
    produce_n(&producer, "left", 0, 10).await;
    produce_n(&producer, "right", 0, 10).await;

    let sticky = |group: &str| -> StreamConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", "6000")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .create()
            .unwrap()
    };
    let first = sticky("pair");
    let second = sticky("pair");
    first.subscribe(&["left", "right"]).unwrap();
    second.subscribe(&["left", "right"]).unwrap();

    let mut offsets = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while offsets.len() < 20 {
        let message = tokio::select! {
            m = first.recv() => m,
            m = second.recv() => m,
            _ = tokio::time::sleep_until(deadline) => panic!("consume timed out"),
        };
        offsets.push(message.unwrap().offset());
    }
    offsets.sort_unstable();
    assert_eq!(offsets, (0..10).flat_map(|i| [i, i]).collect::<Vec<_>>());

    let settled = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let groups = first
            .client()
            .fetch_group_list(Some("pair"), Duration::from_secs(10))
            .unwrap();
        let group = groups
            .groups()
            .iter()
            .find(|group| group.name() == "pair")
            .expect("the group is listed");
        if group.state() == "Stable" && group.members().len() == 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < settled,
            "the group never settled with both members: {} with {} members",
            group.state(),
            group.members().len()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let owned = |consumer: &StreamConsumer| -> Vec<String> {
        consumer
            .assignment()
            .unwrap()
            .elements()
            .iter()
            .map(|tp| tp.topic().to_owned())
            .collect()
    };
    let mut all = owned(&first);
    all.extend(owned(&second));
    all.sort();
    assert_eq!(all, ["left", "right"], "each topic is owned exactly once");

    server.shutdown().await;
}

/// Batches are stored as the producer sent them, so a compressed batch
/// reaches a Kafka consumer intact and an HTTP reader sees its records
/// decoded.
#[tokio::test(flavor = "multi_thread")]
async fn compressed_batches_round_trip_to_kafka_consumers() {
    let dir = tempfile::tempdir().unwrap();
    let (server, bootstrap) = broker(dir.path(), 1).await;
    let http = reqwest::Client::new();

    for codec in ["lz4", "snappy"] {
        let topic = format!("packed-{codec}");
        create_topic(&bootstrap, &topic).await;
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("compression.type", codec)
            .set("linger.ms", "50")
            .set("message.timeout.ms", "15000")
            .create()
            .unwrap();
        let mut sends = tokio::task::JoinSet::new();
        for i in 0..20 {
            let producer = producer.clone();
            let topic = topic.clone();
            sends.spawn(async move {
                producer
                    .send(
                        FutureRecord::to(&topic)
                            .key(&format!("k{i}"))
                            .payload(&format!("v{i}")),
                        Duration::from_secs(15),
                    )
                    .await
                    .unwrap();
            });
        }
        while sends.join_next().await.is_some() {}

        let reader = consumer(&bootstrap, &format!("{codec}-readers"));
        reader.subscribe(&[topic.as_str()]).unwrap();
        let records = consume_n(&reader, 20).await;
        let mut values: Vec<String> = records.into_iter().map(|(_, value, _)| value).collect();
        values.sort();
        let mut expected: Vec<String> = (0..20).map(|i| format!("v{i}")).collect();
        expected.sort();
        assert_eq!(values, expected, "{codec}");

        let read = http
            .get(format!("http://{}/{topic}?seq=0", server.local_addr()))
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), 200, "{codec}");
        let page: Vec<serde_json::Value> = read.json().await.unwrap();
        let mut bodies: Vec<String> = page
            .iter()
            .map(|record| record["body"].as_str().unwrap().to_owned())
            .collect();
        bodies.sort();
        assert_eq!(bodies, expected, "{codec} over http");
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sasl_plain_clients_produce_and_consume() {
    let dir = tempfile::tempdir().unwrap();
    let (root, _) = AccessToken::issue("ops/root").unwrap();
    let server = picomq_runtime::start(ServerConfig {
        auth_mode: AuthMode::Required,
        bootstrap_token: Some(root.render()),
        ..config(dir.path(), 1)
    })
    .await
    .unwrap();
    let bootstrap = server.kafka_addr().to_string();
    let sasl = |config: &mut ClientConfig, password: &str| {
        config
            .set("bootstrap.servers", &bootstrap)
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "PLAIN")
            .set("sasl.username", &root.id)
            .set("sasl.password", password)
            .set("message.timeout.ms", "5000");
    };

    let admin: AdminClient<DefaultClientContext> = {
        let mut config = ClientConfig::new();
        sasl(&mut config, &root.render());
        config.create().unwrap()
    };
    admin
        .create_topics(
            [&NewTopic::new("secured", 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap()[0]
        .as_ref()
        .unwrap();
    let producer: FutureProducer = {
        let mut config = ClientConfig::new();
        sasl(&mut config, &root.render());
        config.set("enable.idempotence", "true").create().unwrap()
    };
    produce_n(&producer, "secured", 0, 5).await;
    let reader: StreamConsumer = {
        let mut config = ClientConfig::new();
        sasl(&mut config, &root.render());
        config
            .set("group.id", "secured-readers")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .unwrap()
    };
    reader.subscribe(&["secured"]).unwrap();
    assert_eq!(consume_n(&reader, 5).await.last().unwrap().2, 4);

    let wrong: FutureProducer = {
        let mut config = ClientConfig::new();
        sasl(&mut config, "not-the-token");
        config.create().unwrap()
    };
    let refused = wrong
        .send(
            FutureRecord::<(), str>::to("secured").payload("x"),
            Duration::from_secs(5),
        )
        .await;
    assert!(refused.is_err(), "a wrong password must not produce");

    let plaintext = producer_without_sasl(&bootstrap);
    let refused = plaintext
        .send(
            FutureRecord::<(), str>::to("secured").payload("x"),
            Duration::from_secs(5),
        )
        .await;
    assert!(
        refused.is_err(),
        "an unauthenticated client must not produce"
    );

    server.shutdown().await;
}

fn producer_without_sasl(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_recovers_data_and_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let (server, bootstrap) = broker(dir.path(), 1).await;
    create_topic(&bootstrap, "ledger").await;
    produce_n(&producer(&bootstrap), "ledger", 0, 30).await;

    let reader = consumer(&bootstrap, "auditors");
    reader.subscribe(&["ledger"]).unwrap();
    assert_eq!(consume_n(&reader, 30).await.last().unwrap().2, 29);
    reader
        .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
        .unwrap();
    drop(reader);
    server.shutdown().await;

    let (server, bootstrap) = broker(dir.path(), 2).await;
    produce_n(&producer(&bootstrap), "ledger", 30, 10).await;
    let reader = consumer(&bootstrap, "auditors");
    reader.subscribe(&["ledger"]).unwrap();
    let records = consume_n(&reader, 10).await;
    assert_eq!(records[0].2, 30, "committed offset survived the restart");
    assert_eq!(records.last().unwrap().2, 39);

    server.shutdown().await;
}

/// Load gate: sustained produce then consume of `TOTAL` records, reporting
/// throughput. Run in release mode.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "run explicitly: cargo test --release -p picomq-runtime --test kafka -- --ignored --nocapture"]
async fn produce_consume_load() {
    const TOTAL: usize = 200_000;
    const VALUE_BYTES: usize = 512;

    let dir = tempfile::tempdir().unwrap();
    let (server, bootstrap) = broker(dir.path(), 1).await;
    create_topic(&bootstrap, "load").await;

    // Note: librdkafka's idempotent producer sends one produce request at a
    // time regardless of max.in.flight (fixed upstream in librdkafka#4989),
    // so this measures serial request latency, not broker pipelining.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("enable.idempotence", "true")
        .set("linger.ms", "5")
        .set("batch.size", "1048576")
        .set("message.timeout.ms", "60000")
        .create()
        .unwrap();
    let payload = vec![b'x'; VALUE_BYTES];

    let started = Instant::now();
    let mut inflight = Vec::with_capacity(TOTAL);
    for i in 0..TOTAL {
        let key = format!("k{i}");
        loop {
            match producer.send_result(
                FutureRecord::to("load")
                    .key(&key)
                    .payload(payload.as_slice()),
            ) {
                Ok(delivery) => {
                    inflight.push(delivery);
                    break;
                }
                Err((error, _))
                    if error.rdkafka_error_code()
                        == Some(rdkafka::types::RDKafkaErrorCode::QueueFull) =>
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err((error, _)) => panic!("produce failed: {error}"),
            }
        }
    }
    for delivery in inflight {
        delivery.await.unwrap().unwrap();
    }
    let produce_elapsed = started.elapsed();
    let mb = (TOTAL * VALUE_BYTES) as f64 / (1024.0 * 1024.0);
    println!(
        "produce {TOTAL} x {VALUE_BYTES}B: {produce_elapsed:?} ({:.0} msg/s, {:.1} MiB/s)",
        TOTAL as f64 / produce_elapsed.as_secs_f64(),
        mb / produce_elapsed.as_secs_f64()
    );

    let consumer = consumer(&bootstrap, "load-reader");
    let mut assignment = rdkafka::TopicPartitionList::new();
    assignment
        .add_partition_offset("load", 0, rdkafka::Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();
    let started = Instant::now();
    let mut seen = 0usize;
    while seen < TOTAL {
        let message = tokio::time::timeout(Duration::from_secs(60), consumer.recv())
            .await
            .expect("consume timed out")
            .unwrap();
        assert_eq!(message.payload().unwrap().len(), VALUE_BYTES);
        seen += 1;
    }
    let consume_elapsed = started.elapsed();
    println!(
        "consume {TOTAL} x {VALUE_BYTES}B: {consume_elapsed:?} ({:.0} msg/s, {:.1} MiB/s)",
        TOTAL as f64 / consume_elapsed.as_secs_f64(),
        mb / consume_elapsed.as_secs_f64()
    );

    server.shutdown().await;
}
