//! End-to-end against a two-node cluster (Postgres + object storage).

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{
    Harness, assert_exactly_once, first_admin, produce, read_partitions, run, token, unique,
    wait_ready, write_continuously,
};
use picomq_client::pico::ProducerRef;
use picomq_client::producer::{Producer, ProducerConfig};
use picomq_client::{CreateOptions, Live, PicoClient, ReadLimits, RetryPolicy, StreamApi};
use rdkafka::admin::{AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{Message, Offset, TopicPartitionList};

fn second_bootstrap() -> String {
    std::env::var("PICO_KAFKA_2").unwrap_or_else(|_| "127.0.0.1:9093".into())
}

fn second_admin() -> String {
    std::env::var("PICO_ADMIN_2").unwrap_or_else(|_| "http://127.0.0.1:9091".into())
}

fn pico_client(h: &Harness) -> PicoClient {
    let config = picomq_client::ClientConfig {
        token: Some(token()),
        ..Default::default()
    };
    PicoClient::with_config(&h.base, &config).unwrap()
}

/// The node holding `name`, once no transfer is pending.
async fn settled_owner(h: &Harness, name: &str) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let described: serde_json::Value = h
            .http
            .get(format!(
                "{}/admin/topics/{}/partitions/0",
                first_admin(),
                picomq_protocol::stream_segment(name)
            ))
            .bearer_auth(token())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if described["pendingTransfer"].is_null() {
            return described["ownerNodeId"].as_i64().unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "transfer of {name} never settled"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn bodies(client: &PicoClient, name: &str) -> Vec<String> {
    let page = client
        .read(
            name,
            None,
            &client.beginning(),
            Live::Off,
            ReadLimits::server_default(),
        )
        .await
        .unwrap();
    page.records
        .into_iter()
        .map(|record| String::from_utf8(record.body.unwrap().to_vec()).unwrap())
        .collect()
}

fn cluster() -> Harness {
    let harness = Harness::new();
    Harness {
        bootstrap: format!("{},{}", harness.bootstrap, second_bootstrap()),
        ..harness
    }
}

fn group_consumer(harness: &Harness, group: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = harness
        .kafka()
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .create()
        .unwrap();
    consumer.subscribe(&[topic]).unwrap();
    consumer
}

fn leaders(harness: &Harness, topic: &str) -> BTreeMap<i32, i32> {
    let client: BaseConsumer = harness.kafka().create().unwrap();
    let metadata = client
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .unwrap();

    metadata.topics()[0]
        .partitions()
        .iter()
        .map(|partition| (partition.id(), partition.leader()))
        .collect()
}

async fn eventually(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn tokens_on_every_node(h: &Harness, ids: &[&str]) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut everywhere = true;
        for admin in [first_admin(), second_admin()] {
            for id in ids {
                let listed: serde_json::Value = h
                    .http
                    .get(format!("{admin}/admin/tokens?prefix={id}"))
                    .bearer_auth(token())
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                everywhere &= listed["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["id"].as_str() == Some(id));
            }
        }
        if everywhere {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "tokens {ids:?} never reached every node"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn consume_values(consumer: &StreamConsumer, count: usize) -> Vec<(i32, i64, String)> {
    let mut consumed = Vec::with_capacity(count);
    while consumed.len() < count {
        let message = tokio::time::timeout(Duration::from_secs(60), consumer.recv())
            .await
            .expect("consume timed out")
            .unwrap();
        consumed.push((
            message.partition(),
            message.offset(),
            String::from_utf8(message.payload().unwrap_or_default().to_vec()).unwrap(),
        ));
    }
    consumed
}

async fn produce_range(harness: &Harness, topic: &str, values: std::ops::Range<usize>) {
    let producer = harness.producer();
    for value in values {
        let key = format!("k{}", value % 10);
        produce(&producer, topic, Some(&key), &value.to_string()).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn partitions_spread_across_nodes_and_serve_both_bootstraps() {
    let topic = unique("spread");
    let brokers: [&[i32]; 6] = [&[1], &[2], &[1], &[2], &[1], &[2]];
    let created = cluster()
        .admin()
        .create_topics(
            [&NewTopic::new(
                &topic,
                brokers.len() as i32,
                TopicReplication::Variable(&brokers),
            )],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    created[0].as_ref().unwrap();

    let expected: BTreeMap<i32, i32> = (0..6)
        .map(|index| (index, brokers[index as usize][0]))
        .collect();
    let second = Harness {
        bootstrap: second_bootstrap(),
        ..Harness::new()
    };
    assert_eq!(leaders(&Harness::new(), &topic), expected);
    assert_eq!(leaders(&second, &topic), expected);

    produce_range(&Harness::new(), &topic, 0..300).await;

    let consumer = group_consumer(&second, &unique("spread"), &topic);
    let consumed = consume_values(&consumer, 300).await;

    let values: BTreeSet<usize> = consumed
        .iter()
        .map(|(_, _, v)| v.parse().unwrap())
        .collect();
    assert_eq!(values, (0..300).collect());

    let mut last: BTreeMap<i32, (i64, usize)> = BTreeMap::new();
    for (partition, offset, value) in &consumed {
        let value: usize = value.parse().unwrap();
        if let Some((previous_offset, previous_value)) = last.insert(*partition, (*offset, value)) {
            assert!(*offset > previous_offset, "partition {partition} went back");
            assert!(value > previous_value, "partition {partition} reordered");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn http_reads_of_a_partition_reach_its_owner() {
    let h = cluster();
    let topic = unique("owner");
    h.create_topic(&topic, 2).await;

    let leaders = leaders(&h, &topic);
    let (partition, _) = leaders
        .iter()
        .find(|(_, leader)| **leader == 2)
        .expect("a partition on node 2");
    let producer = h.producer();
    producer
        .send(
            FutureRecord::<(), str>::to(&topic)
                .partition(*partition)
                .payload("on-node-2"),
            Duration::from_secs(20),
        )
        .await
        .unwrap();

    let path = format!("/{topic}?partition={partition}&seq=0");
    let redirected = h
        .http
        .get(h.url(&path))
        .bearer_auth(token())
        .send()
        .await
        .unwrap();
    assert_eq!(redirected.status(), 307);
    let location = redirected.headers()["location"]
        .to_str()
        .unwrap()
        .to_owned();

    let read = h
        .http
        .get(&location)
        .bearer_auth(token())
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 200);
    assert!(
        read.bytes()
            .await
            .unwrap()
            .windows(9)
            .any(|window| window == b"on-node-2")
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn each_node_answers_for_the_other() {
    let h = cluster();

    for (via, asked) in [(first_admin(), "2"), (second_admin(), "1")] {
        let response = h
            .http
            .get(format!("{via}/admin/nodes/{asked}/metrics"))
            .bearer_auth(token())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("pico-node-id")
                .and_then(|value| value.to_str().ok()),
            Some(asked)
        );
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("pico_requests_total")
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn admin_apis_reach_partitions_on_both_nodes() {
    let h = cluster();
    let topic = unique("admin");
    h.create_topic(&topic, 2).await;
    let producer = h.producer();
    for partition in 0..2 {
        for value in 0..5 {
            producer
                .send(
                    FutureRecord::<(), str>::to(&topic)
                        .partition(partition)
                        .payload(&value.to_string()),
                    Duration::from_secs(20),
                )
                .await
                .unwrap();
        }
    }

    let admin = h.admin();
    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

    let configs = admin
        .describe_configs([&ResourceSpecifier::Topic(&topic)], &options)
        .await
        .unwrap();
    let config = configs[0].as_ref().unwrap();
    assert_eq!(
        config.entry_map()["cleanup.policy"].value.as_deref(),
        Some("delete")
    );

    let mut trim = TopicPartitionList::new();
    trim.add_partition_offset(&topic, 0, Offset::Offset(3))
        .unwrap();
    trim.add_partition_offset(&topic, 1, Offset::Offset(3))
        .unwrap();
    let trimmed = admin.delete_records(&trim, &options).await.unwrap();
    for element in trimmed.elements() {
        element.error().unwrap();
        assert_eq!(element.offset(), Offset::Offset(3));
    }

    let group = unique("admin-group");
    {
        let consumer = group_consumer(&h, &group, &topic);
        consume_values(&consumer, 4).await;
        consumer.commit_consumer_state(CommitMode::Sync).unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let deleted = admin.delete_groups(&[&group], &options).await.unwrap();
        if deleted[0].is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "group never became deletable: {deleted:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_dead_node_fails_over_and_committed_offsets_survive() {
    let (Ok(kill), Ok(start)) = (
        std::env::var("PICO_KILL_CMD"),
        std::env::var("PICO_START_CMD"),
    ) else {
        eprintln!("set PICO_KILL_CMD and PICO_START_CMD to run the failover test");
        return;
    };

    let h = cluster();
    let survivor = Harness::new();
    let topic = unique("failover");
    let group = unique("failover-group");
    h.create_topic(&topic, 4).await;
    produce_range(&h, &topic, 0..200).await;

    let before: BTreeSet<String> = {
        let consumer = group_consumer(&h, &group, &topic);
        let consumed = consume_values(&consumer, 100).await;
        consumer.commit_consumer_state(CommitMode::Sync).unwrap();
        consumed.into_iter().map(|(_, _, value)| value).collect()
    };

    run(&kill);
    eventually(
        "partitions to fail over to node 1",
        Duration::from_secs(90),
        || {
            leaders(&survivor, &topic)
                .values()
                .all(|leader| *leader == 1)
        },
    )
    .await;

    produce_range(&survivor, &topic, 200..400).await;

    let consumer = group_consumer(&survivor, &group, &topic);
    let after: BTreeSet<String> = consume_values(&consumer, 300)
        .await
        .into_iter()
        .map(|(_, _, value)| value)
        .collect();
    drop(consumer);

    assert!(before.is_disjoint(&after), "committed offsets were lost");
    let all: BTreeSet<String> = before.union(&after).cloned().collect();
    assert_eq!(all, (0..400).map(|value| value.to_string()).collect());

    run(&start);
    wait_ready(&second_admin()).await;

    let rejoined = unique("rejoined");
    h.create_topic(&rejoined, 4).await;
    assert!(leaders(&h, &rejoined).values().any(|leader| *leader == 2));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_narrow_token_is_enforced_over_kafka() {
    let h = cluster();
    let topic = unique("narrow");
    h.create_topic(&topic, 1).await;
    produce(&h.producer(), &topic, None, "from-root").await;

    let scope = |read: bool, write: bool| {
        serde_json::json!({
            "streams": [{ "prefix": format!("/{topic}") }],
            "groups": [{ "prefix": "narrow-" }],
            "access": {
                "stream": { "read": read, "write": write },
                "group": { "read": read }
            },
            "audiences": ["kafka"]
        })
    };
    let (reader_id, writer_id) = (unique("it/reader"), unique("it/writer"));
    let reader = h.issue_token(&reader_id, scope(true, false)).await;
    let writer = h.issue_token(&writer_id, scope(false, true)).await;
    tokens_on_every_node(&h, &[&reader_id, &writer_id]).await;

    let reading: StreamConsumer = h
        .kafka_as(&reader)
        .set("group.id", unique("narrow-readers"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(&topic, 0, Offset::Beginning)
        .unwrap();
    reading.assign(&assignment).unwrap();
    let consumed = consume_values(&reading, 1).await;
    assert_eq!(consumed[0].2, "from-root");

    let writing: FutureProducer = h
        .kafka_as(&reader)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();
    let refused = writing
        .send(
            FutureRecord::<(), str>::to(&topic).payload("from-reader"),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert_eq!(
        refused.0.rdkafka_error_code(),
        Some(RDKafkaErrorCode::TopicAuthorizationFailed)
    );

    let producer: FutureProducer = h
        .kafka_as(&writer)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();
    produce(&producer, &topic, None, "from-writer").await;
    let peeking: StreamConsumer = h
        .kafka_as(&writer)
        .set("group.id", unique("narrow-writers"))
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    peeking.assign(&assignment).unwrap();
    let refused = tokio::time::timeout(Duration::from_secs(20), peeking.recv())
        .await
        .expect("the fetch is answered")
        .unwrap_err();
    assert!(
        matches!(
            refused.rdkafka_error_code(),
            Some(RDKafkaErrorCode::TopicAuthorizationFailed)
                | Some(RDKafkaErrorCode::GroupAuthorizationFailed)
        ),
        "a writer was allowed to read: {refused}"
    );
}

/// A producer that retries its last batch after the stream moved to another
/// node is answered as a duplicate, and nothing is stored twice.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn an_http_producer_retry_is_deduplicated_across_a_transfer() {
    let h = cluster();
    let client = pico_client(&h);
    let name = format!("/{}", unique("moved"));
    client
        .create(&name, &CreateOptions::new("text/plain"))
        .await
        .unwrap();
    for seq in 0..5 {
        let producer = ProducerRef {
            id: "w1",
            epoch: 1,
            seq,
        };
        let ack = client
            .append_as(&name, None, &[Bytes::from(format!("r{seq}"))], &producer)
            .await
            .unwrap();
        assert!(ack.applied);
    }

    let from = settled_owner(&h, &name).await;
    let to = 3 - from;
    let accepted = h
        .http
        .post(format!("{}/admin/transfer", first_admin()))
        .bearer_auth(token())
        .json(&serde_json::json!({ "stream": name, "toNode": to }))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202);
    assert_eq!(settled_owner(&h, &name).await, to);

    let retried = ProducerRef {
        id: "w1",
        epoch: 1,
        seq: 4,
    };
    let duplicate = client
        .append_as(&name, None, &[Bytes::from("r4")], &retried)
        .await
        .unwrap();
    assert!(
        duplicate.duplicate,
        "the retried batch must be known after the move"
    );
    let next = ProducerRef {
        id: "w1",
        epoch: 1,
        seq: 5,
    };
    let applied = client
        .append_as(&name, None, &[Bytes::from("r5")], &next)
        .await
        .unwrap();
    assert!(applied.applied);
    let stale = ProducerRef {
        id: "w1",
        epoch: 0,
        seq: 6,
    };
    let fenced = client
        .append_as(&name, None, &[Bytes::from("stale")], &stale)
        .await
        .unwrap_err();
    assert_eq!(fenced.status, 403);

    let expected: Vec<String> = (0..6).map(|i| format!("r{i}")).collect();
    assert_eq!(bodies(&client, &name).await, expected);
}

/// An HTTP producer that keeps writing through a killed owner sees every
/// acknowledged record stored exactly once after the survivor takes over.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn an_http_producer_survives_a_killed_owner_exactly_once() {
    let (Ok(kill), Ok(start)) = (
        std::env::var("PICO_KILL_CMD"),
        std::env::var("PICO_START_CMD"),
    ) else {
        eprintln!("set PICO_KILL_CMD and PICO_START_CMD to run the failover test");
        return;
    };

    let h = cluster();
    let survivor = Harness::new();
    let topic = unique("http-failover");
    h.create_topic(&topic, 4).await;
    let partition = leaders(&h, &topic)
        .into_iter()
        .find(|(_, leader)| *leader == 2)
        .map(|(partition, _)| partition)
        .expect("a partition on node 2");
    let producer = Producer::new(
        Arc::new(pico_client(&h)),
        &format!("/{topic}"),
        "w1",
        ProducerConfig {
            partition: Some(partition as u32),
            retry: RetryPolicy {
                max_attempts: 400,
                initial_backoff: Duration::from_millis(50),
                max_backoff: Duration::from_millis(500),
                multiplier: 2.0,
            },
            ..Default::default()
        },
    );

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut acked = Vec::new();
            while !stop.load(Ordering::SeqCst) {
                let value = acked.len().to_string();
                let seq = producer
                    .send_durable(Bytes::from(value.clone()))
                    .await
                    .unwrap();
                assert_eq!(seq as usize, acked.len(), "positions stay dense");
                acked.push(value);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            producer.close().await.unwrap();
            acked
        })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;

    run(&kill);
    eventually(
        "the partition to fail over to node 1",
        Duration::from_secs(90),
        || leaders(&survivor, &topic)[&partition] == 1,
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::SeqCst);
    let acked = writer.await.unwrap();
    assert!(
        acked.len() > 50,
        "the writer kept going through the failover"
    );

    let read = read_partitions(&survivor, &topic, 4, acked.len()).await;
    assert_exactly_once(&acked, &read);

    run(&start);
    wait_ready(&second_admin()).await;
}

/// Reads `topic` as a group member until stopped, committing as it goes and
/// riding out the errors a failover produces. Returns every value it saw.
fn consume_continuously(
    harness: &Harness,
    group: &str,
    topic: &str,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<(StreamConsumer, Vec<String>)> {
    let consumer = group_consumer(harness, group, topic);
    tokio::spawn(async move {
        let mut seen = Vec::new();
        while !stop.load(Ordering::SeqCst) {
            if let Ok(Ok(message)) =
                tokio::time::timeout(Duration::from_secs(1), consumer.recv()).await
            {
                seen.push(String::from_utf8(message.payload().unwrap().to_vec()).unwrap());
                if seen.len() % 20 == 0 {
                    let _ = consumer.commit_consumer_state(CommitMode::Async);
                }
            }
        }
        (consumer, seen)
    })
}

/// A Kafka producer and a group consumer keep going while the node owning
/// half the partitions, and possibly the group, is killed. Every acknowledged
/// record is stored once and the group ends up committed at the tail.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn kafka_clients_survive_a_killed_coordinator_and_owner() {
    let (Ok(kill), Ok(start)) = (
        std::env::var("PICO_KILL_CMD"),
        std::env::var("PICO_START_CMD"),
    ) else {
        eprintln!("set PICO_KILL_CMD and PICO_START_CMD to run the failover test");
        return;
    };

    let outcome = tokio::spawn(survive_a_killed_coordinator_and_owner(kill)).await;
    run(&start);
    wait_ready(&second_admin()).await;
    if let Err(failure) = outcome {
        std::panic::resume_unwind(failure.into_panic());
    }
}

async fn survive_a_killed_coordinator_and_owner(kill: String) {
    let h = cluster();
    let survivor = Harness::new();
    let topic = unique("kafka-failover");
    let group = unique("kafka-failover-group");
    h.create_topic(&topic, 4).await;
    assert!(
        leaders(&h, &topic).values().any(|leader| *leader == 2),
        "a partition on node 2"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let writer = write_continuously(&survivor, &topic, Arc::clone(&stop));
    let reader = consume_continuously(&survivor, &group, &topic, Arc::clone(&stop));
    tokio::time::sleep(Duration::from_secs(3)).await;

    run(&kill);
    eventually(
        "every partition to fail over to node 1",
        Duration::from_secs(90),
        || {
            leaders(&survivor, &topic)
                .values()
                .all(|leader| *leader == 1)
        },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop.store(true, Ordering::SeqCst);
    let acked = writer.await.unwrap();
    let (consumer, seen) = reader.await.unwrap();
    assert!(
        acked.len() > 100,
        "the writer kept going through the failover"
    );

    let read = read_partitions(&survivor, &topic, 4, acked.len()).await;
    assert_exactly_once(&acked, &read);

    let seen: BTreeSet<String> = seen.into_iter().collect();
    let remaining = consume_values(
        &consumer,
        acked.iter().filter(|value| !seen.contains(*value)).count(),
    )
    .await;
    let seen: BTreeSet<String> = seen
        .into_iter()
        .chain(remaining.into_iter().map(|(_, _, value)| value))
        .collect();
    let unseen: Vec<&String> = acked
        .iter()
        .filter(|value| !seen.contains(*value))
        .collect();
    assert!(unseen.is_empty(), "the group never saw {unseen:?}");

    match consumer.commit_consumer_state(CommitMode::Sync) {
        Ok(()) | Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => {}
        Err(error) => panic!("final commit failed: {error}"),
    }
    let committed = consumer.committed(Duration::from_secs(10)).unwrap();
    for element in committed.elements() {
        let (_, high) = consumer
            .fetch_watermarks(&topic, element.partition(), Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            element.offset(),
            Offset::Offset(high),
            "partition {} is committed at its tail",
            element.partition()
        );
    }
}

/// An idempotent producer keeps writing while one partition is moved to the
/// other node. Nothing is lost and nothing is stored twice.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_kafka_producer_is_deduplicated_across_a_transfer() {
    let h = cluster();
    let topic = unique("kafka-moved");
    h.create_topic(&topic, 2).await;
    let from = leaders(&h, &topic)[&0];
    let to = 3 - from;

    let stop = Arc::new(AtomicBool::new(false));
    let writer = write_continuously(&h, &topic, Arc::clone(&stop));
    tokio::time::sleep(Duration::from_secs(2)).await;

    let accepted = h
        .http
        .post(format!("{}/admin/transfer", first_admin()))
        .bearer_auth(token())
        .json(&serde_json::json!({ "stream": format!("/{topic}"), "partition": 0, "toNode": to }))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202);
    eventually("partition 0 to move", Duration::from_secs(60), || {
        leaders(&h, &topic)[&0] == to
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::SeqCst);
    let acked = writer.await.unwrap();
    assert!(acked.len() > 50, "the writer kept going through the move");

    let read = read_partitions(&h, &topic, 2, acked.len()).await;
    assert_exactly_once(&acked, &read);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn retention_trims_expired_data_in_every_partition() {
    let harness = cluster();
    let topic = unique("retained");
    let created = harness
        .admin()
        .create_topics(
            [&NewTopic::new(&topic, 4, TopicReplication::Fixed(1)).set("retention.ms", "2000")],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    created[0].as_ref().unwrap();
    produce_range(&harness, &topic, 0..20).await;

    let client: BaseConsumer = harness.kafka().create().unwrap();
    eventually("every partition to expire", Duration::from_secs(60), || {
        let watermarks: Vec<_> = (0..4)
            .filter_map(|partition| {
                client
                    .fetch_watermarks(&topic, partition, Duration::from_secs(5))
                    .ok()
            })
            .collect();
        watermarks.len() == 4
            && watermarks.iter().map(|(_, high)| high).sum::<i64>() == 20
            && watermarks.iter().all(|(low, high)| low == high)
    })
    .await;
}
