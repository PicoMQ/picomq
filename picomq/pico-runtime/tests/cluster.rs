//! End-to-end against a two-node cluster (Postgres + object storage).

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use common::{Harness, produce, unique};
use rdkafka::admin::{AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::FutureRecord;
use rdkafka::{Message, Offset, TopicPartitionList};

const TOKEN: &str = "ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY";

fn second_bootstrap() -> String {
    std::env::var("PICO_KAFKA_2").unwrap_or_else(|_| "127.0.0.1:9093".into())
}

fn second_admin() -> String {
    std::env::var("PICO_ADMIN_2").unwrap_or_else(|_| "http://127.0.0.1:9091".into())
}

fn token() -> String {
    std::env::var("PICO_TOKEN").unwrap_or_else(|_| TOKEN.into())
}

fn cluster() -> Harness {
    let harness = Harness::new();
    Harness {
        bootstrap: format!("{},{}", harness.bootstrap, second_bootstrap()),
        ..harness
    }
}

fn group_consumer(bootstrap: &str, group: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000")
        .create()
        .unwrap();
    consumer.subscribe(&[topic]).unwrap();
    consumer
}

fn leaders(bootstrap: &str, topic: &str) -> BTreeMap<i32, i32> {
    let client: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
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

fn run(command: &str) {
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .status()
        .unwrap();
    assert!(status.success(), "{command} failed");
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
    assert_eq!(leaders(&Harness::new().bootstrap, &topic), expected);
    assert_eq!(leaders(&second_bootstrap(), &topic), expected);

    produce_range(&Harness::new(), &topic, 0..300).await;

    let consumer = group_consumer(&second_bootstrap(), &unique("spread"), &topic);
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

    let leaders = leaders(&h.bootstrap, &topic);
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
        let consumer = group_consumer(&h.bootstrap, &group, &topic);
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
        std::env::var("PICOMQ_KILL_CMD"),
        std::env::var("PICOMQ_START_CMD"),
    ) else {
        eprintln!("set PICOMQ_KILL_CMD and PICOMQ_START_CMD to run the failover test");
        return;
    };

    let h = cluster();
    let survivor = Harness::new();
    let topic = unique("failover");
    let group = unique("failover-group");
    h.create_topic(&topic, 4).await;
    produce_range(&h, &topic, 0..200).await;

    let before: BTreeSet<String> = {
        let consumer = group_consumer(&h.bootstrap, &group, &topic);
        let consumed = consume_values(&consumer, 100).await;
        consumer.commit_consumer_state(CommitMode::Sync).unwrap();
        consumed.into_iter().map(|(_, _, value)| value).collect()
    };

    run(&kill);
    eventually(
        "partitions to fail over to node 1",
        Duration::from_secs(90),
        || {
            leaders(&survivor.bootstrap, &topic)
                .values()
                .all(|leader| *leader == 1)
        },
    )
    .await;

    produce_range(&survivor, &topic, 200..400).await;

    let consumer = group_consumer(&survivor.bootstrap, &group, &topic);
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
    eventually("node 2 to come back", Duration::from_secs(90), || {
        std::process::Command::new("curl")
            .args([
                "-sf",
                "-o",
                "/dev/null",
                &format!("{}/ready", second_admin()),
            ])
            .status()
            .is_ok_and(|status| status.success())
    })
    .await;

    let rejoined = unique("rejoined");
    h.create_topic(&rejoined, 4).await;
    assert!(
        leaders(&h.bootstrap, &rejoined)
            .values()
            .any(|leader| *leader == 2)
    );
}
