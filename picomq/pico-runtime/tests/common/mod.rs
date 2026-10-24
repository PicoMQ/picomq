//! Shared harness for the docker end-to-end suites. Each suite uses a
//! subset of it.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};

pub fn endpoint() -> String {
    std::env::var("PICO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4437".into())
}

pub fn bootstrap() -> String {
    std::env::var("PICO_KAFKA").unwrap_or_else(|_| "127.0.0.1:9092".into())
}

const TOKEN: &str = "ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY";

pub fn token() -> String {
    std::env::var("PICO_TOKEN").unwrap_or_else(|_| TOKEN.into())
}

fn auth_required() -> bool {
    std::env::var("PICO_AUTH").is_ok_and(|mode| mode == "required")
}

pub fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

pub struct Harness {
    pub http: reqwest::Client,
    pub base: String,
    pub bootstrap: String,
}

impl Harness {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            base: endpoint().trim_end_matches('/').to_owned(),
            bootstrap: bootstrap(),
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    pub fn kafka(&self) -> ClientConfig {
        if auth_required() {
            return self.kafka_as(&token());
        }

        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap);
        config
    }

    pub fn kafka_as(&self, token: &str) -> ClientConfig {
        let token = picomq_auth::AccessToken::parse(token).unwrap();
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap)
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "PLAIN")
            .set("sasl.username", &token.id)
            .set("sasl.password", token.render());
        config
    }

    pub async fn issue_token(&self, id: &str, scope: serde_json::Value) -> String {
        let issued: serde_json::Value = self
            .http
            .post(format!("{}/admin/tokens", first_admin()))
            .bearer_auth(token())
            .json(&serde_json::json!({ "id": id, "scope": scope }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        issued["token"].as_str().unwrap().to_owned()
    }

    pub fn producer(&self) -> FutureProducer {
        self.kafka()
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("message.timeout.ms", "30000")
            .create()
            .unwrap()
    }

    pub fn consumer(&self, topic: &str) -> StreamConsumer {
        let consumer: StreamConsumer = self
            .kafka()
            .set("group.id", format!("e2e-{topic}-{}", unique("g")))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        let mut assignment = rdkafka::TopicPartitionList::new();
        assignment
            .add_partition_offset(topic, 0, rdkafka::Offset::Beginning)
            .unwrap();
        consumer.assign(&assignment).unwrap();
        consumer
    }

    pub fn admin(&self) -> AdminClient<DefaultClientContext> {
        self.kafka().create().unwrap()
    }

    pub async fn create_topic(&self, topic: &str, partitions: i32) {
        let results = self
            .admin()
            .create_topics(
                [&NewTopic::new(
                    topic,
                    partitions,
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::new(),
            )
            .await
            .unwrap();
        results[0].as_ref().unwrap();
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Consumed {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

pub async fn consume_n(consumer: &StreamConsumer, count: usize) -> Vec<Consumed> {
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("consume timed out")
            .unwrap();
        let headers = message
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|h| (h.key.to_owned(), h.value.unwrap_or_default().to_vec()))
                    .collect()
            })
            .unwrap_or_default();
        out.push(Consumed {
            offset: message.offset(),
            key: message.key().map(<[u8]>::to_vec),
            value: message.payload().unwrap_or_default().to_vec(),
            headers,
        });
    }
    out
}

pub async fn produce(
    producer: &FutureProducer,
    topic: &str,
    key: Option<&str>,
    value: &str,
) -> i64 {
    let mut record = FutureRecord::to(topic).payload(value);
    if let Some(key) = key {
        record = record.key(key);
    }
    producer
        .send(record, Duration::from_secs(20))
        .await
        .unwrap()
        .offset
}

pub fn assert_exactly_once(acked: &[String], read: &[String]) {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for value in read {
        *counts.entry(value.as_str()).or_default() += 1;
    }

    let lost: Vec<&String> = acked
        .iter()
        .filter(|value| !counts.contains_key(value.as_str()))
        .collect();
    let doubled: Vec<&&str> = counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(value, _)| value)
        .collect();
    assert!(lost.is_empty(), "acknowledged writes lost: {lost:?}");
    assert!(doubled.is_empty(), "writes stored twice: {doubled:?}");
}

pub fn first_admin() -> String {
    std::env::var("PICO_ADMIN").unwrap_or_else(|_| "http://127.0.0.1:9090".into())
}

pub fn run(command: &str) {
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .status()
        .unwrap();
    assert!(status.success(), "{command} failed");
}

pub async fn wait_ready(admin: &str) {
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let ready = http
            .get(format!("{admin}/ready"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        if ready {
            return;
        }
        assert!(Instant::now() < deadline, "{admin} never became ready");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Writes `0..` to `topic` until stopped, returning every acknowledged value.
pub fn write_continuously(
    harness: &Harness,
    topic: &str,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<Vec<String>> {
    let producer: FutureProducer = harness
        .kafka()
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "120000")
        .create()
        .unwrap();
    let topic = topic.to_owned();

    tokio::spawn(async move {
        let acked = Arc::new(Mutex::new(Vec::new()));
        let mut inflight = tokio::task::JoinSet::new();
        let mut value = 0u64;

        while !stop.load(Ordering::SeqCst) {
            let (producer, topic, acked) = (producer.clone(), topic.clone(), acked.clone());
            let payload = value.to_string();
            inflight.spawn(async move {
                let record = FutureRecord::to(&topic).key(&payload).payload(&payload);
                if producer
                    .send(record, Duration::from_secs(120))
                    .await
                    .is_ok()
                {
                    acked.lock().unwrap().push(payload);
                }
            });
            value += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        while inflight.join_next().await.is_some() {}
        Arc::try_unwrap(acked).unwrap().into_inner().unwrap()
    })
}

/// Every value in the first `partitions` of `topic`, read from the beginning.
pub async fn read_partitions(
    harness: &Harness,
    topic: &str,
    partitions: i32,
    expected: usize,
) -> Vec<String> {
    let consumer: StreamConsumer = harness
        .kafka()
        .set("group.id", unique("reader"))
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut assignment = TopicPartitionList::new();
    for partition in 0..partitions {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .unwrap();
    }
    consumer.assign(&assignment).unwrap();

    let mut values = Vec::new();
    while values.len() < expected {
        match tokio::time::timeout(Duration::from_secs(20), consumer.recv()).await {
            Ok(message) => {
                let message = message.unwrap();
                values.push(String::from_utf8(message.payload().unwrap().to_vec()).unwrap());
            }
            Err(_) => break,
        }
    }
    values
}
