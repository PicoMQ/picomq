//! Shared harness for the docker end-to-end suites. Each suite uses a
//! subset of it.

#![allow(dead_code)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers as _;
use rdkafka::producer::{FutureProducer, FutureRecord};

pub fn endpoint() -> String {
    std::env::var("PICO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4437".into())
}

pub fn bootstrap() -> String {
    std::env::var("PICO_KAFKA").unwrap_or_else(|_| "127.0.0.1:9092".into())
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

    pub fn producer(&self) -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("message.timeout.ms", "30000")
            .create()
            .unwrap()
    }

    pub fn consumer(&self, topic: &str) -> StreamConsumer {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap)
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
        ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap)
            .create()
            .unwrap()
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
