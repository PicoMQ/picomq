//! End-to-end against a running node (Postgres + object storage).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use picomq_protocol::record::{encode_batch_append, PicoRecord};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::Message;
use serde_json::Value;

const CT_BATCH_BINARY: &str = "application/vnd.picomq.batch";

fn endpoint() -> String {
    std::env::var("PICO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4437".into())
}

fn bootstrap() -> String {
    std::env::var("PICO_KAFKA").unwrap_or_else(|_| "127.0.0.1:9092".into())
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

struct Harness {
    http: reqwest::Client,
    base: String,
    bootstrap: String,
}

impl Harness {
    fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            base: endpoint().trim_end_matches('/').to_owned(),
            bootstrap: bootstrap(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    fn producer(&self) -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("message.timeout.ms", "30000")
            .create()
            .unwrap()
    }

    fn consumer(&self, topic: &str) -> StreamConsumer {
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

    fn admin(&self) -> AdminClient<DefaultClientContext> {
        ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap)
            .create()
            .unwrap()
    }

    async fn create_topic(&self, topic: &str) {
        let results = self
            .admin()
            .create_topics(
                [&NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap();
        results[0].as_ref().unwrap();
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Consumed {
    offset: i64,
    key: Option<Vec<u8>>,
    value: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
}

async fn consume_n(consumer: &StreamConsumer, count: usize) -> Vec<Consumed> {
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

async fn produce(producer: &FutureProducer, topic: &str, key: Option<&str>, value: &str) -> i64 {
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

async fn pico_read_json(h: &Harness, path: &str) -> Vec<Value> {
    let page = h
        .http
        .get(h.url(&format!("{path}?seq=0")))
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), 200, "{path}");
    page.json::<Value>()
        .await
        .unwrap()
        .as_array()
        .unwrap()
        .clone()
}

async fn wait_head_gone(h: &Harness, path: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let status = h.http.head(h.url(path)).send().await.unwrap().status();
        if status == 404 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{path} still present ({status})"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn pico_writes_kafka_reads() {
    let h = Harness::new();
    let name = format!("/e2e/pk-{}", unique("s"));
    let topic = name.trim_start_matches('/').replace('/', ".");

    let created = h
        .http
        .put(h.url(&name))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(created.headers()["Pico-Kafka-Topic"], topic.as_str());

    h.http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .body("plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    h.http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-Key", "order-7")
        .body("keyed")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let batch = encode_batch_append(&[
        PicoRecord::new("one")
            .with_key("k1")
            .with_header("trace", "abc"),
        PicoRecord::new(Bytes::new()),
    ]);
    assert_eq!(
        h.http
            .post(h.url(&name))
            .header("Content-Type", CT_BATCH_BINARY)
            .body(batch.to_vec())
            .send()
            .await
            .unwrap()
            .headers()["Pico-Next-Seq"],
        "4"
    );

    let consumed = consume_n(&h.consumer(&topic), 4).await;
    assert_eq!(consumed[0].value, b"plain");
    assert_eq!(consumed[1].key.as_deref(), Some(b"order-7".as_slice()));
    assert_eq!(consumed[1].value, b"keyed");
    assert_eq!(consumed[2].key.as_deref(), Some(b"k1".as_slice()));
    assert_eq!(consumed[2].headers, vec![("trace".into(), b"abc".to_vec())]);
    assert!(consumed[3].value.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn kafka_writes_pico_reads() {
    let h = Harness::new();
    let topic = unique("e2e-kp");
    h.create_topic(&topic).await;
    let producer = h.producer();
    produce(&producer, &topic, Some("k"), "from-kafka").await;

    let path = format!("/{topic}");
    let records = pico_read_json(&h, &path).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["body"], "from-kafka");
    assert_eq!(records[0]["key"], "k");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn mixed_writers_share_one_log() {
    let h = Harness::new();
    let topic = unique("e2e-mix");
    h.create_topic(&topic).await;
    let path = format!("/{topic}");
    let producer = h.producer();

    produce(&producer, &topic, None, "k0").await;
    h.http
        .post(h.url(&path))
        .header("Content-Type", "application/octet-stream")
        .body("h1")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    produce(&producer, &topic, None, "k2").await;

    let records = pico_read_json(&h, &path).await;
    let bodies: Vec<&str> = records
        .iter()
        .map(|r| r["body"].as_str().unwrap())
        .collect();
    assert_eq!(bodies, ["k0", "h1", "k2"]);

    let consumed = consume_n(&h.consumer(&topic), 3).await;
    let values: Vec<&[u8]> = consumed.iter().map(|c| c.value.as_slice()).collect();
    assert_eq!(values, [&b"k0"[..], b"h1", b"k2"]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn close_blocks_every_writer() {
    let h = Harness::new();
    let name = format!("/e2e/close-{}", unique("s"));
    let topic = name.trim_start_matches('/').replace('/', ".");
    h.http
        .put(h.url(&name))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    h.http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-Closed", "true")
        .body("last")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let late = h
        .http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .body("nope")
        .send()
        .await
        .unwrap();
    assert_eq!(late.status(), 409);

    let started = std::time::Instant::now();
    let rejected = h
        .producer()
        .send(
            FutureRecord::<(), str>::to(&topic).payload("kafka-late"),
            Duration::from_secs(10),
        )
        .await
        .unwrap_err()
        .0;
    assert_eq!(
        rejected.rdkafka_error_code(),
        Some(RDKafkaErrorCode::PolicyViolation)
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "sealed stream must fail fast, took {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn ttl_expires_for_every_protocol() {
    let h = Harness::new();
    let name = format!("/e2e/ttl-{}", unique("s"));
    let topic = name.trim_start_matches('/').replace('/', ".");
    h.http
        .put(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-TTL", "2")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    wait_head_gone(&h, &name).await;

    let metadata = h
        .consumer("_probe")
        .fetch_metadata(Some(&topic), Duration::from_secs(10))
        .unwrap();
    let entry = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .expect("requested topic is echoed back");
    assert_eq!(
        entry.error().map(RDKafkaErrorCode::from),
        Some(RDKafkaErrorCode::UnknownTopicOrPartition)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn absolute_expiry_deletes_the_stream() {
    let h = Harness::new();
    let name = format!("/e2e/exp-{}", unique("s"));
    let expires = SystemTime::now() + Duration::from_secs(3);
    let rfc3339 = humantime::format_rfc3339_seconds(expires).to_string();
    h.http
        .put(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-Expires-At", rfc3339)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    wait_head_gone(&h, &name).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn trim_moves_kafka_log_start() {
    let h = Harness::new();
    let name = format!("/e2e/trim-{}", unique("s"));
    let topic = name.trim_start_matches('/').replace('/', ".");
    h.http
        .put(h.url(&name))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    for body in ["a", "b", "c", "d"] {
        h.http
            .post(h.url(&name))
            .header("Content-Type", "text/plain")
            .body(body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let trimmed = h
            .http
            .post(h.url(&name))
            .header("Pico-Trim-Seq", "2")
            .send()
            .await
            .unwrap();
        assert_eq!(trimmed.status(), 200);
        if trimmed.headers()["Pico-Start-Seq"] == "2" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "trim never committed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let consumed = consume_n(&h.consumer(&topic), 2).await;
    assert_eq!(consumed[0].offset, 2);
    assert_eq!(consumed[0].value, b"c");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn schema_binding_applies_to_every_writer() {
    let h = Harness::new();
    let schema_name = unique("person");
    let schema = r#"{
        "type": "object",
        "properties": {
            "value": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"]
            }
        }
    }"#;
    assert_eq!(
        h.http
            .put(h.url(&format!("/_schemas/{schema_name}")))
            .header("Content-Type", "application/schema+json")
            .body(schema)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    let path = format!("/e2e/schema-{}", unique("s"));
    let topic = path.trim_start_matches('/').replace('/', ".");
    assert_eq!(
        h.http
            .put(h.url(&path))
            .header("Content-Type", "application/json")
            .header("Pico-Schema", schema_name.as_str())
            .header("Pico-Schema-Validate", "true")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );

    let producer = h.producer();
    assert_eq!(
        produce(&producer, &topic, None, r#"{"name":"alice"}"#).await,
        0
    );
    let rejected = producer
        .send(
            FutureRecord::to(&topic).payload(r#"{"name":1}"#).key("bad"),
            Duration::from_secs(15),
        )
        .await
        .unwrap_err()
        .0;
    assert_eq!(
        rejected.rdkafka_error_code(),
        Some(RDKafkaErrorCode::InvalidRecord)
    );

    let rejected = h
        .http
        .post(h.url(&path))
        .header("Content-Type", "application/json")
        .body(r#"{"name":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 400);
    let body: Value = rejected.json().await.unwrap();
    assert_eq!(body["error"], "schema_violation");
    assert_eq!(pico_read_json(&h, &path).await.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn http_producer_gap_and_fence() {
    let h = Harness::new();
    let name = format!("/e2e/prod-{}", unique("s"));
    h.http
        .put(h.url(&name))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let ok = h
        .http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-Producer-Id", "w1")
        .header("Pico-Producer-Epoch", "1")
        .header("Pico-Producer-Seq", "0")
        .body("first")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);

    let gap = h
        .http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-Producer-Id", "w1")
        .header("Pico-Producer-Epoch", "1")
        .header("Pico-Producer-Seq", "2")
        .body("skip")
        .send()
        .await
        .unwrap();
    assert_eq!(gap.status(), 409);
    let body: Value = gap.json().await.unwrap();
    assert_eq!(body["error"], "sequence_gap");

    let fence = h
        .http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-Producer-Id", "w1")
        .header("Pico-Producer-Epoch", "0")
        .header("Pico-Producer-Seq", "1")
        .body("stale")
        .send()
        .await
        .unwrap();
    assert_eq!(fence.status(), 403);
    let body: Value = fence.json().await.unwrap();
    assert_eq!(body["error"], "fenced");

    let next = h
        .http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-Producer-Id", "w1")
        .header("Pico-Producer-Epoch", "1")
        .header("Pico-Producer-Seq", "1")
        .body("second")
        .send()
        .await
        .unwrap();
    assert_eq!(next.status(), 200);
    assert_eq!(pico_read_json(&h, &name).await.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn concurrent_http_producers_do_not_collide() {
    let h = Harness::new();
    let name = format!("/e2e/mp-{}", unique("s"));
    h.http
        .put(h.url(&name))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let writers = 8u32;
    let each = 25u32;
    let mut tasks = Vec::new();
    for w in 0..writers {
        let h = Harness::new();
        let name = name.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..each {
                let response = h
                    .http
                    .post(h.url(&name))
                    .header("Content-Type", "text/plain")
                    .header("Pico-Producer-Id", format!("w{w}"))
                    .header("Pico-Producer-Epoch", "0")
                    .header("Pico-Producer-Seq", i.to_string())
                    .body(format!("w{w}-{i}"))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), 200, "writer {w} seq {i}");
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let records = pico_read_json(&h, &name).await;
    assert_eq!(records.len(), (writers * each) as usize);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn concurrent_kafka_producers_keep_offsets_dense() {
    let h = Harness::new();
    let topic = unique("e2e-kmp");
    h.create_topic(&topic).await;
    let each = 40;
    let left = h.producer();
    let right = h.producer();
    let topic_l = topic.clone();
    let topic_r = topic.clone();
    let (a, b) = tokio::join!(
        async move {
            for i in 0..each {
                produce(&left, &topic_l, None, &format!("a{i}")).await;
            }
        },
        async move {
            for i in 0..each {
                produce(&right, &topic_r, None, &format!("b{i}")).await;
            }
        }
    );
    let _ = (a, b);
    let consumed = consume_n(&h.consumer(&topic), each * 2).await;
    let offsets: Vec<i64> = consumed.iter().map(|c| c.offset).collect();
    let expected: Vec<i64> = (0..(each * 2) as i64).collect();
    assert_eq!(offsets, expected);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn match_seq_is_http_only_and_does_not_break_kafka() {
    let h = Harness::new();
    let name = format!("/e2e/match-{}", unique("s"));
    let topic = name.trim_start_matches('/').replace('/', ".");
    h.http
        .put(h.url(&name))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let miss = h
        .http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-Match-Seq", "3")
        .body("no")
        .send()
        .await
        .unwrap();
    assert_eq!(miss.status(), 412);
    produce(&h.producer(), &topic, None, "k").await;
    let hit = h
        .http
        .post(h.url(&name))
        .header("Content-Type", "text/plain")
        .header("Pico-Match-Seq", "1")
        .body("yes")
        .send()
        .await
        .unwrap();
    assert_eq!(hit.status(), 200);
    assert_eq!(pico_read_json(&h, &name).await.len(), 2);
}
