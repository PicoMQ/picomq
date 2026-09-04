//! Cross-protocol reads and writes against one shared log.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use picomq_auth::AccessToken;
use picomq_http::HttpProtocol;
use picomq_protocol::record::{PicoRecord, decode_batch_read, encode_batch_append};
use picomq_runtime::{AuthMode, KafkaConfig, MetaBackend, PicoServer, ServerConfig};
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use serde_json::Value;

const CT_BATCH_BINARY: &str = "application/vnd.picomq.batch";

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn config(dir: &Path, protocol: HttpProtocol, node_epoch: i64, kafka: bool) -> ServerConfig {
    ServerConfig {
        node_epoch,
        addr: loopback(),
        admin_addr: Some(loopback()),
        http_protocol: protocol,
        kafka: kafka.then(|| KafkaConfig {
            listen: loopback(),
            advertise: None,
        }),
        meta_backend: MetaBackend::parse(&format!("sqlite:{}", dir.join("meta.db").display()))
            .unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!(
            "2@file://{}?batchInterval=5",
            dir.join("wal").display()
        )),
        schema_registry: Some(format!("3@file://{}", dir.join("schemas").display())),
        engine: s3stream::Config {
            wal_upload_interval_ms: 200,
            ..Default::default()
        },
        ..Default::default()
    }
}

struct Node {
    server: PicoServer,
    http: reqwest::Client,
    base: String,
    bootstrap: Option<String>,
}

impl Node {
    async fn start(dir: &Path, protocol: HttpProtocol, epoch: i64, kafka: bool) -> Self {
        let server = picomq_runtime::start(config(dir, protocol, epoch, kafka))
            .await
            .unwrap();
        Self {
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            base: format!("http://{}", server.local_addr()),
            bootstrap: server.kafka_addr().map(|a| a.to_string()),
            server,
        }
    }

    fn bootstrap(&self) -> &str {
        self.bootstrap.as_deref().expect("kafka listener enabled")
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    fn producer(&self) -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", self.bootstrap())
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "15000")
            .create()
            .unwrap()
    }

    fn consumer_at(&self, topic: &str, offset: rdkafka::Offset) -> StreamConsumer {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.bootstrap())
            .set("group.id", format!("g-{topic}-{}", rand_suffix()))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        let mut assignment = rdkafka::TopicPartitionList::new();
        assignment.add_partition_offset(topic, 0, offset).unwrap();
        consumer.assign(&assignment).unwrap();
        consumer
    }

    fn consumer(&self, topic: &str) -> StreamConsumer {
        self.consumer_at(topic, rdkafka::Offset::Beginning)
    }

    fn admin(&self) -> AdminClient<DefaultClientContext> {
        ClientConfig::new()
            .set("bootstrap.servers", self.bootstrap())
            .create()
            .unwrap()
    }

    async fn create_topic(&self, topic: &str) -> Result<(), RDKafkaErrorCode> {
        let results = self
            .admin()
            .create_topics(
                [&NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap();
        results[0].as_ref().map(|_| ()).map_err(|(_, code)| *code)
    }

    async fn topics(&self) -> Vec<String> {
        let metadata = self
            .consumer("_probe")
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        let mut names: Vec<String> = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_owned())
            .collect();
        names.sort();
        names
    }

    async fn shutdown(self) {
        self.server.shutdown().await;
    }
}

fn rand_suffix() -> String {
    format!("{}", std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos())
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
        .send(record, Duration::from_secs(15))
        .await
        .unwrap()
        .offset
}

async fn pico_read_json(node: &Node, path: &str) -> Vec<Value> {
    let page = node
        .http
        .get(node.url(&format!("{path}?seq=0")))
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

#[tokio::test(flavor = "multi_thread")]
async fn pico_writes_kafka_reads() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(dir.path(), HttpProtocol::Pico, 1, true).await;

    let created = node
        .http
        .put(node.url("/orders/eu"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(created.headers()["Pico-Kafka-Topic"], "orders.eu");

    node.http
        .post(node.url("/orders/eu"))
        .header("Content-Type", "text/plain")
        .body("plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    node.http
        .post(node.url("/orders/eu"))
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
            .with_header("trace", "abc")
            .with_header("bin", Bytes::from_static(&[0xff, 0x00])),
        PicoRecord::new(Bytes::new()),
    ]);
    let appended = node
        .http
        .post(node.url("/orders/eu"))
        .header("Content-Type", CT_BATCH_BINARY)
        .body(batch.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(appended.status(), 200);
    assert_eq!(appended.headers()["Pico-Next-Seq"], "4");

    assert!(node.topics().await.contains(&"orders.eu".to_owned()));
    let consumed = consume_n(&node.consumer("orders.eu"), 4).await;
    assert_eq!(
        consumed,
        vec![
            Consumed {
                offset: 0,
                key: None,
                value: b"plain".to_vec(),
                headers: vec![],
            },
            Consumed {
                offset: 1,
                key: Some(b"order-7".to_vec()),
                value: b"keyed".to_vec(),
                headers: vec![],
            },
            Consumed {
                offset: 2,
                key: Some(b"k1".to_vec()),
                value: b"one".to_vec(),
                headers: vec![
                    ("trace".to_owned(), b"abc".to_vec()),
                    ("bin".to_owned(), vec![0xff, 0x00]),
                ],
            },
            Consumed {
                offset: 3,
                key: None,
                value: Vec::new(),
                headers: vec![],
            },
        ]
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let trimmed = node
            .http
            .post(node.url("/orders/eu"))
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
    let after_trim = consume_n(&node.consumer("orders.eu"), 2).await;
    assert_eq!(after_trim[0].offset, 2);
    assert_eq!(after_trim[0].value, b"one");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_writes_pico_reads() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(dir.path(), HttpProtocol::Pico, 1, true).await;
    node.create_topic("events").await.unwrap();

    let producer = node.producer();
    assert_eq!(produce(&producer, "events", Some("a"), "first").await, 0);
    assert_eq!(produce(&producer, "events", None, "second").await, 1);
    assert_eq!(
        produce(&producer, "events", Some("c"), r#"{"n":3}"#).await,
        2
    );

    let head = node.http.head(node.url("/events")).send().await.unwrap();
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers()["Pico-Next-Seq"], "3");
    assert_eq!(head.headers()["Pico-Kafka-Topic"], "events");

    let records = pico_read_json(&node, "/events").await;
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["seq"], 0);
    assert_eq!(records[0]["key"], "a");
    assert_eq!(records[0]["body"], "first");
    assert!(records[0]["timestamp"].as_i64().unwrap() > 1_600_000_000_000);
    assert!(records[1].get("key").is_none());
    assert_eq!(records[1]["body"], "second");
    assert_eq!(records[2]["body"], r#"{"n":3}"#);

    let binary = node
        .http
        .get(node.url("/events?seq=1&format=binary"))
        .send()
        .await
        .unwrap();
    assert_eq!(binary.headers()["Content-Type"], CT_BATCH_BINARY);
    let decoded = decode_batch_read(&binary.bytes().await.unwrap()).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].seq, 1);
    assert_eq!(decoded[1].record.key.as_deref(), Some(&b"c"[..]));

    let raw = node
        .http
        .get(node.url("/events?seq=0&format=raw"))
        .send()
        .await
        .unwrap();
    assert_eq!(&raw.bytes().await.unwrap()[..], b"firstsecond{\"n\":3}");

    let listing: Value = node
        .http
        .get(node.url("/?prefix=/"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = listing["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"/events"), "{names:?}");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ds_and_kafka_share_a_json_stream() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(dir.path(), HttpProtocol::Ds, 1, true).await;

    let created = node
        .http
        .put(node.url("/feed"))
        .header("Content-Type", "application/json")
        .body(r#"[{"id":1},{"id":2}]"#)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(
        created.headers()["Stream-Next-Offset"],
        "00000000000000000002"
    );

    node.http
        .post(node.url("/feed"))
        .header("Content-Type", "application/json")
        .body(r#"{"id":3}"#)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let consumed = consume_n(&node.consumer("feed"), 3).await;
    let values: Vec<&[u8]> = consumed.iter().map(|c| c.value.as_slice()).collect();
    assert_eq!(values, [&b"{\"id\":1}"[..], b"{\"id\":2}", b"{\"id\":3}"]);
    assert!(consumed.iter().all(|c| c.key.is_none()));

    produce(&node.producer(), "feed", Some("k"), r#"{"id":4}"#).await;

    let page = node
        .http
        .get(node.url("/feed?offset=-1"))
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), 200);
    assert_eq!(page.headers()["Stream-Next-Offset"], "00000000000000000004");
    assert_eq!(page.headers()["Content-Type"], "application/json");
    let body: Value = page.json().await.unwrap();
    assert_eq!(
        body,
        serde_json::json!([{"id":1},{"id":2},{"id":3},{"id":4}])
    );

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_writers_share_one_offset_space() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(dir.path(), HttpProtocol::Pico, 1, true).await;
    node.http
        .put(node.url("/mixed"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let producer = node.producer();

    let mut expected = Vec::new();
    for i in 0..10 {
        let value = format!("v{i}");
        if i % 2 == 0 {
            let appended = node
                .http
                .post(node.url("/mixed"))
                .header("Content-Type", "text/plain")
                .body(value.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(appended.headers()["Pico-Start-Seq"], i.to_string());
        } else {
            assert_eq!(produce(&producer, "mixed", None, &value).await, i as i64);
        }
        expected.push(value);
    }

    let consumed = consume_n(&node.consumer("mixed"), 10).await;
    let kafka_values: Vec<String> = consumed
        .iter()
        .map(|c| String::from_utf8(c.value.clone()).unwrap())
        .collect();
    assert_eq!(kafka_values, expected);
    assert!(
        consumed
            .iter()
            .enumerate()
            .all(|(i, c)| c.offset == i as i64)
    );

    let records = pico_read_json(&node, "/mixed").await;
    let pico_values: Vec<&str> = records
        .iter()
        .map(|r| r["body"].as_str().unwrap())
        .collect();
    assert_eq!(pico_values, expected);
    let timestamps: Vec<i64> = records
        .iter()
        .map(|r| r["timestamp"].as_i64().unwrap())
        .collect();
    assert!(timestamps.iter().all(|t| *t > 1_600_000_000_000));

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_alias_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(dir.path(), HttpProtocol::Pico, 1, true).await;

    let created = node
        .http
        .put(node.url("/tenant:1/orders"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert!(created.headers().get("Pico-Kafka-Topic").is_none());
    assert!(!node.topics().await.iter().any(|t| t.contains("tenant")));

    let config: Value = node
        .http
        .patch(node.url("/_streams/tenant:1/orders"))
        .json(&serde_json::json!({"kafkaTopic": "tenant1.orders"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(config["kafkaTopic"], "tenant1.orders");

    produce(&node.producer(), "tenant1.orders", None, "hello").await;
    let records = pico_read_json(&node, "/tenant:1/orders").await;
    assert_eq!(records[0]["body"], "hello");

    let clash = node
        .http
        .put(node.url("/other"))
        .header("Content-Type", "text/plain")
        .header("Pico-Kafka-Topic", "tenant1.orders")
        .send()
        .await
        .unwrap();
    assert_eq!(clash.status(), 409);
    assert!(
        node.http
            .head(node.url("/other"))
            .send()
            .await
            .unwrap()
            .status()
            == 404
    );

    node.http
        .put(node.url("/orders/eu"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        node.create_topic("orders.eu").await,
        Err(RDKafkaErrorCode::TopicAlreadyExists)
    );

    let cleared: Value = node
        .http
        .patch(node.url("/_streams/tenant:1/orders"))
        .json(&serde_json::json!({"kafkaTopic": null}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cleared["kafkaTopic"], Value::Null);
    assert!(!node.topics().await.contains(&"tenant1.orders".to_owned()));

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_binding_applies_to_every_writer() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(dir.path(), HttpProtocol::Pico, 1, true).await;
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
        node.http
            .put(node.url("/_schemas/person"))
            .header("Content-Type", "application/schema+json")
            .body(schema)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        node.http
            .put(node.url("/people"))
            .header("Content-Type", "application/json")
            .header("Pico-Schema", "person")
            .header("Pico-Schema-Validate", "true")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );

    let producer = node.producer();
    assert_eq!(
        produce(&producer, "people", None, r#"{"name":"alice"}"#).await,
        0
    );
    let rejected = producer
        .send(
            FutureRecord::to("people")
                .payload(r#"{"name":1}"#)
                .key("bad"),
            Duration::from_secs(15),
        )
        .await
        .unwrap_err()
        .0;
    assert_eq!(
        rejected.rdkafka_error_code(),
        Some(RDKafkaErrorCode::InvalidRecord)
    );

    let rejected = node
        .http
        .post(node.url("/people"))
        .header("Content-Type", "application/json")
        .body(r#"{"name":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 400);
    let body: Value = rejected.json().await.unwrap();
    assert_eq!(body["error"], "schema_violation");

    let records = pico_read_json(&node, "/people").await;
    assert_eq!(records.len(), 1);

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_gates_http_while_kafka_reads_the_same_stream() {
    let dir = tempfile::tempdir().unwrap();
    let (root, _) = AccessToken::issue("ops/root").unwrap();
    let mut config = config(dir.path(), HttpProtocol::Pico, 1, true);
    config.auth_mode = AuthMode::Required;
    config.bootstrap_token = Some(root.render());
    let server = picomq_runtime::start(config).await.unwrap();
    let node = Node {
        http: reqwest::Client::new(),
        base: format!("http://{}", server.local_addr()),
        bootstrap: server.kafka_addr().map(|a| a.to_string()),
        server,
    };

    let anonymous = node
        .http
        .put(node.url("/secure"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 401);
    node.http
        .put(node.url("/secure"))
        .header("Content-Type", "text/plain")
        .bearer_auth(root.render())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    node.http
        .post(node.url("/secure"))
        .header("Content-Type", "text/plain")
        .bearer_auth(root.render())
        .body("via-http")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    produce(&node.producer(), "secure", None, "via-kafka").await;
    let consumed = consume_n(&node.consumer("secure"), 2).await;
    assert_eq!(consumed[0].value, b"via-http");
    assert_eq!(consumed[1].value, b"via-kafka");

    let anonymous_read = node
        .http
        .get(node.url("/secure?seq=0"))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous_read.status(), 401);

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_listener_can_be_enabled_after_the_fact() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(dir.path(), HttpProtocol::Pico, 1, false).await;
    assert!(node.bootstrap.is_none());
    node.http
        .put(node.url("/late/bloomer"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    for i in 0..3 {
        node.http
            .post(node.url("/late/bloomer"))
            .header("Content-Type", "text/plain")
            .body(format!("r{i}"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    node.shutdown().await;

    let node = Node::start(dir.path(), HttpProtocol::Pico, 2, true).await;
    assert!(node.topics().await.contains(&"late.bloomer".to_owned()));
    let consumed = consume_n(&node.consumer("late.bloomer"), 3).await;
    let values: Vec<&[u8]> = consumed.iter().map(|c| c.value.as_slice()).collect();
    assert_eq!(values, [&b"r0"[..], b"r1", b"r2"]);

    produce(&node.producer(), "late.bloomer", None, "r3").await;
    node.shutdown().await;

    let node = Node::start(dir.path(), HttpProtocol::Pico, 3, false).await;
    let records = pico_read_json(&node, "/late/bloomer").await;
    assert_eq!(records.len(), 4);
    assert_eq!(records[3]["body"], "r3");
    node.shutdown().await;
}
