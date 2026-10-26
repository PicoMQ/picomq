//! Pico protocol integration tests over a loopback node.
//!
//! Requests are spelled out with `reqwest` rather than the client crate so
//! the wire shapes stay visible. Headers use the `Pico-*` prefix.

mod common;

use std::time::Duration;

use bytes::Bytes;
use picomq_protocol::record::{PicoRecord, decode_batch_read, encode_batch_append};
use picomq_server::ownership::OwnershipService;
use picomq_server::{CreateCommand, Log, NodeMeta, Owner, ServiceError};
use serde_json::Value;

use common::picomq_server;

const CT_BATCH_JSON: &str = "application/vnd.picomq.batch+json";
const CT_BATCH_BINARY: &str = "application/vnd.picomq.batch";

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// Names in the `/_` namespace and paths that do not decode to text are
/// refused before anything is created.
#[tokio::test]
async fn reserved_and_malformed_names_are_refused() {
    let server = picomq_server().await;
    let http = client();

    for path in ["/_groups/a/b/c", "/_other/x", "/_p/1/0", "/a%ff"] {
        let refused = http
            .put(format!("{}{path}", server.base_url))
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), 400, "{path}");
    }
    let listed = http
        .get(format!("{}/?prefix=%ff", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), 400);

    let created = http
        .put(format!("{}/orders/_internal", server.base_url))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
}

/// (createSemantics +
/// appendAndRead + matchSeq + producerSession + listTrimCloseDelete).
#[tokio::test]
async fn protocol_end_to_end() {
    let server = picomq_server().await;
    let http = client();
    let url = format!("{}/native/orders", server.base_url);

    let created = http
        .put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(created.headers()["Location"], "/native/orders");
    assert_eq!(created.headers()["Pico-Next-Seq"], "0");

    let again = http
        .put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 200);

    let clash = http
        .put(&url)
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(clash.status(), 409);
    let body: Value = clash.json().await.unwrap();
    assert_eq!(body["error"], "conflict");

    let raw = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body("hello-world")
        .send()
        .await
        .unwrap();
    assert_eq!(raw.status(), 200);
    assert_eq!(raw.headers()["Pico-Start-Seq"], "0");
    assert_eq!(raw.headers()["Pico-Next-Seq"], "1");
    let raw_timestamp: i64 = raw.headers()["Pico-Timestamp"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(raw_timestamp > 0);

    let batch = encode_batch_append(&[
        PicoRecord::new(Bytes::from_static(b"one"))
            .with_key("order-1")
            .with_header("key", "a"),
        PicoRecord::new(Bytes::from_static(b"two")),
    ]);
    let batched = http
        .post(&url)
        .header("Content-Type", CT_BATCH_BINARY)
        .body(batch.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(batched.status(), 200);
    assert_eq!(batched.headers()["Pico-Start-Seq"], "1");
    assert_eq!(batched.headers()["Pico-Next-Seq"], "3");
    let batch_timestamp: i64 = batched.headers()["Pico-Timestamp"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(batch_timestamp >= raw_timestamp);

    // JSON batch append with a base64 body.
    let json = http
        .post(&url)
        .header("Content-Type", CT_BATCH_JSON)
        .body(r#"{"records":[{"headers":{"k":"v"},"body":"three"},{"body_b64":"AAECgP8="}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(json.status(), 200);
    assert_eq!(json.headers()["Pico-Start-Seq"], "3");
    assert_eq!(json.headers()["Pico-Next-Seq"], "5");

    // Head: user content type and tail.
    let head = http.head(&url).send().await.unwrap();
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers()["Pico-Next-Seq"], "5");
    assert_eq!(head.headers()["Content-Type"], "text/plain");
    assert_eq!(head.headers()["Pico-Start-Seq"], "0");

    let page = http.get(format!("{url}?seq=0")).send().await.unwrap();
    assert_eq!(page.status(), 200);
    assert_eq!(page.headers()["Content-Type"], "application/json");
    assert_eq!(page.headers()["Pico-Next-Seq"], "5");
    assert_eq!(page.headers()["Pico-Up-To-Date"], "true");
    let etag = page.headers()["ETag"].to_str().unwrap().to_owned();
    let records: Value = page.json().await.unwrap();
    let records = records.as_array().unwrap();
    assert_eq!(records.len(), 5);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record["seq"], i as u64);
    }
    assert_eq!(records[1]["headers"]["key"], "a");
    assert_eq!(records[1]["key"], "order-1");
    assert!(records[2].get("key").is_none());
    assert_eq!(records[3]["body"], "three");
    let mut previous = 0;
    for record in records {
        let timestamp = record["timestamp"].as_i64().unwrap();
        assert!(timestamp >= previous);
        previous = timestamp;
    }

    let binary = http
        .get(format!("{url}?seq=0&format=binary"))
        .send()
        .await
        .unwrap();
    assert_eq!(binary.headers()["Content-Type"], CT_BATCH_BINARY);
    let decoded = decode_batch_read(&binary.bytes().await.unwrap()).unwrap();
    assert_eq!(decoded.len(), 5);
    assert_eq!(&decoded[0].record.body[..], b"hello-world");
    assert_eq!(decoded[1].record.key.as_deref(), Some(&b"order-1"[..]));

    let raw_read = http
        .get(format!("{url}?seq=0&format=raw"))
        .send()
        .await
        .unwrap();
    assert_eq!(raw_read.headers()["Content-Type"], "text/plain");
    let raw_body = raw_read.bytes().await.unwrap();
    assert!(raw_body.starts_with(b"hello-worldonetwothree"));

    // Conditional read: same etag answers 304.
    let cached = http
        .get(format!("{url}?seq=0"))
        .header("If-None-Match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(cached.status(), 304);

    let cas = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .header("Pico-Match-Seq", "5")
        .body("cas")
        .send()
        .await
        .unwrap();
    assert_eq!(cas.status(), 200);
    assert_eq!(cas.headers()["Pico-Next-Seq"], "6");

    let stale = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .header("Pico-Match-Seq", "5")
        .body("stale")
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), 412);
    assert_eq!(stale.headers()["Pico-Next-Seq"], "6");
    let body: Value = stale.json().await.unwrap();
    assert_eq!(body["error"], "match_failed");
    assert_eq!(body["next_seq"], 6);

    let producer = |seq: &'static str| {
        http.post(&url)
            .header("Content-Type", "text/plain")
            .header("Pico-Producer-Id", "p2")
            .header("Pico-Producer-Epoch", "1")
            .header("Pico-Producer-Seq", seq)
            .body([1u8].to_vec())
    };
    let first = producer("0").send().await.unwrap();
    assert_eq!(first.status(), 200);
    assert_eq!(first.headers()["Pico-Next-Seq"], "7");
    assert_eq!(first.headers()["Pico-Producer-Epoch"], "1");
    assert_eq!(first.headers()["Pico-Producer-Seq"], "0");
    let retry = producer("0").send().await.unwrap();
    assert_eq!(retry.status(), 200);
    assert_eq!(retry.headers()["Pico-Next-Seq"], "7");
    let head = http.head(&url).send().await.unwrap();
    assert_eq!(head.headers()["Pico-Next-Seq"], "7");

    // A sequence gap is a 409 with the expected/received pair.
    let gap = producer("5").send().await.unwrap();
    assert_eq!(gap.status(), 409);
    assert_eq!(gap.headers()["Pico-Expected-Seq"], "1");
    assert_eq!(gap.headers()["Pico-Received-Seq"], "5");

    let other = format!("{}/native/other", server.base_url);
    assert_eq!(http.put(&other).send().await.unwrap().status(), 201);

    let listing: Value = http
        .get(format!("{}/?prefix=/native/", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = listing["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["/native/orders", "/native/other"]);
    assert_eq!(listing["next"], Value::Null);
    assert_eq!(
        listing["items"][1]["content_type"],
        "application/octet-stream"
    );

    let page: Value = http
        .get(format!("{}/?prefix=/native/&limit=1", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    let cursor = page["next"].as_str().unwrap();
    let rest: Value = http
        .get(format!(
            "{}/?prefix=/native/&limit=1&cursor={cursor}",
            server.base_url
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rest["items"][0]["name"], "/native/other");
    assert_eq!(rest["next"], Value::Null);
    let elsewhere = http
        .get(format!(
            "{}/?prefix=/other/&limit=1&cursor={cursor}",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(elsewhere.status(), 400);

    // Trim commits asynchronously against the engine's start offset.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let trimmed = http
            .post(&url)
            .header("Pico-Trim-Seq", "2")
            .send()
            .await
            .unwrap();
        assert_eq!(trimmed.status(), 200);
        let start: u64 = trimmed.headers()["Pico-Start-Seq"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        if start >= 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "trim to 2 never committed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let head = http.head(&url).send().await.unwrap();
    assert_eq!(head.headers()["Pico-Start-Seq"], "2");

    // Close: an empty append with Pico-Closed, then appends bounce with 409.
    let closed = http
        .post(&url)
        .header("Pico-Closed", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(closed.status(), 200);
    assert_eq!(closed.headers()["Pico-Closed"], "true");
    let tail = closed.headers()["Pico-Next-Seq"]
        .to_str()
        .unwrap()
        .to_owned();

    let head = http.head(&url).send().await.unwrap();
    assert_eq!(head.headers()["Pico-Closed"], "true");

    let bounced = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body([1u8].to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(bounced.status(), 409);
    assert_eq!(bounced.headers()["Pico-Closed"], "true");
    assert_eq!(bounced.headers()["Pico-Next-Seq"].to_str().unwrap(), tail);
    let body: Value = bounced.json().await.unwrap();
    assert_eq!(body["error"], "closed");

    // Delete: 204 then 404, and head goes 404.
    assert_eq!(http.delete(&other).send().await.unwrap().status(), 204);
    assert_eq!(http.delete(&other).send().await.unwrap().status(), 404);
    assert_eq!(http.head(&other).send().await.unwrap().status(), 404);
}

/// TTL
/// and Expires-At round out through HEAD.`seq=now` is the live tail.
#[tokio::test]
async fn create_options_and_tail_seq() {
    let server = picomq_server().await;
    let http = client();

    let ttl_url = format!("{}/options/ttl", server.base_url);
    let created = http
        .put(&ttl_url)
        .header("Content-Type", "text/plain")
        .header("Pico-TTL", "60")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);

    let expires_url = format!("{}/options/expires", server.base_url);
    let created = http
        .put(&expires_url)
        .header("Content-Type", "text/plain")
        .header("Pico-Expires-At", "2100-01-01T00:00:00Z")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);

    // Both together is a 400.
    let both = http
        .put(format!("{}/options/both", server.base_url))
        .header("Pico-TTL", "60")
        .header("Pico-Expires-At", "2100-01-01T00:00:00Z")
        .send()
        .await
        .unwrap();
    assert_eq!(both.status(), 400);

    let head = http.head(&ttl_url).send().await.unwrap();
    assert_eq!(head.headers()["Pico-TTL"], "60");
    let head = http.head(&expires_url).send().await.unwrap();
    assert_eq!(head.headers()["Pico-Expires-At"], "2100-01-01T00:00:00Z");

    let now = http.get(format!("{ttl_url}?seq=now")).send().await.unwrap();
    assert_eq!(now.status(), 200);
    assert_eq!(now.headers()["Pico-Next-Seq"], "0");

    http.post(&ttl_url)
        .header("Content-Type", "text/plain")
        .body("one")
        .send()
        .await
        .unwrap();
    let now = http.get(format!("{ttl_url}?seq=now")).send().await.unwrap();
    assert_eq!(now.headers()["Pico-Next-Seq"], "1");

    let missing = http
        .get(format!("{}/options/missing?seq=now", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
}

#[tokio::test]
async fn live_reads() {
    let server = picomq_server().await;
    let http = client();
    let url = format!("{}/live/feed", server.base_url);
    http.put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    // Idle long-poll times out into 204 + up-to-date.
    let idle = http
        .get(format!("{url}?seq=0&live=long-poll"))
        .send()
        .await
        .unwrap();
    assert_eq!(idle.status(), 204);
    assert_eq!(idle.headers()["Pico-Up-To-Date"], "true");
    assert!(idle.headers().contains_key("Pico-Cursor"));

    // A parked long-poll wakes on append.
    let waiting = {
        let http = http.clone();
        let url = url.clone();
        tokio::spawn(async move {
            http.get(format!("{url}?seq=0&live=long-poll"))
                .send()
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    http.post(&url)
        .header("Content-Type", "text/plain")
        .body("ping")
        .send()
        .await
        .unwrap();
    let data = waiting.await.unwrap();
    assert_eq!(data.status(), 200);
    assert_eq!(data.headers()["Pico-Next-Seq"], "1");
    assert!(data.text().await.unwrap().contains(r#""body":"ping""#));

    http.post(&url)
        .header("Content-Type", "text/plain")
        .body("pong")
        .send()
        .await
        .unwrap();

    // SSE catch-up: data + control events. Ends at the 2s cap.
    let sse = http
        .get(format!("{url}?seq=0&live=sse"))
        .send()
        .await
        .unwrap();
    assert_eq!(sse.status(), 200);
    assert!(
        sse.headers()["Content-Type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let body = sse.text().await.unwrap();
    assert!(body.contains("event: data"));
    assert!(body.contains("id: 2"));
    assert!(body.contains(r#""body":"ping""#));
    assert!(body.contains("event: control"));

    // SSE resume from Last-Event-ID replays only what follows.
    let resumed = http
        .get(format!("{url}?live=sse"))
        .header("Last-Event-ID", "1")
        .send()
        .await
        .unwrap();
    let body = resumed.text().await.unwrap();
    assert!(body.contains(r#""body":"pong""#));
    assert!(!body.contains(r#""body":"ping""#));
}

struct RemoteOwnership {
    local: Log,
}

#[async_trait::async_trait]
impl OwnershipService for RemoteOwnership {
    async fn owner_of(&self, log: &Log) -> Result<Owner, ServiceError> {
        if *log == self.local {
            return Ok(Owner::local(None, -1));
        }

        Ok(Owner::remote(
            7,
            1,
            2,
            "http://owner.example:4437/".to_owned(),
        ))
    }

    fn local_node(&self) -> NodeMeta {
        NodeMeta {
            node_id: 1,
            advertised_address: "http://127.0.0.1:4437".to_owned(),
        }
    }
}

/// Ownership routing: a stream owned by another node redirects with 307
/// (path + query preserved). PUT and the list root stay local. Driven
/// through a stub ownership service.
#[tokio::test]
async fn remote_owner_redirects() {
    let node = common::start_node().await;
    node.topics()
        .create(picomq_server::CreateCommand::new(
            "/streams/demo",
            "text/plain",
        ))
        .await
        .unwrap();
    let spread = node
        .topics()
        .create(CreateCommand {
            partitions: 2,
            ..CreateCommand::new("/streams/spread", "text/plain")
        })
        .await
        .unwrap()
        .meta
        .topic;
    let local = picomq_server::partition_log(&spread, 0).unwrap();
    let frontend = std::sync::Arc::new(picomq_http::PicoFrontend::new(
        node.topics(),
        std::sync::Arc::new(RemoteOwnership { local }),
        picomq_http::RoutingMode::Redirect,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, frontend.router()).await.unwrap();
    });

    let http = client();
    let redirected = http
        .get(format!("{base_url}/streams/demo?seq=0&format=raw"))
        .send()
        .await
        .unwrap();
    assert_eq!(redirected.status(), 307);
    assert_eq!(
        redirected.headers()["Location"],
        "http://owner.example:4437/streams/demo?seq=0&format=raw"
    );

    let remote_partition = http
        .get(format!("{base_url}/streams/spread?partition=1&seq=0"))
        .send()
        .await
        .unwrap();
    assert_eq!(remote_partition.status(), 307);
    assert_eq!(
        remote_partition.headers()["Location"],
        "http://owner.example:4437/streams/spread?partition=1&seq=0"
    );
    let local_partition = http
        .get(format!("{base_url}/streams/spread?partition=0&seq=0"))
        .send()
        .await
        .unwrap();
    assert_eq!(local_partition.status(), 200);
    let without_partition = http
        .get(format!("{base_url}/streams/spread?seq=0"))
        .send()
        .await
        .unwrap();
    assert_eq!(without_partition.status(), 400);

    let existing = http
        .put(format!("{base_url}/streams/demo"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(existing.status(), 307);
    assert_eq!(
        existing.headers()["Location"],
        "http://owner.example:4437/streams/demo"
    );

    let put = http
        .put(format!("{base_url}/streams/fresh"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    let list = http.get(format!("{base_url}/")).send().await.unwrap();
    assert_eq!(list.status(), 200);
}

#[tokio::test]
async fn partitions_are_addressed_one_at_a_time() {
    let server = picomq_server().await;
    let http = client();
    let url = format!("{}/spread", server.base_url);
    server
        .node
        .topics()
        .create(CreateCommand {
            partitions: 3,
            ..CreateCommand::new("/spread", "text/plain")
        })
        .await
        .unwrap();

    for (partition, body) in [(0, "zero"), (1, "one"), (2, "two"), (2, "two-again")] {
        let appended = http
            .post(format!("{url}?partition={partition}"))
            .header("Content-Type", "text/plain")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(appended.status(), 200);
    }

    let without_partition = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body("lost")
        .send()
        .await
        .unwrap();
    assert_eq!(without_partition.status(), 400);
    let beyond = http
        .get(format!("{url}?partition=3&seq=0"))
        .send()
        .await
        .unwrap();
    assert_eq!(beyond.status(), 404);
    let malformed = http
        .get(format!("{url}?partition=x&seq=0"))
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);

    let head = http.head(&url).send().await.unwrap();
    assert_eq!(head.headers()["Pico-Partitions"], "3");
    assert!(head.headers().get("Pico-Next-Seq").is_none());
    let head = http
        .head(format!("{url}?partition=2"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.headers()["Pico-Next-Seq"], "2");

    let first = http
        .get(format!("{url}?partition=0&seq=0"))
        .send()
        .await
        .unwrap();
    let second = http
        .get(format!("{url}?partition=1&seq=0"))
        .send()
        .await
        .unwrap();
    assert_ne!(first.headers()["ETag"], second.headers()["ETag"]);
    assert!(first.text().await.unwrap().contains(r#""body":"zero""#));
    assert!(second.text().await.unwrap().contains(r#""body":"one""#));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let trimmed = http
            .post(format!("{url}?partition=2"))
            .header("Pico-Trim-Seq", "1")
            .send()
            .await
            .unwrap();
        assert_eq!(trimmed.status(), 200);
        if trimmed.headers()["Pico-Start-Seq"] == "1" {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "trim never committed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let trimmed = http
        .head(format!("{url}?partition=2"))
        .send()
        .await
        .unwrap();
    let untouched = http
        .head(format!("{url}?partition=1"))
        .send()
        .await
        .unwrap();
    assert_eq!(trimmed.headers()["Pico-Start-Seq"], "1");
    assert_eq!(untouched.headers()["Pico-Start-Seq"], "0");

    let waiting = {
        let http = http.clone();
        let url = url.clone();
        tokio::spawn(async move {
            http.get(format!("{url}?partition=1&seq=1&live=long-poll"))
                .send()
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    for (partition, body) in [(0, "elsewhere"), (1, "late")] {
        http.post(format!("{url}?partition={partition}"))
            .header("Content-Type", "text/plain")
            .body(body)
            .send()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let woken = waiting.await.unwrap().text().await.unwrap();
    assert!(woken.contains(r#""body":"late""#), "{woken}");

    let streamed = http
        .get(format!("{url}?partition=2&seq=1&live=sse"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(streamed.contains(r#""body":"two-again""#));
    assert!(!streamed.contains(r#""body":"zero""#));

    let close = http
        .post(format!("{url}?partition=0"))
        .header("Pico-Closed", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(close.status(), 400);
}

/// A write to a stream owned elsewhere is redirected after its body is read,
/// so the HTTP/2 stream closes cleanly and a busy client keeps its connection.
#[tokio::test]
async fn redirected_writes_keep_the_http2_connection() {
    use futures::StreamExt as _;

    let node = common::start_node().await;
    node.topics()
        .create(CreateCommand::new("/streams/elsewhere", "text/plain"))
        .await
        .unwrap();
    let frontend = std::sync::Arc::new(picomq_http::PicoFrontend::new(
        node.topics(),
        std::sync::Arc::new(RemoteOwnership {
            local: Log::Group("none".to_owned()),
        }),
        picomq_http::RoutingMode::Redirect,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/streams/elsewhere",
        listener.local_addr().unwrap()
    );
    tokio::spawn(async move {
        axum::serve(listener, frontend.router()).await.unwrap();
    });

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let slow_body = || {
        reqwest::Body::wrap_stream(futures::stream::iter(0..8).then(|_| async {
            tokio::time::sleep(Duration::from_millis(2)).await;
            Ok::<_, std::io::Error>(Bytes::from(vec![b'x'; 8 * 1024]))
        }))
    };
    let statuses: Vec<_> = futures::stream::iter(0..4096)
        .map(|_| {
            http.post(&url)
                .header("Content-Type", "text/plain")
                .body(slow_body())
                .send()
        })
        .buffer_unordered(512)
        .map(|response| response.map(|response| response.status().as_u16()))
        .collect()
        .await;

    assert!(statuses.iter().all(|status| matches!(status, Ok(307))));
}

#[tokio::test]
async fn shutdown_ends_live_reads_instead_of_waiting_on_them() {
    let server = common::picomq_server().await;
    let http = reqwest::Client::new();
    let url = format!("{}/live/stopping", server.base_url);
    http.put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let subscription = http.get(format!("{url}?live=sse")).send().await.unwrap();
    let poll = tokio::spawn(http.get(format!("{url}?seq=0&live=long-poll")).send());
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::time::timeout(std::time::Duration::from_secs(5), server.shutdown())
        .await
        .expect("shutdown waited on live reads");
    assert_eq!(poll.await.unwrap().unwrap().status(), 503);
    assert!(subscription.bytes().await.is_ok(), "the subscription ended");
}

#[tokio::test]
async fn retention_is_set_on_create_and_changed_through_stream_config() {
    let server = picomq_server().await;
    let client = reqwest::Client::new();
    let base = &server.base_url;

    let created = client
        .put(format!("{base}/retained"))
        .header("Pico-Retention-Ms", "60000")
        .header("Pico-Retention-Bytes", "4096")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(created.headers()["Pico-Retention-Ms"], "60000");
    let head = client
        .head(format!("{base}/retained"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.headers()["Pico-Retention-Bytes"], "4096");

    let cleared: Value = client
        .patch(format!("{base}/_streams/retained"))
        .json(&serde_json::json!({"retentionMs": null}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cleared["retentionMs"], Value::Null);
    assert_eq!(cleared["retentionBytes"], 4096);

    let zero = client
        .put(format!("{base}/other"))
        .header("Pico-Retention-Ms", "0")
        .send()
        .await
        .unwrap();
    assert_eq!(zero.status(), 400);
    let negative = client
        .patch(format!("{base}/_streams/retained"))
        .json(&serde_json::json!({"retentionBytes": -5}))
        .send()
        .await
        .unwrap();
    assert_eq!(negative.status(), 400);
}

#[tokio::test]
async fn partitions_are_set_on_create_and_grown_through_stream_config() {
    let server = picomq_server().await;
    let client = reqwest::Client::new();
    let base = &server.base_url;

    let created = client
        .put(format!("{base}/spread"))
        .header("Pico-Partitions", "2")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let head = client.head(format!("{base}/spread")).send().await.unwrap();
    assert_eq!(head.headers()["Pico-Partitions"], "2");

    let grown: Value = client
        .patch(format!("{base}/_streams/spread"))
        .json(&serde_json::json!({"partitions": 4}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(grown["partitions"], 4);
    let appended = client
        .post(format!("{base}/spread?partition=3"))
        .header("Content-Type", "text/plain")
        .body("new")
        .send()
        .await
        .unwrap();
    assert_eq!(appended.status(), 200);

    for partitions in [
        serde_json::json!(4),
        serde_json::json!(3),
        serde_json::json!(-1),
    ] {
        let refused = client
            .patch(format!("{base}/_streams/spread"))
            .json(&serde_json::json!({ "partitions": partitions }))
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), 400, "partitions {partitions}");
    }

    let zero = client
        .put(format!("{base}/empty"))
        .header("Pico-Partitions", "0")
        .send()
        .await
        .unwrap();
    assert_eq!(zero.status(), 400);
}
