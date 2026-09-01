//! Pico protocol integration tests over a loopback node.
//!
//! Requests are spelled out with `reqwest` rather than the client crate so
//! the wire shapes stay visible. Headers use the `Pico-*` prefix.

mod common;

use std::time::Duration;

use bytes::Bytes;
use picomq_protocol::record::{decode_batch_read, encode_batch_append, PicoRecord};
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
    let names: Vec<&str> = listing["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["/native/orders", "/native/other"]);
    assert_eq!(listing["has_more"], false);
    assert_eq!(
        listing["streams"][1]["content_type"],
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
    assert_eq!(page["streams"].as_array().unwrap().len(), 1);
    assert_eq!(page["has_more"], true);

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
    assert!(sse.headers()["Content-Type"]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
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

/// Ownership routing: a stream owned by another node redirects with 307
/// (path + query preserved). PUT and the list root stay local. Driven
/// through a stub ownership service.
#[tokio::test]
async fn remote_owner_redirects() {
    use picomq_server::ownership::OwnershipService;
    use picomq_server::{NodeMeta, Owner, ServiceError};

    struct RemoteOwnership;

    #[async_trait::async_trait]
    impl OwnershipService for RemoteOwnership {
        async fn owner_of(&self, _name: &str) -> Result<Owner, ServiceError> {
            Ok(Owner::remote(7, 2, "http://owner.example:4437/".to_owned()))
        }

        fn local_node(&self) -> NodeMeta {
            NodeMeta {
                node_id: 1,
                advertised_address: "http://127.0.0.1:4437".to_owned(),
            }
        }
    }

    let node = common::start_node().await;
    let frontend = std::sync::Arc::new(picomq_http::PicoFrontend::new(
        node.service(),
        std::sync::Arc::new(RemoteOwnership),
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

    // PUT is always local: create places the stream here.
    let put = http
        .put(format!("{base_url}/streams/demo"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    // The list root is always local too.
    let list = http.get(format!("{base_url}/")).send().await.unwrap();
    assert_eq!(list.status(), 200);
}
