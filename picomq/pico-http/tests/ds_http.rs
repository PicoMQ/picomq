//! Durable Streams protocol tests over a loopback node.
//!
//! Scenarios are driven with raw
//! HTTP, plus an independent conformance pass driven through the official
//! `durable-streams` Rust client (dev-dependency only, the frontend is
//! implemented from the protocol spec, not from the client).

mod common;

use std::time::Duration;

use serde_json::Value;

use common::ds_server;

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// Create/append/read/head/close/delete with the exact spec headers.
#[tokio::test]
async fn ds_protocol_end_to_end() {
    let server = ds_server().await;
    let http = client();
    let url = format!("{}/ds/orders", server.base_url);

    // Create: 201 + Location, idempotent 200.
    let created = http
        .put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(
        created.headers()["Location"],
        format!(
            "{}/ds/orders",
            server.node.advertised_address().trim_end_matches('/')
        )
    );
    assert_eq!(
        created.headers()["Stream-Next-Offset"],
        "00000000000000000000"
    );
    let again = http
        .put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 200);

    // Create with an initial body seeds the stream.
    let seeded_url = format!("{}/ds/seeded", server.base_url);
    let seeded = http
        .put(&seeded_url)
        .header("Content-Type", "text/plain")
        .body("seed")
        .send()
        .await
        .unwrap();
    assert_eq!(seeded.status(), 201);
    assert_eq!(
        seeded.headers()["Stream-Next-Offset"],
        "00000000000000000001"
    );

    // Plain append: 204 with the advanced offset. The body needs its type.
    let appended = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(appended.status(), 204);
    assert_eq!(
        appended.headers()["Stream-Next-Offset"],
        "00000000000000000001"
    );

    let untyped = http.post(&url).body("naked").send().await.unwrap();
    assert_eq!(untyped.status(), 400);
    assert_eq!(untyped.text().await.unwrap(), "missing Content-Type");

    let empty = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), 400);

    // Catch-up read: raw concatenated body, ETag honors If-None-Match.
    http.post(&url)
        .header("Content-Type", "text/plain")
        .body(" world")
        .send()
        .await
        .unwrap();
    let read = http.get(format!("{url}?offset=-1")).send().await.unwrap();
    assert_eq!(read.status(), 200);
    assert_eq!(read.headers()["Content-Type"], "text/plain");
    assert_eq!(read.headers()["Stream-Up-To-Date"], "true");
    let next = read.headers()["Stream-Next-Offset"]
        .to_str()
        .unwrap()
        .to_owned();
    let etag = read.headers()["ETag"].to_str().unwrap().to_owned();
    assert_eq!(read.text().await.unwrap(), "hello world");

    let cached = http
        .get(format!("{url}?offset=-1"))
        .header("If-None-Match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(cached.status(), 304);

    // Reading from the returned offset is an empty up-to-date tail.
    let tail = http
        .get(format!("{url}?offset={next}"))
        .send()
        .await
        .unwrap();
    assert_eq!(tail.status(), 200);
    assert_eq!(tail.headers()["Stream-Up-To-Date"], "true");
    assert_eq!(tail.text().await.unwrap(), "");

    // Head reports content type and tail offset.
    let head = http.head(&url).send().await.unwrap();
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers()["Content-Type"], "text/plain");
    assert_eq!(head.headers()["Stream-Next-Offset"], next);

    // Close, then appends bounce with 409 + Stream-Closed.
    let closed = http
        .post(&url)
        .header("Stream-Closed", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(closed.status(), 204);
    assert_eq!(closed.headers()["Stream-Closed"], "true");

    let bounced = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body("late")
        .send()
        .await
        .unwrap();
    assert_eq!(bounced.status(), 409);
    assert_eq!(bounced.headers()["Stream-Closed"], "true");

    // Delete: 204 then 404.
    assert_eq!(http.delete(&url).send().await.unwrap().status(), 204);
    assert_eq!(http.delete(&url).send().await.unwrap().status(), 404);
}

/// Producer fencing per spec: epoch bumps fence older writers (403), gaps are
/// 409 with `Producer-Expected-Seq`/`Producer-Received-Seq`, and replays ack
/// without duplicating.
#[tokio::test]
async fn ds_producer_fencing() {
    let server = ds_server().await;
    let http = client();
    let url = format!("{}/ds/producers", server.base_url);
    http.put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let producer =
        |id: &'static str, epoch: &'static str, seq: &'static str, body: &'static str| {
            http.post(&url)
                .header("Content-Type", "text/plain")
                .header("Producer-Id", id)
                .header("Producer-Epoch", epoch)
                .header("Producer-Seq", seq)
                .body(body)
        };

    // Producer appends answer 200 and echo the session.
    let first = producer("p1", "1", "0", "a").send().await.unwrap();
    assert_eq!(first.status(), 200);
    assert_eq!(first.headers()["Producer-Epoch"], "1");
    assert_eq!(first.headers()["Producer-Seq"], "0");
    let next = first.headers()["Stream-Next-Offset"]
        .to_str()
        .unwrap()
        .to_owned();

    // `producerAcceptReturns200AndDuplicateReturns204`), nothing appended.
    let replay = producer("p1", "1", "0", "a").send().await.unwrap();
    assert_eq!(replay.status(), 204);
    assert_eq!(
        replay.headers()["Stream-Next-Offset"].to_str().unwrap(),
        next
    );

    // Sequence gap: 409 + expected/received.
    let gap = producer("p1", "1", "5", "b").send().await.unwrap();
    assert_eq!(gap.status(), 409);
    assert_eq!(gap.headers()["Producer-Expected-Seq"], "1");
    assert_eq!(gap.headers()["Producer-Received-Seq"], "5");

    // A newer epoch fences the old one: 403 + current epoch.
    let claimed = producer("p1", "2", "0", "c").send().await.unwrap();
    assert_eq!(claimed.status(), 200);
    let stale = producer("p1", "1", "1", "d").send().await.unwrap();
    assert_eq!(stale.status(), 403);
    assert_eq!(stale.headers()["Producer-Epoch"], "2");

    // Partial producer headers are rejected.
    let partial = http
        .post(&url)
        .header("Content-Type", "text/plain")
        .header("Producer-Id", "p1")
        .body("e")
        .send()
        .await
        .unwrap();
    assert_eq!(partial.status(), 400);
}

/// JSON streams: reads come back as a JSON array of the appended messages,
/// and SSE data events carry the array form.
#[tokio::test]
async fn ds_json_streams_and_live() {
    let server = ds_server().await;
    let http = client();
    let url = format!("{}/ds/events", server.base_url);
    http.put(&url)
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();

    // Two JSON appends. One single message, one array (split by the service).
    http.post(&url)
        .header("Content-Type", "application/json")
        .body(r#"{"n":1}"#)
        .send()
        .await
        .unwrap();
    http.post(&url)
        .header("Content-Type", "application/json")
        .body(r#"[{"n":2},{"n":3}]"#)
        .send()
        .await
        .unwrap();

    let read = http.get(format!("{url}?offset=-1")).send().await.unwrap();
    assert_eq!(read.status(), 200);
    let body: Value = read.json().await.unwrap();
    assert_eq!(body, serde_json::json!([{"n": 1}, {"n": 2}, {"n": 3}]));

    // Live modes require an explicit offset.
    let missing = http
        .get(format!("{url}?live=long-poll"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 400);

    // Idle long-poll from the tail: 204 up-to-date with a cursor.
    let now = http.get(format!("{url}?offset=now")).send().await.unwrap();
    let tail = now.headers()["Stream-Next-Offset"]
        .to_str()
        .unwrap()
        .to_owned();
    let idle = http
        .get(format!("{url}?offset={tail}&live=long-poll"))
        .send()
        .await
        .unwrap();
    assert_eq!(idle.status(), 204);
    assert_eq!(idle.headers()["Stream-Up-To-Date"], "true");
    assert!(idle.headers().contains_key("Stream-Cursor"));

    // A parked long-poll wakes on append.
    let waiting = {
        let http = http.clone();
        let url = url.clone();
        let tail = tail.clone();
        tokio::spawn(async move {
            http.get(format!("{url}?offset={tail}&live=long-poll"))
                .send()
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    http.post(&url)
        .header("Content-Type", "application/json")
        .body(r#"{"n":4}"#)
        .send()
        .await
        .unwrap();
    let woke = waiting.await.unwrap();
    assert_eq!(woke.status(), 200);
    let body: Value = woke.json().await.unwrap();
    assert_eq!(body, serde_json::json!([{"n": 4}]));

    // SSE catch-up: JSON array data event + control event.
    let sse = http
        .get(format!("{url}?offset=-1&live=sse"))
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
    assert!(body.contains(r#"{"n":1}"#));
    assert!(body.contains("event: control"));
    assert!(body.contains("streamNextOffset"));
}

/// Binary streams announce base64 SSE data encoding.
#[tokio::test]
async fn ds_binary_sse_base64() {
    let server = ds_server().await;
    let http = client();
    let url = format!("{}/ds/blobs", server.base_url);
    http.put(&url)
        .header("Content-Type", "application/octet-stream")
        .send()
        .await
        .unwrap();
    http.post(&url)
        .header("Content-Type", "application/octet-stream")
        .body(vec![0u8, 1, 2, 255])
        .send()
        .await
        .unwrap();

    let sse = http
        .get(format!("{url}?offset=-1&live=sse"))
        .send()
        .await
        .unwrap();
    assert_eq!(sse.headers()["Stream-SSE-Data-Encoding"], "base64");
    let body = sse.text().await.unwrap();
    assert!(body.contains("data:AAEC/w=="));
}

/// Conformance: the official `durable-streams` Rust client against our
/// server. Create, append, head, catch-up read, live long-poll, producer
/// session, close, delete.
#[tokio::test]
async fn ds_official_client_conformance() {
    use durable_streams::{Client, CreateOptions, LiveMode, Offset};

    let server = ds_server().await;
    let client = Client::new();
    let mut stream = client.stream(&format!("{}/ds/conformance", server.base_url));
    // The client sends its stream-level content type on every append (the
    // server rejects mismatches with 409 per spec).
    stream.set_content_type("text/plain");

    // Create (typed), then append and read back through the client.
    stream
        .create_with(CreateOptions::new().content_type("text/plain"))
        .await
        .unwrap();
    stream.append("hello ").await.unwrap();
    let ack = stream.append("world").await.unwrap();

    let head = stream.head().await.unwrap();
    assert_eq!(head.content_type.as_deref(), Some("text/plain"));
    assert_eq!(head.next_offset.as_str(), ack.next_offset.as_str());
    assert!(!head.stream_closed);

    let mut reader = stream.read().offset(Offset::Beginning).build().unwrap();
    let mut data = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        data.extend_from_slice(&chunk.data);
        if chunk.up_to_date {
            break;
        }
    }
    assert_eq!(String::from_utf8(data).unwrap(), "hello world");

    // Live long-poll through the client: park, append, receive.
    let live_stream = client.stream(&format!("{}/ds/conformance", server.base_url));
    let tail = ack.next_offset.clone();
    let waiting = tokio::spawn(async move {
        let mut reader = live_stream
            .read()
            .offset(tail)
            .live(LiveMode::LongPoll)
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        loop {
            let chunk = reader.next_chunk().await.unwrap().expect("live chunk");
            if !chunk.data.is_empty() {
                return chunk;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    stream.append("!").await.unwrap();
    let chunk = waiting.await.unwrap();
    assert_eq!(&chunk.data[..], b"!");

    // Producer session: batched appends flush through one fenced session.
    let producer = stream.producer("writer-1").build();
    producer.append("a");
    producer.append("b");
    producer.flush().await.unwrap();
    producer.close().await.unwrap();

    let mut reader = stream.read().offset(Offset::Beginning).build().unwrap();
    let mut data = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.unwrap() {
        data.extend_from_slice(&chunk.data);
        if chunk.up_to_date {
            break;
        }
    }
    assert_eq!(String::from_utf8(data).unwrap(), "hello world!ab");

    // Close and delete.
    stream.close().await.unwrap();
    let head = stream.head().await.unwrap();
    assert!(head.stream_closed);
    stream.delete().await.unwrap();
    assert!(stream.head().await.is_err());
}
