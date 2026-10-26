mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{Console, create, issue};
use picomq_auth::{Audience, Operation, ResourceSet, Scope};
use picomq_server::{AppendCommand, CreateCommand, LogRecord};
use serde_json::{Value, json};

fn offsets(items: &[Value]) -> Vec<u64> {
    items
        .iter()
        .map(|offset| offset.as_u64().unwrap())
        .collect()
}

fn record(key: &str, headers: &[(&str, &str)], value: &[u8]) -> LogRecord {
    LogRecord {
        timestamp_ms: 0,
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        value: Some(Bytes::copy_from_slice(value)),
        headers: headers
            .iter()
            .map(|(name, value)| {
                let value = Bytes::copy_from_slice(value.as_bytes());
                ((*name).to_owned(), Some(value))
            })
            .collect(),
    }
}

async fn write(console: &Console, topic: &str, records: Vec<LogRecord>) {
    console
        .node
        .topics()
        .append(
            topic,
            0,
            AppendCommand {
                records,
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn records_page_from_an_offset_and_from_a_timestamp() {
    let console = Console::start().await;
    create(&console.node, "/log", 1).await;
    console.append("/log", 0, 30).await;
    let records = "/admin/topics/%2Flog/partitions/0/records";

    let (all, pages) = console
        .every(&format!("{records}?limit=10"), &console.root, "offset")
        .await;
    assert_eq!(offsets(&all), (0..30).collect::<Vec<_>>());
    assert_eq!(pages, 3);

    let (status, tail) = console
        .get(&format!("{records}?offset=25"), &console.root)
        .await;
    assert_eq!(status, 200, "{tail}");
    assert_eq!(tail["items"].as_array().unwrap().len(), 5);
    assert_eq!(tail["items"][0]["value"]["data"], "record 25");
    assert_eq!(tail["next"], Value::Null);

    let (_, twelfth) = console.get(&format!("{records}/12"), &console.root).await;
    let timestamp = twelfth["timestamp"].as_i64().unwrap();
    let (status, timed) = console
        .get(
            &format!("{records}?timestamp={timestamp}&limit=1"),
            &console.root,
        )
        .await;
    assert_eq!(status, 200, "{timed}");
    assert_eq!(timed["items"][0]["offset"], 12);

    let (status, beyond) = console
        .get(
            &format!("{records}?timestamp={}", timestamp + 3_600_000),
            &console.root,
        )
        .await;
    assert_eq!(status, 200, "{beyond}");
    assert_eq!(beyond["items"], json!([]));
    assert_eq!(beyond["next"], Value::Null);

    let (status, both) = console
        .get(&format!("{records}?offset=1&timestamp=1"), &console.root)
        .await;
    assert_eq!(status, 400, "{both}");

    console.server.shutdown().await;
}

#[tokio::test]
async fn previews_are_cut_and_a_single_record_is_shown_whole() {
    let console = Console::start().await;
    create(&console.node, "/blobs", 1).await;
    let text = "é".repeat(5_000);
    let mut big = record("big", &[("trace", "a"), ("trace", "b")], text.as_bytes());
    big.headers.push(("flag".to_owned(), None));
    write(
        &console,
        "/blobs",
        vec![
            big,
            record("binary", &[], &[0xff, 0x00, 0xfe]),
            LogRecord {
                value: None,
                ..record("gone", &[], b"")
            },
        ],
    )
    .await;
    let records = "/admin/topics/%2Fblobs/partitions/0/records";

    let (status, page) = console.get(records, &console.root).await;
    assert_eq!(status, 200, "{page}");
    let big = &page["items"][0];
    assert_eq!(big["key"]["data"], "big");
    assert_eq!(big["value"]["encoding"], "utf8");
    assert_eq!(big["value"]["size"], 10_000);
    assert_eq!(big["value"]["truncated"], true);
    assert_eq!(big["value"]["data"].as_str().unwrap(), "é".repeat(2_048));
    assert_eq!(
        big["headers"],
        json!([
            {"name": "trace", "value": {"data": "a", "encoding": "utf8", "size": 1, "truncated": false}},
            {"name": "trace", "value": {"data": "b", "encoding": "utf8", "size": 1, "truncated": false}},
            {"name": "flag", "value": null},
        ])
    );
    let binary = &page["items"][1]["value"];
    assert_eq!(binary["encoding"], "base64");
    assert_eq!(binary["data"], "/wD+");
    let tombstone = &page["items"][2];
    assert_eq!(tombstone["key"]["data"], "gone");
    assert_eq!(tombstone["value"], Value::Null);
    let (_, valued) = console
        .get(&format!("{records}?value=%C3%A9"), &console.root)
        .await;
    assert_eq!(valued["items"].as_array().unwrap().len(), 1);

    let (status, whole) = console.get(&format!("{records}/0"), &console.root).await;
    assert_eq!(status, 200, "{whole}");
    assert_eq!(whole["value"]["truncated"], false);
    assert_eq!(whole["value"]["data"].as_str().unwrap(), text);

    let (status, _) = console.get(&format!("{records}/3"), &console.root).await;
    assert_eq!(status, 404);
    let (status, _) = console
        .get("/admin/topics/%2Fblobs/partitions/1/records", &console.root)
        .await;
    assert_eq!(status, 404);

    console.server.shutdown().await;
}

#[tokio::test]
async fn filters_match_keys_headers_and_values() {
    let console = Console::start().await;
    create(&console.node, "/orders", 1).await;
    write(
        &console,
        "/orders",
        (0..10)
            .map(|n| {
                let region = if n % 2 == 0 { "eu-west" } else { "us-east" };
                record(
                    &format!("user-{n}"),
                    &[("region", region)],
                    format!("order {n} shipped").as_bytes(),
                )
            })
            .collect(),
    )
    .await;
    let records = "/admin/topics/%2Forders/partitions/0/records";
    let matching = |query: &'static str| {
        let console = &console;
        async move {
            let (all, _) = console
                .every(&format!("{records}?{query}"), &console.root, "offset")
                .await;
            offsets(&all)
        }
    };

    assert_eq!(matching("key=user-3").await, [3]);
    assert_eq!(
        matching("header=region&headerValue=eu").await,
        [0, 2, 4, 6, 8]
    );
    assert_eq!(matching("header=region").await.len(), 10);
    assert_eq!(matching("header=zone").await, Vec::<u64>::new());
    assert_eq!(matching("value=order%207").await, [7]);
    assert_eq!(
        matching("key=user&header=region&headerValue=us&value=shipped").await,
        [1, 3, 5, 7, 9]
    );

    let (status, _) = console
        .get(&format!("{records}?headerValue=eu"), &console.root)
        .await;
    assert_eq!(status, 400);

    console.server.shutdown().await;
}

#[tokio::test]
async fn a_filter_that_matches_nothing_spends_its_budget_and_resumes() {
    let console = Console::start().await;
    create(&console.node, "/quiet", 1).await;
    console.append("/quiet", 0, 50).await;

    let (status, first) = console
        .get(
            "/admin/topics/%2Fquiet/partitions/0/records?limit=1&value=absent",
            &console.root,
        )
        .await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(first["items"], json!([]));
    assert!(first["next"].is_string());

    let (found, pages) = console
        .every(
            "/admin/topics/%2Fquiet/partitions/0/records?limit=1&value=absent",
            &console.root,
            "offset",
        )
        .await;
    assert!(found.is_empty());
    assert_eq!(pages, 5);

    console.server.shutdown().await;
}

#[tokio::test]
async fn a_wait_returns_records_appended_while_waiting() {
    let console = Arc::new(Console::start().await);
    create(&console.node, "/tail", 1).await;
    console.append("/tail", 0, 2).await;
    let records = "/admin/topics/%2Ftail/partitions/0/records";

    let started = Instant::now();
    let (status, idle) = console
        .get(&format!("{records}?offset=2&wait=200"), &console.root)
        .await;
    assert_eq!(status, 200, "{idle}");
    assert_eq!(idle["items"], json!([]));
    assert_eq!(idle["next"], Value::Null);
    assert!(started.elapsed() >= Duration::from_millis(200));

    let appender = console.clone();
    let appended = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        appender.append("/tail", 0, 1).await;
    });
    let started = Instant::now();
    let (status, woken) = console
        .get(&format!("{records}?offset=2&wait=5000"), &console.root)
        .await;
    appended.await.unwrap();
    assert_eq!(status, 200, "{woken}");
    assert_eq!(woken["items"][0]["offset"], 2);
    assert!(woken["next"].is_string(), "a waiting reader always resumes");
    assert!(started.elapsed() < Duration::from_secs(4));

    let (status, _) = console
        .get(&format!("{records}?wait=5001"), &console.root)
        .await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn browsing_does_not_extend_a_topics_ttl() {
    let console = Console::start().await;
    console
        .node
        .topics()
        .create(CreateCommand {
            ttl_seconds: Some(10),
            ..CreateCommand::new("/brief", "text/plain")
        })
        .await
        .unwrap();
    console.append("/brief", 0, 1).await;
    let deadline = || async {
        let topic = console.node.topics().get("/brief").await.unwrap().unwrap();
        topic.deadline_ms
    };
    let created = deadline().await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let (status, _) = console
        .get("/admin/topics/%2Fbrief/partitions/0/records", &console.root)
        .await;
    assert_eq!(status, 200);
    let (status, _) = console
        .get(
            "/admin/topics/%2Fbrief/partitions/0/records/0",
            &console.root,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(deadline().await, created);

    let read = console
        .http
        .get(console.data("/brief?offset=0"))
        .bearer_auth(&console.root)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 200);
    assert!(deadline().await > created, "a consumer read extends it");

    console.server.shutdown().await;
}

#[tokio::test]
async fn records_need_read_on_the_topic() {
    let console = Console::start().await;
    create(&console.node, "/guarded", 1).await;
    console.append("/guarded", 0, 1).await;
    let token = |ops: &[Operation]| {
        issue(
            &console.node,
            if ops.contains(&Operation::Read) {
                "reader"
            } else {
                "header"
            },
            Scope {
                streams: ResourceSet::prefix("/guarded"),
                ops: ops.iter().copied().collect(),
                audiences: [Audience::Admin].into(),
                ..Scope::default()
            },
        )
    };
    let head = token(&[Operation::Head]).await;
    let read = token(&[Operation::Read]).await;

    for path in [
        "/admin/topics/%2Fguarded/partitions/0/records",
        "/admin/topics/%2Fguarded/partitions/0/records/0",
    ] {
        assert_eq!(console.get(path, &head).await.0, 403, "{path}");
        assert_eq!(console.get(path, &read).await.0, 200, "{path}");
    }

    console.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn record_pages_stay_bounded_on_a_large_partition() {
    let console = Console::start().await;
    create(&console.node, "/large", 1).await;
    let value = "v".repeat(4 * 1024);
    for _ in 0..1_024 {
        write(
            &console,
            "/large",
            (0..64)
                .map(|_| record("k", &[], value.as_bytes()))
                .collect(),
        )
        .await;
    }
    let records = "/admin/topics/%2Flarge/partitions/0/records";
    let (_, near_end) = console
        .get(&format!("{records}/65000"), &console.root)
        .await;
    let timestamp = near_end["timestamp"].as_i64().unwrap();

    for (path, found) in [
        (format!("{records}?limit=1000&value=absent"), false),
        (format!("{records}?limit=1000&offset=60000"), true),
        (format!("{records}?limit=10&timestamp={timestamp}"), true),
    ] {
        let started = Instant::now();
        let response = console
            .http
            .get(console.admin(&path))
            .bearer_auth(&console.root)
            .send()
            .await
            .unwrap();
        let body = response.bytes().await.unwrap();
        let elapsed = started.elapsed();
        let page: Value = serde_json::from_slice(&body).unwrap();

        println!("{path}: {} bytes in {elapsed:?}", body.len());
        assert_eq!(!page["items"].as_array().unwrap().is_empty(), found);
        assert!(page["next"].is_string());
        assert!(body.len() <= (1 << 20) + 8 * 1024);
        assert!(elapsed < Duration::from_millis(250));
    }

    console.server.shutdown().await;
}

#[tokio::test]
async fn produced_records_read_back_with_keys_headers_and_nulls() {
    let console = Console::start().await;
    create(&console.node, "/made", 1).await;
    let produce = |body: Value| {
        console
            .http
            .post(console.admin("/admin/topics/%2Fmade/partitions/0/records"))
            .bearer_auth(&console.root)
            .json(&body)
            .send()
    };

    let first = produce(json!({
        "key": { "data": "cust-1", "encoding": "utf8" },
        "value": { "data": "{\"amount\":7}", "encoding": "utf8" },
        "headers": [
            { "name": "trace-id", "value": { "data": "abc", "encoding": "utf8" } },
            { "name": "empty", "value": null },
        ],
    }))
    .await
    .unwrap();
    assert_eq!(first.status(), 201);
    let first: Value = first.json().await.unwrap();
    assert_eq!(first["offset"], 0);
    assert!(first["timestamp"].as_i64().unwrap() > 0);

    let second = produce(json!({
        "key": { "data": "AP8Q", "encoding": "base64" },
        "value": null,
    }))
    .await
    .unwrap();
    assert_eq!(second.status(), 201);
    assert_eq!(second.json::<Value>().await.unwrap()["offset"], 1);

    let (_, read) = console
        .get(
            "/admin/topics/%2Fmade/partitions/0/records/0",
            &console.root,
        )
        .await;
    assert_eq!(read["key"]["data"], "cust-1");
    assert_eq!(read["value"]["data"], "{\"amount\":7}");
    assert_eq!(read["headers"][0]["name"], "trace-id");
    assert_eq!(read["headers"][0]["value"]["data"], "abc");
    assert_eq!(
        read["headers"][1],
        json!({ "name": "empty", "value": null })
    );

    let (_, read) = console
        .get(
            "/admin/topics/%2Fmade/partitions/0/records/1",
            &console.root,
        )
        .await;
    assert_eq!(read["key"]["encoding"], "base64");
    assert_eq!(read["key"]["data"], "AP8Q");
    assert_eq!(read["value"], Value::Null);

    console.server.shutdown().await;
}

#[tokio::test]
async fn produce_refuses_malformed_bodies_and_missing_partitions() {
    let console = Console::start().await;
    create(&console.node, "/made", 1).await;
    let produce = |path: &str, body: Value| {
        console
            .http
            .post(console.admin(path))
            .bearer_auth(&console.root)
            .json(&body)
            .send()
    };
    let records = "/admin/topics/%2Fmade/partitions/0/records";

    for body in [
        json!({ "value": { "data": "x", "encoding": "hex" } }),
        json!({ "value": { "data": "not base64!", "encoding": "base64" } }),
        json!({ "value": "x" }),
        json!({ "value": { "data": "x" } }),
        json!({ "value": { "data": "x", "encoding": "utf8", "size": 1 } }),
        json!({ "headers": [{ "value": null }] }),
        json!({ "headers": [{ "name": "a", "value": null, "extra": 1 }] }),
        json!({ "offset": 3 }),
        json!([1]),
    ] {
        let response = produce(records, body.clone()).await.unwrap();
        assert_eq!(response.status(), 400, "{body}");
    }
    for path in [
        "/admin/topics/%2Fabsent/partitions/0/records",
        "/admin/topics/%2Fmade/partitions/1/records",
    ] {
        let response = produce(path, json!({ "value": null })).await.unwrap();
        assert_eq!(response.status(), 404, "{path}");
    }

    console.server.shutdown().await;
}

#[tokio::test]
async fn produce_needs_append_on_the_topic() {
    let console = Console::start().await;
    create(&console.node, "/guarded", 1).await;
    let token = |id: &'static str, op: Operation| {
        issue(
            &console.node,
            id,
            Scope {
                streams: ResourceSet::prefix("/guarded"),
                ops: [op].into(),
                audiences: [Audience::Admin].into(),
                ..Scope::default()
            },
        )
    };
    let reader = token("reader", Operation::Read).await;
    let producer = token("producer", Operation::Append).await;
    let produce = |bearer: String| {
        console
            .http
            .post(console.admin("/admin/topics/%2Fguarded/partitions/0/records"))
            .bearer_auth(bearer)
            .json(&json!({ "value": { "data": "x", "encoding": "utf8" } }))
            .send()
    };

    assert_eq!(produce(reader).await.unwrap().status(), 403);
    assert_eq!(produce(producer).await.unwrap().status(), 201);

    console.server.shutdown().await;
}
