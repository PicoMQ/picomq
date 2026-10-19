//! `pico serve` as an operator runs it: flags in, a serving process out.

mod common;

use std::process::Command;
use std::time::Duration;

use common::{await_ready, start, start_node};

#[tokio::test]
async fn serves_streams_with_only_flags() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), "pico");
    let client = reqwest::Client::new();
    await_ready(&client, &server.admin_url).await;

    // The storage directories did not exist: serve created them.
    assert!(dir.path().join("objects").is_dir());
    assert!(dir.path().join("wal").is_dir());

    let url = format!("{}/streams/cli", server.base_url);
    assert_eq!(
        client
            .put(&url)
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(
        client
            .post(&url)
            .header("Content-Type", "text/plain")
            .body("from-the-cli")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let records: serde_json::Value = client
        .get(format!("{url}?seq=0"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(records[0]["body"], "from-the-cli");
}

/// Kafka is a listener that runs alongside the HTTP frontend, not a frontend
/// choice: `--protocol kafka` is a client-side notion only.
#[tokio::test]
async fn serve_rejects_protocol_kafka() {
    let output = Command::new(env!("CARGO_BIN_EXE_pico"))
        .env("PICO_CONFIG", common::no_config())
        .env("PICO_NO_KEYRING", "1")
        .args([
            "--protocol",
            "kafka",
            "serve",
            "--meta-url",
            "sqlite::memory:",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--protocol pico or ds"), "{stderr}");
}

#[tokio::test]
async fn rejects_an_unsupported_meta_url() {
    let output = Command::new(env!("CARGO_BIN_EXE_pico"))
        .args(["serve", "--meta-url", "mysql://localhost/pico"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported --meta-url"), "{stderr}");
}

async fn owner_of(client: &reqwest::Client, admin_url: &str, stream: &str) -> i64 {
    let body: serde_json::Value = client
        .get(format!("{admin_url}/admin/streams/{stream}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    body["ownerNodeId"].as_i64().unwrap()
}

async fn kill_and_fail_over(dir: &std::path::Path, wal: impl Fn(i32) -> String, extra: &[&str]) {
    let liveness = [
        &["--heartbeat-interval-sec", "1", "--node-timeout-sec", "3"][..],
        extra,
    ]
    .concat();
    let client = reqwest::Client::new();

    let first = start_node(dir, 1, &wal(1), &liveness);
    await_ready(&client, &first.admin_url).await;
    let second = start_node(
        dir,
        2,
        &wal(2),
        &[&liveness[..], &["--wal-upload-interval-ms", "600000"]].concat(),
    );
    await_ready(&client, &second.admin_url).await;

    let mut stranded = None;
    for index in 0..8 {
        let stream = format!("fail/{index}");
        let created = client
            .put(format!("{}/{stream}", first.base_url))
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), 201);

        if owner_of(&client, &first.admin_url, &stream).await == 2 {
            stranded = Some(stream);
            break;
        }
    }
    let stream = stranded.expect("one topic placed on node 2");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while client
        .head(format!("{}/{stream}", second.base_url))
        .send()
        .await
        .unwrap()
        .status()
        != 200
    {
        assert!(
            std::time::Instant::now() < deadline,
            "node 2 never saw {stream}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for body in ["one", "two", "three"] {
        let appended = client
            .post(format!("{}/{stream}", second.base_url))
            .header("Content-Type", "text/plain")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(appended.status(), 200);
    }

    drop(second);

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let records: Vec<serde_json::Value> = loop {
        let read = client
            .get(format!("{}/{stream}?seq=0", first.base_url))
            .send()
            .await;
        if let Ok(read) = read
            && read.status() == 200
        {
            break read.json().await.unwrap();
        }

        assert!(
            std::time::Instant::now() < deadline,
            "{stream} never failed over"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let bodies: Vec<&str> = records
        .iter()
        .map(|record| record["body"].as_str().unwrap())
        .collect();
    assert_eq!(bodies, ["one", "two", "three"]);

    let returned = start_node(dir, 2, &wal(2), &liveness);
    await_ready(&client, &returned.admin_url).await;

    let created = client
        .put(format!("{}/fail/back", first.base_url))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(owner_of(&client, &first.admin_url, "fail/back").await, 2);
    assert_eq!(owner_of(&client, &first.admin_url, &stream).await, 1);
}

#[tokio::test]
async fn sigterm_hands_the_node_off_and_exits_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let wal = |node_id: i32| {
        format!(
            "2@file://{}",
            dir.path().join(format!("wal-{node_id}")).display()
        )
    };
    let liveness = ["--heartbeat-interval-sec", "1", "--node-timeout-sec", "60"];
    let client = reqwest::Client::new();
    let first = start_node(dir.path(), 1, &wal(1), &liveness);
    await_ready(&client, &first.admin_url).await;
    let second = start_node(dir.path(), 2, &wal(2), &liveness);
    await_ready(&client, &second.admin_url).await;

    let status = second.terminate();

    assert!(status.success(), "{status}");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let nodes: serde_json::Value = client
            .get(format!("{}/admin/nodes", first.admin_url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let fenced = nodes["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["nodeId"] == 2 && node["fenced"] == true);
        if fenced {
            break;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "a clean exit hands the node off long before its timeout"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_killed_node_fails_over_to_the_survivor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();

    kill_and_fail_over(
        dir.path(),
        |node_id| format!("2@file://{}", path.join(format!("wal-{node_id}")).display()),
        &[],
    )
    .await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-cli --test serve_cli -- --ignored"]
async fn a_killed_node_fails_over_from_a_postgres_wal() {
    let url = std::env::var("PICO_TEST_PG_URL").expect("PICO_TEST_PG_URL is not set");
    let dir = tempfile::tempdir().unwrap();
    let cluster = format!(
        "failover-{}",
        std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
    );

    kill_and_fail_over(dir.path(), |_| url.clone(), &["--cluster-id", &cluster]).await;
}
