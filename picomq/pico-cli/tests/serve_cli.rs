//! `pico serve` as an operator runs it: flags in, a serving process out.

mod common;

use std::process::Command;

use common::{await_ready, start};

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

/// `--no-kafka` leaves the Kafka port closed while HTTP still serves.
#[tokio::test]
async fn no_kafka_closes_the_kafka_listener() {
    let dir = tempfile::tempdir().unwrap();
    let server = common::start_with(dir.path(), "pico", &["--no-kafka"]);
    let client = reqwest::Client::new();
    await_ready(&client, &server.admin_url).await;
    assert!(
        std::net::TcpStream::connect(server.kafka_addr.as_str()).is_err(),
        "kafka port must be closed"
    );
    assert_eq!(
        client
            .put(format!("{}/streams/cli", server.base_url))
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
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
