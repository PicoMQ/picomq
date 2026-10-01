use std::net::SocketAddr;
use std::path::Path;

use picomq_http::HttpProtocol;
use picomq_runtime::{MetaBackend, ServerConfig};

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn config(dir: &Path, wal: &str, cluster_id: &str, node_epoch: i64) -> ServerConfig {
    ServerConfig {
        node_epoch,
        addr: loopback(),
        admin_addr: None,
        http_protocol: HttpProtocol::Pico,
        kafka: None,
        meta_backend: MetaBackend::parse(&format!("sqlite:{}", dir.join("meta.db").display()))
            .unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(wal.to_owned()),
        cluster_id: cluster_id.to_owned(),
        engine: s3stream::Config {
            wal_upload_interval_ms: 3_600_000,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn objects(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            if entry.path().is_dir() {
                objects(&entry.path())
            } else {
                1
            }
        })
        .sum()
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-runtime --test postgres -- --ignored"]
async fn acked_records_survive_a_crash_through_the_postgres_wal() {
    let wal = std::env::var("PICO_TEST_PG_URL").expect("PICO_TEST_PG_URL is not set");
    let dir = tempfile::tempdir().unwrap();
    let cluster_id = format!("crash-{}", std::process::id());
    let http = reqwest::Client::new();

    let first = picomq_runtime::start(config(dir.path(), &wal, &cluster_id, 1))
        .await
        .unwrap();
    let url = format!("http://{}/streams/crash", first.local_addr());
    let created = http
        .put(&url)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    for i in 0..3 {
        let appended = http
            .post(&url)
            .header("Content-Type", "text/plain")
            .body(format!("record-{i}"))
            .send()
            .await
            .unwrap();
        assert_eq!(appended.status(), 200);
    }
    std::mem::forget(first);
    assert_eq!(objects(&dir.path().join("objects")), 0);

    let second = picomq_runtime::start(config(dir.path(), &wal, &cluster_id, 2))
        .await
        .unwrap();
    let url = format!("http://{}/streams/crash", second.local_addr());
    let head = http.head(&url).send().await.unwrap();
    assert_eq!(head.headers()["Pico-Next-Seq"], "3");
    let records: serde_json::Value = http
        .get(format!("{url}?seq=0"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let bodies: Vec<_> = records
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["body"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(bodies, ["record-0", "record-1", "record-2"]);
    second.shutdown().await;
}
