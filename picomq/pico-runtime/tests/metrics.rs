use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use picomq_auth::AccessToken;
use picomq_runtime::{AuthMode, KafkaConfig, MetaBackend, PicoServer, ServerConfig};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

struct Node {
    server: PicoServer,
    http: reqwest::Client,
    root: String,
}

impl Node {
    fn base(&self) -> String {
        format!("http://{}", self.server.local_addr())
    }

    async fn scrape(&self) -> HashMap<String, f64> {
        let text = self
            .http
            .get(format!(
                "http://{}/metrics",
                self.server.admin_addr().unwrap()
            ))
            .bearer_auth(&self.root)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        text.lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
            .filter_map(|line| {
                let (series, value) = line.rsplit_once(' ')?;
                Some((series.to_owned(), value.parse().ok()?))
            })
            .collect()
    }
}

fn value(scrape: &HashMap<String, f64>, series: &str) -> f64 {
    scrape.get(series).copied().unwrap_or(0.0)
}

fn delta(before: &HashMap<String, f64>, after: &HashMap<String, f64>, series: &str) -> f64 {
    value(after, series) - value(before, series)
}

fn kafka(node: &Node, password: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", node.server.kafka_addr().to_string())
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "ops/root")
        .set("sasl.password", password)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_metric_moves_with_the_work_it_measures() {
    let dir = tempfile::tempdir().unwrap();
    let (root, _) = AccessToken::issue("ops/root").unwrap();
    let server = picomq_runtime::start(ServerConfig {
        addr: loopback(),
        admin_addr: Some(loopback()),
        kafka: KafkaConfig::new(loopback()),
        meta_backend: MetaBackend::parse(&format!(
            "sqlite:{}",
            dir.path().join("meta.db").display()
        ))
        .unwrap(),
        storage_uri: format!("1@file://{}", dir.path().join("objects").display()),
        wal_uri: Some(format!(
            "2@file://{}?batchInterval=5",
            dir.path().join("wal").display()
        )),
        auth_mode: AuthMode::Required,
        bootstrap_token: Some(root.render()),
        ..Default::default()
    })
    .await
    .unwrap();
    let node = Node {
        server,
        http: reqwest::Client::new(),
        root: root.render(),
    };

    let unauthenticated = node
        .http
        .get(format!(
            "http://{}/metrics",
            node.server.admin_addr().unwrap()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), 401);

    let before = node.scrape().await;
    node.http
        .put(format!("{}/orders", node.base()))
        .bearer_auth(&node.root)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    for body in ["one", "two", "three"] {
        let appended = node
            .http
            .post(format!("{}/orders", node.base()))
            .bearer_auth(&node.root)
            .header("Content-Type", "text/plain")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(appended.status(), 200);
    }
    node.http
        .get(format!("{}/orders?seq=0", node.base()))
        .bearer_auth(&node.root)
        .send()
        .await
        .unwrap();
    let refused = node
        .http
        .get(format!("{}/orders", node.base()))
        .bearer_auth("not-a-token")
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 401);
    let after = node.scrape().await;

    assert_eq!(delta(&before, &after, "pico_records_in_total"), 3.0);
    assert_eq!(delta(&before, &after, "pico_bytes_in_total"), 11.0);
    assert_eq!(delta(&before, &after, "pico_records_out_total"), 3.0);
    assert_eq!(
        delta(
            &before,
            &after,
            r#"pico_requests_total{protocol="pico",operation="POST",status="2xx"}"#
        ),
        3.0
    );
    assert_eq!(
        delta(
            &before,
            &after,
            r#"pico_auth_failures_total{protocol="pico",reason="malformed"}"#
        ),
        1.0
    );
    assert!(
        after
            .keys()
            .any(|series| series.starts_with("pico_request_duration_seconds_bucket{")),
        "request durations are histograms"
    );
    assert!(
        after.keys().all(|series| series.starts_with("pico_")),
        "the engine's own metrics are not collected"
    );
    assert!(value(&after, "pico_logs_owned") >= 1.0);
    assert!(
        value(&after, r#"pico_connections{listener="stream"}"#) >= 1.0,
        "the client's pooled connection is open"
    );
    assert!(value(&after, r#"pico_connections{listener="admin"}"#) >= 1.0);

    let before = node.scrape().await;
    let producer = kafka(&node, &node.root);
    for payload in ["k1", "k2"] {
        producer
            .send(
                FutureRecord::<(), str>::to("orders").payload(payload),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }
    let during = node.scrape().await;
    let refused = kafka(&node, "not-the-token")
        .send(
            FutureRecord::<(), str>::to("orders").payload("x"),
            Duration::from_secs(5),
        )
        .await;
    assert!(refused.is_err());
    let after = node.scrape().await;

    assert_eq!(delta(&before, &after, "pico_records_in_total"), 2.0);
    assert!(
        delta(
            &before,
            &after,
            r#"pico_requests_total{protocol="kafka",operation="Produce",status="ok"}"#
        ) >= 1.0
    );
    assert!(value(&during, r#"pico_connections{listener="kafka"}"#) >= 1.0);
    assert!(
        delta(
            &before,
            &after,
            r#"pico_auth_failures_total{protocol="kafka",reason="credential"}"#
        ) >= 1.0
    );
    drop(producer);

    let waiting = {
        let http = node.http.clone();
        let url = format!("{}/orders?seq=5&live=long-poll", node.base());
        let root = node.root.clone();
        tokio::spawn(async move { http.get(url).bearer_auth(root).send().await.unwrap() })
    };
    let mut held = 0.0;
    for _ in 0..50 {
        held = value(&node.scrape().await, "pico_live_readers");
        if held >= 1.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(held, 1.0, "the long poll is counted while it waits");
    node.http
        .post(format!("{}/orders", node.base()))
        .bearer_auth(&node.root)
        .header("Content-Type", "text/plain")
        .body("four")
        .send()
        .await
        .unwrap();
    assert_eq!(waiting.await.unwrap().status(), 200);
    assert_eq!(value(&node.scrape().await, "pico_live_readers"), 0.0);

    let before = node.scrape().await;
    let joined = node
        .http
        .post(format!("{}/_groups/workers/members", node.base()))
        .bearer_auth(&node.root)
        .json(&json!({ "subscription": ["/orders"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(joined.status(), 200);
    let after = node.scrape().await;

    assert_eq!(delta(&before, &after, "pico_group_rebalances_total"), 1.0);
    assert_eq!(value(&after, "pico_groups_coordinated"), 1.0);

    node.server.shutdown().await;
}
