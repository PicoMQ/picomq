use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;

fn endpoint() -> String {
    std::env::var("PICO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4437".into())
}

fn standby() -> String {
    std::env::var("PICO_STANDBY_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4438".into())
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/e2e/ext-{prefix}-{nanos}")
}

fn run(variable: &str) {
    let command = std::env::var(variable).unwrap_or_else(|_| panic!("{variable} is not set"));
    let status = Command::new("sh").args(["-c", &command]).status().unwrap();
    assert!(status.success(), "{variable} failed: {command}");
}

fn records(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("record-{i}")).collect()
}

struct Harness {
    http: reqwest::Client,
    base: String,
}

impl Harness {
    fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
            base: endpoint().trim_end_matches('/').to_owned(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn create(&self, path: &str) {
        let created = self
            .http
            .put(self.url(path))
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), 201, "{path}");
    }

    async fn append(&self, path: &str, body: &str) {
        let appended = self
            .http
            .post(self.url(path))
            .header("Content-Type", "text/plain")
            .body(body.to_owned())
            .send()
            .await
            .unwrap();
        assert_eq!(appended.status(), 200, "{path}");
    }

    async fn read(&self, path: &str) -> Vec<String> {
        let records: Value = self
            .http
            .get(self.url(&format!("{path}?seq=0")))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        records
            .as_array()
            .unwrap()
            .iter()
            .map(|record| record["body"].as_str().unwrap().to_owned())
            .collect()
    }

    async fn wait(&self, path: &str) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if let Ok(head) = self.http.head(self.url(path)).send().await
                && head.status().is_success()
            {
                return;
            }
            assert!(Instant::now() < deadline, "{path} never became readable");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn fill(&self, path: &str, count: usize) -> Vec<String> {
        let expected = records(count);
        self.create(path).await;
        for record in &expected {
            self.append(path, record).await;
        }
        expected
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "run via scripts/e2e.sh extension"]
async fn acked_records_survive_killing_postgres() {
    let h = Harness::new();
    let path = unique("crash");
    let expected = h.fill(&path, 50).await;

    run("PICO_CRASH_CMD");

    h.wait(&path).await;
    assert_eq!(h.read(&path).await, expected);
    h.append(&path, "after").await;
    assert_eq!(h.read(&path).await.len(), expected.len() + 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "run via scripts/e2e.sh extension"]
async fn a_clean_restart_keeps_every_record() {
    let h = Harness::new();
    let path = unique("restart");
    let expected = h.fill(&path, 20).await;

    run("PICO_RESTART_CMD");

    h.wait(&path).await;
    assert_eq!(h.read(&path).await, expected);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "run via scripts/e2e.sh extension"]
async fn a_standby_runs_no_pico() {
    let h = Harness::new();
    let path = unique("standby");
    h.fill(&path, 1).await;

    let probe = h
        .http
        .head(format!("{}{path}", standby().trim_end_matches('/')))
        .timeout(Duration::from_secs(5))
        .send()
        .await;
    assert!(probe.is_err(), "standby answered: {probe:?}");
}
