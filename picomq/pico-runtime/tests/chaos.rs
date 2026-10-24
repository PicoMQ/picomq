//! Fault injection against a two-node cluster: paused nodes, a paused
//! leader, a metadata outage and a clean stop. No acknowledged write may be
//! lost and no healthy node may be fenced.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Harness, assert_exactly_once, produce, read_partitions, unique, write_continuously};
use serde_json::Value;

const TOKEN: &str = "ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY";
const PARTITIONS: i32 = 4;

struct Cluster {
    compose: String,
    admins: [String; 2],
    http: reqwest::Client,
    harness: Harness,
}

impl Cluster {
    fn from_env() -> Option<Self> {
        let compose = std::env::var("PICOMQ_COMPOSE").ok()?;
        let admin2 =
            std::env::var("PICO_ADMIN_2").unwrap_or_else(|_| "http://127.0.0.1:9091".into());
        let bootstrap2 = std::env::var("PICO_KAFKA_2").unwrap_or_else(|_| "127.0.0.1:9093".into());
        let harness = Harness::new();

        Some(Self {
            compose,
            admins: ["http://127.0.0.1:9090".into(), admin2],
            http: reqwest::Client::new(),
            harness: Harness {
                bootstrap: format!("{},{bootstrap2}", harness.bootstrap),
                ..harness
            },
        })
    }

    fn compose(&self, args: &str) {
        let command = format!("{} {args}", self.compose);
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(&command)
            .status()
            .unwrap();
        assert!(status.success(), "{command} failed");
    }

    async fn get(&self, node: usize, path: &str) -> Option<Value> {
        let response = self
            .http
            .get(format!("{}{path}", self.admins[node - 1]))
            .bearer_auth(std::env::var("PICO_TOKEN").unwrap_or_else(|_| TOKEN.into()))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .ok()?;
        response.json().await.ok()
    }

    async fn ready(&self, node: usize) -> bool {
        self.get(node, "/ready")
            .await
            .is_some_and(|ready| ready["ready"] == true)
    }

    /// Node `id`'s row as `asker` sees it: (epoch, fenced).
    async fn row(&self, asker: usize, id: i64) -> Option<(i64, bool)> {
        let nodes = self.get(asker, "/admin/nodes").await?;
        nodes["nodes"].as_array()?.iter().find_map(|node| {
            (node["nodeId"] == id).then(|| {
                (
                    node["nodeEpoch"].as_i64().unwrap_or_default(),
                    node["fenced"] == true,
                )
            })
        })
    }

    async fn both_ready(&self) {
        eventually(
            "both nodes to be ready",
            Duration::from_secs(120),
            || async { self.ready(1).await && self.ready(2).await },
        )
        .await;
    }

    /// The node holding the maintenance lease, which runs failover.
    async fn leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            for node in [1, 2] {
                if let Some(cluster) = self.get(node, "/admin/cluster").await
                    && cluster["leaseHolder"] == true
                {
                    return node;
                }
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for a lease holder"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

async fn eventually<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_paused_node_loses_no_acknowledged_write() {
    let Some(cluster) = Cluster::from_env() else {
        eprintln!("set PICOMQ_COMPOSE to run the chaos tests");
        return;
    };
    cluster.both_ready().await;
    let topic = unique("chaos-paused");
    cluster.harness.create_topic(&topic, PARTITIONS).await;
    let (epoch, _) = cluster.row(1, 2).await.unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = write_continuously(&cluster.harness.bootstrap, &topic, stop.clone());
    tokio::time::sleep(Duration::from_secs(2)).await;

    cluster.compose("pause pico2");
    eventually("node 2 to be fenced", Duration::from_secs(60), || async {
        cluster.row(1, 2).await == Some((epoch, true))
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    cluster.compose("unpause pico2");
    cluster.both_ready().await;

    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let acked = writer.await.unwrap();
    assert!(!acked.is_empty());
    assert_eq!(
        cluster.row(2, 1).await.map(|(_, fenced)| fenced),
        Some(false)
    );

    let read = read_partitions(&cluster.harness.bootstrap, &topic, PARTITIONS, acked.len()).await;
    assert_exactly_once(&acked, &read);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_paused_leader_does_not_fence_the_node_that_replaced_it() {
    let Some(cluster) = Cluster::from_env() else {
        eprintln!("set PICOMQ_COMPOSE to run the chaos tests");
        return;
    };
    cluster.both_ready().await;
    let leader = cluster.leader().await;
    let survivor = 3 - leader;
    let (survivor_epoch, _) = cluster.row(survivor, survivor as i64).await.unwrap();

    cluster.compose(&format!("pause pico{leader}"));
    eventually(
        "the paused leader to be fenced",
        Duration::from_secs(90),
        || async {
            cluster
                .row(survivor, leader as i64)
                .await
                .is_some_and(|(_, fenced)| fenced)
        },
    )
    .await;
    cluster.compose(&format!("unpause pico{leader}"));

    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(
        cluster.row(survivor, survivor as i64).await,
        Some((survivor_epoch, false)),
        "the node that took over must stay live"
    );
    assert!(cluster.ready(survivor).await);
    cluster.both_ready().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_metadata_outage_fences_nobody() {
    let Some(cluster) = Cluster::from_env() else {
        eprintln!("set PICOMQ_COMPOSE to run the chaos tests");
        return;
    };
    cluster.both_ready().await;
    let before = [cluster.row(1, 1).await, cluster.row(1, 2).await];

    cluster.compose("pause postgres");
    tokio::time::sleep(Duration::from_secs(15)).await;
    cluster.compose("unpause postgres");

    tokio::time::sleep(Duration::from_secs(10)).await;
    let after = [cluster.row(1, 1).await, cluster.row(1, 2).await];
    assert_eq!(
        after, before,
        "no node is fenced or restarted by the outage"
    );

    let topic = unique("chaos-outage");
    cluster.harness.create_topic(&topic, PARTITIONS).await;
    let producer = cluster.harness.producer();
    for value in 0..20 {
        let payload = value.to_string();
        produce(&producer, &topic, Some(&payload), &payload).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_stopped_node_hands_off_at_once() {
    let Some(cluster) = Cluster::from_env() else {
        eprintln!("set PICOMQ_COMPOSE to run the chaos tests");
        return;
    };
    cluster.both_ready().await;
    let (epoch, _) = cluster.row(1, 2).await.unwrap();

    cluster.compose("stop pico2");
    let stopped = Instant::now();
    eventually("node 2 to be fenced", Duration::from_secs(60), || async {
        cluster.row(1, 2).await == Some((epoch, true))
    })
    .await;
    assert!(
        stopped.elapsed() < Duration::from_secs(2),
        "a clean stop fences at once, not after the node timeout"
    );

    cluster.compose("start pico2");
    cluster.both_ready().await;
}
