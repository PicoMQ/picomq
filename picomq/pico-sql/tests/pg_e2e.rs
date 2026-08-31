//! Postgres sink e2e. The SqlSink behaviors that matter in the clustered
//! posture, against a real Postgres when `PICOMQ_PG_URL` is set:
//!
//! ```text
//! PICOMQ_PG_URL=postgres://user:pass@localhost:5432/picomq \
//!     cargo test -p picomq-sql --test pg_e2e
//! ```
//!
//! Skipped (pass, with a note) when the variable is absent. WIPES the three
//! metadata tables first. Point it at a dedicated test database.

use std::sync::Arc;
use std::time::Duration;

use picomq_metadata::{CommandSink, MetadataCommand, MetadataResult};
use picomq_sql::{LeaseConfig, LeaseKeeper, MetaStore, PgStore, SqlSink, SqlSinkConfig};

fn config() -> SqlSinkConfig {
    SqlSinkConfig {
        poll_interval: Duration::from_millis(5),
        // Rows, not commands: group commit can pack all 60 creates into one
        // row per sink. 2 sequential registers + 1 row per sink is the
        // guaranteed minimum, so 4 is always reached.
        snapshot_every: 4,
        snapshot_min_interval: Duration::ZERO,
        ..SqlSinkConfig::default()
    }
}

fn register(node_id: i32, node_epoch: i64) -> MetadataCommand {
    MetadataCommand::RegisterNode {
        node_id,
        node_epoch,
        http_address: String::new(),
        slots: 1,
        protocol_addresses: Default::default(),
    }
}

#[tokio::test]
async fn postgres_sink_multi_writer_snapshot_and_lease() {
    let Ok(url) = std::env::var("PICOMQ_PG_URL") else {
        eprintln!("PICOMQ_PG_URL not set; skipping postgres e2e");
        return;
    };

    let admin = sqlx::PgPool::connect(&url)
        .await
        .expect("connect for cleanup");
    sqlx::query("DROP TABLE IF EXISTS meta_log, meta_snapshot, meta_lease")
        .execute(&admin)
        .await
        .expect("drop tables");
    admin.close().await;

    // Two independent sinks (two "nodes") over the same database.
    let store_a: Arc<dyn MetaStore> = Arc::new(PgStore::connect(&url).await.unwrap());
    let store_b: Arc<dyn MetaStore> = Arc::new(PgStore::connect(&url).await.unwrap());
    let (sink_a, views_a) = SqlSink::open(store_a.clone(), config()).await.unwrap();
    let (sink_b, views_b) = SqlSink::open(store_b.clone(), config()).await.unwrap();
    let (sink_a, sink_b) = (Arc::new(sink_a), Arc::new(sink_b));

    sink_a.propose(register(1, 10)).await.unwrap();
    sink_b.propose(register(2, 20)).await.unwrap();

    // Concurrent multi-writer creates: unique ids, nothing lost.
    let mut handles = Vec::new();
    for i in 0..60u32 {
        let (sink, node_id, node_epoch) = if i % 2 == 0 {
            (sink_a.clone(), 1, 10)
        } else {
            (sink_b.clone(), 2, 20)
        };
        handles.push(tokio::spawn(async move {
            sink.propose(MetadataCommand::CreateStream {
                node_id,
                node_epoch,
            })
            .await
            .unwrap()
            .result
        }));
    }
    let mut ids = Vec::new();
    for handle in handles {
        match handle.await.unwrap() {
            MetadataResult::Id(id) => ids.push(id),
            other => panic!("unexpected result {other:?}"),
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..60).collect::<Vec<u64>>());

    // Both replicas converge.
    let target = views_a
        .load()
        .applied_index
        .max(views_b.load().applied_index);
    let view_a = views_a.wait_applied(target).await;
    let view_b = views_b.wait_applied(target).await;
    assert_eq!(view_a.state, view_b.state);
    assert_eq!(view_a.state.streams.len(), 60);

    // The snapshot cycle ran against Postgres (62 rows >> snapshot_every=16).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while store_a.load_snapshot().await.unwrap().is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "snapshot never written"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Lease election on Postgres: exactly one leader, clean handover.
    let lease_config = LeaseConfig {
        ttl_ms: 1_000,
        check_interval: Duration::from_millis(50),
    };
    let keeper_a = LeaseKeeper::spawn(store_a.clone(), "node-a".into(), lease_config.clone());
    let mut rx_a = keeper_a.leadership();
    tokio::time::timeout(Duration::from_secs(5), rx_a.wait_for(|v| *v))
        .await
        .expect("a never became leader")
        .unwrap();
    let keeper_b = LeaseKeeper::spawn(store_b.clone(), "node-b".into(), lease_config);
    let mut rx_b = keeper_b.leadership();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!*rx_b.borrow(), "two leaders on one lease");
    keeper_a.shutdown().await;
    tokio::time::timeout(Duration::from_secs(5), rx_b.wait_for(|v| *v))
        .await
        .expect("b never took over")
        .unwrap();
    keeper_b.shutdown().await;

    // Restart recovery from Postgres (snapshot + tail).
    drop(sink_a);
    drop(sink_b);
    let store: Arc<dyn MetaStore> = Arc::new(PgStore::connect(&url).await.unwrap());
    let (sink, views) = SqlSink::open(store, config()).await.unwrap();
    assert_eq!(views.load().state.streams.len(), 60);
    let next = sink
        .propose(MetadataCommand::CreateStream {
            node_id: 1,
            node_epoch: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        next.result,
        MetadataResult::Id(60),
        "id counter survived restart"
    );
}
