//! Scale gates for the SQL sink. Every command pays for a durable SQL append,
//! so the gates assert that throughput is bounded by batches (fewer durable
//! INSERTs than commands) and that the snapshot cycle keeps the log table from
//! growing with history.
//!
//! The default gate runs 20k creates (CI-friendly, a few seconds on SQLite
//! with full-sync durability). The 1M gate is `#[ignore]`d. Run it explicitly
//! with `cargo test -p pico-sql --test scale -- --ignored`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pico_metadata::{CommandSink, MetadataCommand, MetadataResult};
use pico_sql::{MetaStore, SqlSink, SqlSinkConfig, SqliteStore};

const NODE: i32 = 1;
const EPOCH: i64 = 1;
const SNAPSHOT_EVERY: u64 = 512;
/// In-flight propose fan-out. Wider than `max_batch` (256) so group commit has
/// material to coalesce.
const CONCURRENCY: usize = 1024;

fn config() -> SqlSinkConfig {
    SqlSinkConfig {
        poll_interval: Duration::from_millis(1),
        snapshot_every: SNAPSHOT_EVERY,
        ..SqlSinkConfig::default()
    }
}

async fn run_gate(total: u64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scale.db");
    let store: Arc<dyn MetaStore> = Arc::new(SqliteStore::open(&path).await.unwrap());

    let (sink, views) = SqlSink::open(store.clone(), config()).await.unwrap();
    let sink = Arc::new(sink);
    sink.propose(MetadataCommand::RegisterNode {
        node_id: NODE,
        node_epoch: EPOCH,
        http_address: String::new(),
        slots: 1,
        protocol_addresses: Default::default(),
    })
    .await
    .unwrap();

    // Create `total` streams with CONCURRENCY proposes in flight.
    let started = Instant::now();
    let mut ids: Vec<u64> = Vec::with_capacity(total as usize);
    let mut inflight = tokio::task::JoinSet::new();
    let mut submitted = 0u64;
    while ids.len() < total as usize {
        while submitted < total && inflight.len() < CONCURRENCY {
            let sink = sink.clone();
            inflight.spawn(async move {
                sink.propose(MetadataCommand::CreateStream {
                    node_id: NODE,
                    node_epoch: EPOCH,
                })
                .await
                .unwrap()
                .result
            });
            submitted += 1;
        }
        match inflight
            .join_next()
            .await
            .expect("inflight not empty")
            .unwrap()
        {
            MetadataResult::Id(id) => ids.push(id),
            other => panic!("unexpected result {other:?}"),
        }
    }
    let elapsed = started.elapsed();
    let per_sec = total as f64 / elapsed.as_secs_f64();
    println!("{total} creates through SqlSink: {elapsed:?} ({per_sec:.0}/s)");

    // No creates lost or duplicated.
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len() as u64, total, "every create got a unique id");
    assert_eq!(views.load().state.streams.len() as u64, total);

    // Group commit did its job: far fewer durable rows than commands. The
    // tailer truncates as it snapshots, so measure rows-ever-written via the
    // top log index instead of surviving rows.
    let last = store.last_idx().await.unwrap();
    println!(
        "log rows for {total} commands: {last} ({:.1} commands/row)",
        total as f64 / last as f64
    );
    assert!(
        last < total / 2,
        "group commit should coalesce: {last} rows for {total} commands"
    );

    // The snapshot cycle kept the log table bounded by the cycle length, not
    // by history. (The tailer may still be mid-cycle: allow one extra window.)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let surviving = loop {
        let rows = store.fetch_after(0, u32::MAX).await.unwrap().len() as u64;
        if rows <= 2 * SNAPSHOT_EVERY {
            break rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "log never truncated: {rows} rows"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    println!("surviving log rows: {surviving} (snapshot_every = {SNAPSHOT_EVERY})");
    drop(sink);

    // Cold start at scale: snapshot + tail restores everything.
    let started = Instant::now();
    let store: Arc<dyn MetaStore> = Arc::new(SqliteStore::open(&path).await.unwrap());
    let (sink, views) = SqlSink::open(store, config()).await.unwrap();
    println!("cold start with {total} streams: {:?}", started.elapsed());
    assert_eq!(views.load().state.streams.len() as u64, total);
    let next = sink
        .propose(MetadataCommand::CreateStream {
            node_id: NODE,
            node_epoch: EPOCH,
        })
        .await
        .unwrap();
    assert_eq!(
        next.result,
        MetadataResult::Id(total),
        "id counter survived at scale"
    );
}

/// CI-friendly gate: 20k creates through the durable sink.
#[tokio::test]
async fn twenty_k_creates_gate() {
    run_gate(20_000).await;
}

/// The full 1M gate. Run explicitly in release mode (see module docs).
#[tokio::test]
#[ignore = "run explicitly: cargo test --release -p pico-sql --test scale -- --ignored --nocapture"]
async fn million_creates_gate() {
    run_gate(1_000_000).await;
}
