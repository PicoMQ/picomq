//! Scale gates for the data plane: many named streams on one node, each
//! taking appends through the service. Complements the metadata-plane gates
//! (`picomq-metadata`/`picomq-sql` scale tests) by exercising the per-stream
//! state this layer holds: registry entries, entry cache, gates, and open
//! engine streams.
//!
//! The default gate runs 50k streams. The 1M gate is `#[ignore]`d; run it
//! explicitly with
//! `cargo test --release -p picomq-server --test scale -- --ignored --nocapture`.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use picomq_metadata::{CommandSink, LocalSink};
use picomq_server::{AppendCommand, CreateCommand, LogRecord, NodeConfig, OffsetToken, PicoNode};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};

/// Append durability waits are dominated by the WAL group-commit window
/// (250ms default), so cross-stream throughput scales with in-flight depth.
const CONCURRENCY: usize = 2048;

fn rss_mib() -> f64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse::<f64>()
        .unwrap_or(0.0)
        / 1024.0
}

async fn for_each_stream<F, Fut>(total: u64, f: F)
where
    F: Fn(u64) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut inflight = tokio::task::JoinSet::new();
    let mut submitted = 0u64;
    let mut finished = 0u64;
    while finished < total {
        while submitted < total && inflight.len() < CONCURRENCY {
            inflight.spawn(f.clone()(submitted));
            submitted += 1;
        }
        inflight.join_next().await.expect("nonempty").unwrap();
        finished += 1;
    }
}

async fn run_gate(total: u64) {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 1,
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
        None,
    )
    .await
    .unwrap();
    let service = node.service();
    let base_rss = rss_mib();

    // Phase 1: create `total` streams.
    let started = Instant::now();
    let create_service = service.clone();
    for_each_stream(total, move |i| {
        let service = create_service.clone();
        async move {
            service
                .create(CreateCommand::new(
                    format!("/scale/{i}"),
                    "application/octet-stream",
                ))
                .await
                .unwrap();
        }
    })
    .await;
    let create_elapsed = started.elapsed();
    let create_rss = rss_mib();
    println!(
        "create {total} streams: {create_elapsed:?} ({:.0}/s), rss {base_rss:.0} -> {create_rss:.0} MiB ({:.1} KiB/stream)",
        total as f64 / create_elapsed.as_secs_f64(),
        (create_rss - base_rss) * 1024.0 / total as f64
    );

    // One registry entry, one `idx/sid/` and one `idx/topic/` record per
    // stream.
    {
        let view = node.views().load();
        assert_eq!(view.state.kv.len() as u64, 3 * total);
        assert!(view.state.kv_bytes > 0);
        println!(
            "kv entries: {}, kv bytes: {} ({:.0} B/stream)",
            view.state.kv.len(),
            view.state.kv_bytes,
            view.state.kv_bytes as f64 / total as f64
        );
    }

    // Phase 2: publish one durable record to every stream.
    let started = Instant::now();
    let append_service = service.clone();
    for_each_stream(total, move |i| {
        let service = append_service.clone();
        async move {
            let appended = service
                .append(AppendCommand {
                    name: format!("/scale/{i}"),
                    records: vec![LogRecord::value(Bytes::from_static(&[7u8; 128]))],
                    content_type: Some("application/octet-stream".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(appended.next_offset.record_offset(), 1);
        }
    })
    .await;
    let append_elapsed = started.elapsed();
    let append_rss = rss_mib();
    println!(
        "append 1 record to each of {total} streams: {append_elapsed:?} ({:.0}/s), rss {append_rss:.0} MiB ({:.1} KiB/stream total)",
        total as f64 / append_elapsed.as_secs_f64(),
        (append_rss - base_rss) * 1024.0 / total as f64
    );

    // Phase 3: point reads across the keyspace stay fast at scale.
    let started = Instant::now();
    let step = (total / 1_000).max(1);
    for i in (0..total).step_by(step as usize) {
        let read = service
            .read(&format!("/scale/{i}"), OffsetToken::beginning(), 0, 0)
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
    }
    println!(
        "1k sampled reads across {total} streams: {:?}",
        started.elapsed()
    );

    // Second appends hit warm caches: no per-stream setup cost remains.
    let started = Instant::now();
    let warm_service = service.clone();
    let warm = (total / 10).clamp(1, 10_000);
    for_each_stream(warm, move |i| {
        let service = warm_service.clone();
        let name = format!("/scale/{}", i * (total / warm.max(1)).max(1) % total);
        async move {
            service
                .append(AppendCommand {
                    name,
                    records: vec![LogRecord::value(Bytes::from_static(&[8u8; 128]))],
                    content_type: Some("application/octet-stream".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
    })
    .await;
    println!(
        "{warm} warm re-appends: {:?} ({:.0}/s)",
        started.elapsed(),
        warm as f64 / started.elapsed().as_secs_f64()
    );

    node.close().await;
}

/// CI-friendly gate: 50k streams with a record published on each.
#[tokio::test(flavor = "multi_thread")]
async fn fifty_k_streams_with_appends_gate() {
    run_gate(50_000).await;
}

/// The full 1M gate. Run explicitly in release mode (see module docs).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "run explicitly: cargo test --release -p picomq-server --test scale -- --ignored --nocapture"]
async fn million_streams_with_appends_gate() {
    run_gate(1_000_000).await;
}
