mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use s3stream_wal::WriteAheadLog;

use common::{cluster, record, url, wal};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-wal-pg --test retry -- --ignored"]
async fn acks_survive_killed_connections_exactly_once() {
    let url = url();
    let cluster = cluster();
    let first = wal(&url, &cluster, 1, "batchInterval=0&maxBytesInBatch=512");
    first.start().await.unwrap();

    let killer = sqlx::PgPool::connect(&url).await.unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let chaos = tokio::spawn({
        let stop = Arc::clone(&stop);
        async move {
            let mut killed = 0i64;
            while !stop.load(Ordering::SeqCst) {
                killed += sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE application_name = 'picomq-wal' AND pid <> pg_backend_pid()) killed",
                )
                .fetch_one(&killer)
                .await
                .unwrap();
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            killed
        }
    });

    let mut acked = Vec::new();
    for round in 0..40u64 {
        let pending: Vec<_> = (0..10u64)
            .map(|i| {
                first
                    .submit(record(6, round * 10 + i, &[i as u8; 100]))
                    .unwrap()
            })
            .collect();
        for append in pending {
            acked.push(append.durable.await.unwrap().record_offset.offset);
        }
    }
    stop.store(true, Ordering::SeqCst);
    let killed = chaos.await.unwrap();
    assert!(killed > 0, "no wal connection was ever terminated");
    first.shutdown_gracefully().await;

    let second = wal(&url, &cluster, 2, "");
    second.start().await.unwrap();
    let recovered: Vec<_> = futures::StreamExt::collect::<Vec<_>>(second.recover())
        .await
        .into_iter()
        .map(|result| result.unwrap())
        .collect();
    let offsets: Vec<_> = recovered.iter().map(|r| r.record_offset.offset).collect();
    assert_eq!(offsets, acked);
    for (i, result) in recovered.iter().enumerate() {
        assert_eq!(result.record.base_offset(), i as u64);
    }
    second.shutdown_gracefully().await;
}
