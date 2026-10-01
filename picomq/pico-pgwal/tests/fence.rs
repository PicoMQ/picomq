mod common;

use s3stream_wal::{WalError, WriteAheadLog};

use common::{cluster, record, recovered, url, wal};

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-pgwal --test fence -- --ignored"]
async fn a_newer_epoch_fences_the_running_writer() {
    let url = url();
    let cluster = cluster();

    let stale = wal(&url, &cluster, 1, "");
    stale.start().await.unwrap();
    let acked = stale.append(record(1, 0, b"before")).await.unwrap();

    let fresh = wal(&url, &cluster, 2, "");
    fresh.start().await.unwrap();

    let error = stale.append(record(1, 1, b"zombie")).await.unwrap_err();
    assert!(matches!(error, WalError::Fenced { .. }), "{error}");
    let error = stale.submit(record(1, 2, b"zombie")).err().unwrap();
    assert!(matches!(error, WalError::Fenced { .. }), "{error}");
    let error = stale.trim(acked.record_offset).await.unwrap_err();
    assert!(matches!(error, WalError::Fenced { .. }), "{error}");

    let payloads: Vec<_> = recovered(&fresh)
        .await
        .iter()
        .map(|r| r.payload().to_vec())
        .collect();
    assert_eq!(payloads, vec![b"before".to_vec()]);
    fresh.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-pgwal --test fence -- --ignored"]
async fn an_older_epoch_cannot_start() {
    let url = url();
    let cluster = cluster();

    let current = wal(&url, &cluster, 5, "");
    current.start().await.unwrap();

    let stale = wal(&url, &cluster, 3, "");
    let error = stale.start().await.unwrap_err();
    assert!(matches!(error, WalError::Fenced { .. }), "{error}");
    current.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-pgwal --test fence -- --ignored"]
async fn concurrent_commits_never_land_after_the_fence() {
    let url = url();
    let cluster = cluster();

    let stale = wal(&url, &cluster, 1, "batchInterval=0&maxBytesInBatch=256");
    stale.start().await.unwrap();
    let pending: Vec<_> = (0..200u64)
        .filter_map(|i| stale.submit(record(1, i, &[1u8; 64])).ok())
        .collect();

    let fresh = wal(&url, &cluster, 2, "");
    fresh.start().await.unwrap();

    let mut acked = 0usize;
    for append in pending {
        if append.durable.await.is_ok() {
            acked += 1;
        }
    }
    let recovered = recovered(&fresh).await.len();
    assert!(
        recovered >= acked,
        "acked {acked} records but only {recovered} survived the fence"
    );
    fresh.shutdown_gracefully().await;
}
