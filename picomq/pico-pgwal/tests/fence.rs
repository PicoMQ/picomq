mod common;

use s3stream_wal::{WalError, WriteAheadLog};

use common::{cluster, record, recovered, recovering, url, wal};

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

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-pgwal --test fence -- --ignored"]
async fn a_failover_claim_fences_the_writer_at_the_same_epoch() {
    let url = url();
    let cluster = cluster();

    let owner = wal(&url, &cluster, 4, "");
    owner.start().await.unwrap();
    let acked = owner.append(record(1, 0, b"before")).await.unwrap();

    let recovery = recovering(&url, &cluster, 4, "").await;
    recovery.start().await.unwrap();

    let error = owner.append(record(1, 1, b"zombie")).await.unwrap_err();
    assert!(matches!(error, WalError::Fenced { .. }), "{error}");
    let error = owner.trim(acked.record_offset).await.unwrap_err();
    assert!(matches!(error, WalError::Fenced { .. }), "{error}");

    let payloads: Vec<_> = recovered(&recovery)
        .await
        .iter()
        .map(|r| r.payload().to_vec())
        .collect();
    assert_eq!(payloads, vec![b"before".to_vec()]);

    let restarted = wal(&url, &cluster, 4, "");
    let error = restarted.start().await.unwrap_err();
    assert!(matches!(error, WalError::Fenced { .. }), "{error}");

    let newer = wal(&url, &cluster, 5, "");
    newer.start().await.unwrap();

    recovery.shutdown_gracefully().await;
    newer.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-pgwal --test fence -- --ignored"]
async fn a_failover_claim_cannot_go_back_an_epoch() {
    let url = url();
    let cluster = cluster();

    let owner = wal(&url, &cluster, 6, "");
    owner.start().await.unwrap();

    let stale = recovering(&url, &cluster, 5, "").await;
    let error = stale.start().await.unwrap_err();
    assert!(matches!(error, WalError::Fenced { .. }), "{error}");

    owner.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-pgwal --test fence -- --ignored"]
async fn recovery_keeps_the_ring_shape_the_owner_stored() {
    let url = url();
    let cluster = cluster();

    let owner = wal(
        &url,
        &cluster,
        3,
        "segments=4&segmentBytes=2097152&maxUnflushedBytes=1048576",
    );
    owner.start().await.unwrap();
    owner.append(record(1, 0, b"unflushed")).await.unwrap();

    let recovery = recovering(&url, &cluster, 3, "segments=8").await;
    recovery.start().await.unwrap();

    let payloads: Vec<_> = recovered(&recovery)
        .await
        .iter()
        .map(|r| r.payload().to_vec())
        .collect();
    assert_eq!(payloads, vec![b"unflushed".to_vec()]);

    recovery.shutdown_gracefully().await;
}
