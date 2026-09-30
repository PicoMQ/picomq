mod common;

use std::sync::{Arc, Mutex};

use s3stream_wal::{WalError, WriteAheadLog};

use common::{cluster, record, recovered, url, wal};

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-wal-pg --test wal -- --ignored"]
async fn append_recover_reset_cycle() {
    let url = url();
    let cluster = cluster();

    let first = wal(&url, &cluster, 1, "");
    first.start().await.unwrap();
    let mut last = 0;
    for i in 0..5u64 {
        let appended = first
            .append(record(9, i, format!("payload-{i}").as_bytes()))
            .await
            .unwrap();
        last = appended.next_offset.offset;
    }
    assert_eq!(first.confirm_offset().offset, last);
    first.shutdown_gracefully().await;

    let second = wal(&url, &cluster, 2, "");
    second.start().await.unwrap();
    let records = recovered(&second).await;
    assert_eq!(records.len(), 5);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.base_offset(), i as u64);
        assert_eq!(record.payload().as_ref(), format!("payload-{i}").as_bytes());
    }
    second.reset().await.unwrap();
    second.shutdown_gracefully().await;

    let third = wal(&url, &cluster, 3, "");
    third.start().await.unwrap();
    assert!(recovered(&third).await.is_empty());
    let appended = third.append(record(9, 5, b"after-reset")).await.unwrap();
    assert!(appended.record_offset.offset >= last);
    third.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-wal-pg --test wal -- --ignored"]
async fn get_and_get_range() {
    let url = url();
    let wal = wal(&url, &cluster(), 1, "");
    wal.start().await.unwrap();

    let first = wal.append(record(1, 0, b"alpha")).await.unwrap();
    let second = wal.append(record(1, 1, b"beta")).await.unwrap();
    let third = wal.append(record(1, 2, b"gamma")).await.unwrap();

    let got = wal.get(second.record_offset).await.unwrap();
    assert_eq!(got.payload().as_ref(), b"beta");

    let got = wal
        .get_range(first.record_offset, third.next_offset)
        .await
        .unwrap();
    let payloads: Vec<_> = got.iter().map(|r| r.payload().to_vec()).collect();
    assert_eq!(
        payloads,
        vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
    );

    let got = wal
        .get_range(first.record_offset, first.record_offset)
        .await
        .unwrap();
    assert!(got.is_empty());
    wal.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-wal-pg --test wal -- --ignored"]
async fn trim_inside_a_batch_recovers_only_the_suffix() {
    let url = url();
    let cluster = cluster();
    let first = wal(&url, &cluster, 1, "batchInterval=50");
    first.start().await.unwrap();

    let pending = vec![
        first.submit(record(1, 0, b"committed")).unwrap(),
        first.submit(record(1, 1, b"survivor-a")).unwrap(),
        first.submit(record(1, 2, b"survivor-b")).unwrap(),
    ];
    let mut appended = Vec::new();
    for append in pending {
        appended.push(append.durable.await.unwrap());
    }
    first.trim(appended[0].record_offset).await.unwrap();
    first.shutdown_gracefully().await;

    let second = wal(&url, &cluster, 2, "batchInterval=50");
    second.start().await.unwrap();
    let payloads: Vec<_> = recovered(&second)
        .await
        .iter()
        .map(|r| r.payload().to_vec())
        .collect();
    assert_eq!(
        payloads,
        vec![b"survivor-a".to_vec(), b"survivor-b".to_vec()]
    );
    second.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-wal-pg --test wal -- --ignored"]
async fn recovers_without_a_clean_shutdown() {
    let url = url();
    let cluster = cluster();
    let first = wal(&url, &cluster, 1, "");
    first.start().await.unwrap();
    first.append(record(4, 0, b"acked")).await.unwrap();
    drop(first);

    let second = wal(&url, &cluster, 2, "");
    second.start().await.unwrap();
    let records = recovered(&second).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload().as_ref(), b"acked");
    second.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-wal-pg --test wal -- --ignored"]
async fn listener_runs_in_offset_order_before_acks() {
    let url = url();
    let wal = wal(&url, &cluster(), 1, "maxBytesInBatch=64&batchInterval=0");
    wal.start().await.unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    wal.set_append_listener(Arc::new(move |_, offset, _| {
        sink.lock().unwrap().push(offset.offset);
    }));

    let pending: Vec<_> = (0..50u64)
        .map(|i| wal.submit(record(2, i, &[i as u8; 40])).unwrap())
        .collect();
    let mut acked = Vec::new();
    for append in pending {
        let result = append.durable.await.unwrap();
        assert!(seen.lock().unwrap().contains(&result.record_offset.offset));
        acked.push(result.record_offset.offset);
    }
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen, acked);
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]));
    wal.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-wal-pg --test wal -- --ignored"]
async fn full_ring_backpressures_until_trimmed() {
    let url = url();
    let params =
        "segments=2&segmentBytes=4096&maxBytesInBatch=1024&maxUnflushedBytes=2048&batchInterval=0";
    let wal = wal(&url, &cluster(), 1, params);
    wal.start().await.unwrap();

    let mut last = None;
    let blocked = loop {
        match wal.append(record(3, 0, &[7u8; 400])).await {
            Ok(appended) => last = Some(appended.record_offset),
            Err(error) => break error,
        }
    };
    assert!(
        matches!(blocked, WalError::OverCapacity { .. }),
        "{blocked}"
    );
    assert!(wal.confirm_offset().offset >= 4096);

    wal.trim(last.unwrap()).await.unwrap();
    wal.append(record(3, 0, &[7u8; 400])).await.unwrap();
    wal.shutdown_gracefully().await;
}

#[tokio::test]
#[ignore = "run explicitly: PICO_TEST_PG_URL=postgres://... cargo test -p picomq-wal-pg --test wal -- --ignored"]
async fn ring_geometry_changes_only_once_drained() {
    let url = url();
    let cluster = cluster();

    let first = wal(&url, &cluster, 1, "segments=4");
    first.start().await.unwrap();
    first.append(record(5, 0, b"unflushed")).await.unwrap();
    first.shutdown_gracefully().await;

    let resized = wal(&url, &cluster, 2, "segments=6");
    let error = resized.start().await.unwrap_err();
    assert!(error.to_string().contains("ring changed"), "{error}");

    let drained = wal(&url, &cluster, 3, "segments=4");
    drained.start().await.unwrap();
    assert_eq!(recovered(&drained).await.len(), 1);
    drained.reset().await.unwrap();
    drained.shutdown_gracefully().await;

    let resized = wal(&url, &cluster, 4, "segments=6");
    resized.start().await.unwrap();
    assert!(recovered(&resized).await.is_empty());
    resized.append(record(5, 1, b"fresh")).await.unwrap();
    resized.shutdown_gracefully().await;
}
