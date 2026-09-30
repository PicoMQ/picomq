#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use picomq_wal_pg::{Config, PgWal};
use s3stream_codec::StreamRecordBatch;
use s3stream_wal::WriteAheadLog;

static SEQUENCE: AtomicU32 = AtomicU32::new(0);

pub fn url() -> String {
    std::env::var("PICO_TEST_PG_URL").expect("PICO_TEST_PG_URL is not set")
}

pub fn cluster() -> String {
    format!(
        "test-{}-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::SeqCst),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

pub fn wal(url: &str, cluster: &str, epoch: u64, params: &str) -> PgWal {
    let separator = if url.contains('?') { '&' } else { '?' };
    let uri = if params.is_empty() {
        url.to_owned()
    } else {
        format!("{url}{separator}{params}")
    };
    PgWal::new(Config::parse(&uri).unwrap().identity(cluster, 7, epoch)).unwrap()
}

pub fn record(stream_id: u64, base_offset: u64, payload: &[u8]) -> StreamRecordBatch {
    StreamRecordBatch::new(
        stream_id,
        1,
        base_offset,
        1,
        Bytes::copy_from_slice(payload),
    )
}

pub async fn recovered(wal: &PgWal) -> Vec<StreamRecordBatch> {
    wal.recover()
        .map(|result| result.unwrap().record)
        .collect()
        .await
}
