#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use picomq_pgwal::{Config, PgWal};
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

pub const NODE: u32 = 7;

pub fn config(url: &str, cluster: &str, epoch: u64, params: &str) -> Config {
    let separator = if url.contains('?') { '&' } else { '?' };
    let uri = if params.is_empty() {
        url.to_owned()
    } else {
        format!("{url}{separator}{params}")
    };

    Config::parse(&uri).unwrap().identity(cluster, NODE, epoch)
}

pub fn wal(url: &str, cluster: &str, epoch: u64, params: &str) -> PgWal {
    PgWal::new(config(url, cluster, epoch, params)).unwrap()
}

pub async fn recovering(url: &str, cluster: &str, epoch: u64, params: &str) -> PgWal {
    PgWal::recovering(config(url, cluster, 0, params), NODE, epoch)
        .await
        .unwrap()
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

pub async fn slots(url: &str, cluster: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.unwrap();
    let count = sqlx::query_scalar("SELECT count(*) FROM pg_tables WHERE tablename LIKE $1")
        .bind(format!(
            "pico\\_wal\\_{:08x}\\_{NODE}\\_%",
            crc32fast::hash(cluster.as_bytes())
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    count
}

pub async fn recovered(wal: &PgWal) -> Vec<StreamRecordBatch> {
    wal.recover()
        .map(|result| result.unwrap().record)
        .collect()
        .await
}
