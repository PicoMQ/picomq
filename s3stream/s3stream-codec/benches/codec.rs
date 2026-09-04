//! Perf regression tripwire for the hot-path codec.
//!
//! Run: cargo bench -p s3stream-codec

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use s3stream_codec::{StreamRecordBatch, wal_crc32};
use std::hint::black_box;

fn bench_codec(c: &mut Criterion) {
    for payload_size in [128usize, 4096, 65536] {
        let payload = Bytes::from(vec![0xA5u8; payload_size]);
        let encoded = StreamRecordBatch::new(1, 1, 0, 1, payload.clone()).encoded();

        let mut group = c.benchmark_group(format!("payload_{payload_size}"));
        group.throughput(Throughput::Bytes(payload_size as u64));

        group.bench_function("encode", |b| {
            b.iter(|| {
                StreamRecordBatch::new(
                    black_box(1),
                    black_box(1),
                    black_box(0),
                    black_box(1),
                    payload.clone(),
                )
            })
        });

        group.bench_function("parse", |b| {
            b.iter(|| {
                let mut buf = encoded.clone();
                StreamRecordBatch::parse(black_box(&mut buf)).unwrap()
            })
        });

        group.bench_function("wal_crc32", |b| b.iter(|| wal_crc32(black_box(&payload))));

        group.finish();
    }
}

criterion_group!(benches, bench_codec);
criterion_main!(benches);
