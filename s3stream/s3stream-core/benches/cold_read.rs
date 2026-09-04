//! Cold-read-path benchmarks: StreamReaders (readahead + DataBlockCache) over
//! in-memory object storage. Measures the cache machinery, index walking, and record
//! decode with storage latency removed. See the README's perf table.

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};

use s3stream_codec::StreamRecordBatch;
use s3stream_core::cache::block_cache::S3BlockCache;
use s3stream_core::cache::blockcache::StreamReaders;
use s3stream_core::manager::{
    CommitStreamSetObjectRequest, ObjectManager, StreamManager, StreamObject,
};
use s3stream_core::memory::MemoryMetadataManager;
use s3stream_object::{
    MemoryObjectStorage, NOOP_OBJECT_ID, ObjectStorage, ObjectWriter, WriteOptions,
};

const RECORD_PAYLOAD: usize = 1024;
const RECORDS_PER_OBJECT: u64 = 1024;
const OBJECTS: u64 = 8;
const TOTAL_RECORDS: u64 = RECORDS_PER_OBJECT * OBJECTS;

struct Env {
    manager: Arc<MemoryMetadataManager>,
    storage: Arc<MemoryObjectStorage>,
    stream_id: u64,
    runtime: tokio::runtime::Runtime,
}

fn setup() -> Env {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let manager = MemoryMetadataManager::new();
    let storage = Arc::new(MemoryObjectStorage::new(0));
    let stream_id = runtime.block_on(async {
        let stream_id = manager.create_stream(HashMap::new()).await.unwrap();
        manager
            .open_stream(stream_id, 1, HashMap::new())
            .await
            .unwrap();
        for object_index in 0..OBJECTS {
            let object_id = object_index + 1;
            let start = object_index * RECORDS_PER_OBJECT;
            let records: Vec<StreamRecordBatch> = (start..start + RECORDS_PER_OBJECT)
                .map(|o| {
                    StreamRecordBatch::new(stream_id, 1, o, 1, vec![o as u8; RECORD_PAYLOAD].into())
                })
                .collect();
            // 1 MiB blocks like ObjectWriter defaults in production configs.
            let mut writer = ObjectWriter::open(
                object_id,
                storage.as_ref(),
                1 << 20,
                16 << 20,
                WriteOptions::default(),
            )
            .await
            .unwrap();
            writer.write(stream_id, &records).await.unwrap();
            let size = writer.close().await.unwrap();
            manager
                .commit_stream_set_object(CommitStreamSetObjectRequest {
                    object_id: NOOP_OBJECT_ID,
                    stream_objects: vec![StreamObject {
                        object_id,
                        object_size: size,
                        stream_id,
                        start_offset: start,
                        end_offset: start + RECORDS_PER_OBJECT,
                        attributes: 0,
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        stream_id
    });
    Env {
        manager,
        storage,
        stream_id,
        runtime,
    }
}

fn readers(env: &Env) -> Arc<StreamReaders> {
    let _guard = env.runtime.enter();
    StreamReaders::new(
        256 << 20,
        env.manager.clone() as Arc<dyn ObjectManager>,
        env.storage.clone() as Arc<dyn ObjectStorage>,
        4,
    )
}

fn bench_cold_read(c: &mut Criterion) {
    let env = setup();
    let mut group = c.benchmark_group("cold_read");
    group.throughput(Throughput::Bytes(TOTAL_RECORDS * RECORD_PAYLOAD as u64));

    // Fresh cache every iteration: every block load goes to storage (cold).
    group.bench_function("scan_8mib_cold_cache", |b| {
        b.iter_batched(
            || readers(&env),
            |readers| {
                env.runtime.block_on(async {
                    let mut next = 0u64;
                    while next < TOTAL_RECORDS {
                        let read = readers
                            .read(env.stream_id, next, TOTAL_RECORDS, 1 << 20)
                            .await
                            .unwrap();
                        next = read.records.last().unwrap().last_offset();
                    }
                })
            },
            BatchSize::PerIteration,
        );
    });

    // Reader + cache reused: sequential consumer with readahead warm-up.
    let warm_readers = readers(&env);
    group.bench_function("scan_8mib_sequential_readahead", |b| {
        b.iter(|| {
            env.runtime.block_on(async {
                let mut next = 0u64;
                while next < TOTAL_RECORDS {
                    let read = warm_readers
                        .read(env.stream_id, next, TOTAL_RECORDS, 1 << 20)
                        .await
                        .unwrap();
                    next = read.records.last().unwrap().last_offset();
                }
            })
        });
    });
    group.finish();
}

criterion_group!(benches, bench_cold_read);
criterion_main!(benches);
