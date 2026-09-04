//! Sharded registry of stream readers. The `S3BlockCache` entry point.
//!
//! A reader is REMOVED from the registry
//! `nextReadOffset`. Two consumers at the same progress collapse onto one reader
//! (the displaced one closes). Idle readers expire after 1 minute, checked lazily on
//! reads and by a background sweep.
//!
//! `std::sync::Mutex` maps (never held across awaits) and the sweep is a tokio task.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;

use s3stream_object::ObjectStorage;

use crate::api::StreamError;
use crate::cache::block_cache::S3BlockCache;
use crate::manager::ObjectManager;
use crate::storage::ReadDataBlock;

use super::data_block::DataBlockCache;
use super::now_ms;
use super::object_reader_cache::ObjectReaderCache;
use super::stream_reader::{ReaderDeps, StreamReader};

const STREAM_READER_EXPIRED_MS: u64 = 60_000;
const STREAM_READER_EXPIRED_CHECK_INTERVAL_MS: u64 = 60_000;

type ReaderKey = (u64, u64); // (stream_id, start_offset)

struct Shard {
    readers: HashMap<ReaderKey, Arc<StreamReader>>,
    last_expired_check_ms: u64,
}

pub struct StreamReaders {
    shards: Vec<Mutex<Shard>>,
    deps: Arc<ReaderDeps>,
    expired_ms: u64,
    check_interval_ms: u64,
}

impl StreamReaders {
    /// `StreamReaders(size, objectManager, objectStorage, objectReaderFactory,
    /// concurrency)` — `cache_size` is the DataBlockCache budget in bytes.
    pub fn new(
        cache_size: u64,
        object_manager: Arc<dyn ObjectManager>,
        object_storage: Arc<dyn ObjectStorage>,
        concurrency: usize,
    ) -> Arc<Self> {
        Self::with_config(
            cache_size,
            object_manager,
            object_storage,
            concurrency,
            STREAM_READER_EXPIRED_MS,
            STREAM_READER_EXPIRED_CHECK_INTERVAL_MS,
        )
    }

    pub fn with_config(
        cache_size: u64,
        object_manager: Arc<dyn ObjectManager>,
        object_storage: Arc<dyn ObjectStorage>,
        concurrency: usize,
        expired_ms: u64,
        check_interval_ms: u64,
    ) -> Arc<Self> {
        let concurrency = concurrency.max(1);
        let deps = Arc::new(ReaderDeps {
            object_manager,
            readers: Arc::new(ObjectReaderCache::new(object_storage)),
            data_block_cache: DataBlockCache::new(cache_size, concurrency),
        });
        let this = Arc::new(Self {
            shards: (0..concurrency)
                .map(|_| {
                    Mutex::new(Shard {
                        readers: HashMap::new(),
                        last_expired_check_ms: now_ms(),
                    })
                })
                .collect(),
            deps,
            expired_ms,
            check_interval_ms,
        });
        // Threads.COMMON_SCHEDULER expiry sweep. Ends when the registry drops.
        let weak: Weak<Self> = Arc::downgrade(&this);
        let interval = check_interval_ms;
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_millis(interval.max(10)));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(this) = weak.upgrade() else { return };
                this.cleanup_all_expired().await;
            }
        });
        this
    }

    pub fn data_block_cache(&self) -> &Arc<DataBlockCache> {
        &self.deps.data_block_cache
    }

    fn shard(&self, stream_id: u64) -> &Mutex<Shard> {
        &self.shards[(stream_id % self.shards.len() as u64) as usize]
    }

    pub fn active_reader_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().expect("shard poisoned").readers.len())
            .sum()
    }

    async fn cleanup_all_expired(&self) {
        for shard in &self.shards {
            let expired = {
                let mut shard = shard.lock().expect("shard poisoned");
                Self::take_expired(&mut shard, self.expired_ms)
            };
            for reader in expired {
                reader.close().await;
            }
        }
    }

    fn take_expired(shard: &mut Shard, expired_ms: u64) -> Vec<Arc<StreamReader>> {
        let now = now_ms();
        let mut expired = Vec::new();
        shard.readers.retain(|_, reader| {
            if now > reader.last_access_ms() + expired_ms {
                expired.push(Arc::clone(reader));
                false
            } else {
                true
            }
        });
        expired
    }
}

#[async_trait]
impl S3BlockCache for StreamReaders {
    async fn read(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<ReadDataBlock, StreamError> {
        let (reader, expired) = {
            let mut shard = self.shard(stream_id).lock().expect("shard poisoned");
            let expired = {
                let now = now_ms();
                if now > shard.last_expired_check_ms + self.check_interval_ms {
                    shard.last_expired_check_ms = now;
                    Self::take_expired(&mut shard, self.expired_ms)
                } else {
                    Vec::new()
                }
            };
            let reader = shard
                .readers
                .remove(&(stream_id, start_offset))
                .unwrap_or_else(|| {
                    StreamReader::new(stream_id, start_offset, Arc::clone(&self.deps))
                });
            (reader, expired)
        };
        for old in expired {
            old.close().await;
        }

        let result = reader.read(start_offset, end_offset, max_bytes).await;
        match &result {
            Ok(_) => {
                let displaced = {
                    let mut shard = self.shard(stream_id).lock().expect("shard poisoned");
                    shard
                        .readers
                        .insert((stream_id, reader.next_read_offset()), Arc::clone(&reader))
                };
                // Two readers converged on the same progress: keep one.
                if let Some(old) = displaced {
                    old.close().await;
                }
            }
            Err(e) => {
                tracing::error!(
                    "read {stream_id} [{start_offset}, {end_offset}), maxBytes: {max_bytes} from block cache fail: {e}"
                );
                reader.close().await;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use s3stream_codec::StreamRecordBatch;
    use s3stream_object::{MemoryObjectStorage, NOOP_OBJECT_ID, ObjectWriter, WriteOptions};

    use crate::api::results::CacheAccessType;
    use crate::manager::{CommitStreamSetObjectRequest, StreamManager, StreamObject};
    use crate::memory::MemoryMetadataManager;

    struct Fixture {
        manager: Arc<MemoryMetadataManager>,
        storage: Arc<MemoryObjectStorage>,
        stream_id: u64,
    }

    async fn fixture() -> Fixture {
        let manager = MemoryMetadataManager::new();
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let stream_id = manager.create_stream(HashMap::new()).await.unwrap();
        manager
            .open_stream(stream_id, 1, HashMap::new())
            .await
            .unwrap();
        Fixture {
            manager,
            storage,
            stream_id,
        }
    }

    /// Write `[start, end)` of the stream as one committed stream object.
    async fn commit_stream_object(
        f: &Fixture,
        object_id: u64,
        start: u64,
        end: u64,
        payload_len: usize,
        block_size: usize,
    ) {
        let records: Vec<StreamRecordBatch> = (start..end)
            .map(|o| {
                StreamRecordBatch::new(f.stream_id, 1, o, 1, vec![o as u8; payload_len].into())
            })
            .collect();
        let mut writer = ObjectWriter::open(
            object_id,
            f.storage.as_ref(),
            block_size,
            16 << 20,
            WriteOptions::default(),
        )
        .await
        .unwrap();
        writer.write(f.stream_id, &records).await.unwrap();
        let size = writer.close().await.unwrap();
        let request = CommitStreamSetObjectRequest {
            object_id: NOOP_OBJECT_ID,
            stream_objects: vec![StreamObject {
                object_id,
                object_size: size,
                stream_id: f.stream_id,
                start_offset: start,
                end_offset: end,
                attributes: 0,
            }],
            ..Default::default()
        };
        f.manager.commit_stream_set_object(request).await.unwrap();
    }

    fn readers(f: &Fixture, cache_size: u64) -> Arc<StreamReaders> {
        StreamReaders::new(
            cache_size,
            f.manager.clone() as Arc<dyn ObjectManager>,
            f.storage.clone() as Arc<dyn ObjectStorage>,
            2,
        )
    }

    fn offsets(block: &ReadDataBlock) -> Vec<u64> {
        block.records.iter().map(|r| r.base_offset()).collect()
    }

    /// Sequential reads across block and
    /// object boundaries return contiguous records. The follow-up read is served by
    /// the readahead-warmed cache.
    #[tokio::test]
    async fn sequential_read_warms_readahead() {
        let f = fixture().await;
        // Two objects x 32 records, small blocks so the window has many blocks.
        commit_stream_object(&f, 1, 0, 32, 64, 128).await;
        commit_stream_object(&f, 2, 32, 64, 64, 128).await;
        let readers = readers(&f, 64 << 20);

        let first = readers.read(f.stream_id, 0, 8, usize::MAX).await.unwrap();
        assert_eq!(offsets(&first), (0..8).collect::<Vec<_>>());
        assert_eq!(first.cache_access, CacheAccessType::BlockCacheMiss);
        assert_eq!(readers.active_reader_count(), 1);

        // Give the spawned readahead a moment to prefetch.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let second = readers.read(f.stream_id, 8, 24, usize::MAX).await.unwrap();
        assert_eq!(offsets(&second), (8..24).collect::<Vec<_>>());
        assert_eq!(
            second.cache_access,
            CacheAccessType::BlockCacheHit,
            "readahead should have prefetched the next blocks"
        );

        // Cross the object boundary.
        let third = readers.read(f.stream_id, 24, 64, usize::MAX).await.unwrap();
        assert_eq!(offsets(&third), (24..64).collect::<Vec<_>>());
        assert_eq!(
            readers.active_reader_count(),
            1,
            "one reader tracks the progress"
        );
    }

    #[tokio::test]
    async fn max_bytes_caps_but_returns_progress() {
        let f = fixture().await;
        commit_stream_object(&f, 1, 0, 16, 128, 256).await;
        let readers = readers(&f, 64 << 20);

        let read = readers.read(f.stream_id, 0, 16, 1).await.unwrap();
        assert!(!read.records.is_empty());
        assert!(read.records.len() < 16);
        let next = read.records.last().unwrap().last_offset();
        let rest = readers
            .read(f.stream_id, next, 16, usize::MAX)
            .await
            .unwrap();
        assert_eq!(rest.records.last().unwrap().last_offset(), 16);
    }

    /// Object deleted by compaction between reads. The reader resets
    /// its window and retries against fresh metadata (recoverable path).
    #[tokio::test]
    async fn compaction_race_resets_and_retries() {
        let f = fixture().await;
        commit_stream_object(&f, 1, 0, 16, 64, 128).await;
        // Tiny data cache so nothing survives. Every read hits storage.
        let readers = readers(&f, 1);

        let first = readers.read(f.stream_id, 0, 4, usize::MAX).await.unwrap();
        assert_eq!(offsets(&first), vec![0, 1, 2, 3]);

        // "Compaction": rewrite [0,16) into object 2 (same start offset replaces the
        // metadata entry) and delete object 1's bytes.
        commit_stream_object(&f, 2, 0, 16, 64, 128).await;
        let key1 = s3stream_object::gen_object_key(0, 1);
        f.storage
            .delete(&[s3stream_object::ObjectPath {
                bucket_id: 0,
                key: key1,
            }])
            .await
            .unwrap();

        // Cached window still points at object 1. The 404 triggers reset + retry.
        let second = readers.read(f.stream_id, 4, 16, usize::MAX).await.unwrap();
        assert_eq!(offsets(&second), (4..16).collect::<Vec<_>>());
    }

    /// Two consumers at the same progress collapse onto one reader. Non-sequential
    /// entry points get their own readers.
    #[tokio::test]
    async fn reader_registry_collapses_same_progress() {
        let f = fixture().await;
        commit_stream_object(&f, 1, 0, 32, 64, 512).await;
        let readers = readers(&f, 64 << 20);

        readers.read(f.stream_id, 0, 8, usize::MAX).await.unwrap();
        assert_eq!(readers.active_reader_count(), 1);
        // A second consumer starting at 0 creates a second reader...
        readers.read(f.stream_id, 0, 8, usize::MAX).await.unwrap();
        // ...but both now sit at offset 8, so the registry keeps only one.
        assert_eq!(readers.active_reader_count(), 1);

        // A cold entry point at offset 16 is a separate reader.
        readers.read(f.stream_id, 16, 20, usize::MAX).await.unwrap();
        assert_eq!(readers.active_reader_count(), 2);
    }

    /// Idle readers expire after the TTL and are swept.
    #[tokio::test]
    async fn idle_readers_expire() {
        let f = fixture().await;
        commit_stream_object(&f, 1, 0, 8, 64, 512).await;
        let readers = StreamReaders::with_config(
            64 << 20,
            f.manager.clone() as Arc<dyn ObjectManager>,
            f.storage.clone() as Arc<dyn ObjectStorage>,
            1,
            20, // expire after 20ms
            10, // sweep every 10ms
        );
        readers.read(f.stream_id, 0, 4, usize::MAX).await.unwrap();
        assert_eq!(readers.active_reader_count(), 1);
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        assert_eq!(readers.active_reader_count(), 0, "idle reader swept");
    }
}
