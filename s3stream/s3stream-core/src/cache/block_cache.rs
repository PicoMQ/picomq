//! BlockCache: caches committed-object data blocks with stream-aware readahead.
//!
//! (per-stream
//! reader registry sharded across event loops), `StreamReader` (readahead state
//! machine), `DataBlockCache` (block-level LRU with inflight dedup), `ObjectReaders`
//! (reader/index cache). Plus the `S3BlockCache` facade interface.
//!
//! This is the largest performance-bearing subsystem after compaction. The skeleton
//! captures the facade and the two key internal contracts. Port order: DataBlockCache
//! -> StreamReader -> StreamReaders facade.

use std::sync::Arc;

use async_trait::async_trait;

use s3stream_object::{ObjectReader, ObjectStorage, decode_data_block};

use crate::api::StreamError;
use crate::api::results::CacheAccessType;
use crate::manager::ObjectManager;
use crate::storage::ReadDataBlock;

/// The read-path cache facade S3Storage calls.
///
/// Returned records are offset-contiguous, cover `start_offset` (the first
/// record may start below it), and stop at `end_offset` or near the
/// `max_bytes` cap.
#[async_trait]
pub trait S3BlockCache: Send + Sync {
    async fn read(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<ReadDataBlock, StreamError>;
}

/// Uncached read-through implementation: resolves objects via the metadata plane and
/// range-GETs data blocks directly.
///
/// PHASE-5 TODO: replace with the full `blockcache` port (DataBlockCache LRU, inflight
/// dedup, StreamReader readahead). This implementation is correctness-complete against
/// the `S3BlockCache` contract. Every read reports `BlockCacheMiss`.
pub struct DirectBlockCache {
    object_manager: Arc<dyn ObjectManager>,
    object_storage: Arc<dyn ObjectStorage>,
}

impl DirectBlockCache {
    pub fn new(
        object_manager: Arc<dyn ObjectManager>,
        object_storage: Arc<dyn ObjectStorage>,
    ) -> Self {
        Self {
            object_manager,
            object_storage,
        }
    }
}

#[async_trait]
impl S3BlockCache for DirectBlockCache {
    async fn read(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<ReadDataBlock, StreamError> {
        let mut records = Vec::new();
        let mut next_start = start_offset;
        let mut remaining = max_bytes;
        'outer: while next_start < end_offset && remaining > 0 {
            let objects = self
                .object_manager
                .get_objects(stream_id, next_start, end_offset, 4)
                .await?;
            if objects.is_empty() {
                break;
            }
            let progress_before = next_start;
            for object in objects {
                let reader = ObjectReader::new(object, Arc::clone(&self.object_storage));
                let find = reader
                    .find(stream_id, next_start, end_offset, remaining)
                    .await?;
                for index in &find.blocks {
                    let block = reader.read_block(index).await?;
                    for record in decode_data_block(&block)? {
                        if record.stream_id() != stream_id || record.last_offset() <= next_start {
                            continue;
                        }
                        if record.base_offset() >= end_offset {
                            break 'outer;
                        }
                        next_start = record.last_offset();
                        remaining = remaining.saturating_sub(record.size());
                        records.push(record);
                        if remaining == 0 {
                            break 'outer;
                        }
                    }
                }
                if next_start >= end_offset {
                    break 'outer;
                }
            }
            if next_start == progress_before {
                // Metadata says more objects exist but none advanced the cursor.
                // Stop instead of spinning (caller's continuity check reports gaps).
                break;
            }
        }
        Ok(ReadDataBlock {
            records,
            cache_access: CacheAccessType::BlockCacheMiss,
        })
    }
}

// The full cached/readahead implementation (`DataBlockCache`, `StreamReader`,
// `StreamReaders`, `ObjectReaderCache`) lives in `crate::cache::blockcache`.
// `StreamReaders` is the production `S3BlockCache`. `DirectBlockCache` above stays as
// the dependency-free read-through used by focused storage tests.
