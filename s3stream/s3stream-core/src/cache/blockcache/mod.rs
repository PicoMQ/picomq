//! The cold read path: cached blocks + readahead.
//!
//! - [`StreamReaders`]. The `S3BlockCache` facade. A sharded registry of per-(stream,
//!   offset) readers with idle expiry.
//! - [`stream_reader::StreamReader`]. The readahead state machine over a window of
//!   block indexes.
//! - [`DataBlockCache`] / [`DataBlock`]. The page-cache layer: one S3 GET per block
//!   shared by all waiters, drop-behind freeing, LRU + TTL eviction, size-limited.
//! - [`ObjectReaderCache`]. Parsed object indexes, LRU by index bytes.

mod data_block;
mod object_reader_cache;
mod size_limiter;
mod stream_reader;
mod stream_readers;

pub use data_block::{DATA_TTL_MS, DataBlock, DataBlockCache, GetOptions};
pub use object_reader_cache::{MAX_OBJECT_READER_SIZE, ObjectReaderCache};
pub use size_limiter::AsyncSizeLimiter;
pub use stream_reader::{GET_OBJECT_STEP, READAHEAD_SIZE_UNIT, max_readahead_size};
pub use stream_readers::StreamReaders;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

pub(crate) fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64 + 86_400_000
}

/// Tracks in-flight cold reads for availability signal collection. Owned by
/// [`DataBlockCache`]. The tracked entry clears itself on drop.
pub struct ColdReadInflightRegistry {
    inflight: Mutex<std::collections::HashMap<u64, u64>>, // id -> start now_ms
    next_id: AtomicU64,
}

impl Default for ColdReadInflightRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ColdReadInflightRegistry {
    pub fn new() -> Self {
        Self {
            inflight: Mutex::new(std::collections::HashMap::new()),
            next_id: AtomicU64::new(0),
        }
    }

    pub fn track(self: &std::sync::Arc<Self>) -> ColdReadGuard {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.inflight
            .lock()
            .expect("registry poisoned")
            .insert(id, now_ms());
        ColdReadGuard {
            registry: std::sync::Arc::clone(self),
            id,
        }
    }

    pub fn has_pending_older_than(&self, threshold_ms: u64) -> bool {
        let now = now_ms();
        self.inflight
            .lock()
            .expect("registry poisoned")
            .values()
            .any(|start| now.saturating_sub(*start) >= threshold_ms)
    }

    pub fn clear(&self) {
        self.inflight.lock().expect("registry poisoned").clear();
    }
}

pub struct ColdReadGuard {
    registry: std::sync::Arc<ColdReadInflightRegistry>,
    id: u64,
}

impl Drop for ColdReadGuard {
    fn drop(&mut self) {
        self.registry
            .inflight
            .lock()
            .expect("registry poisoned")
            .remove(&self.id);
    }
}
