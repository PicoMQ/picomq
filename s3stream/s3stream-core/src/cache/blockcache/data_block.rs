//! DataBlock + DataBlockCache: the page-cache layer of the cold read path.
//!
//! Protocol:
//! - inflight coalescing: one S3 GET per (objectId, blockIndex), shared by all waiters.
//! - drop-behind: a block freed as soon as every interested reader `mark_read`s it
//!   (unread count reaches 0), so the cache holds only the prefetch→consume window.
//! - LRU + TTL eviction (`DATA_TTL` 1 min) when the size limiter needs permits.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tokio::sync::watch;

use s3stream_codec::StreamRecordBatch;
use s3stream_object::{DataBlockIndex, ObjectReader, decode_data_block};

use crate::api::StreamError;

use super::size_limiter::AsyncSizeLimiter;
use super::{ColdReadInflightRegistry, now_ms};

pub const DATA_TTL_MS: u64 = 60_000;
pub const CHECK_EXPIRED_DATA_INTERVAL_MS: u64 = 60_000;

const UNREAD_INIT: i32 = -1;

/// (readahead loads use the catch-up throttle class
/// and count against readahead metrics).
#[derive(Debug, Clone, Copy, Default)]
pub struct GetOptions {
    pub readahead: bool,
}

type LoadState = Option<Result<(), Arc<StreamError>>>;

type FreeListener = Box<dyn FnOnce(&DataBlock) + Send>;

/// One data block's lifecycle: created (loading) → loaded → freed.
pub struct DataBlock {
    object_id: u64,
    index: DataBlockIndex,
    unread_cnt: AtomicI32,
    last_access_ms: AtomicU64,
    load_tx: watch::Sender<LoadState>,
    /// Decoded once at `complete` (zero-copy views into the fetched bytes).
    records: OnceLock<Vec<StreamRecordBatch>>,
    freed: AtomicBool,
    free_listeners: Mutex<Vec<(u64, FreeListener)>>,
    next_listener_id: AtomicU64,
    limiter: Arc<AsyncSizeLimiter>,
}

impl DataBlock {
    fn new(object_id: u64, index: DataBlockIndex, limiter: Arc<AsyncSizeLimiter>) -> Self {
        let (load_tx, _) = watch::channel(None);
        Self {
            object_id,
            index,
            unread_cnt: AtomicI32::new(UNREAD_INIT),
            last_access_ms: AtomicU64::new(now_ms()),
            load_tx,
            records: OnceLock::new(),
            freed: AtomicBool::new(false),
            free_listeners: Mutex::new(Vec::new()),
            next_listener_id: AtomicU64::new(0),
            limiter,
        }
    }

    pub fn object_id(&self) -> u64 {
        self.object_id
    }

    pub fn index(&self) -> &DataBlockIndex {
        &self.index
    }

    pub fn is_loaded(&self) -> bool {
        matches!(&*self.load_tx.borrow(), Some(Ok(())))
    }

    /// Await load completion (coalesces all waiters onto the single fetch).
    pub async fn wait_load(&self) -> Result<(), StreamError> {
        let mut rx = self.load_tx.subscribe();
        let result = rx
            .wait_for(|state| state.is_some())
            .await
            .map_err(|_| StreamError::Unexpected("data block load dropped".into()))?;
        match result.as_ref().expect("waited for Some") {
            Ok(()) => Ok(()),
            Err(shared) => Err(clone_shared_error(shared)),
        }
    }

    fn complete(&self, bytes: bytes::Bytes) -> Result<(), StreamError> {
        match decode_data_block(&bytes) {
            Ok(records) => {
                let _ = self.records.set(records);
                self.load_tx.send_replace(Some(Ok(())));
                Ok(())
            }
            Err(e) => {
                let e = StreamError::from(e);
                self.fail_shared(&Arc::new(e));
                Err(StreamError::Unexpected("data block decode failed".into()))
            }
        }
    }

    fn fail_shared(&self, error: &Arc<StreamError>) {
        self.load_tx.send_replace(Some(Err(Arc::clone(error))));
        self.free();
    }

    pub fn mark_unread(&self) {
        debug_assert!(self.is_loaded(), "markUnread before load complete");
        self.last_access_ms.store(now_ms(), Ordering::Relaxed);
        if self
            .unread_cnt
            .compare_exchange(UNREAD_INIT, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.unread_cnt.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Decrement the unread count. Returns the count after the decrement.
    fn mark_read_delta(&self) -> i32 {
        self.unread_cnt.fetch_sub(1, Ordering::AcqRel) - 1
    }

    fn is_expired(&self, expired_before_ms: u64) -> bool {
        self.last_access_ms.load(Ordering::Relaxed) < expired_before_ms
    }

    /// Idempotent. Releases the size-limiter permits and fires registered
    /// free listeners exactly once.
    fn free(&self) {
        if self.freed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.limiter.release(self.index.size as u64);
        let listeners = {
            let mut guard = self.free_listeners.lock().expect("listeners poisoned");
            std::mem::take(&mut *guard)
        };
        for (_, listener) in listeners {
            listener(self);
        }
    }

    pub fn register_free_listener(self: &Arc<Self>, listener: FreeListener) -> FreeListenerHandle {
        if self.freed.load(Ordering::Acquire) {
            listener(self);
            return FreeListenerHandle {
                block: Weak::new(),
                id: 0,
            };
        }
        let id = self.next_listener_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut guard = self.free_listeners.lock().expect("listeners poisoned");
            guard.push((id, listener));
        }
        // Racing free(): if free ran between the check and the push, fire now.
        if self.freed.load(Ordering::Acquire) {
            let mut guard = self.free_listeners.lock().expect("listeners poisoned");
            if let Some(pos) = guard.iter().position(|(lid, _)| *lid == id) {
                let (_, listener) = guard.remove(pos);
                drop(guard);
                listener(self);
                return FreeListenerHandle {
                    block: Weak::new(),
                    id: 0,
                };
            }
        }
        FreeListenerHandle {
            block: Arc::downgrade(self),
            id,
        }
    }

    /// Extract records overlapping `[start_offset, end_offset)` up to
    /// ~`max_bytes`.
    ///
    /// A record is included when
    /// `base < end_offset && last > start_offset`. The byte budget is checked *after*
    /// including a record (so at least one record returns). Iteration stops at the
    /// first record with `base >= endOffset`.
    pub fn get_records(
        &self,
        start_offset: u64,
        end_offset: u64,
        max_bytes: i64,
    ) -> Vec<StreamRecordBatch> {
        let all = self
            .records
            .get()
            .expect("get_records before load complete");
        let mut out = Vec::new();
        let mut remaining = max_bytes;
        for record in all {
            if record.base_offset() < end_offset && record.last_offset() > start_offset {
                remaining -= record.size() as i64;
                out.push(record.clone());
                if remaining <= 0 {
                    break;
                }
                continue;
            }
            if record.base_offset() >= end_offset {
                break;
            }
        }
        out
    }
}

/// Removable registration of a free listener.
pub struct FreeListenerHandle {
    block: Weak<DataBlock>,
    id: u64,
}

impl FreeListenerHandle {
    pub fn close(self) {
        if let Some(block) = self.block.upgrade() {
            let mut guard = block.free_listeners.lock().expect("listeners poisoned");
            guard.retain(|(id, _)| *id != self.id);
        }
    }
}

fn clone_shared_error(error: &Arc<StreamError>) -> StreamError {
    // Preserve the variants the retry protocol matches on. The rest degrade to a
    // stringly error (the read path only stringifies them anyway).
    match &**error {
        StreamError::ObjectNotExist { object_id } => StreamError::ObjectNotExist {
            object_id: *object_id,
        },
        StreamError::BlockNotContinuous => StreamError::BlockNotContinuous,
        StreamError::Object(s3stream_object::ObjectError::NotFound { key }) => {
            StreamError::Object(s3stream_object::ObjectError::NotFound { key: key.clone() })
        }
        other => StreamError::Unexpected(other.to_string()),
    }
}

type BlockKey = (u64, DataBlockIndex);

struct Shard {
    blocks: HashMap<BlockKey, Arc<DataBlock>>,
    lru: lru::LruCache<BlockKey, ()>,
    last_evict_check_ms: u64,
}

/// The sharded block cache. Shards sit under `std::sync::Mutex` (never held
/// across await points).
pub struct DataBlockCache {
    shards: Vec<Mutex<Shard>>,
    limiter: Arc<AsyncSizeLimiter>,
    ttl_ms: u64,
    check_interval_ms: u64,
    cold_reads: Arc<ColdReadInflightRegistry>,
}

impl DataBlockCache {
    pub fn new(max_size: u64, concurrency: usize) -> Arc<Self> {
        Self::with_config(
            max_size,
            concurrency,
            DATA_TTL_MS,
            CHECK_EXPIRED_DATA_INTERVAL_MS,
        )
    }

    pub fn with_config(
        max_size: u64,
        concurrency: usize,
        ttl_ms: u64,
        check_interval_ms: u64,
    ) -> Arc<Self> {
        let shards = (0..concurrency.max(1))
            .map(|_| {
                Mutex::new(Shard {
                    blocks: HashMap::new(),
                    lru: lru::LruCache::unbounded(),
                    last_evict_check_ms: now_ms(),
                })
            })
            .collect();
        Arc::new(Self {
            shards,
            limiter: Arc::new(AsyncSizeLimiter::new(max_size)),
            ttl_ms,
            check_interval_ms,
            cold_reads: Arc::new(ColdReadInflightRegistry::new()),
        })
    }

    fn shard(&self, stream_id: u64) -> &Mutex<Shard> {
        &self.shards[(stream_id % self.shards.len() as u64) as usize]
    }

    pub fn available(&self) -> i64 {
        self.limiter.permits()
    }

    pub fn cold_read_registry(&self) -> &Arc<ColdReadInflightRegistry> {
        &self.cold_reads
    }

    /// Get-or-start-loading a block. The returned handle coalesces all
    /// waiters onto one S3 GET. Await `handle.wait_load()` for the data.
    /// Handle acquisition is synchronous so `StreamReader` can account
    /// hit/miss before awaiting.
    pub fn get_block_handle(
        self: &Arc<Self>,
        options: GetOptions,
        reader: Arc<ObjectReader>,
        index: DataBlockIndex,
    ) -> Arc<DataBlock> {
        let object_id = reader.metadata().object_id;
        let key = (object_id, index);
        let block = {
            let mut shard = self.shard(index.stream_id).lock().expect("shard poisoned");
            if let Some(existing) = shard.blocks.get(&key) {
                let existing = Arc::clone(existing);
                shard.lru.promote(&key);
                existing
            } else {
                let block = Arc::new(DataBlock::new(object_id, index, Arc::clone(&self.limiter)));
                shard.blocks.insert(key, Arc::clone(&block));
                self.spawn_load(options, reader, Arc::clone(&block), key);
                block
            }
        };
        self.try_evict_expired(index.stream_id);
        block
    }

    fn spawn_load(
        self: &Arc<Self>,
        options: GetOptions,
        reader: Arc<ObjectReader>,
        block: Arc<DataBlock>,
        key: BlockKey,
    ) {
        let cache = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match cache.limiter.try_acquire(block.index.size as u64) {
                    Ok(()) => break,
                    Err(waiter) => {
                        cache.evict_all();
                        if waiter.await.is_err() {
                            // Limiter gone (cache dropped). Fail the load.
                            block.fail_shared(&Arc::new(StreamError::Unexpected(
                                "block cache shut down".into(),
                            )));
                            return;
                        }
                    }
                }
            }
            // `throttleStrategy = getOptions.readahead
            // ? ThrottleStrategy.CATCH_UP : ThrottleStrategy.BYPASS`.
            let throttle = if options.readahead {
                s3stream_object::storage::ThrottleStrategy::CatchUp
            } else {
                s3stream_object::storage::ThrottleStrategy::Bypass
            };
            let tracked = (!options.readahead).then(|| cache.cold_reads.track());
            let result = reader.read_block_throttled(&block.index, throttle).await;
            drop(tracked);
            match result {
                Ok(bytes) => match block.complete(bytes) {
                    Ok(()) => {
                        let mut shard =
                            cache.shard(key.1.stream_id).lock().expect("shard poisoned");
                        shard.lru.push(key, ());
                    }
                    Err(_) => {
                        let mut shard =
                            cache.shard(key.1.stream_id).lock().expect("shard poisoned");
                        remove_if_same(&mut shard, &key, &block);
                    }
                },
                Err(e) => {
                    {
                        let mut shard =
                            cache.shard(key.1.stream_id).lock().expect("shard poisoned");
                        remove_if_same(&mut shard, &key, &block);
                    }
                    block.fail_shared(&Arc::new(StreamError::from(e)));
                }
            }
            if cache.limiter.required_release() {
                cache.evict_all();
            }
        });
    }

    /// Drop-behind: the last interested reader frees the block from the
    /// cache.
    pub fn mark_read(&self, block: &Arc<DataBlock>) {
        if block.mark_read_delta() > 0 {
            return;
        }
        let key = (block.object_id, block.index);
        let removed = {
            let mut shard = self
                .shard(block.index.stream_id)
                .lock()
                .expect("shard poisoned");
            remove_if_same(&mut shard, &key, block)
        };
        if removed {
            block.free();
        }
    }

    pub fn evict_all(&self) {
        for shard in &self.shards {
            let mut shard = shard.lock().expect("shard poisoned");
            Self::evict_shard(&mut shard, &self.limiter, self.ttl_ms);
        }
    }

    fn try_evict_expired(&self, stream_id: u64) {
        let now = now_ms();
        let mut shard = self.shard(stream_id).lock().expect("shard poisoned");
        if now.saturating_sub(shard.last_evict_check_ms) > self.check_interval_ms {
            shard.last_evict_check_ms = now;
            Self::evict_shard(&mut shard, &self.limiter, self.ttl_ms);
        }
    }

    fn evict_shard(shard: &mut Shard, limiter: &AsyncSizeLimiter, ttl_ms: u64) {
        let expired_before = now_ms().saturating_sub(ttl_ms);
        while let Some((key, ())) = shard.lru.peek_lru() {
            let key = *key;
            let block = shard.blocks.get(&key).cloned();
            let Some(block) = block else {
                // LRU tombstone (block already freed via mark_read). Drop the entry.
                shard.lru.pop_lru();
                continue;
            };
            if !block.is_expired(expired_before) && !limiter.required_release() {
                break;
            }
            shard.lru.pop_lru();
            shard.blocks.remove(&key);
            block.free();
        }
    }
}

fn remove_if_same(shard: &mut Shard, key: &BlockKey, block: &Arc<DataBlock>) -> bool {
    if shard.blocks.get(key).is_some_and(|b| Arc::ptr_eq(b, block)) {
        shard.blocks.remove(key);
        shard.lru.pop(key);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    use s3stream_object::{
        MemoryObjectStorage, ObjectAttributes, ObjectStorage, ObjectWriter, S3ObjectMetadata,
        S3ObjectType, WriteOptions, gen_object_key,
    };

    async fn write_object(
        storage: &Arc<MemoryObjectStorage>,
        object_id: u64,
        stream_id: u64,
        records: usize,
    ) -> S3ObjectMetadata {
        let batches: Vec<StreamRecordBatch> = (0..records)
            .map(|i| StreamRecordBatch::new(stream_id, 1, i as u64, 1, vec![i as u8; 64].into()))
            .collect();
        let mut writer = ObjectWriter::open(
            object_id,
            storage.as_ref(),
            128,
            16 << 20,
            WriteOptions::default(),
        )
        .await
        .unwrap();
        writer.write(stream_id, &batches).await.unwrap();
        let size = writer.close().await.unwrap();
        S3ObjectMetadata {
            object_id,
            object_type: S3ObjectType::StreamSet,
            offset_ranges: vec![],
            object_size: size,
            attributes: ObjectAttributes::new(0, false, false),
            committed_timestamp_ms: 0,
            data_timestamp_ms: 0,
        }
    }

    /// Concurrent readers of the same block share one S3 GET (inflight
    /// coalescing).
    #[tokio::test]
    async fn concurrent_readers_share_one_fetch() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let metadata = write_object(&storage, 1, 7, 4).await;

        // Counting storage wrapper via read counter on MemoryObjectStorage? Count via
        // a wrapper ObjectReader is not injectable. Instead count loads by observing
        // that all handles are the same Arc (single DataBlock == single spawned GET).
        let cache = DataBlockCache::new(1 << 20, 2);
        let reader = Arc::new(ObjectReader::new(
            metadata,
            storage.clone() as Arc<dyn ObjectStorage>,
        ));
        let index = reader.find(7, 0, 4, usize::MAX).await.unwrap().blocks[0];

        let handles: Vec<Arc<DataBlock>> = (0..8)
            .map(|_| cache.get_block_handle(GetOptions::default(), Arc::clone(&reader), index))
            .collect();
        for pair in handles.windows(2) {
            assert!(
                Arc::ptr_eq(&pair[0], &pair[1]),
                "all callers share one block"
            );
        }
        handles[0].wait_load().await.unwrap();
        let records = handles[0].get_records(0, 4, i64::MAX);
        assert_eq!(records.len(), index.record_count as usize);
        let _ = gen_object_key(0, 0);
    }

    /// Drop-behind: markUnread + markRead frees the block and releases permits.
    #[tokio::test]
    async fn mark_read_frees_block() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let metadata = write_object(&storage, 2, 9, 2).await;
        let cache = DataBlockCache::new(1 << 20, 1);
        let reader = Arc::new(ObjectReader::new(
            metadata,
            storage.clone() as Arc<dyn ObjectStorage>,
        ));
        let index = reader.find(9, 0, 2, usize::MAX).await.unwrap().blocks[0];

        let block = cache.get_block_handle(GetOptions::default(), Arc::clone(&reader), index);
        block.wait_load().await.unwrap();
        let before = cache.available();
        assert!(before < (1 << 20)); // permits held by the loaded block

        let freed = Arc::new(AtomicUsize::new(0));
        let freed_c = Arc::clone(&freed);
        block.register_free_listener(Box::new(move |_| {
            freed_c.fetch_add(1, Ordering::SeqCst);
        }));

        block.mark_unread();
        cache.mark_read(&block);
        assert_eq!(freed.load(Ordering::SeqCst), 1, "free listener fired");
        assert_eq!(cache.available(), 1 << 20, "permits released");

        // A fresh handle for the same index is a new block (reload).
        let block2 = cache.get_block_handle(GetOptions::default(), reader, index);
        assert!(!Arc::ptr_eq(&block, &block2));
        block2.wait_load().await.unwrap();
    }

    /// Size-limiter starvation evicts read (cold) blocks so queued loads proceed.
    #[tokio::test]
    async fn starved_limiter_evicts_and_reschedules() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let m1 = write_object(&storage, 3, 11, 2).await;
        let m2 = write_object(&storage, 4, 12, 2).await;
        // Cache smaller than one block: each load takes permits negative. The next
        // load queues until the previous block frees.
        let cache = DataBlockCache::new(1, 1);
        let r1 = Arc::new(ObjectReader::new(
            m1,
            storage.clone() as Arc<dyn ObjectStorage>,
        ));
        let r2 = Arc::new(ObjectReader::new(
            m2,
            storage.clone() as Arc<dyn ObjectStorage>,
        ));
        let i1 = r1.find(11, 0, 2, usize::MAX).await.unwrap().blocks[0];
        let i2 = r2.find(12, 0, 2, usize::MAX).await.unwrap().blocks[0];

        let b1 = cache.get_block_handle(GetOptions::default(), r1, i1);
        b1.wait_load().await.unwrap();
        // Second block queues on the limiter. Eviction of b1 (permits <= 0 forces
        // required_release) lets it proceed.
        let b2 = cache.get_block_handle(GetOptions::default(), r2, i2);
        b2.wait_load().await.unwrap();
        assert_eq!(b2.get_records(0, 2, i64::MAX).len(), 2);
    }
}
