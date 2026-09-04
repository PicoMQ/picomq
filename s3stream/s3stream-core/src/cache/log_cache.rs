//! LogCache: the readable in-memory twin of the delta WAL.
//!
//! Structure: an active mutable block receiving puts, plus sealed (archived) blocks
//! awaiting upload. Records are grouped per stream inside each block, offset-ordered.
//! Blocks are freed (`mark_free`) after their upload commits, physically released by
//! `try_real_free` once the cache is over 90% capacity or holds more than 64 blocks,
//! and small adjacent free blocks are merged to speed up gets.
//!
//! - fully-cached range => the records.
//! - right-intersect => the cached tail only (block cache serves the head).
//! - left-intersect or miss => empty.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use s3stream_codec::StreamRecordBatch;
use s3stream_wal::RecordOffset;

pub const MATCH_ALL_STREAMS: u64 = u64::MAX;
const MAX_BLOCKS_COUNT: usize = 64;
const MERGE_BLOCK_THRESHOLD: usize = 8;
pub const DEFAULT_MAX_BLOCK_STREAM_COUNT: usize = 10_000;

static BLOCK_ID_ALLOC: AtomicU64 = AtomicU64::new(0);

/// Per-stream, offset-ordered records within one block.
struct StreamCache {
    records: Vec<StreamRecordBatch>,
    start_offset: Option<u64>,
    end_offset: Option<u64>,
    offset_index_map: HashMap<u64, (usize, u32)>,
}

impl StreamCache {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            start_offset: None,
            end_offset: None,
            offset_index_map: HashMap::new(),
        }
    }

    fn add(&mut self, record: StreamRecordBatch) -> bool {
        if let Some(end) = self.end_offset
            && record.base_offset() != end
        {
            tracing::error!(
                stream_id = record.stream_id(),
                expect = end,
                actual = record.base_offset(),
                "[FATAL] record batch base offset mismatch"
            );
        }
        let effective_start = self.start_offset.unwrap_or(record.base_offset());
        if record.last_offset().wrapping_sub(effective_start) > i32::MAX as u64 {
            return false;
        }
        if self.start_offset.is_none() {
            self.start_offset = Some(record.base_offset());
        }
        self.end_offset = Some(record.last_offset());
        self.records.push(record);
        true
    }

    fn get(
        &mut self,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Vec<StreamRecordBatch> {
        let (Some(cache_start), Some(cache_end)) = (self.start_offset, self.end_offset) else {
            return Vec::new();
        };
        if cache_start > start_offset || cache_end <= start_offset {
            return Vec::new();
        }
        let Some(start_index) = self.search_start_index(start_offset) else {
            return Vec::new();
        };
        let mut end_index = start_index;
        let mut remaining = max_bytes;
        let mut rst_end_offset: Option<u64> = None;
        for (i, record) in self.records.iter().enumerate().skip(start_index) {
            end_index = i + 1;
            remaining -= remaining.min(record.size());
            rst_end_offset = Some(record.last_offset());
            if record.last_offset() >= end_offset || remaining == 0 {
                break;
            }
        }
        if let Some(rst_end) = rst_end_offset {
            let entry = self
                .offset_index_map
                .entry(rst_end)
                .or_insert((end_index, 0));
            entry.0 = end_index;
            entry.1 += 1;
        }
        self.records[start_index..end_index].to_vec()
    }

    fn search_start_index(&mut self, start_offset: u64) -> Option<usize> {
        if let Some(&(index, count)) = self.offset_index_map.get(&start_offset) {
            if count <= 1 {
                self.offset_index_map.remove(&start_offset);
            } else {
                self.offset_index_map
                    .insert(start_offset, (index, count - 1));
            }
            return Some(index);
        }
        let mut lo = 0usize;
        let mut hi = self.records.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let record = &self.records[mid];
            if start_offset < record.base_offset() {
                hi = mid;
            } else if start_offset >= record.last_offset() {
                lo = mid + 1;
            } else {
                // At the base or inside the batch: serve the covering batch.
                // Batches are stored verbatim, so a mid-batch read must return
                // the whole batch and let the reader skip leading records
                // (block-cache reads are block-granular and behave the same).
                return Some(mid);
            }
        }
        None
    }

    fn range(&self) -> Option<(u64, u64)> {
        match (self.start_offset, self.end_offset) {
            (Some(s), Some(e)) => Some((s, e)),
            _ => None,
        }
    }

    fn free(&mut self) {
        self.records.clear();
        self.offset_index_map.clear();
    }
}

/// One sealed (or active) cache block.
pub struct LogCacheBlock {
    block_id: u64,
    max_size: u64,
    max_stream_count: usize,
    created: std::time::Instant,
    inner: Mutex<BlockInner>,
    size: AtomicU64,
    overflow: AtomicBool,
    free: AtomicBool,
}

/// Offset range of one stream inside a cache block, reported to free listeners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRangeBound {
    pub stream_id: u64,
    pub start_offset: u64,
    pub end_offset: u64,
}

/// Per-block callback when the block's memory is physically released.
/// Listeners see the bounds of every stream in the released block.
pub type FreeListener = Arc<dyn Fn(&[StreamRangeBound]) + Send + Sync>;

struct BlockInner {
    map: HashMap<u64, StreamCache>,
    last_record_offset: Option<RecordOffset>,
    free_listeners: Vec<FreeListener>,
}

impl LogCacheBlock {
    pub fn new(max_size: u64, max_stream_count: usize) -> Self {
        Self {
            block_id: BLOCK_ID_ALLOC.fetch_add(1, Ordering::Relaxed),
            max_size,
            max_stream_count,
            created: std::time::Instant::now(),
            inner: Mutex::new(BlockInner {
                map: HashMap::new(),
                last_record_offset: None,
                free_listeners: Vec::new(),
            }),
            size: AtomicU64::new(0),
            overflow: AtomicBool::new(false),
            free: AtomicBool::new(false),
        }
    }

    pub fn block_id(&self) -> u64 {
        self.block_id
    }

    pub fn created(&self) -> std::time::Instant {
        self.created
    }

    pub fn is_full(&self) -> bool {
        self.overflow.load(Ordering::Relaxed)
            || self.size.load(Ordering::Relaxed) >= self.max_size
            || self.inner.lock().expect("block poisoned").map.len() >= self.max_stream_count
    }

    pub fn put(&self, record: StreamRecordBatch) -> bool {
        if self.is_full() {
            return false;
        }
        let occupied = record.occupied_size() as u64;
        let mut inner = self.inner.lock().expect("block poisoned");
        let cache = inner
            .map
            .entry(record.stream_id())
            .or_insert_with(StreamCache::new);
        if !cache.add(record) {
            self.overflow.store(true, Ordering::Relaxed);
            return false;
        }
        self.size.fetch_add(occupied, Ordering::Relaxed);
        true
    }

    pub fn get(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Vec<StreamRecordBatch> {
        let mut inner = self.inner.lock().expect("block poisoned");
        match inner.map.get_mut(&stream_id) {
            Some(cache) => cache.get(start_offset, end_offset, max_bytes),
            None => Vec::new(),
        }
    }

    fn stream_range(&self, stream_id: u64) -> Option<(u64, u64)> {
        let inner = self.inner.lock().expect("block poisoned");
        inner.map.get(&stream_id).and_then(|c| c.range())
    }

    /// Per-stream records, offset-ordered (the upload task's input).
    pub fn records(&self) -> HashMap<u64, Vec<StreamRecordBatch>> {
        let inner = self.inner.lock().expect("block poisoned");
        inner
            .map
            .iter()
            .map(|(id, cache)| (*id, cache.records.clone()))
            .collect()
    }

    /// WAL offset of the last record confirmed before this block sealed. The upload
    /// commit trims the WAL to here.
    pub fn last_record_offset(&self) -> Option<RecordOffset> {
        self.inner
            .lock()
            .expect("block poisoned")
            .last_record_offset
    }

    pub fn set_last_record_offset(&self, offset: RecordOffset) {
        self.inner
            .lock()
            .expect("block poisoned")
            .last_record_offset = Some(offset);
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    /// Swap a record in place (recovery link-record materialization).
    pub fn replace_record(&self, stream_id: u64, base_offset: u64, decoded: StreamRecordBatch) {
        let mut inner = self.inner.lock().expect("block poisoned");
        if let Some(cache) = inner.map.get_mut(&stream_id)
            && let Some(slot) = cache
                .records
                .iter_mut()
                .find(|r| r.base_offset() == base_offset)
        {
            *slot = decoded;
        }
    }

    pub fn contains_stream(&self, stream_id: u64) -> bool {
        if stream_id == MATCH_ALL_STREAMS {
            return true;
        }
        self.inner
            .lock()
            .expect("block poisoned")
            .map
            .contains_key(&stream_id)
    }

    /// Snapshot-read `put` attaches
    /// `LogCacheBlockFreeListener` on the archived block before `markFree`.
    pub fn add_free_listener(&self, listener: FreeListener) {
        self.inner
            .lock()
            .expect("block poisoned")
            .free_listeners
            .push(listener);
    }

    /// Collect bounds, free records, then notify listeners, in that order:
    /// 1. collect one `(stream, start, end)` bound per stream
    /// 2. free the records and clear the map
    /// 3. notify the free listeners
    ///
    /// `try_real_free` fires the cache-level listener inline. Listeners see
    /// the bounds of the released block.
    fn free_and_notify(&self) {
        let (bounds, listeners) = {
            let mut inner = self.inner.lock().expect("block poisoned");
            let bounds: Vec<StreamRangeBound> = inner
                .map
                .iter()
                .filter_map(|(stream_id, cache)| {
                    let (start_offset, end_offset) = cache.range()?;
                    Some(StreamRangeBound {
                        stream_id: *stream_id,
                        start_offset,
                        end_offset,
                    })
                })
                .collect();
            for cache in inner.map.values_mut() {
                cache.free();
            }
            inner.map.clear();
            let listeners = std::mem::take(&mut inner.free_listeners);
            (bounds, listeners)
        };
        for listener in listeners {
            listener(&bounds);
        }
    }

    fn free_stream(&self, stream_id: u64) -> u64 {
        let mut inner = self.inner.lock().expect("block poisoned");
        match inner.map.remove(&stream_id) {
            Some(cache) => cache.records.iter().map(|r| r.occupied_size() as u64).sum(),
            None => 0,
        }
    }
}

/// Callback invoked when a block's memory is physically released.
pub type BlockFreeListener = Arc<dyn Fn(&LogCacheBlock) + Send + Sync>;

/// The delta-WAL cache.
pub struct LogCache {
    capacity: u64,
    cache_block_max_size: u64,
    max_cache_block_stream_count: usize,
    size: AtomicU64,
    state: RwLock<CacheState>,
    block_free_listener: Option<BlockFreeListener>,
}

struct CacheState {
    blocks: Vec<Arc<LogCacheBlock>>,
    last_record_offset: Option<RecordOffset>,
}

impl CacheState {
    fn active(&self) -> &Arc<LogCacheBlock> {
        self.blocks
            .last()
            .expect("cache always has an active block")
    }
}

impl LogCache {
    pub fn new(
        capacity: u64,
        cache_block_max_size: u64,
        max_cache_block_stream_count: usize,
    ) -> Self {
        Self::with_listener(
            capacity,
            cache_block_max_size,
            max_cache_block_stream_count,
            None,
        )
    }

    pub fn with_listener(
        capacity: u64,
        cache_block_max_size: u64,
        max_cache_block_stream_count: usize,
        block_free_listener: Option<BlockFreeListener>,
    ) -> Self {
        let active = Arc::new(LogCacheBlock::new(
            cache_block_max_size,
            max_cache_block_stream_count,
        ));
        Self {
            capacity,
            cache_block_max_size,
            max_cache_block_stream_count,
            size: AtomicU64::new(0),
            state: RwLock::new(CacheState {
                blocks: vec![active],
                last_record_offset: None,
            }),
            block_free_listener,
        }
    }

    pub fn put(&self, record: StreamRecordBatch) -> bool {
        self.try_real_free();
        let occupied = record.occupied_size() as u64;
        let added = {
            let state = self.state.read().expect("cache poisoned");
            state.active().put(record)
        };
        if added {
            self.size.fetch_add(occupied, Ordering::Relaxed);
        }
        added
    }

    pub fn get(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Vec<StreamRecordBatch> {
        let state = self.state.read().expect("cache poisoned");
        Self::get0(
            &state.blocks,
            stream_id,
            start_offset,
            end_offset,
            max_bytes,
        )
    }

    fn get0(
        blocks: &[Arc<LogCacheBlock>],
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Vec<StreamRecordBatch> {
        let mut rst = Vec::new();
        let mut next_start_offset = start_offset;
        let mut next_max_bytes = max_bytes;
        let mut fulfill = false;
        for block in blocks {
            let records = block.get(stream_id, next_start_offset, end_offset, next_max_bytes);
            if records.is_empty() {
                continue;
            }
            next_start_offset = records.last().unwrap().last_offset();
            let records_size: usize = records.iter().map(|r| r.size()).sum();
            next_max_bytes -= next_max_bytes.min(records_size);
            rst.extend(records);
            if next_start_offset >= end_offset || next_max_bytes == 0 {
                fulfill = true;
                break;
            }
        }
        if fulfill {
            return rst;
        }
        // Not fulfilled: find the longest continuous tail across blocks and serve it
        // if it right-intersects the requested range.
        let mut last_block_stream_start: Option<u64> = None;
        for block in blocks.iter().rev() {
            let Some((range_start, range_end)) = block.stream_range(stream_id) else {
                continue;
            };
            match last_block_stream_start {
                None => last_block_stream_start = Some(range_start),
                Some(current) if current == range_end => {
                    last_block_stream_start = Some(range_start)
                }
                Some(_) => break,
            }
        }
        match last_block_stream_start {
            None => Vec::new(),                               // mismatch
            Some(tail) if tail >= end_offset => Vec::new(),   // non-right intersect
            Some(tail) if tail <= start_offset => Vec::new(), // left intersect
            Some(tail) => Self::get0(blocks, stream_id, tail, end_offset, max_bytes),
        }
    }

    /// Seal the active block for upload.
    pub fn archive_current_block(&self) -> Arc<LogCacheBlock> {
        let mut state = self.state.write().expect("cache poisoned");
        self.archive_current_block0(&mut state)
    }

    fn archive_current_block0(&self, state: &mut CacheState) -> Arc<LogCacheBlock> {
        let block = Arc::clone(state.active());
        if let Some(offset) = state.last_record_offset {
            block.set_last_record_offset(offset);
        }
        let active = Arc::new(LogCacheBlock::new(
            self.cache_block_max_size,
            self.max_cache_block_stream_count,
        ));
        state.blocks.push(active);
        block
    }

    pub fn archive_current_block_if_contains(&self, stream_id: u64) -> Option<Arc<LogCacheBlock>> {
        let mut state = self.state.write().expect("cache poisoned");
        let active = state.active();
        let should_archive = if stream_id == MATCH_ALL_STREAMS {
            active.size() > 0
        } else {
            active.contains_stream(stream_id)
        };
        should_archive.then(|| self.archive_current_block0(&mut state))
    }

    /// Mark a committed block's memory reclaimable.
    pub fn mark_free(&self, block: &Arc<LogCacheBlock>) {
        block.free.store(true, Ordering::Release);
        self.try_real_free();
        self.try_merge();
    }

    fn try_real_free(&self) {
        let over =
            |size: u64, blocks: usize| size > self.capacity / 10 * 9 || blocks > MAX_BLOCKS_COUNT;
        {
            let state = self.state.read().expect("cache poisoned");
            if !over(self.size.load(Ordering::Relaxed), state.blocks.len()) {
                return;
            }
        }
        let mut removed = Vec::new();
        let mut free_size = 0u64;
        {
            let mut state = self.state.write().expect("cache poisoned");
            let current = self.size.load(Ordering::Relaxed);
            while let Some(block) = state.blocks.first() {
                if !over(current - free_size, state.blocks.len()) {
                    break;
                }
                if !block.free.load(Ordering::Acquire) {
                    break;
                }
                let block = state.blocks.remove(0);
                free_size += block.size();
                removed.push(block);
            }
        }
        self.size.fetch_sub(free_size, Ordering::Relaxed);
        for block in removed {
            if let Some(listener) = &self.block_free_listener {
                listener(&block);
            }
            // `blockFreeListener.accept(b); b.free();`).
            block.free_and_notify();
        }
    }

    fn try_merge(&self) {
        let mut merge_start_index = 0usize;
        loop {
            let mut state = self.state.write().expect("cache poisoned");
            if state.blocks.len() <= MERGE_BLOCK_THRESHOLD
                || merge_start_index + 1 >= state.blocks.len()
            {
                return;
            }
            let left = Arc::clone(&state.blocks[merge_start_index]);
            let right = Arc::clone(&state.blocks[merge_start_index + 1]);
            if !left.free.load(Ordering::Acquire) || !right.free.load(Ordering::Acquire) {
                return;
            }
            if left.size() + right.size() >= self.cache_block_max_size {
                merge_start_index += 1;
                continue;
            }
            if Self::is_discontinuous(&left, &right) {
                merge_start_index += 1;
                continue;
            }
            let merged = Arc::new(LogCacheBlock::new(u64::MAX, DEFAULT_MAX_BLOCK_STREAM_COUNT));
            Self::merge_block(&merged, &left);
            Self::merge_block(&merged, &right);
            merged.free.store(true, Ordering::Release);
            state.blocks[merge_start_index] = merged;
            state.blocks.remove(merge_start_index + 1);
        }
    }

    fn is_discontinuous(left: &LogCacheBlock, right: &LogCacheBlock) -> bool {
        let left_inner = left.inner.lock().expect("block poisoned");
        let right_inner = right.inner.lock().expect("block poisoned");
        for (stream_id, left_cache) in &left_inner.map {
            let Some(right_cache) = right_inner.map.get(stream_id) else {
                continue;
            };
            if left_cache.end_offset != right_cache.start_offset {
                return true;
            }
        }
        false
    }

    fn merge_block(merged: &LogCacheBlock, source: &LogCacheBlock) {
        let source_inner = source.inner.lock().expect("block poisoned");
        let mut merged_inner = merged.inner.lock().expect("block poisoned");
        merged.size.fetch_add(source.size(), Ordering::Relaxed);
        if source_inner.last_record_offset.is_some() {
            merged_inner.last_record_offset = source_inner.last_record_offset;
        }
        for (stream_id, source_cache) in &source_inner.map {
            match merged_inner.map.get_mut(stream_id) {
                None => {
                    let mut copy = StreamCache::new();
                    copy.records = source_cache.records.clone();
                    copy.start_offset = source_cache.start_offset;
                    copy.end_offset = source_cache.end_offset;
                    merged_inner.map.insert(*stream_id, copy);
                }
                Some(merged_cache) => {
                    merged_cache
                        .records
                        .extend(source_cache.records.iter().cloned());
                    merged_cache.end_offset = source_cache.end_offset;
                    merged_cache.offset_index_map.clear();
                }
            }
        }
    }

    pub fn set_last_record_offset(&self, offset: RecordOffset) {
        let mut state = self.state.write().expect("cache poisoned");
        state.last_record_offset = Some(offset);
    }

    pub fn last_record_offset(&self) -> Option<RecordOffset> {
        self.state
            .read()
            .expect("cache poisoned")
            .last_record_offset
    }

    pub fn clear_stream_records(&self, stream_id: u64) {
        let state = self.state.write().expect("cache poisoned");
        let mut freed = 0u64;
        for block in &state.blocks {
            freed += block.free_stream(stream_id);
        }
        drop(state);
        self.size.fetch_sub(freed, Ordering::Relaxed);
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn block_count(&self) -> usize {
        self.state.read().expect("cache poisoned").blocks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn record(stream_id: u64, base_offset: u64, count: i32, size: usize) -> StreamRecordBatch {
        StreamRecordBatch::new(
            stream_id,
            0,
            base_offset,
            count,
            Bytes::from(vec![0u8; size]),
        )
    }

    /// Documented get-intersect examples: cached [0,10] and [100,200].
    #[test]
    fn get_intersect_semantics_match_java() {
        let cache = LogCache::new(1 << 30, 1 << 20, DEFAULT_MAX_BLOCK_STREAM_COUNT);
        assert!(cache.put(record(1, 0, 10, 16))); // [0, 10)
        cache.archive_current_block();
        assert!(cache.put(record(1, 100, 100, 16))); // [100, 200)

        // Fully satisfied.
        let got = cache.get(1, 0, 10, usize::MAX);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].base_offset(), 0);

        // Left intersect => empty.
        assert!(cache.get(1, 0, 11, usize::MAX).is_empty());
        assert!(cache.get(1, 5, 20, usize::MAX).is_empty());

        // Right intersect => cached tail [100, 110).
        let got = cache.get(1, 90, 110, usize::MAX);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].base_offset(), 100);

        // Miss.
        assert!(cache.get(1, 40, 50, usize::MAX).is_empty());
    }

    /// A start offset inside a batch returns the covering batch; readers skip
    /// leading records themselves (same contract as block-granular reads).
    #[test]
    fn get_mid_batch_returns_covering_batch() {
        let cache = LogCache::new(1 << 30, 1 << 20, DEFAULT_MAX_BLOCK_STREAM_COUNT);
        assert!(cache.put(record(1, 0, 3, 16))); // [0, 3)
        assert!(cache.put(record(1, 3, 2, 16))); // [3, 5)

        let got = cache.get(1, 1, 3, usize::MAX);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].base_offset(), 0);

        let got = cache.get(1, 4, 5, usize::MAX);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].base_offset(), 3);
    }

    /// Continuous ranges spanning blocks are served together.
    #[test]
    fn get_spans_blocks() {
        let cache = LogCache::new(1 << 30, 1 << 20, DEFAULT_MAX_BLOCK_STREAM_COUNT);
        assert!(cache.put(record(1, 0, 10, 16)));
        cache.archive_current_block();
        assert!(cache.put(record(1, 10, 10, 16)));

        let got = cache.get(1, 0, 20, usize::MAX);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].base_offset(), 0);
        assert_eq!(got[1].base_offset(), 10);

        let got = cache.get(1, 0, 20, 1);
        assert_eq!(got.len(), 1);
    }

    /// Block put returns false when full. Overflow marks the block full.
    #[test]
    fn block_full_forces_archive() {
        let cache = LogCache::new(1 << 30, 32, DEFAULT_MAX_BLOCK_STREAM_COUNT);
        assert!(cache.put(record(1, 0, 1, 64))); // crosses the 32-byte block max
        assert!(!cache.put(record(1, 1, 1, 64))); // block now full
        let block = cache.archive_current_block();
        assert!(block.is_full());
        assert!(cache.put(record(1, 1, 1, 64))); // fresh active block accepts
    }

    /// Per-block FreeListener fires with StreamRangeBound when the block is released.
    #[test]
    fn per_block_free_listener_reports_bounds() {
        let seen: Arc<Mutex<Vec<StreamRangeBound>>> = Arc::new(Mutex::new(Vec::new()));
        let listener: FreeListener = {
            let seen = Arc::clone(&seen);
            Arc::new(move |bounds: &[StreamRangeBound]| {
                seen.lock()
                    .expect("seen poisoned")
                    .extend_from_slice(bounds);
            })
        };
        let cache = LogCache::new(10, 1 << 20, DEFAULT_MAX_BLOCK_STREAM_COUNT);
        assert!(cache.put(record(7, 10, 5, 64)));
        let block = cache.archive_current_block();
        block.add_free_listener(listener);
        cache.mark_free(&block);
        assert!(cache.put(record(8, 0, 1, 8)));
        let bounds = seen.lock().expect("seen poisoned").clone();
        assert_eq!(
            bounds,
            vec![StreamRangeBound {
                stream_id: 7,
                start_offset: 10,
                end_offset: 15
            }]
        );
    }

    /// Freed blocks are physically released once over capacity, and the listener runs.
    #[test]
    fn seal_and_free_lifecycle() {
        let freed = Arc::new(AtomicU64::new(0));
        let listener: BlockFreeListener = {
            let freed = Arc::clone(&freed);
            Arc::new(move |_block: &LogCacheBlock| {
                freed.fetch_add(1, Ordering::SeqCst);
            })
        };
        // Tiny capacity: any content is over 90%.
        let cache =
            LogCache::with_listener(10, 1 << 20, DEFAULT_MAX_BLOCK_STREAM_COUNT, Some(listener));
        assert!(cache.put(record(1, 0, 10, 64)));
        let block = cache.archive_current_block();
        let size_before = cache.size();
        assert!(size_before > 0);

        // Not yet freed: still readable.
        assert_eq!(cache.get(1, 0, 10, usize::MAX).len(), 1);

        cache.mark_free(&block);
        // Next put triggers try_real_free. The freed block is gone.
        assert!(cache.put(record(2, 0, 1, 8)));
        assert_eq!(freed.load(Ordering::SeqCst), 1);
        assert!(cache.get(1, 0, 10, usize::MAX).is_empty());
    }

    #[test]
    fn conditional_archive() {
        let cache = LogCache::new(1 << 30, 1 << 20, DEFAULT_MAX_BLOCK_STREAM_COUNT);
        assert!(cache.archive_current_block_if_contains(1).is_none());
        assert!(cache.put(record(1, 0, 1, 8)));
        assert!(cache.archive_current_block_if_contains(2).is_none());
        assert!(cache.archive_current_block_if_contains(1).is_some());
        // MATCH_ALL only when non-empty.
        assert!(
            cache
                .archive_current_block_if_contains(MATCH_ALL_STREAMS)
                .is_none()
        );
        assert!(cache.put(record(3, 0, 1, 8)));
        assert!(
            cache
                .archive_current_block_if_contains(MATCH_ALL_STREAMS)
                .is_some()
        );
    }

    /// The archived block carries the last confirmed record offset for WAL trimming.
    #[test]
    fn archived_block_carries_trim_offset() {
        let cache = LogCache::new(1 << 30, 1 << 20, DEFAULT_MAX_BLOCK_STREAM_COUNT);
        assert!(cache.put(record(1, 0, 1, 8)));
        let offset = RecordOffset {
            epoch: 1,
            offset: 4096,
            size: 32,
        };
        cache.set_last_record_offset(offset);
        let block = cache.archive_current_block();
        assert_eq!(block.last_record_offset(), Some(offset));
    }
}
