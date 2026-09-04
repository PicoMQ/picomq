//! Per-(stream, offset) reader with the readahead state machine.
//!
//! Constants, ramp-up/decay rules,
//! block-window continuity checks, recoverable-error retry (reset + 2 retries), and
//! the drop-behind mark-unread/mark-read protocol.
//!
//! Reader state lives behind a `tokio::sync::Mutex` that is held only in
//! slices, serializing read, readahead, and index loading.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::Mutex as TokioMutex;

use s3stream_object::{DataBlockIndex, NOOP_OFFSET, S3ObjectMetadata};

use crate::api::StreamError;
use crate::api::results::CacheAccessType;
use crate::manager::ObjectManager;
use crate::storage::ReadDataBlock;

use super::data_block::{DataBlock, DataBlockCache, FreeListenerHandle, GetOptions};
use super::now_ms;
use super::object_reader_cache::ObjectReaderCache;

pub const GET_OBJECT_STEP: usize = 4;
pub const READAHEAD_SIZE_UNIT: usize = 1024 * 1024 / 2;
const READAHEAD_RESET_COLD_DOWN_MS: u64 = 60_000;
const READAHEAD_AVAILABLE_BYTES_THRESHOLD: i64 = 32 * 1024 * 1024;

pub fn max_readahead_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var("PICO_MAX_READAHEAD_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(32 * 1024 * 1024)
    })
}

pub(crate) struct ReaderDeps {
    pub object_manager: Arc<dyn ObjectManager>,
    pub readers: Arc<ObjectReaderCache>,
    pub data_block_cache: Arc<DataBlockCache>,
}

struct BlockSlot {
    metadata: S3ObjectMetadata,
    index: DataBlockIndex,
    data: Option<Arc<DataBlock>>,
    free_listener: Option<FreeListenerHandle>,
}

struct ReadaheadState {
    next_offset: u64,
    next_size: usize,
    mark_offset: u64,
    reset_ts_ms: u64,
    require_reset: bool,
    inflight: bool,
    cache_miss_count: usize,
}

impl ReadaheadState {
    fn new() -> Self {
        Self {
            next_offset: 0,
            next_size: READAHEAD_SIZE_UNIT,
            mark_offset: 0,
            reset_ts_ms: 0,
            require_reset: false,
            inflight: false,
            cache_miss_count: 0,
        }
    }

    fn reset(&mut self) {
        self.require_reset = true;
        self.reset_ts_ms = now_ms();
    }
}

struct ReaderState {
    blocks: BTreeMap<u64, BlockSlot>,
    /// (start, end) of the newest window block.
    last_block: Option<(u64, u64)>,
    loaded_end_offset: u64,
    blocks_epoch: u64,
    readahead: ReadaheadState,
}

pub struct StreamReader {
    stream_id: u64,
    deps: Arc<ReaderDeps>,
    state: Arc<TokioMutex<ReaderState>>,
    index_load_lock: TokioMutex<()>,
    next_read_offset: AtomicU64,
    last_access_ms: AtomicU64,
    reading: AtomicBool,
    closed: Arc<AtomicBool>,
}

/// One block handed to a read/readahead context.
struct BlockRead {
    index: DataBlockIndex,
    handle: Arc<DataBlock>,
    was_loaded: bool,
}

enum WalkOutcome {
    Fulfilled,
    NeedMore,
    /// Readahead raced a faster user read past the window. Return what we have.
    ReadaheadDone,
}

impl StreamReader {
    pub(crate) fn new(stream_id: u64, next_read_offset: u64, deps: Arc<ReaderDeps>) -> Arc<Self> {
        Arc::new(Self {
            stream_id,
            deps,
            state: Arc::new(TokioMutex::new(ReaderState {
                blocks: BTreeMap::new(),
                last_block: None,
                loaded_end_offset: 0,
                blocks_epoch: 0,
                readahead: ReadaheadState::new(),
            })),
            index_load_lock: TokioMutex::new(()),
            next_read_offset: AtomicU64::new(next_read_offset),
            last_access_ms: AtomicU64::new(now_ms()),
            reading: AtomicBool::new(false),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn next_read_offset(&self) -> u64 {
        self.next_read_offset.load(Ordering::Acquire)
    }

    pub fn last_access_ms(&self) -> u64 {
        self.last_access_ms.load(Ordering::Relaxed)
    }

    pub async fn read(
        self: &Arc<Self>,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<ReadDataBlock, StreamError> {
        if start_offset != self.next_read_offset() {
            return Err(StreamError::Unexpected(format!(
                "[BUG] stream={} read offset not match, expect {} but {}",
                self.stream_id,
                self.next_read_offset(),
                start_offset
            )));
        }
        if self.reading.swap(true, Ordering::AcqRel) {
            return Err(StreamError::Unexpected(
                "stream reader is in reading state, can't read again".into(),
            ));
        }
        let result = self
            .read_with_retries(start_offset, end_offset, max_bytes, 2)
            .await;
        self.reading.store(false, Ordering::Release);
        result
    }

    async fn read_with_retries(
        self: &Arc<Self>,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
        mut left_retries: u32,
    ) -> Result<ReadDataBlock, StreamError> {
        self.last_access_ms.store(now_ms(), Ordering::Relaxed);
        loop {
            match self.read0(start_offset, end_offset, max_bytes).await {
                Ok(result) => {
                    self.after_read(&result).await;
                    return Ok(result);
                }
                Err(e) if left_retries > 0 && is_recoverable(&e) => {
                    self.reset_blocks().await;
                    left_retries -= 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn read0(
        self: &Arc<Self>,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<ReadDataBlock, StreamError> {
        let mut records = Vec::new();
        let mut access = CacheAccessType::BlockCacheHit;
        let mut next_start = start_offset;
        let mut remaining: i64 = max_bytes.min(i64::MAX as usize) as i64;
        loop {
            let reads = self
                .get_blocks(
                    next_start,
                    Some(end_offset),
                    remaining.max(0) as usize,
                    false,
                )
                .await?;
            if reads.is_empty() {
                return Err(StreamError::Unexpected(format!(
                    "[UNEXPECTED] streamId={} Get empty blocks [{next_start}, {end_offset})",
                    self.stream_id
                )));
            }
            if reads.iter().any(|r| !r.was_loaded) {
                access = CacheAccessType::BlockCacheMiss;
            }
            for read in &reads {
                read.handle.wait_load().await?;
            }
            // Attach loaded data to window slots in order, BEFORE extraction/afterRead
            // before the consumer can markRead the block).
            {
                let mut state = self.state.lock().await;
                for read in &reads {
                    Self::attach_data(self, &mut state, &read.index, &read.handle);
                }
            }
            let iteration_start = next_start;
            let mut fulfilled = false;
            for read in &reads {
                let index = &read.index;
                if next_start < index.start_offset || next_start >= index.end_offset() {
                    return Err(StreamError::Unexpected(format!(
                        "[BUG] nextStartOffset:{next_start} is not in the range of index:{}-{}",
                        index.start_offset,
                        index.end_offset()
                    )));
                }
                let next_end = end_offset.min(index.end_offset());
                let new_records = read.handle.get_records(next_start, next_end, remaining);
                next_start = next_end;
                remaining -= new_records.iter().map(|r| r.size() as i64).sum::<i64>();
                records.extend(new_records);
                if next_start >= end_offset || remaining <= 0 {
                    fulfilled = true;
                    break;
                }
            }
            if fulfilled {
                return Ok(ReadDataBlock {
                    records,
                    cache_access: access,
                });
            }
            if next_start == iteration_start {
                return Err(StreamError::Unexpected(
                    "[UNEXPECTED] Can't read any record from the blocks".into(),
                ));
            }
            // Data block sizes include headers, so the index-based budget can
        }
    }

    fn attach_data(
        self: &Arc<Self>,
        state: &mut ReaderState,
        index: &DataBlockIndex,
        handle: &Arc<DataBlock>,
    ) {
        let Some(slot) = state.blocks.get_mut(&index.start_offset) else {
            return; // window advanced past this block (user read faster than readahead)
        };
        if slot.index != *index {
            return; // window was reset and reloaded differently
        }
        if slot.data.as_ref().is_some_and(|d| Arc::ptr_eq(d, handle)) {
            return;
        }
        if let Some(old) = slot.free_listener.take() {
            old.close();
        }
        slot.data = Some(Arc::clone(handle));
        handle.mark_unread();
        let state_w = Arc::downgrade(&self.state);
        let closed = Arc::clone(&self.closed);
        let block_w = Arc::downgrade(handle);
        let start = index.start_offset;
        slot.free_listener = Some(handle.register_free_listener(Box::new(move |_| {
            if closed.load(Ordering::Acquire) {
                return;
            }
            let (Some(state), Some(block)) = (state_w.upgrade(), block_w.upgrade()) else {
                return;
            };
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let mut s = state.lock().await;
                    let still_current = s
                        .blocks
                        .get(&start)
                        .and_then(|slot| slot.data.as_ref())
                        .is_some_and(|d| Arc::ptr_eq(d, &block));
                    if still_current {
                        s.readahead.reset();
                        tracing::warn!(
                            "the unread block is evicted, please increase the block cache size"
                        );
                    }
                });
            }
        })));
    }

    async fn after_read(self: &Arc<Self>, result: &ReadDataBlock) {
        if let Some(last) = result.records.last() {
            self.next_read_offset
                .store(last.last_offset(), Ordering::Release);
        }
        let next_read_offset = self.next_read_offset();
        {
            let mut state = self.state.lock().await;
            while let Some((&start, slot)) = state.blocks.iter().next() {
                if slot.index.end_offset() > next_read_offset {
                    break;
                }
                let slot = state.blocks.remove(&start).expect("just observed");
                self.retire_slot(slot);
            }
        }
        let cache_miss = result.cache_access == CacheAccessType::BlockCacheMiss;
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.try_readahead(cache_miss).await;
        });
    }

    fn retire_slot(&self, slot: BlockSlot) {
        if let Some(data) = slot.data {
            self.deps.data_block_cache.mark_read(&data);
        }
        if let Some(handle) = slot.free_listener {
            handle.close();
        }
    }

    async fn reset_blocks(&self) {
        let mut state = self.state.lock().await;
        let slots = std::mem::take(&mut state.blocks);
        for (_, slot) in slots {
            self.retire_slot(slot);
        }
        state.last_block = None;
        state.loaded_end_offset = 0;
        state.blocks_epoch += 1;
        tracing::info!("the stream reader's blocks are reset, cause of the object compaction");
    }

    pub async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let mut state = self.state.lock().await;
        let slots = std::mem::take(&mut state.blocks);
        for (_, slot) in slots {
            self.retire_slot(slot);
        }
    }

    /// Walk the cached window and load more indexes when it runs out. The
    /// budget subtracts index sizes except for a partial first block.
    async fn get_blocks(
        self: &Arc<Self>,
        start_offset: u64,
        end_offset: Option<u64>,
        max_bytes: usize,
        readahead: bool,
    ) -> Result<Vec<BlockRead>, StreamError> {
        let mut collected: Vec<BlockRead> = Vec::new();
        let mut remaining: i64 = max_bytes.min(i64::MAX as usize) as i64;
        let mut cursor = start_offset;
        loop {
            let outcome = {
                let mut state = self.state.lock().await;
                self.walk_window(
                    &mut state,
                    cursor,
                    end_offset,
                    &mut remaining,
                    readahead,
                    &mut collected,
                )?
            };
            match outcome {
                WalkOutcome::Fulfilled | WalkOutcome::ReadaheadDone => return Ok(collected),
                WalkOutcome::NeedMore => {}
            }
            let more = self.load_more_blocks(end_offset).await?;
            if readahead {
                if !more {
                    return Ok(collected);
                }
            } else if !more {
                let loaded_end = self.state.lock().await.loaded_end_offset;
                if end_offset.is_some_and(|end| end > loaded_end) {
                    return Err(StreamError::Unexpected(format!(
                        "[BUG] streamId={} expect load blocks to endOffset={:?}, current loadedBlockIndexEndOffset={}",
                        self.stream_id, end_offset, loaded_end
                    )));
                }
            }
            cursor = collected
                .last()
                .map(|b| b.index.end_offset())
                .unwrap_or(cursor);
        }
    }

    fn walk_window(
        self: &Arc<Self>,
        state: &mut ReaderState,
        cursor: u64,
        end_offset: Option<u64>,
        remaining: &mut i64,
        readahead: bool,
        collected: &mut Vec<BlockRead>,
    ) -> Result<WalkOutcome, StreamError> {
        let floor = state.blocks.range(..=cursor).next_back().map(|(k, _)| *k);
        if floor.is_none() && !state.blocks.is_empty() {
            if readahead {
                // The user read outpaced the readahead and cleared these blocks.
                return Ok(WalkOutcome::ReadaheadDone);
            }
            return Err(StreamError::Unexpected(format!(
                "[BUG] streamId={} cannot find floor block for startOffset={cursor}",
                self.stream_id
            )));
        }
        let Some(floor) = floor else {
            return Ok(WalkOutcome::NeedMore);
        };
        if cursor >= state.loaded_end_offset {
            return Ok(WalkOutcome::NeedMore);
        }
        let keys: Vec<u64> = state.blocks.range(floor..).map(|(k, _)| *k).collect();
        let mut first = true;
        for key in keys {
            let slot = state.blocks.get(&key).expect("key just collected");
            let index = slot.index;
            if !first || index.start_offset == cursor {
                *remaining -= index.size as i64;
            }
            first = false;
            let reader = self.deps.readers.get(&slot.metadata);
            let handle = self.deps.data_block_cache.get_block_handle(
                GetOptions { readahead },
                reader,
                index,
            );
            let was_loaded = handle.is_loaded();
            collected.push(BlockRead {
                index,
                handle,
                was_loaded,
            });
            let end_reached = end_offset.is_some_and(|end| index.end_offset() >= end);
            if end_reached || *remaining <= 0 {
                return Ok(WalkOutcome::Fulfilled);
            }
        }
        Ok(WalkOutcome::NeedMore)
    }

    async fn load_more_blocks(
        self: &Arc<Self>,
        end_offset: Option<u64>,
    ) -> Result<bool, StreamError> {
        let before = self.state.lock().await.loaded_end_offset;
        self.load_more_blocks0(end_offset).await?;
        Ok(self.state.lock().await.loaded_end_offset != before)
    }

    async fn load_more_blocks0(
        self: &Arc<Self>,
        end_offset: Option<u64>,
    ) -> Result<(), StreamError> {
        let _guard = self.index_load_lock.lock().await;
        let (epoch, next_loading_offset) = {
            let state = self.state.lock().await;
            if end_offset.is_some_and(|end| end <= state.loaded_end_offset) {
                return Ok(());
            }
            let next = state
                .last_block
                .map(|(_, end)| end)
                .unwrap_or(0)
                .max(self.next_read_offset());
            (state.blocks_epoch, next)
        };
        let objects = self
            .deps
            .object_manager
            .get_objects(
                self.stream_id,
                next_loading_offset,
                end_offset.unwrap_or(NOOP_OFFSET),
                GET_OBJECT_STEP,
            )
            .await?;
        let mut next_find_offset = next_loading_offset;
        for metadata in objects {
            let reader = self.deps.readers.get(&metadata);
            let find = reader
                .find(self.stream_id, next_find_offset, NOOP_OFFSET, usize::MAX)
                .await?;
            let mut state = self.state.lock().await;
            if state.blocks_epoch != epoch {
                // The window was reset while we were fetching. Discard.
                return Ok(());
            }
            for index in find.blocks {
                if !Self::put_block(&mut state, self.next_read_offset(), &metadata, index) {
                    return Err(StreamError::BlockNotContinuous);
                }
                next_find_offset = index.end_offset();
            }
        }
        Ok(())
    }

    fn put_block(
        state: &mut ReaderState,
        next_read_offset: u64,
        metadata: &S3ObjectMetadata,
        index: DataBlockIndex,
    ) -> bool {
        match state.last_block {
            None => {
                if !(index.start_offset <= next_read_offset
                    && index.end_offset() > next_read_offset)
                {
                    tracing::error!(
                        "[BUG] the first block should contain the nextReadOffset, block={:?} nextReadOffset={}",
                        index,
                        next_read_offset
                    );
                    return false;
                }
            }
            Some((_, last_end)) => {
                if last_end != index.start_offset {
                    return false;
                }
            }
        }
        state.last_block = Some((index.start_offset, index.end_offset()));
        state.loaded_end_offset = index.end_offset();
        state.blocks.insert(
            index.start_offset,
            BlockSlot {
                metadata: metadata.clone(),
                index,
                data: None,
                free_listener: None,
            },
        );
        true
    }

    pub(crate) async fn try_readahead(self: &Arc<Self>, cache_miss: bool) {
        let (offset, size) = {
            let mut state = self.state.lock().await;
            let ra = &mut state.readahead;
            if now_ms().saturating_sub(ra.reset_ts_ms) < READAHEAD_RESET_COLD_DOWN_MS {
                return;
            }
            ra.cache_miss_count += usize::from(cache_miss);
            if ra.inflight {
                return;
            }
            ra.next_size = (ra.next_size + ra.cache_miss_count * READAHEAD_SIZE_UNIT)
                .min(max_readahead_size());
            ra.cache_miss_count = 0;
            if ra.require_reset {
                ra.next_offset = 0;
                ra.next_size = READAHEAD_SIZE_UNIT;
                ra.mark_offset = 0;
                ra.require_reset = false;
            }
            let next_read_offset = self.next_read_offset();
            if next_read_offset >= ra.next_offset {
                ra.next_offset = next_read_offset;
            } else if next_read_offset <= ra.mark_offset {
                // The user read hasn't reached the previous readahead mark yet.
                return;
            }
            if self.deps.data_block_cache.available()
                < ra.next_size as i64 + READAHEAD_AVAILABLE_BYTES_THRESHOLD
            {
                return;
            }
            ra.mark_offset = ra.next_offset;
            ra.inflight = true;
            (ra.next_offset, ra.next_size)
        };
        let result = self.get_blocks(offset, None, size, true).await;
        match &result {
            Ok(blocks) => {
                // Attach data to window slots as loads complete (drop-behind wiring).
                for block in blocks {
                    let this = Arc::clone(self);
                    let handle = Arc::clone(&block.handle);
                    let index = block.index;
                    tokio::spawn(async move {
                        if handle.wait_load().await.is_ok() {
                            let mut state = this.state.lock().await;
                            Self::attach_data(&this, &mut state, &index, &handle);
                        }
                    });
                }
            }
            Err(e) => {
                if !is_recoverable(e) {
                    tracing::error!("readahead failed: {e}");
                }
            }
        }
        let mut state = self.state.lock().await;
        if let Ok(blocks) = &result
            && let Some(last) = blocks.last()
        {
            state.readahead.next_offset = last.index.end_offset();
        }
        state.readahead.inflight = false;
    }
}

/// ObjectNotExist / NoSuchKey /
/// BlockNotContinuous reset the window and retry.
pub(crate) fn is_recoverable(error: &StreamError) -> bool {
    match error {
        StreamError::ObjectNotExist { .. } | StreamError::BlockNotContinuous => true,
        StreamError::Object(e) => e.is_not_found(),
        _ => false,
    }
}
