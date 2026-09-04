//! Snapshot-read cache: this node serves tail reads for streams it does not own.
//!
//! WAL ranges and committed objects via [`SnapshotReadCache::replay_wal`] /
//! [`SnapshotReadCache::replay_objects`]. The engine does **not** auto-replay from
//! ConfirmWAL. Puts land in a dedicated `LogCache`. Snapshot-read fetches hit that
//!
//!   `tokio::sync::Mutex` so `put` / `tryLoad` / `tryPutIntoCache` never interleave.
//!   The lock is **not** held across WAL/object I/O.
//!   last-access map + sweep on put/replay. Expiry still `clearStream`.
//!   `Arc` and exposes [`S3Storage::snapshot_read_cache`].
//!   `eventLoop.submit(clearOverloadedTask).join()` → drain waiting under the mutex
//!   when the cap is full, then enqueue.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use s3stream_codec::StreamRecordBatch;
use s3stream_object::{ObjectReader, ObjectStorage, S3ObjectMetadata, decode_data_block};
use s3stream_wal::{RecordOffset, WriteAheadLog};
use tokio::sync::oneshot;

use crate::api::{LinkRecordDecoder, StreamError};
use crate::cache::log_cache::{FreeListener, LogCache, StreamRangeBound};
use crate::manager::StreamManager;

/// Guava `expireAfterAccess(10, TimeUnit.MINUTES)`.
const ACTIVE_EXPIRE: Duration = Duration::from_secs(10 * 60);
const MAX_INFLIGHT_LOAD_BYTES: u64 = 100 * 1024 * 1024;
const TASK_WAITING_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_WAITING_LOAD_TASK_COUNT: usize = 4096;

pub trait EventListener: Send + Sync {
    /// [`RequestCommitEvent`].
    fn on_event(&self, event: RequestCommitEvent);
}

/// Ask the owner node to commit its WAL so snapshot-read cache pressure can drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestCommitEvent {
    pub node_id: i32,
}

#[derive(Clone)]
pub struct SnapshotReadCache {
    shared: Arc<Shared>,
}

///
/// Protocol (one cache, one WAL replay, one object replay) is unchanged.
struct Shared {
    /// Serializes `put` / `tryLoad` / `tryPutIntoCache` / `clearStream`.
    ///
    /// Protocol (those methods never interleave) is unchanged. Not held across WAL
    /// `get` / object reads.
    state: tokio::sync::Mutex<State>,
    cache: Arc<LogCache>,
    object_storage: Arc<dyn ObjectStorage>,
    link_decoder: Option<Arc<dyn LinkRecordDecoder>>,
    event_listeners: Arc<Mutex<Vec<Arc<dyn EventListener>>>>,
    cache_free_listener: FreeListener,
}

/// EventLoop-owned maps plus the two replay machines.
struct State {
    stream_next_offsets: HashMap<u64, u64>,
    active_streams: HashMap<u64, tokio::time::Instant>,
    wal: WalReplayState,
    object: ObjectReplayState,
}

struct WalReplayState {
    waiting: VecDeque<Arc<WalReplayTask>>,
    loading: VecDeque<Arc<WalReplayTask>>,
    max_inflight: usize,
}

struct ObjectReplayState {
    inflight_load_bytes: u64,
    waiting: VecDeque<Arc<ObjectReplayTask>>,
    loading: VecDeque<Arc<ObjectReplayTask>>,
}

struct WalReplayTask {
    timestamp: tokio::time::Instant,
    wal: Arc<dyn WriteAheadLog>,
    start: RecordOffset,
    end: RecordOffset,
    wal_records: Option<Vec<StreamRecordBatch>>,
    records: Mutex<Vec<StreamRecordBatch>>,
    /// Protocol (puts wait on the
    /// loading-queue head until load finishes, including failed loads) is unchanged.
    load_done: AtomicBool,
    replay_tx: Mutex<Option<oneshot::Sender<()>>>,
}

struct ObjectReplayTask {
    reader: ObjectReader,
    /// `None` until `basicObjectInfo` returns
    blocks: Mutex<Option<Vec<BlockLoad>>>,
    replay_tx: Mutex<Option<oneshot::Sender<()>>>,
}

/// One `reader.read(blockIndex)` slot in `ObjectReplayTask#blocks`.
///
/// `cf.isDone()`.`Err` is `isCompletedExceptionally` / `isCancelled` (skip).
struct BlockLoad {
    done: AtomicBool,
    records: Mutex<Option<Result<Vec<StreamRecordBatch>, ()>>>,
}

impl SnapshotReadCache {
    pub fn new(
        stream_manager: Arc<dyn StreamManager>,
        cache: Arc<LogCache>,
        object_storage: Arc<dyn ObjectStorage>,
        link_decoder: Option<Arc<dyn LinkRecordDecoder>>,
    ) -> Self {
        let event_listeners = Arc::new(Mutex::new(Vec::<Arc<dyn EventListener>>::new()));
        let cache_free_listener: FreeListener = {
            let stream_manager = Arc::clone(&stream_manager);
            let listeners = Arc::clone(&event_listeners);
            Arc::new(move |bounds: &[StreamRangeBound]| {
                // EventLoop. We cannot block a tokio worker. Spawn and notify. Protocol
                // (`bound.endOffset() > streamMetadata.endOffset()` → RequestCommitEvent
                // for `nodeId`) is unchanged.
                let bounds = bounds.to_vec();
                let stream_manager = Arc::clone(&stream_manager);
                let listeners = Arc::clone(&listeners);
                tokio::spawn(async move {
                    notify_commit_if_ahead(&stream_manager, &listeners, &bounds).await;
                });
            })
        };

        let max_inflight = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            * 4;
        Self {
            shared: Arc::new(Shared {
                state: tokio::sync::Mutex::new(State {
                    stream_next_offsets: HashMap::new(),
                    active_streams: HashMap::new(),
                    wal: WalReplayState {
                        waiting: VecDeque::new(),
                        loading: VecDeque::new(),
                        max_inflight,
                    },
                    object: ObjectReplayState {
                        inflight_load_bytes: 0,
                        waiting: VecDeque::new(),
                        loading: VecDeque::new(),
                    },
                }),
                cache,
                object_storage,
                link_decoder,
                event_listeners,
                cache_free_listener,
            }),
        }
    }

    pub fn add_event_listener(&self, listener: Arc<dyn EventListener>) {
        self.shared
            .event_listeners
            .lock()
            .expect("listeners poisoned")
            .push(listener);
    }

    /// Ingest confirmed records (already loaded). Serialized with replay puts.
    pub async fn put(&self, records: Vec<StreamRecordBatch>) {
        let mut state = self.shared.state.lock().await;
        self.sweep_expired(&mut state);
        self.put_locked(&mut state, records);
    }

    /// Replay committed objects into the snapshot cache. An empty list is
    /// rejected as [`StreamError::Unexpected`].
    pub async fn replay_objects(&self, objects: Vec<S3ObjectMetadata>) -> Result<(), StreamError> {
        if objects.is_empty() {
            return Err(StreamError::Unexpected(
                "The objects is an empty list".into(),
            ));
        }
        let mut last_rx = None;
        {
            let mut state = self.shared.state.lock().await;
            for object in objects {
                let (tx, rx) = oneshot::channel();
                last_rx = Some(rx);
                let task = Arc::new(ObjectReplayTask {
                    reader: ObjectReader::new(object, Arc::clone(&self.shared.object_storage)),
                    blocks: Mutex::new(None),
                    replay_tx: Mutex::new(Some(tx)),
                });
                state.object.waiting.push_back(task);
            }
            self.try_load_objects(&mut state);
        }
        let rx = last_rx.expect("non-empty objects");
        let _ = rx.await;
        Ok(())
    }

    /// Replay a confirmed WAL range.
    ///
    /// `wal.get(start, end)`. Overload (wait > 5s or waiting queue full) drops all
    /// waiting tasks as success and fires [`RequestCommitEvent`] per WAL `nodeId`.
    pub async fn replay_wal(
        &self,
        wal: Arc<dyn WriteAheadLog>,
        start: RecordOffset,
        end: RecordOffset,
        wal_records: Option<Vec<StreamRecordBatch>>,
    ) -> Result<(), StreamError> {
        let (tx, rx) = oneshot::channel();
        let task = Arc::new(WalReplayTask {
            timestamp: tokio::time::Instant::now(),
            wal,
            start,
            end,
            wal_records,
            records: Mutex::new(Vec::new()),
            load_done: AtomicBool::new(false),
            replay_tx: Mutex::new(Some(tx)),
        });
        {
            let mut state = self.shared.state.lock().await;
            while state.wal.waiting.len() >= MAX_WAITING_LOAD_TASK_COUNT {
                self.clear_overloaded(&mut state);
            }
            state.wal.waiting.push_back(Arc::clone(&task));
            self.try_load_wal(&mut state);
        }
        let _ = rx.await;
        {
            let mut state = self.shared.state.lock().await;
            self.try_load_wal(&mut state);
        }
        Ok(())
    }

    /// The LogCache this snapshot-read cache writes.
    ///
    /// `S3Storage#read0` uses that same instance as `firstCache` when
    /// to snapshot-read fetches on this cache) is unchanged.
    pub fn log_cache(&self) -> &Arc<LogCache> {
        &self.shared.cache
    }

    fn put_locked(&self, state: &mut State, records: Vec<StreamRecordBatch>) {
        let cache = &self.shared.cache;
        let mut stream_id: Option<u64> = None;
        for batch in records {
            let new_stream_id = batch.stream_id();
            if stream_id != Some(new_stream_id) {
                stream_id = Some(new_stream_id);
                state
                    .stream_next_offsets
                    .entry(new_stream_id)
                    .or_insert(batch.base_offset());
                self.active_stream(state, new_stream_id);
            }
            let expected = *state
                .stream_next_offsets
                .get(&new_stream_id)
                .expect("just inserted");
            if batch.base_offset() < expected {
                continue;
            } else if batch.base_offset() > expected {
                // LogCacheBlock does not accept discontinuous batches.
                cache.clear_stream_records(new_stream_id);
            }
            let copy = batch.clone();
            let last = copy.last_offset();
            if !cache.put(copy.clone()) {
                let block = cache.archive_current_block();
                block.add_free_listener(Arc::clone(&self.shared.cache_free_listener));
                cache.mark_free(&block);
                cache.put(copy);
            }
            state.stream_next_offsets.insert(new_stream_id, last);
        }
    }

    fn active_stream(&self, state: &mut State, stream_id: u64) {
        state
            .active_streams
            .insert(stream_id, tokio::time::Instant::now());
    }

    /// Guava removal listener → `clearStream`. Sweep idle > 10 minutes.
    fn sweep_expired(&self, state: &mut State) {
        let now = tokio::time::Instant::now();
        let expired: Vec<u64> = state
            .active_streams
            .iter()
            .filter(|(_, at)| now.saturating_duration_since(**at) >= ACTIVE_EXPIRE)
            .map(|(id, _)| *id)
            .collect();
        for stream_id in expired {
            self.clear_stream(state, stream_id);
        }
    }

    fn clear_stream(&self, state: &mut State, stream_id: u64) {
        self.shared.cache.clear_stream_records(stream_id);
        state.stream_next_offsets.remove(&stream_id);
        state.active_streams.remove(&stream_id);
    }

    /// If the loading queue is at `max_inflight`, **break** (do not peek
    /// waiting timeout). Else peek waiting. If wait > 5s `clearOverloadedTask` and
    /// return. Else poll, add to loading, `task.run()`.
    fn try_load_wal(&self, state: &mut State) {
        loop {
            if state.wal.loading.len() >= state.wal.max_inflight {
                break;
            }
            let Some(head) = state.wal.waiting.front() else {
                break;
            };
            if tokio::time::Instant::now().saturating_duration_since(head.timestamp)
                > TASK_WAITING_TIMEOUT
            {
                self.clear_overloaded(state);
                return;
            }
            let task = state.wal.waiting.pop_front().expect("front existed");
            state.wal.loading.push_back(Arc::clone(&task));
            let shared = Arc::clone(&self.shared);
            tokio::spawn(async move {
                run_wal_load(Arc::clone(&shared), task).await;
                let cache = SnapshotReadCache {
                    shared: Arc::clone(&shared),
                };
                let mut state = shared.state.lock().await;
                cache.try_put_wal(&mut state);
                cache.try_load_wal(&mut state);
            });
        }
    }

    /// Triggered when wait > 5s **or**
    /// `waitingLoadTasks.offer` fails (queue cap 4096). Completes every waiting
    /// `replayCf`/`loadCf` as success and `notifyListener(nodeId)` per WAL.
    fn clear_overloaded(&self, state: &mut State) {
        let mut node_ids = HashSet::new();
        let mut drop_count = 0usize;
        while let Some(task) = state.wal.waiting.pop_front() {
            node_ids.insert(task.wal.metadata().node_id);
            complete_oneshot(&task.replay_tx);
            drop_count += 1;
        }
        for node_id in &node_ids {
            self.notify_listener(*node_id as i32);
        }
        tracing::warn!(
            drop_count,
            ?node_ids,
            "wal replay is overloaded, drop all waiting tasks and request nodes to commit"
        );
    }

    /// `WalReplay#tryPutIntoCache`. Puts run in load order: skip until the head
    /// `loadCf` is done, then put that task's records (empty if load failed).
    fn try_put_wal(&self, state: &mut State) {
        while let Some(head) = state.wal.loading.front() {
            if !head.load_done.load(Ordering::Acquire) {
                break;
            }
            let task = state.wal.loading.pop_front().expect("front existed");
            let records = std::mem::take(&mut *task.records.lock().expect("records poisoned"));
            self.put_locked(state, records);
            complete_oneshot(&task.replay_tx);
        }
    }

    fn try_load_objects(&self, state: &mut State) {
        loop {
            if state.object.inflight_load_bytes >= MAX_INFLIGHT_LOAD_BYTES {
                break;
            }
            let Some(task) = state.object.waiting.pop_front() else {
                break;
            };
            state.object.inflight_load_bytes += task.reader.metadata().object_size;
            state.object.loading.push_back(Arc::clone(&task));
            let shared = Arc::clone(&self.shared);
            tokio::spawn(async move {
                run_object_load(Arc::clone(&shared), task).await;
                let cache = SnapshotReadCache {
                    shared: Arc::clone(&shared),
                };
                let mut state = shared.state.lock().await;
                cache.try_put_objects(&mut state);
            });
        }
    }

    fn try_put_objects(&self, state: &mut State) {
        loop {
            let Some(head) = state.object.loading.front().cloned() else {
                break;
            };
            if !self.put_object_into_cache(state, &head) {
                break;
            }
            let task = state.object.loading.pop_front().expect("front existed");
            // Decrement inflight load bytes, then try loading more.
            state.object.inflight_load_bytes = state
                .object
                .inflight_load_bytes
                .saturating_sub(task.reader.metadata().object_size);
            complete_oneshot(&task.replay_tx);
            self.try_load_objects(state);
        }
    }

    /// `true` when every block is consumed
    /// (`blocks.peek() == null` → `cf.complete(null)`). `false` if `blocks` is still
    /// null (info not loaded) or the head block's CF is not done.
    fn put_object_into_cache(&self, state: &mut State, task: &ObjectReplayTask) -> bool {
        loop {
            let records = {
                let mut blocks = task.blocks.lock().expect("blocks poisoned");
                let Some(slots) = blocks.as_mut() else {
                    return false;
                };
                if slots.is_empty() {
                    // → `cf.complete(null); return true`.
                    return true;
                }
                if !slots[0].done.load(Ordering::Acquire) {
                    return false;
                }
                let slot = slots.remove(0);
                // EventLoop. The CF queue is not held during `put`).

                slot.records.lock().expect("slot poisoned").take()
            };
            if let Some(Ok(records)) = records {
                self.put_locked(state, records);
            }
            // → poll and continue.
        }
    }

    fn notify_listener(&self, node_id: i32) {
        let listeners = self
            .shared
            .event_listeners
            .lock()
            .expect("listeners poisoned")
            .clone();
        for listener in listeners {
            listener.on_event(RequestCommitEvent { node_id });
        }
    }
}

/// `ObjectReplayTask#cf`. Protocol (callers waiting on `replay` resolve with success
/// even when the load was dropped or the object failed) is unchanged.
fn complete_oneshot(tx: &Mutex<Option<oneshot::Sender<()>>>) {
    if let Some(tx) = tx.lock().expect("oneshot poisoned").take() {
        let _ = tx.send(());
    }
}

async fn run_wal_load(shared: Arc<Shared>, task: Arc<WalReplayTask>) {
    let loaded = if let Some(records) = &task.wal_records {
        Ok(records.clone())
    } else {
        task.wal
            .get_range(task.start, task.end)
            .await
            .map_err(|e| e.to_string())
    };
    match loaded {
        Err(e) => {
            tracing::error!(
                start = ?task.start,
                end = ?task.end,
                error = %e,
                "Replay WAL fail"
            );
        }
        Ok(wal_records) => {
            let mut decoded = Vec::with_capacity(wal_records.len());
            for record in wal_records {
                if record.count() >= 0 {
                    decoded.push(record);
                    continue;
                }
                match &shared.link_decoder {
                    None => {
                        tracing::error!("Replay WAL link decode fail: decoder is not installed");
                        decoded.clear();
                        break;
                    }
                    Some(decoder) => match decoder.decode(record).await {
                        Ok(plain) => decoded.push(plain),
                        Err(e) => {
                            tracing::error!(error = %e, "Replay WAL link decode fail");
                            decoded.clear();
                            break;
                        }
                    },
                }
            }
            *task.records.lock().expect("records poisoned") = decoded;
        }
    }
    task.load_done.store(true, Ordering::Release);
}

async fn run_object_load(shared: Arc<Shared>, task: Arc<ObjectReplayTask>) {
    let indexes = match task.reader.basic_object_info().await {
        Ok(info) => info.index_block.entries().to_vec(),
        Err(e) => {
            // → log + `cf.complete(null)`.
            // The task stays in `loadingTasks` with `blocks == null`. Later
            tracing::error!(error = %e, metadata = ?task.reader.metadata(), "Failed to load object");
            complete_oneshot(&task.replay_tx);
            return;
        }
    };
    let n = indexes.len();
    let slots: Vec<BlockLoad> = (0..n)
        .map(|_| BlockLoad {
            done: AtomicBool::new(false),
            records: Mutex::new(None),
        })
        .collect();
    *task.blocks.lock().expect("blocks poisoned") = Some(slots);
    let reads = indexes.into_iter().enumerate().map(|(i, index)| {
        let task = Arc::clone(&task);
        async move {
            let result = match task.reader.read_block(&index).await {
                Ok(bytes) => decode_data_block(&bytes).map_err(|_| ()),
                Err(e) => {
                    tracing::error!(error = %e, metadata = ?task.reader.metadata(), "Failed to load object blocks");
                    Err(())
                }
            };
            {
                let blocks = task.blocks.lock().expect("blocks poisoned");
                if let Some(slots) = blocks.as_ref() {
                    *slots[i].records.lock().expect("slot poisoned") = Some(result);
                    slots[i].done.store(true, Ordering::Release);
                }
            }
        }
    });
    futures::future::join_all(reads).await;
    let cache = SnapshotReadCache {
        shared: Arc::clone(&shared),
    };
    let mut state = shared.state.lock().await;
    cache.try_put_objects(&mut state);
}

async fn notify_commit_if_ahead(
    stream_manager: &Arc<dyn StreamManager>,
    listeners: &Arc<Mutex<Vec<Arc<dyn EventListener>>>>,
    bounds: &[StreamRangeBound],
) {
    let ids: Vec<u64> = bounds.iter().map(|b| b.stream_id).collect();
    let metas = match stream_manager.get_streams(&ids).await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "snapshot-read free listener getStreams fail");
            return;
        }
    };
    let mut nodes = HashSet::new();
    for meta in metas {
        if let Some(bound) = bounds.iter().find(|b| b.stream_id == meta.stream_id)
            && (bound.end_offset as i64) > (meta.end_offset as i64)
        {
            nodes.insert(meta.node_id);
        }
    }
    let listeners = listeners.lock().expect("listeners poisoned").clone();
    for node_id in nodes {
        for listener in &listeners {
            listener.on_event(RequestCommitEvent { node_id });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::log_cache::DEFAULT_MAX_BLOCK_STREAM_COUNT;
    use crate::memory::MemoryMetadataManager;
    use bytes::Bytes;
    use s3stream_object::{MemoryObjectStorage, ObjectWriter, WriteOptions};
    use s3stream_wal::memory::MemoryWriteAheadLog;
    use std::sync::Mutex as StdMutex;

    fn record(stream_id: u64, base_offset: u64, payload: &[u8]) -> StreamRecordBatch {
        StreamRecordBatch::new(
            stream_id,
            1,
            base_offset,
            1,
            Bytes::copy_from_slice(payload),
        )
    }

    fn cache() -> (SnapshotReadCache, Arc<LogCache>, Arc<MemoryMetadataManager>) {
        let manager = MemoryMetadataManager::new();
        let log = Arc::new(LogCache::new(
            1 << 30,
            1 << 20,
            DEFAULT_MAX_BLOCK_STREAM_COUNT,
        ));
        let object_storage: Arc<dyn ObjectStorage> = Arc::new(MemoryObjectStorage::new(0));
        let snapshot = SnapshotReadCache::new(
            manager.clone() as Arc<dyn StreamManager>,
            Arc::clone(&log),
            object_storage,
            None,
        );
        (snapshot, log, manager)
    }

    struct CollectingListener {
        events: StdMutex<Vec<RequestCommitEvent>>,
    }

    impl EventListener for CollectingListener {
        fn on_event(&self, event: RequestCommitEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[tokio::test]
    async fn put_continuous_is_readable() {
        let (snapshot, log, _) = cache();
        snapshot
            .put(vec![
                record(1, 0, b"a"),
                record(1, 1, b"b"),
                record(1, 2, b"c"),
            ])
            .await;
        let got = log.get(1, 0, 3, usize::MAX);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].base_offset(), 0);
        assert_eq!(got[2].last_offset(), 3);
    }

    /// → `clearStreamRecords` then accept the suffix.
    #[tokio::test]
    async fn gap_put_clears_stream_then_accepts_suffix() {
        let (snapshot, log, _) = cache();
        snapshot
            .put(vec![record(1, 0, b"a"), record(1, 1, b"b")])
            .await;
        snapshot.put(vec![record(1, 10, b"z")]).await;
        assert!(log.get(1, 0, 2, usize::MAX).is_empty());
        let got = log.get(1, 10, 11, usize::MAX);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].base_offset(), 10);
    }

    /// → drop.
    #[tokio::test]
    async fn older_put_is_dropped() {
        let (snapshot, log, _) = cache();
        snapshot.put(vec![record(1, 5, b"a")]).await;
        snapshot.put(vec![record(1, 3, b"old")]).await;
        let got = log.get(1, 5, 6, usize::MAX);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].payload(), Bytes::from_static(b"a"));
        assert!(log.get(1, 3, 4, usize::MAX).is_empty());
    }

    #[tokio::test]
    async fn replay_wal_via_get_range() {
        let (snapshot, log, _) = cache();
        let wal = Arc::new(MemoryWriteAheadLog::new(9, 1));
        wal.start().await.unwrap();
        let a = wal.append(record(1, 0, b"w0")).await.unwrap();
        let b = wal.append(record(1, 1, b"w1")).await.unwrap();
        snapshot
            .replay_wal(
                wal.clone() as Arc<dyn WriteAheadLog>,
                a.record_offset,
                b.next_offset,
                None,
            )
            .await
            .unwrap();
        let got = log.get(1, 0, 2, usize::MAX);
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].payload(), Bytes::from_static(b"w1"));
    }

    /// `tryLoad` checks `loading.size() >= maxInflight` **first** and `break`s. It
    /// never peeks waiting timeout while inflight is full. This test fills the waiting
    /// queue to `MAX_WAITING_LOAD_TASK_COUNT` (4096) so the next `replay` takes the
    /// offer-fail path, drops waiting as success, and fires `RequestCommitEvent`.
    #[tokio::test]
    async fn wal_replay_overload_requests_commit() {
        let (snapshot, _, _) = cache();
        let events = Arc::new(CollectingListener {
            events: StdMutex::new(Vec::new()),
        });
        snapshot.add_event_listener(events.clone() as Arc<dyn EventListener>);

        let hanging: Arc<dyn WriteAheadLog> = Arc::new(HangingWal { node_id: 42 });
        let start = RecordOffset {
            epoch: 1,
            offset: 0,
            size: 0,
        };
        let end = RecordOffset {
            epoch: 1,
            offset: 1,
            size: 0,
        };
        {
            let mut state = snapshot.shared.state.lock().await;
            for _ in 0..MAX_WAITING_LOAD_TASK_COUNT {
                let (tx, _rx) = oneshot::channel();
                state.wal.waiting.push_back(Arc::new(WalReplayTask {
                    timestamp: tokio::time::Instant::now(),
                    wal: Arc::clone(&hanging),
                    start,
                    end,
                    wal_records: None,
                    records: Mutex::new(Vec::new()),
                    load_done: AtomicBool::new(false),
                    replay_tx: Mutex::new(Some(tx)),
                }));
            }
        }
        snapshot
            .replay_wal(Arc::clone(&hanging), start, end, Some(Vec::new()))
            .await
            .unwrap();

        let seen = events.events.lock().unwrap().clone();
        assert!(
            seen.iter().any(|e| e.node_id == 42),
            "expected RequestCommitEvent(42), got {seen:?}"
        );
    }

    /// `ObjectReplay` loads via `ObjectReader` and puts blocks in index order.
    #[tokio::test]
    async fn replay_objects_puts_records() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let inputs: Vec<_> = (0..8).map(|i| record(3, i, &[i as u8; 8])).collect();
        let mut writer =
            ObjectWriter::open(7, storage.as_ref(), 32, 16 << 20, WriteOptions::default())
                .await
                .unwrap();
        writer.write(3, &inputs).await.unwrap();
        let size = writer.close().await.unwrap();
        let meta = S3ObjectMetadata {
            object_id: 7,
            object_type: s3stream_object::S3ObjectType::StreamSet,
            offset_ranges: vec![],
            object_size: size,
            attributes: s3stream_object::ObjectAttributes::new(0, false, false),
            committed_timestamp_ms: 0,
            data_timestamp_ms: 0,
        };
        // Rebuild cache against this storage so ObjectReader hits the written object.
        let manager = MemoryMetadataManager::new();
        let log = Arc::new(LogCache::new(
            1 << 30,
            1 << 20,
            DEFAULT_MAX_BLOCK_STREAM_COUNT,
        ));
        let snapshot = SnapshotReadCache::new(
            manager as Arc<dyn StreamManager>,
            Arc::clone(&log),
            storage as Arc<dyn ObjectStorage>,
            None,
        );
        tokio::time::timeout(Duration::from_secs(10), snapshot.replay_objects(vec![meta]))
            .await
            .expect("replay_objects timed out")
            .unwrap();
        let got = log.get(3, 0, 8, usize::MAX);
        assert_eq!(got.len(), 8);
    }

    #[tokio::test]
    async fn replay_objects_rejects_empty() {
        let (snapshot, _, _) = cache();
        let err = snapshot.replay_objects(vec![]).await.unwrap_err();
        assert!(matches!(err, StreamError::Unexpected(_)));
    }

    /// WAL whose `get` / `get_range` never complete, occupying a loading slot
    /// forever (for the inflight cap tests).
    struct HangingWal {
        node_id: u32,
    }

    #[async_trait::async_trait]
    impl WriteAheadLog for HangingWal {
        async fn start(&self) -> Result<(), s3stream_wal::WalError> {
            Ok(())
        }
        async fn shutdown_gracefully(&self) {}
        fn metadata(&self) -> s3stream_wal::WalMetadata {
            s3stream_wal::WalMetadata {
                node_id: self.node_id,
                epoch: 1,
            }
        }
        fn uri(&self) -> &str {
            "0@hang://"
        }
        fn submit(
            &self,
            _record: StreamRecordBatch,
        ) -> Result<s3stream_wal::PendingAppend, s3stream_wal::WalError> {
            Err(s3stream_wal::WalError::NotInitialized)
        }
        fn set_append_listener(&self, _listener: s3stream_wal::AppendListener) {}
        async fn get(
            &self,
            _offset: RecordOffset,
        ) -> Result<StreamRecordBatch, s3stream_wal::WalError> {
            std::future::pending().await
        }
        async fn get_range(
            &self,
            _start: RecordOffset,
            _end: RecordOffset,
        ) -> Result<Vec<StreamRecordBatch>, s3stream_wal::WalError> {
            std::future::pending().await
        }
        fn confirm_offset(&self) -> RecordOffset {
            RecordOffset {
                epoch: 1,
                offset: 0,
                size: 0,
            }
        }
        fn recover(&self) -> s3stream_wal::RecoverStream {
            Box::pin(futures::stream::empty())
        }
        async fn reset(&self) -> Result<(), s3stream_wal::WalError> {
            Ok(())
        }
        async fn trim(&self, _offset: RecordOffset) -> Result<(), s3stream_wal::WalError> {
            Ok(())
        }
    }
}
