//! S3Storage: the heart of the engine. Two-log append, stitched reads, upload
//! orchestration, recovery.
//!
//! Specification: `specification/upload-protocol.md`.
//!
//! Cache puts happen in WAL confirm order: the storage registers itself as
//! the WAL's `AppendListener`, invoked from the WAL's ordered completion
//! loop. Backoff records queue behind a 100ms drain timer.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, watch};

use s3stream_codec::StreamRecordBatch;
use s3stream_object::ObjectStorage;
use s3stream_wal::{RecordOffset, WalError, WriteAheadLog};

use crate::api::LinkRecordDecoder;
use crate::api::StreamError;
use crate::api::results::CacheAccessType;
use crate::cache::block_cache::S3BlockCache;
use crate::cache::log_cache::{LogCache, LogCacheBlock, MATCH_ALL_STREAMS};
use crate::cache::snapshot_read::SnapshotReadCache;
use crate::context::{AppendContext, FetchContext};
use crate::manager::{ObjectManager, StreamManager};
use crate::storage::confirm::{ConfirmWal, LazyCommit};
use crate::storage::recovery;
use crate::storage::upload::UploadWalTask;
use crate::storage::{ReadDataBlock, Storage};

pub use crate::failover::{LogStorageFailureHandler, StorageFailureHandler};

/// Configuration for the storage pipeline (subset of the facade `Config` it consumes).
#[derive(Debug, Clone)]
pub struct S3StorageConfig {
    pub wal_cache_size: u64,
    /// Seal threshold for LogCache blocks. Clamped to `<= 2/5 * wal_cache_size`
    /// append).
    pub wal_upload_threshold: u64,
    pub wal_upload_interval_ms: u64,
    /// Streams larger than this in a sealed block split into stream objects.
    pub stream_split_size: u64,
    pub max_stream_num_per_stream_set_object: usize,
    pub object_block_size: usize,
    pub object_part_size: usize,
    pub snapshot_read_enable: bool,
}

impl S3StorageConfig {
    /// Sensible defaults for tests (small thresholds so paths trigger quickly).
    pub fn test_defaults() -> Self {
        Self {
            wal_cache_size: 200 * 1024 * 1024,
            wal_upload_threshold: 16 * 1024 * 1024,
            wal_upload_interval_ms: 0,
            stream_split_size: 16 * 1024 * 1024,
            max_stream_num_per_stream_set_object: 10_000,
            object_block_size: 1024 * 1024,
            object_part_size: 16 * 1024 * 1024,
            snapshot_read_enable: false,
        }
    }
}

#[derive(Clone)]
struct Completion {
    tx: Arc<watch::Sender<Option<Result<(), String>>>>,
}

impl Completion {
    fn new() -> Self {
        let (tx, _) = watch::channel(None);
        Self { tx: Arc::new(tx) }
    }

    fn completed() -> Self {
        let completion = Self::new();
        completion.complete(Ok(()));
        completion
    }

    fn complete(&self, result: Result<(), String>) {
        self.tx.send_if_modified(|value| {
            if value.is_none() {
                *value = Some(result);
                true
            } else {
                false
            }
        });
    }

    async fn wait(&self) -> Result<(), StreamError> {
        let mut rx = self.tx.subscribe();
        let value = rx
            .wait_for(|value| value.is_some())
            .await
            .map_err(|_| StreamError::Unexpected("completion dropped".into()))?;
        value.clone().unwrap().map_err(StreamError::Unexpected)
    }
}

struct WalWriteRequest {
    record: StreamRecordBatch,
    ack: oneshot::Sender<Result<(), StreamError>>,
}

struct TaskContext {
    cache: Arc<LogCacheBlock>,
    task: tokio::sync::Mutex<UploadWalTask>,
    /// Shared with the task's limiter so `burst` needs no task lock.
    burst: Box<dyn Fn() + Send + Sync>,
    cf: Completion,
    trim_cf: Completion,
    upload_done: Completion,
    force: AtomicBool,
}

struct LazyCommitState {
    commit_cf: Completion,
    trim_cf: Completion,
}

struct StorageInner {
    config: S3StorageConfig,
    wal: Arc<dyn WriteAheadLog>,
    confirm_wal: Arc<ConfirmWal>,
    log_cache: Arc<LogCache>,
    snapshot_log_cache: Arc<LogCache>,
    snapshot_read_cache: SnapshotReadCache,
    block_cache: Arc<dyn S3BlockCache>,
    object_storage: Arc<dyn ObjectStorage>,
    object_manager: Arc<dyn ObjectManager>,
    stream_manager: Arc<dyn StreamManager>,
    failure_handler: Arc<dyn StorageFailureHandler>,

    cache_put_lock: Mutex<()>,
    backoff: Mutex<VecDeque<WalWriteRequest>>,
    inflight_tasks: Mutex<Vec<Arc<TaskContext>>>,
    lazy_upload_queue: Mutex<Vec<Arc<LazyCommitState>>>,
    prepare_tx: mpsc::UnboundedSender<Arc<TaskContext>>,
    commit_tx: mpsc::UnboundedSender<Arc<TaskContext>>,
    force_upload_scheduled: AtomicBool,
    need_force_upload: AtomicBool,
    force_ticker_deadline: Mutex<Instant>,
    delay_trim_ms: u64,
    delay_trim_queue: Mutex<VecDeque<(RecordOffset, Completion)>>,
    inflight_trims: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    max_data_write_rate: Mutex<f64>,
    shutdown: AtomicBool,
    background: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// The delta-WAL storage engine.
pub struct S3Storage {
    inner: Arc<StorageInner>,
}

impl S3Storage {
    /// Wire the pipeline (no I/O, `startup` recovers and opens for traffic).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: S3StorageConfig,
        wal: Arc<dyn WriteAheadLog>,
        block_cache: Arc<dyn S3BlockCache>,
        object_storage: Arc<dyn ObjectStorage>,
        object_manager: Arc<dyn ObjectManager>,
        stream_manager: Arc<dyn StreamManager>,
        failure_handler: Arc<dyn StorageFailureHandler>,
        link_record_decoder: Option<Arc<dyn LinkRecordDecoder>>,
    ) -> Self {
        let mut config = config;
        let mut delta_wal_cache_size = config.wal_cache_size;
        let snapshot_read_cache_size = if config.snapshot_read_enable {
            delta_wal_cache_size = (config.wal_cache_size / 3).max(10 * 1024 * 1024);
            (config.wal_cache_size / 3 * 2).max(10 * 1024 * 1024)
        } else {
            0
        };
        let clamped = (delta_wal_cache_size * 2 / 5).min(config.wal_upload_threshold);
        if clamped != config.wal_upload_threshold {
            tracing::info!(
                configured = config.wal_upload_threshold,
                adjusted = clamped,
                "walUploadThreshold too large, adjusted"
            );
            config.wal_upload_threshold = clamped;
        }
        let log_cache = Arc::new(LogCache::new(
            delta_wal_cache_size,
            config.wal_upload_threshold.max(1),
            config.max_stream_num_per_stream_set_object,
        ));
        let snapshot_log_cache = Arc::new(LogCache::new(
            snapshot_read_cache_size,
            (snapshot_read_cache_size / 6).max(1),
            crate::cache::log_cache::DEFAULT_MAX_BLOCK_STREAM_COUNT,
        ));
        let snapshot_read_cache = SnapshotReadCache::new(
            Arc::clone(&stream_manager),
            Arc::clone(&snapshot_log_cache),
            Arc::clone(&object_storage),
            link_record_decoder,
        );
        let delay_trim_ms = if config.snapshot_read_enable {
            30_000
        } else {
            0
        };

        let (prepare_tx, prepare_rx) = mpsc::unbounded_channel();
        let (commit_tx, commit_rx) = mpsc::unbounded_channel();

        let inner = Arc::new_cyclic(|weak: &Weak<StorageInner>| {
            let commit_weak = weak.clone();
            let confirm_wal = Arc::new(ConfirmWal::new(
                Arc::clone(&wal),
                Arc::new(move |lazy: LazyCommit| {
                    let weak = commit_weak.clone();
                    Box::pin(async move {
                        match weak.upgrade() {
                            Some(inner) => inner.lazy_upload(lazy).await,
                            None => Err(StreamError::Unexpected("storage dropped".into())),
                        }
                    })
                        as futures::future::BoxFuture<'static, Result<(), StreamError>>
                }),
            ));
            StorageInner {
                config,
                wal: Arc::clone(&wal),
                confirm_wal,
                log_cache,
                snapshot_log_cache,
                snapshot_read_cache,
                block_cache,
                object_storage,
                object_manager,
                stream_manager,
                failure_handler,
                cache_put_lock: Mutex::new(()),
                backoff: Mutex::new(VecDeque::new()),
                inflight_tasks: Mutex::new(Vec::new()),
                lazy_upload_queue: Mutex::new(Vec::new()),
                prepare_tx,
                commit_tx,
                force_upload_scheduled: AtomicBool::new(false),
                need_force_upload: AtomicBool::new(false),
                force_ticker_deadline: Mutex::new(Instant::now()),
                delay_trim_ms,
                delay_trim_queue: Mutex::new(VecDeque::new()),
                inflight_trims: Mutex::new(Vec::new()),
                max_data_write_rate: Mutex::new(0.0),
                shutdown: AtomicBool::new(false),
                background: Mutex::new(Vec::new()),
            }
        });

        // The in-order confirm hook: cache put + ConfirmWAL fan-out happen in WAL
        {
            let weak = Arc::downgrade(&inner);
            wal.set_append_listener(Arc::new(move |record, offset, next| {
                if let Some(inner) = weak.upgrade() {
                    inner.handle_append_confirm(record, offset, next);
                }
            }));
        }

        let mut handles = Vec::new();
        handles.push(tokio::spawn(prepare_worker(
            Arc::downgrade(&inner),
            prepare_rx,
        )));
        handles.push(tokio::spawn(commit_worker(
            Arc::downgrade(&inner),
            commit_rx,
        )));
        {
            let weak = Arc::downgrade(&inner);
            handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                loop {
                    interval.tick().await;
                    let Some(inner) = weak.upgrade() else { return };
                    if inner.shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    inner.try_drain_backoff_records();
                }
            }));
        }
        if inner.config.wal_upload_interval_ms > 0 {
            let weak = Arc::downgrade(&inner);
            let interval_ms = inner.config.wal_upload_interval_ms;
            handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
                interval.tick().await; // skip the immediate first tick
                loop {
                    interval.tick().await;
                    let Some(inner) = weak.upgrade() else { return };
                    if inner.shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    drop(inner.lazy_upload(LazyCommit {
                        lazy_linger_ms: interval_ms,
                        await_trim: false,
                    }));
                }
            }));
        }
        *inner.background.lock().expect("background poisoned") = handles;

        Self { inner }
    }

    /// The confirm-WAL side door (snapshot-read cache, hosts).
    pub fn confirm_wal(&self) -> Arc<ConfirmWal> {
        Arc::clone(&self.inner.confirm_wal)
    }

    /// Host handle for WAL/object replay into the snapshot-read cache. There
    /// is no process singleton. The host gets this handle from storage.
    pub fn snapshot_read_cache(&self) -> SnapshotReadCache {
        self.inner.snapshot_read_cache.clone()
    }

    /// Recovery, callable against a foreign WAL for failover.
    ///
    /// The invariant sequence:
    /// 1. `stream_manager.get_opening_streams()` -> committed endOffset map
    /// 2. iterate `wal.recover()`, keep only the continuous tail above each stream's
    ///    committed endOffset, grouped into bounded blocks (512 MiB)
    /// 3. upload each recovered block via a normal upload task
    /// 4. `wal.reset()`, then `close_stream` for every opening stream
    pub async fn recover(
        config: &S3StorageConfig,
        wal: &dyn WriteAheadLog,
        object_storage: &Arc<dyn ObjectStorage>,
        stream_manager: &dyn StreamManager,
        object_manager: &Arc<dyn ObjectManager>,
    ) -> Result<(), StreamError> {
        let streams = stream_manager.get_opening_streams().await?;
        let mut stream_end_offsets: HashMap<u64, u64> = streams
            .iter()
            .map(|s| (s.stream_id, s.end_offset))
            .collect();

        let iterator = wal.recover();
        recovery::recover(
            iterator,
            &mut stream_end_offsets,
            1 << 29,
            None,
            |block: Arc<LogCacheBlock>| {
                let object_storage = Arc::clone(object_storage);
                let object_manager = Arc::clone(object_manager);
                async move {
                    if block.size() == 0 {
                        return Ok(());
                    }
                    tracing::info!(bytes = block.size(), "recovering records from crash");
                    let records: BTreeMap<_, _> = block.records().into_iter().collect();
                    let mut task = UploadWalTask::plan(config, records, f64::INFINITY);
                    task.prepare(&*object_manager).await?;
                    task.upload(object_storage).await?;
                    task.commit(&*object_manager).await?;
                    Ok(())
                }
            },
        )
        .await?;

        let reset = wal.reset();
        for stream in &streams {
            let new_end = stream_end_offsets.get(&stream.stream_id).copied();
            tracing::info!(
                stream_id = stream.stream_id,
                ?new_end,
                "recover try close stream"
            );
            stream_manager
                .close_stream(stream.stream_id, stream.epoch)
                .await?;
        }
        reset.await?;
        Ok(())
    }

    async fn read0(
        &self,
        context: FetchContext,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<ReadDataBlock, StreamError> {
        let inner = &self.inner;
        let first_cache = if context.snapshot_read {
            &inner.snapshot_log_cache
        } else {
            &inner.log_cache
        };
        let log_cache_records = first_cache.get(stream_id, start_offset, end_offset, max_bytes);
        if !log_cache_records.is_empty() && log_cache_records[0].base_offset() <= start_offset {
            return Ok(ReadDataBlock {
                records: log_cache_records,
                cache_access: CacheAccessType::DeltaWalCacheHit,
            });
        }
        if context.fast_read {
            // Fast read fails fast when it would touch the block cache.
            return Err(StreamError::FastReadFailFast);
        }
        let cache_end_offset = log_cache_records
            .first()
            .map(|r| r.base_offset())
            .unwrap_or(end_offset);
        let block_result = inner
            .block_cache
            .read(stream_id, start_offset, cache_end_offset, max_bytes)
            .await?;
        let mut records = block_result.records;
        let mut remaining =
            max_bytes.saturating_sub(records.iter().map(|r| r.size()).sum::<usize>());
        for record in log_cache_records {
            if remaining == 0 {
                break;
            }
            remaining = remaining.saturating_sub(record.size());
            records.push(record);
        }
        continuous_check(&records)?;
        Ok(ReadDataBlock {
            records,
            cache_access: block_result.cache_access,
        })
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn startup(&self) -> Result<(), StreamError> {
        tracing::info!("S3Storage starting");
        self.inner.wal.start().await.map_err(StreamError::from)?;
        Self::recover(
            &self.inner.config,
            &*self.inner.wal,
            &self.inner.object_storage,
            &*self.inner.stream_manager,
            &self.inner.object_manager,
        )
        .await?;
        tracing::info!("S3Storage start completed");
        Ok(())
    }

    async fn shutdown(&self) {
        let inner = &self.inner;
        inner.shutdown.store(true, Ordering::Release);
        let drained: Vec<WalWriteRequest> = {
            let mut backoff = inner.backoff.lock().expect("backoff poisoned");
            backoff.drain(..).collect()
        };
        for request in drained {
            let _ = request
                .ack
                .send(Err(StreamError::Unexpected("S3Storage is shutdown".into())));
        }
        inner.delay_trim_close().await;
        inner.wal.shutdown_gracefully().await;
        for handle in inner
            .background
            .lock()
            .expect("background poisoned")
            .drain(..)
        {
            handle.abort();
        }
    }

    /// Protocol: admission check (LogCache capacity) -> WAL append -> on confirm the
    /// WAL listener puts into LogCache (in confirm order) and may seal + schedule an
    /// upload -> caller completes. OverCapacity from the WAL => backpressure + force
    /// upload.
    ///
    /// `S3Storage#append` enqueues on the calling thread and returns its
    /// future. So submit order is persistence order for pipelined callers.
    fn submit(
        &self,
        _context: AppendContext,
        record: StreamRecordBatch,
    ) -> futures::future::BoxFuture<'static, Result<(), StreamError>> {
        let start = std::time::Instant::now();
        let (ack, rx) = oneshot::channel();
        let request = WalWriteRequest { record, ack };
        self.inner.append0(request);
        Box::pin(async move {
            let result = rx
                .await
                .map_err(|_| StreamError::Unexpected("append dropped".into()))?;
            crate::metrics::record_operation_latency(
                crate::metrics::S3Operation::AppendStorage,
                start.elapsed().as_nanos() as i64,
            );
            result
        })
    }

    async fn read(
        &self,
        context: FetchContext,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<ReadDataBlock, StreamError> {
        let start = std::time::Instant::now();
        let result = self
            .read0(context, stream_id, start_offset, end_offset, max_bytes)
            .await;
        crate::metrics::record_operation_latency(
            crate::metrics::S3Operation::ReadStorage,
            start.elapsed().as_nanos() as i64,
        );
        result
    }

    /// 100ms grouped, then wait for every
    /// inflight task containing the stream.
    async fn force_upload(&self, stream_id: u64) -> Result<(), StreamError> {
        let inner = Arc::clone(&self.inner);
        inner.force_tick().await;
        inner.upload_delta_wal(stream_id, true);
        let waits: Vec<Completion> = {
            let tasks = inner.inflight_tasks.lock().expect("inflight poisoned");
            tasks
                .iter()
                .filter(|t| t.cache.contains_stream(stream_id))
                .map(|t| t.cf.clone())
                .collect()
        };
        for cf in waits {
            cf.wait().await?;
        }
        Ok(())
    }
}

impl StorageInner {
    fn try_acquire_permit(&self) -> bool {
        self.log_cache.size() < self.log_cache.capacity()
    }

    /// Admit a fresh append. Returns true when the request was parked in the
    /// backoff queue instead of reaching the WAL.
    fn append0(self: &Arc<Self>, request: WalWriteRequest) -> bool {
        if !self.backoff.lock().expect("backoff poisoned").is_empty() {
            // Queue behind earlier waiting appends to keep stream order.
            self.backoff
                .lock()
                .expect("backoff poisoned")
                .push_back(request);
            return true;
        }
        if !self.try_acquire_permit() {
            self.backoff
                .lock()
                .expect("backoff poisoned")
                .push_back(request);
            tracing::warn!(
                size = self.log_cache.size(),
                capacity = self.log_cache.capacity(),
                "[BACKOFF] log cache full"
            );
            return true;
        }
        // Place the record before returning, so callers that submitted in a
        // given order are in the log in that order. Deferring this into the
        // spawned task below would hand the ordering decision to the scheduler,
        // and the log cache rejects a batch that is not contiguous with the
        // previous one for its stream.
        match self.wal.submit(request.record.clone()) {
            Ok(pending) => {
                self.spawn_durable_ack(pending, request.ack);
                false
            }
            Err(WalError::OverCapacity { .. }) => {
                // WAL-full backpressure. Requeue and force upload.
                tracing::warn!("[BACKOFF] wal over capacity");
                self.maybe_force_upload();
                self.backoff
                    .lock()
                    .expect("backoff poisoned")
                    .push_back(request);
                true
            }
            Err(e) => {
                self.fail_request(request.ack, e);
                false
            }
        }
    }

    /// The 100 ms retry loop for backed-off appends.
    ///
    /// The head request stays queued until the WAL accepts it, like the Java
    /// `tryDrainBackoffRecords` peek-then-poll. Removing it first would drop
    /// the request on failed admission and let fresh appends overtake an
    /// empty-looking queue, leaving offset holes.
    fn try_drain_backoff_records(self: &Arc<Self>) {
        loop {
            let record = {
                let backoff = self.backoff.lock().expect("backoff poisoned");
                match backoff.front() {
                    Some(request) => request.record.clone(),
                    None => return,
                }
            };
            if !self.try_acquire_permit() {
                tracing::warn!("try drain backoff record fail, still backoff");
                return;
            }
            let outcome = self.wal.submit(record);
            if matches!(outcome, Err(WalError::OverCapacity { .. })) {
                tracing::warn!("try drain backoff record fail, still backoff");
                self.maybe_force_upload();
                return;
            }
            let request = self
                .backoff
                .lock()
                .expect("backoff poisoned")
                .pop_front()
                .expect("only the drain task removes backoff entries");
            match outcome {
                Ok(pending) => self.spawn_durable_ack(pending, request.ack),
                Err(WalError::OverCapacity { .. }) => unreachable!("handled above"),
                Err(e) => self.fail_request(request.ack, e),
            }
        }
    }

    /// Complete the append once the WAL reports durability.
    fn spawn_durable_ack(
        self: &Arc<Self>,
        pending: s3stream_wal::PendingAppend,
        ack: oneshot::Sender<Result<(), StreamError>>,
    ) {
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            match pending.durable.await {
                Ok(_result) => {
                    // Cache put already happened in the WAL's in-order confirm hook.
                    let _ = ack.send(Ok(()));
                }
                Err(e) => {
                    tracing::error!(error = %e, "append WAL fail");
                    let error = StreamError::from(e);
                    inner.failure_handler.handle(&error).await;
                    let _ = ack.send(Err(error));
                }
            }
        });
    }

    /// A hard WAL error, not backpressure. Report and fail the append.
    fn fail_request(self: &Arc<Self>, ack: oneshot::Sender<Result<(), StreamError>>, e: WalError) {
        tracing::error!(error = %e, "append WAL fail");
        let error = StreamError::from(e);
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            inner.failure_handler.handle(&error).await;
            let _ = ack.send(Err(error));
        });
    }

    /// The in-order confirm hook. Runs in WAL confirm order by construction.
    fn handle_append_confirm(
        self: &Arc<Self>,
        record: &StreamRecordBatch,
        offset: RecordOffset,
        next: RecordOffset,
    ) {
        self.confirm_wal.on_append(record, offset, next);
        let (archived, added) = {
            let _guard = self.cache_put_lock.lock().expect("cache put poisoned");
            let mut added = self.log_cache.put(record.clone());
            let mut archived = None;
            if !added {
                archived = self
                    .log_cache
                    .archive_current_block_if_contains(MATCH_ALL_STREAMS);
                added = self.log_cache.put(record.clone());
            }
            self.log_cache.set_last_record_offset(offset);
            (archived, added)
        };
        if let Some(block) = archived {
            self.submit_block_upload(block, false);
            self.notify_lazy_upload();
        }
        if !added {
            // Retry also failed (offset-span overflow): trigger another upload.
            self.upload_delta_wal(MATCH_ALL_STREAMS, false);
        }
    }

    fn maybe_force_upload(self: &Arc<Self>) {
        let has_inflight_force = self
            .inflight_tasks
            .lock()
            .expect("inflight poisoned")
            .iter()
            .any(|t| t.force.load(Ordering::Relaxed));
        if has_inflight_force {
            self.need_force_upload.store(true, Ordering::Release);
            return;
        }
        if self
            .force_upload_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.force_upload_all();
        } else {
            self.need_force_upload.store(true, Ordering::Release);
        }
    }

    fn force_upload_all(self: &Arc<Self>) {
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            inner.force_tick().await;
            let cf = inner.upload_delta_wal(MATCH_ALL_STREAMS, true);
            let _ = cf.wait().await;
            inner.force_upload_scheduled.store(false, Ordering::Release);
            if inner
                .need_force_upload
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                inner.force_upload_all();
            }
        });
    }

    /// 100ms batching window shared across callers.
    async fn force_tick(&self) {
        let deadline = {
            let mut deadline = self.force_ticker_deadline.lock().expect("ticker poisoned");
            let now = Instant::now();
            if *deadline <= now {
                *deadline = now + Duration::from_millis(100);
            }
            *deadline
        };
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    }

    fn lazy_upload(
        self: &Arc<Self>,
        lazy: LazyCommit,
    ) -> impl std::future::Future<Output = Result<(), StreamError>> + Send + 'static + use<> {
        let state = Arc::new(LazyCommitState {
            commit_cf: Completion::new(),
            trim_cf: Completion::new(),
        });
        self.lazy_upload_queue
            .lock()
            .expect("lazy poisoned")
            .push(Arc::clone(&state));
        let inner = Arc::clone(self);
        let queued = Arc::clone(&state);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(lazy.lazy_linger_ms)).await;
            let still_queued = inner
                .lazy_upload_queue
                .lock()
                .expect("lazy poisoned")
                .iter()
                .any(|s| Arc::ptr_eq(s, &queued));
            if still_queued {
                if lazy.lazy_linger_ms == 0 {
                    inner.force_upload_all();
                } else {
                    inner.upload_delta_wal(MATCH_ALL_STREAMS, false);
                }
            }
        });
        async move {
            if lazy.await_trim {
                state.trim_cf.wait().await
            } else {
                state.commit_cf.wait().await
            }
        }
    }

    fn upload_delta_wal(self: &Arc<Self>, stream_id: u64, force: bool) -> Completion {
        let block = {
            let _guard = self.cache_put_lock.lock().expect("cache put poisoned");
            self.log_cache.archive_current_block_if_contains(stream_id)
        };
        let cf = match block {
            Some(block) => self.submit_block_upload(block, force),
            None => Completion::completed(),
        };
        self.notify_lazy_upload();
        cf
    }

    /// Build the upload task (rate from write pressure), register it
    /// inflight, and feed the ordered prepare pipeline.
    fn submit_block_upload(self: &Arc<Self>, block: Arc<LogCacheBlock>, force: bool) -> Completion {
        let elapsed_ms = block.created().elapsed().as_millis() as u64;
        let rate = if force || elapsed_ms <= 100 {
            f64::INFINITY
        } else {
            let mut max_rate = self.max_data_write_rate.lock().expect("rate poisoned");
            let rate = block.size() as f64 * 1000.0 / (elapsed_ms.min(20_000) as f64);
            if rate > *max_rate {
                *max_rate = rate;
            }
            *max_rate
        };
        let records: BTreeMap<_, _> = block.records().into_iter().collect();
        let task = UploadWalTask::plan(&self.config, records, rate);
        let limiter_burst = {
            let task_burst: Arc<crate::storage::upload::AsyncRateLimiter> = task.limiter();
            Box::new(move || task_burst.burst()) as Box<dyn Fn() + Send + Sync>
        };
        let context = Arc::new(TaskContext {
            cache: block,
            task: tokio::sync::Mutex::new(task),
            burst: limiter_burst,
            cf: Completion::new(),
            trim_cf: Completion::new(),
            upload_done: Completion::new(),
            force: AtomicBool::new(force),
        });
        {
            let mut inflight = self.inflight_tasks.lock().expect("inflight poisoned");
            inflight.push(Arc::clone(&context));
            if force {
                for ctx in inflight.iter() {
                    ctx.force.store(true, Ordering::Relaxed);
                    (ctx.burst)();
                }
            }
        }
        let _ = self.prepare_tx.send(Arc::clone(&context));
        context.cf.clone()
    }

    fn notify_lazy_upload(self: &Arc<Self>) {
        let tasks: Vec<Arc<LazyCommitState>> = {
            let mut queue = self.lazy_upload_queue.lock().expect("lazy poisoned");
            queue.drain(..).collect()
        };
        if tasks.is_empty() {
            return;
        }
        let inflight: Vec<Arc<TaskContext>> = {
            let inflight = self.inflight_tasks.lock().expect("inflight poisoned");
            inflight.clone()
        };
        tokio::spawn(async move {
            let mut commit_result: Result<(), String> = Ok(());
            for ctx in &inflight {
                if let Err(e) = ctx.cf.wait().await {
                    commit_result = Err(e.to_string());
                    break;
                }
            }
            for task in &tasks {
                task.commit_cf.complete(commit_result.clone());
            }
            let mut trim_result: Result<(), String> = Ok(());
            for ctx in &inflight {
                if let Err(e) = ctx.trim_cf.wait().await {
                    trim_result = Err(e.to_string());
                    break;
                }
            }
            for task in &tasks {
                task.trim_cf.complete(trim_result.clone());
            }
        });
    }

    fn remove_inflight(&self, context: &Arc<TaskContext>) {
        let mut inflight = self.inflight_tasks.lock().expect("inflight poisoned");
        inflight.retain(|c| !Arc::ptr_eq(c, context));
    }

    fn delay_trim(self: &Arc<Self>, offset: RecordOffset, cf: Completion) {
        let handle = if self.delay_trim_ms == 0 {
            tracing::info!(?offset, "try trim WAL");
            let inner = Arc::clone(self);
            tokio::spawn(async move {
                let result = inner.wal.trim(offset).await.map_err(|e| e.to_string());
                cf.complete(result);
            })
        } else {
            self.delay_trim_queue
                .lock()
                .expect("trim queue poisoned")
                .push_back((offset, cf));
            let inner = Arc::clone(self);
            let delay = self.delay_trim_ms;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let entry = inner
                    .delay_trim_queue
                    .lock()
                    .expect("trim queue poisoned")
                    .pop_front();
                if let Some((offset, cf)) = entry {
                    tracing::info!(?offset, "try trim WAL");
                    let result = inner.wal.trim(offset).await.map_err(|e| e.to_string());
                    cf.complete(result);
                }
            })
        };
        let mut inflight = self.inflight_trims.lock().expect("trim tasks poisoned");
        inflight.retain(|h| !h.is_finished());
        inflight.push(handle);
    }

    /// Flush queued trims and await spawned ones, so no trim task survives
    /// shutdown to delete objects under a successor's recovery.
    async fn delay_trim_close(self: &Arc<Self>) {
        let pending: Vec<(RecordOffset, Completion)> = {
            let mut queue = self.delay_trim_queue.lock().expect("trim queue poisoned");
            queue.drain(..).collect()
        };
        for (offset, cf) in pending {
            let result = self.wal.trim(offset).await.map_err(|e| e.to_string());
            cf.complete(result);
        }
        let inflight: Vec<tokio::task::JoinHandle<()>> = {
            let mut tasks = self.inflight_trims.lock().expect("trim tasks poisoned");
            tasks.drain(..).collect()
        };
        for handle in inflight {
            let _ = handle.await;
        }
    }
}

async fn prepare_worker(
    weak: Weak<StorageInner>,
    mut rx: mpsc::UnboundedReceiver<Arc<TaskContext>>,
) {
    while let Some(context) = rx.recv().await {
        let Some(inner) = weak.upgrade() else { return };
        let prepare_result = {
            let mut task = context.task.lock().await;
            task.prepare(&*inner.object_manager).await
        };
        if let Err(e) = prepare_result {
            tracing::error!(error = %e, "unexpected exception when prepare stream set object");
            context.cf.complete(Err(e.to_string()));
            context.trim_cf.complete(Err("prepare failed".into()));
            inner.remove_inflight(&context);
            continue;
        }
        // Upload concurrently. Completion signaled via upload_done.
        {
            let context = Arc::clone(&context);
            let object_storage = Arc::clone(&inner.object_storage);
            tokio::spawn(async move {
                let result = {
                    let mut task = context.task.lock().await;
                    task.upload(object_storage).await
                };
                context
                    .upload_done
                    .complete(result.map_err(|e| e.to_string()));
            });
        }
        let _ = inner.commit_tx.send(context);
    }
}

async fn commit_worker(
    weak: Weak<StorageInner>,
    mut rx: mpsc::UnboundedReceiver<Arc<TaskContext>>,
) {
    let mut poisoned: Option<String> = None;
    while let Some(context) = rx.recv().await {
        let Some(inner) = weak.upgrade() else { return };
        if let Some(reason) = &poisoned {
            context.cf.complete(Err(reason.clone()));
            context.trim_cf.complete(Err(reason.clone()));
            inner.remove_inflight(&context);
            continue;
        }
        let result = async {
            context
                .upload_done
                .wait()
                .await
                .map_err(|e| e.to_string())?;
            let mut task = context.task.lock().await;
            task.commit(&*inner.object_manager)
                .await
                .map_err(|e| e.to_string())
        }
        .await;
        match result {
            Ok(()) => {
                if let Some(offset) = context.cache.last_record_offset() {
                    inner.delay_trim(offset, context.trim_cf.clone());
                } else {
                    context.trim_cf.complete(Ok(()));
                }
                // Transfer records ownership to the block cache era.
                inner.log_cache.mark_free(&context.cache);
                context.cf.complete(Ok(()));
                inner.remove_inflight(&context);
            }
            Err(reason) => {
                tracing::error!(%reason, "[FATAL] commit stream set object failed; poisoning commit pipeline");
                context.cf.complete(Err(reason.clone()));
                context.trim_cf.complete(Err(reason.clone()));
                inner.remove_inflight(&context);
                poisoned = Some(reason);
            }
        }
    }
}

/// Verify a stitched read result is offset-contiguous. A violation is a
/// data-loss bug. Fail loudly.
pub fn continuous_check(records: &[StreamRecordBatch]) -> Result<(), StreamError> {
    let mut expected: Option<u64> = None;
    for record in records {
        match expected {
            None => expected = Some(record.last_offset()),
            Some(e) if record.base_offset() == e => expected = Some(record.last_offset()),
            Some(e) => {
                return Err(StreamError::Unexpected(format!(
                    "continuous check failed, expected offset: {e}, actual: {}",
                    record.base_offset()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::block_cache::DirectBlockCache;
    use crate::memory::MemoryMetadataManager;
    use bytes::Bytes;
    use futures::StreamExt;
    use s3stream_object::MemoryObjectStorage;
    use s3stream_wal::memory::MemoryWriteAheadLog;

    fn record(stream_id: u64, base_offset: u64, count: i32, payload: &[u8]) -> StreamRecordBatch {
        StreamRecordBatch::new(
            stream_id,
            1,
            base_offset,
            count,
            Bytes::copy_from_slice(payload),
        )
    }

    struct Harness {
        storage: S3Storage,
        manager: Arc<MemoryMetadataManager>,
        wal: Arc<MemoryWriteAheadLog>,
    }

    fn harness(config: S3StorageConfig) -> Harness {
        let manager = MemoryMetadataManager::new();
        let object_storage = Arc::new(MemoryObjectStorage::new(0));
        let wal = Arc::new(MemoryWriteAheadLog::new(1, 1));
        let block_cache = Arc::new(DirectBlockCache::new(
            manager.clone() as Arc<dyn ObjectManager>,
            object_storage.clone() as Arc<dyn ObjectStorage>,
        ));
        let storage = S3Storage::new(
            config,
            wal.clone() as Arc<dyn WriteAheadLog>,
            block_cache,
            object_storage.clone() as Arc<dyn ObjectStorage>,
            manager.clone() as Arc<dyn ObjectManager>,
            manager.clone() as Arc<dyn StreamManager>,
            Arc::new(LogStorageFailureHandler),
            None,
        );
        Harness {
            storage,
            manager,
            wal,
        }
    }

    async fn open_stream(manager: &MemoryMetadataManager) -> u64 {
        let stream_id = manager.create_stream(HashMap::new()).await.unwrap();
        manager
            .open_stream(stream_id, 1, HashMap::new())
            .await
            .unwrap();
        stream_id
    }

    async fn assert_wal_drained(wal: &dyn WriteAheadLog) {
        for _ in 0..200 {
            if wal.recover().collect::<Vec<_>>().await.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("WAL not trimmed empty within 2s");
    }

    /// End-to-end: append -> read (LogCache hit) -> force upload -> read (block path).
    #[tokio::test]
    async fn append_read_upload_read() {
        let h = harness(S3StorageConfig::test_defaults());
        h.storage.startup().await.unwrap();
        let s1 = open_stream(&h.manager).await;
        let s2 = open_stream(&h.manager).await;

        for i in 0..10u64 {
            h.storage
                .append(AppendContext::default(), record(s1, i, 1, &[i as u8; 64]))
                .await
                .unwrap();
            h.storage
                .append(
                    AppendContext::default(),
                    record(s2, i * 2, 2, &[i as u8; 32]),
                )
                .await
                .unwrap();
        }

        // LogCache hit.
        let read = h
            .storage
            .read(FetchContext::default(), s1, 0, 10, usize::MAX)
            .await
            .unwrap();
        assert_eq!(read.cache_access, CacheAccessType::DeltaWalCacheHit);
        assert_eq!(read.records.len(), 10);

        // Force upload, then the data must come from committed objects.
        h.storage.force_upload(MATCH_ALL_STREAMS).await.unwrap();
        // WAL trimmed to the confirm offset (asynchronously after commit).
        assert_wal_drained(&*h.wal).await;

        let read = h
            .storage
            .read(FetchContext::default(), s2, 0, 20, usize::MAX)
            .await
            .unwrap();
        assert_eq!(read.records.len(), 10);
        continuous_check(&read.records).unwrap();
        assert_eq!(read.records[0].base_offset(), 0);
        assert_eq!(read.records.last().unwrap().last_offset(), 20);
        // Committed end offsets advanced in the metadata plane.
        assert_eq!(
            h.manager.get_streams(&[s2]).await.unwrap()[0].end_offset,
            20
        );
        h.storage.shutdown().await;
    }

    /// Backpressure regression: appends parked in the backoff queue must all
    /// land. The drain used to pop before admission and destroy the request
    /// on failure, leaving offset holes that poisoned the commit pipeline.
    #[tokio::test]
    async fn backoff_appends_are_never_dropped() {
        let mut config = S3StorageConfig::test_defaults();
        // A cache small enough that the pipelined appends below overrun it.
        config.wal_cache_size = 32 * 1024;
        config.wal_upload_threshold = 8 * 1024;
        let h = harness(config);
        h.storage.startup().await.unwrap();
        let stream = open_stream(&h.manager).await;

        // Enqueue ~200 KiB against a 32 KiB cache. Submit enqueues on the
        // calling thread so offsets are deterministic.
        let pending: Vec<_> = (0..100u64)
            .map(|i| {
                h.storage.submit(
                    AppendContext::default(),
                    record(stream, i, 1, &[i as u8; 2048]),
                )
            })
            .collect();
        for (i, result) in futures::future::join_all(pending)
            .await
            .into_iter()
            .enumerate()
        {
            result.unwrap_or_else(|e| panic!("append {i} dropped: {e}"));
        }

        // Every record committed, no offset holes.
        h.storage.force_upload(MATCH_ALL_STREAMS).await.unwrap();
        assert_eq!(
            h.manager.get_streams(&[stream]).await.unwrap()[0].end_offset,
            100
        );
        h.storage.shutdown().await;
    }

    /// Recovery filters records below the committed end offset and uploads the rest.
    /// Opening streams are closed and the WAL is reset.
    #[tokio::test]
    async fn recovery_filters_committed_and_uploads_tail() {
        let h = harness(S3StorageConfig::test_defaults());
        // Simulate a crashed node: records in the WAL, none uploaded.
        h.wal.start().await.unwrap();
        let stream_id = open_stream(&h.manager).await;
        for i in 0..6u64 {
            h.wal
                .append(record(stream_id, i, 1, &[9u8; 16]))
                .await
                .unwrap();
        }
        // Metadata says offsets < 3 are committed.
        {
            let mut request = crate::manager::CommitStreamSetObjectRequest {
                object_id: s3stream_object::NOOP_OBJECT_ID,
                ..Default::default()
            };
            request.stream_objects.push(crate::manager::StreamObject {
                object_id: 999,
                object_size: 1,
                stream_id,
                start_offset: 0,
                end_offset: 3,
                attributes: 0,
            });
            h.manager.commit_stream_set_object(request).await.unwrap();
        }

        h.storage.startup().await.unwrap();

        // Stream closed by recovery, end offset advanced to 6.
        let stream = &h.manager.get_streams(&[stream_id]).await.unwrap()[0];
        assert_eq!(stream.end_offset, 6);
        assert!(h.manager.get_opening_streams().await.unwrap().is_empty());
        // WAL empty after reset.
        assert_eq!(h.wal.recover().collect::<Vec<_>>().await.len(), 0);

        // The recovered tail [3,6) is readable from committed objects.
        h.manager
            .open_stream(stream_id, 2, HashMap::new())
            .await
            .unwrap();
        let read = h
            .storage
            .read(FetchContext::default(), stream_id, 3, 6, usize::MAX)
            .await
            .unwrap();
        assert_eq!(read.records.len(), 3);
        assert_eq!(read.records[0].base_offset(), 3);
        h.storage.shutdown().await;
    }

    #[tokio::test]
    async fn fast_read_fails_fast_on_cache_miss() {
        let config = S3StorageConfig {
            wal_cache_size: 10, // anything cached is over 90% => real free on next put
            ..S3StorageConfig::test_defaults()
        };
        let h = harness(config);
        h.storage.startup().await.unwrap();
        let stream_id = open_stream(&h.manager).await;
        h.storage
            .append(
                AppendContext::default(),
                record(stream_id, 0, 1, &[1u8; 16]),
            )
            .await
            .unwrap();
        h.storage.force_upload(stream_id).await.unwrap();
        // A second confirmed append triggers try_real_free, releasing the freed block.
        h.storage
            .append(
                AppendContext::default(),
                record(stream_id, 1, 1, &[2u8; 16]),
            )
            .await
            .unwrap();

        let context = FetchContext {
            fast_read: true,
            ..Default::default()
        };
        let result = h.storage.read(context, stream_id, 0, 1, usize::MAX).await;
        assert!(matches!(result, Err(StreamError::FastReadFailFast)));
        h.storage.shutdown().await;
    }

    /// Full LogCache blocks archive and upload automatically once the seal threshold
    /// is crossed. Reads stitch block-cache head + LogCache tail.
    #[tokio::test]
    async fn seal_threshold_triggers_upload_and_stitched_read() {
        let config = S3StorageConfig {
            // Tiny threshold: every ~1KiB seals a block.
            wal_upload_threshold: 1024,
            ..S3StorageConfig::test_defaults()
        };
        let h = harness(config);
        h.storage.startup().await.unwrap();
        let stream_id = open_stream(&h.manager).await;
        for i in 0..64u64 {
            h.storage
                .append(
                    AppendContext::default(),
                    record(stream_id, i, 1, &[i as u8; 128]),
                )
                .await
                .unwrap();
        }
        // Wait for background uploads to commit.
        h.storage.force_upload(MATCH_ALL_STREAMS).await.unwrap();

        let read = h
            .storage
            .read(FetchContext::default(), stream_id, 0, 64, usize::MAX)
            .await
            .unwrap();
        assert_eq!(read.records.len(), 64);
        continuous_check(&read.records).unwrap();
        h.storage.shutdown().await;
    }

    /// End-to-end against the REAL object WAL (batching, pipelined PUTs, ordered
    /// confirm listener, trim of WAL objects) instead of the memory WAL.
    #[tokio::test]
    async fn append_upload_read_with_object_wal() {
        use s3stream_wal::object::{ObjectWalConfig, ObjectWalService};

        let manager = MemoryMetadataManager::new();
        let object_storage = Arc::new(MemoryObjectStorage::new(0));
        let wal_config = ObjectWalConfig {
            cluster_id: "cluster-t".into(),
            node_id: 7,
            epoch: 1,
            batch_interval: Duration::from_millis(5),
            ..ObjectWalConfig::defaults()
        };
        let wal = Arc::new(ObjectWalService::new(
            object_storage.clone() as Arc<dyn ObjectStorage>,
            wal_config,
        ));
        // Full production read path: StreamReaders (readahead + DataBlockCache LRU),
        // not the test-only DirectBlockCache.
        let block_cache = crate::cache::blockcache::StreamReaders::new(
            64 << 20,
            manager.clone() as Arc<dyn ObjectManager>,
            object_storage.clone() as Arc<dyn ObjectStorage>,
            2,
        );
        let storage = S3Storage::new(
            S3StorageConfig::test_defaults(),
            wal.clone() as Arc<dyn WriteAheadLog>,
            block_cache,
            object_storage.clone() as Arc<dyn ObjectStorage>,
            manager.clone() as Arc<dyn ObjectManager>,
            manager.clone() as Arc<dyn StreamManager>,
            Arc::new(LogStorageFailureHandler),
            None,
        );
        storage.startup().await.unwrap();
        let stream_id = open_stream(&manager).await;

        // Concurrent appends across the batch boundary.
        let mut handles = Vec::new();
        for i in 0..20u64 {
            let storage_record = record(stream_id, i, 1, &[i as u8; 100]);
            handles.push(storage.append(AppendContext::default(), storage_record));
        }
        for result in futures::future::join_all(handles).await {
            result.unwrap();
        }

        let read = storage
            .read(FetchContext::default(), stream_id, 0, 20, usize::MAX)
            .await
            .unwrap();
        assert_eq!(read.records.len(), 20);
        continuous_check(&read.records).unwrap();

        storage.force_upload(MATCH_ALL_STREAMS).await.unwrap();
        // Everything committed + WAL trimmed: recovery is empty.
        assert_wal_drained(&*wal).await;
        // Read now stitches from committed objects.
        let read = storage
            .read(FetchContext::default(), stream_id, 0, 20, usize::MAX)
            .await
            .unwrap();
        assert_eq!(read.records.len(), 20);
        continuous_check(&read.records).unwrap();
        storage.shutdown().await;
    }

    /// ConfirmWAL lazy commit resolves once the triggered upload commits (and trims).
    #[tokio::test]
    async fn lazy_commit_resolves_after_upload() {
        let h = harness(S3StorageConfig::test_defaults());
        h.storage.startup().await.unwrap();
        let stream_id = open_stream(&h.manager).await;
        h.storage
            .append(
                AppendContext::default(),
                record(stream_id, 0, 1, &[1u8; 16]),
            )
            .await
            .unwrap();
        let confirm_wal = h.storage.confirm_wal();
        confirm_wal.commit(0, true).await.unwrap();
        assert_wal_drained(&*h.wal).await;
        h.storage.shutdown().await;
    }

    #[tokio::test]
    async fn snapshot_read_splits_cache_budget() {
        let config = S3StorageConfig {
            wal_cache_size: 90 * 1024 * 1024,
            snapshot_read_enable: true,
            ..S3StorageConfig::test_defaults()
        };
        let h = harness(config);
        assert_eq!(h.storage.inner.log_cache.capacity(), 30 * 1024 * 1024);
        assert_eq!(
            h.storage.inner.snapshot_log_cache.capacity(),
            60 * 1024 * 1024
        );
        h.storage.shutdown().await;
    }

    #[tokio::test]
    async fn snapshot_read_fetch_hits_snapshot_cache() {
        let config = S3StorageConfig {
            snapshot_read_enable: true,
            ..S3StorageConfig::test_defaults()
        };
        let h = harness(config);
        h.storage.startup().await.unwrap();
        let cache = h.storage.snapshot_read_cache();
        cache
            .put(vec![record(1, 0, 1, &[1u8; 8]), record(1, 1, 1, &[2u8; 8])])
            .await;
        let read = h
            .storage
            .read(
                FetchContext {
                    snapshot_read: true,
                    ..Default::default()
                },
                1,
                0,
                2,
                usize::MAX,
            )
            .await
            .unwrap();
        assert_eq!(read.cache_access, CacheAccessType::DeltaWalCacheHit);
        assert_eq!(read.records.len(), 2);
        h.storage.shutdown().await;
    }
}
