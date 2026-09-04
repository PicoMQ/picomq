//! S3Stream: the per-stream façade over Storage.
//!
//! Responsibilities (all thin over `Storage`, but each carries contract details):
//! - Offset bookkeeping: `next_offset` assigned at append admission.`confirm_offset`
//! - State guards: append/fetch fail `Closed` once closed. Append marks the stream
//!   fenced when storage rejects with `Fenced`. Fetch is bounds-checked against
//!   `[start_offset, confirm_offset)` => `OffsetOutOfRange`.
//! - `RecordBatch` -> `StreamRecordBatch` encoding happens here (once).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;

use s3stream_codec::StreamRecordBatch;

use crate::api::results::CacheAccessType;
use crate::api::{
    AppendResult, FetchResult, PendingAppend, RecordBatch, RecordBatchWithContext, Stream,
    StreamError,
};
use crate::context::{AppendContext, FetchContext};
use crate::manager::{
    StreamManager, StreamMetadata, StreamMetadataListener, StreamMetadataListenerHandle,
};
use crate::storage::Storage;

const CLOSED_MARK: u32 = 1;
const FENCED_MARK: u32 = 1 << 1;
const DESTROY_MARK: u32 = 1 << 2;

const CLOSE_PENDING_TIMEOUT: Duration = Duration::from_secs(10);

/// `-1` in two's complement, i.e. `u64::MAX`. Fetch bounds compare as `i64`,
/// so this fake offset sorts below every real offset.
pub(crate) const SNAPSHOT_FAKE_OFFSET: u64 = u64::MAX;

/// `(base_offset, record_count, durability)` from a synchronous append
/// admission ([`S3Stream::admit_append`]).
type AdmittedAppend = (
    u64,
    u32,
    futures::future::BoxFuture<'static, Result<(), StreamError>>,
);

/// Counts in-flight requests and lets waiters await the count reaching zero.
struct InflightTracker {
    count: AtomicU64,
    idle: Notify,
}

impl InflightTracker {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            idle: Notify::new(),
        }
    }

    fn begin(self: &Arc<Self>) -> InflightGuard {
        self.count.fetch_add(1, Ordering::AcqRel);
        InflightGuard {
            tracker: Arc::clone(self),
        }
    }

    async fn wait_idle(&self) {
        loop {
            // Subscribe before checking to avoid a missed wakeup.
            let notified = self.idle.notified();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct InflightGuard {
    tracker: Arc<InflightTracker>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if self.tracker.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tracker.idle.notify_waiters();
        }
    }
}

pub struct S3Stream {
    stream_id: u64,
    epoch: u64,
    start_offset: AtomicU64,
    next_offset: AtomicU64,
    confirm_offset: AtomicU64,
    status: AtomicU32,
    storage: Arc<dyn Storage>,
    stream_manager: Arc<dyn StreamManager>,
    pending_appends: Arc<InflightTracker>,
    pending_fetches: Arc<InflightTracker>,
    trim_lock: tokio::sync::Mutex<()>,
    close_result: tokio::sync::Mutex<Option<Result<(), String>>>,
    snapshot_read: bool,
    listener_handle: std::sync::Mutex<Option<Arc<dyn StreamMetadataListenerHandle>>>,
    snapshot_confirm_lock: std::sync::Mutex<()>,
    append_lock: std::sync::Mutex<()>,
}

impl S3Stream {
    pub fn new(
        stream_id: u64,
        epoch: u64,
        start_offset: u64,
        next_offset: u64,
        storage: Arc<dyn Storage>,
        stream_manager: Arc<dyn StreamManager>,
        snapshot_read: bool,
    ) -> Self {
        Self {
            stream_id,
            epoch,
            start_offset: AtomicU64::new(start_offset),
            next_offset: AtomicU64::new(next_offset),
            confirm_offset: AtomicU64::new(next_offset),
            status: AtomicU32::new(0),
            storage,
            stream_manager,
            pending_appends: Arc::new(InflightTracker::new()),
            pending_fetches: Arc::new(InflightTracker::new()),
            trim_lock: tokio::sync::Mutex::new(()),
            close_result: tokio::sync::Mutex::new(None),
            snapshot_read,
            listener_handle: std::sync::Mutex::new(None),
            snapshot_confirm_lock: std::sync::Mutex::new(()),
            append_lock: std::sync::Mutex::new(()),
        }
    }

    pub fn attach_metadata_listener(self: &Arc<Self>) {
        if !self.snapshot_read {
            return;
        }
        let handle = self.stream_manager.add_metadata_listener(
            self.stream_id,
            Arc::clone(self) as Arc<dyn StreamMetadataListener>,
        );
        *self
            .listener_handle
            .lock()
            .expect("listener handle poisoned") = Some(handle);
    }

    pub fn snapshot_read(&self) -> bool {
        self.snapshot_read
    }

    pub fn is_closed(&self) -> bool {
        self.status.load(Ordering::Acquire) & CLOSED_MARK != 0
    }

    fn is_operable(&self) -> bool {
        self.status.load(Ordering::Acquire) == 0
    }

    fn mark(&self, mask: u32) {
        self.status.fetch_or(mask, Ordering::AcqRel);
    }

    fn update_confirm_offset(&self, new_offset: u64) {
        self.confirm_offset.fetch_max(new_offset, Ordering::AcqRel);
    }

    fn update_snapshot_read_confirm_offset(&self, new_offset: u64) {
        let _guard = self
            .snapshot_confirm_lock
            .lock()
            .expect("snapshot confirm poisoned");
        let current = self.confirm_offset.load(Ordering::Acquire);
        if (new_offset as i64) > (current as i64) {
            self.confirm_offset.store(new_offset, Ordering::Release);
        }
        self.next_offset.store(
            self.confirm_offset.load(Ordering::Acquire),
            Ordering::Release,
        );
    }

    /// Admission + reservation + synchronous WAL submit: the shared prefix of
    /// [`Self::append0`] and [`Stream::submit_append`]. After this returns,
    /// the offset range is reserved and the record is in the storage pipeline
    /// in call order.
    fn admit_append(
        &self,
        context: AppendContext,
        batch: RecordBatch,
    ) -> Result<AdmittedAppend, StreamError> {
        if !self.is_operable() {
            return Err(StreamError::Closed {
                stream_id: self.stream_id,
            });
        }
        let count = batch.count;
        // Nothing inside is awaited (`submit` only enqueues), so this never
        // holds across a suspension point and cannot serialize durability.
        let guard = self.append_lock.lock().expect("append lock poisoned");
        let offset = self.next_offset.fetch_add(count as u64, Ordering::AcqRel);
        let record = StreamRecordBatch::new(
            self.stream_id,
            self.epoch,
            offset,
            count as i32,
            batch.payload,
        );
        let durable = self.storage.submit(context, record);
        drop(guard);
        Ok((offset, count, durable))
    }

    /// Completion bookkeeping: confirm-offset advance on success, fencing on
    /// failure.
    fn complete_append(
        &self,
        offset: u64,
        count: u32,
        result: Result<(), StreamError>,
    ) -> Result<AppendResult, StreamError> {
        match result {
            Ok(()) => {
                self.update_confirm_offset(offset + count as u64);
                Ok(AppendResult {
                    base_offset: offset,
                })
            }
            Err(e) => {
                // WAL retries appends internally. An error surfacing here means
                // the stream is fenced (newer epoch) or the WAL is closed. Stop
                // accepting writes either way.
                self.mark(FENCED_MARK);
                if matches!(e, StreamError::Fenced { .. }) {
                    tracing::info!(
                        stream_id = self.stream_id,
                        epoch = self.epoch,
                        "append fenced"
                    );
                } else {
                    tracing::warn!(stream_id = self.stream_id, error = %e, "stream append fail");
                }
                Err(e)
            }
        }
    }

    async fn append0(
        &self,
        context: AppendContext,
        batch: RecordBatch,
    ) -> Result<AppendResult, StreamError> {
        let (offset, count, durable) = self.admit_append(context, batch)?;
        let result = durable.await;
        self.complete_append(offset, count, result)
    }

    async fn fetch0(
        &self,
        context: FetchContext,
        start_offset: u64,
        end_offset: u64,
        max_bytes_hint: usize,
    ) -> Result<FetchResult, StreamError> {
        if !self.is_operable() {
            return Err(StreamError::Closed {
                stream_id: self.stream_id,
            });
        }
        let confirm_offset = self.confirm_offset.load(Ordering::Acquire);
        let stream_start = self.start_offset.load(Ordering::Acquire);
        if (start_offset as i64) < (stream_start as i64)
            || (end_offset as i64) > (confirm_offset as i64)
        {
            return Err(StreamError::OffsetOutOfRange {
                stream_id: self.stream_id,
                start: start_offset,
                end: end_offset,
                valid_start: stream_start,
                valid_end: confirm_offset,
            });
        }
        if start_offset > end_offset {
            return Err(StreamError::Unexpected(format!(
                "fetch startOffset {start_offset} is greater than endOffset {end_offset}"
            )));
        }
        if start_offset == end_offset {
            return Ok(FetchResult {
                records: Vec::new(),
                cache_access: CacheAccessType::DeltaWalCacheHit,
            });
        }
        let block = self
            .storage
            .read(
                context,
                self.stream_id,
                start_offset,
                end_offset,
                max_bytes_hint,
            )
            .await?;
        let records = block
            .records
            .iter()
            .map(|r| RecordBatchWithContext {
                stream_id: self.stream_id,
                base_offset: r.base_offset(),
                last_offset: r.last_offset(),
                count: r.count() as u32,
                properties: std::collections::HashMap::new(),
                payload: r.payload(),
            })
            .collect();
        Ok(FetchResult {
            records,
            cache_access: block.cache_access,
        })
    }

    async fn close0(&self) -> Result<(), StreamError> {
        self.storage.force_upload(self.stream_id).await?;
        self.stream_manager
            .close_stream(self.stream_id, self.epoch)
            .await
    }

    /// `force=false`: drain pending appends, 10s fetch timeout, then upload + close.
    /// `force=true`: mark closed and skip waits, still `force_upload` + `close_stream`.
    /// is no per-request future here. CLOSED makes new ops fail. Inflight appends may
    /// still complete. Protocol (upload then `close_stream`) is unchanged.
    pub async fn close_with(&self, force: bool) -> Result<(), StreamError> {
        // SNAPSHOT_READ: unregister listener only.
        // No markClosed, no forceUpload, no closeStream.
        if self.snapshot_read {
            if let Some(handle) = self
                .listener_handle
                .lock()
                .expect("listener handle poisoned")
                .take()
            {
                handle.close();
            }
            return Ok(());
        }
        self.mark(CLOSED_MARK);
        if !force {
            self.pending_appends.wait_idle().await;
            let _ =
                tokio::time::timeout(CLOSE_PENDING_TIMEOUT, self.pending_fetches.wait_idle()).await;
        }
        let mut done = self.close_result.lock().await;
        if let Some(result) = &*done {
            return result.clone().map_err(StreamError::Unexpected);
        }
        let result = self.close0().await;
        *done = Some(result.as_ref().map(|_| ()).map_err(|e| e.to_string()));
        match &result {
            Ok(()) => tracing::info!(stream_id = self.stream_id, epoch = self.epoch, "closed"),
            Err(e) => tracing::error!(stream_id = self.stream_id, error = %e, "close fail"),
        }
        result
    }
}

#[async_trait]
impl Stream for S3Stream {
    fn stream_id(&self) -> u64 {
        self.stream_id
    }

    fn stream_epoch(&self) -> u64 {
        self.epoch
    }

    fn start_offset(&self) -> u64 {
        self.start_offset.load(Ordering::Acquire)
    }

    fn confirm_offset(&self) -> u64 {
        self.confirm_offset.load(Ordering::Acquire)
    }

    fn confirm_offset_set(&self, offset: u64) -> Result<(), StreamError> {
        if !self.snapshot_read {
            return Err(StreamError::Unexpected(
                "Only snapshot-read mode support set confirmOffset".into(),
            ));
        }
        self.update_snapshot_read_confirm_offset(offset);
        Ok(())
    }

    fn next_offset(&self) -> u64 {
        self.next_offset.load(Ordering::Acquire)
    }

    async fn append(
        &self,
        context: AppendContext,
        batch: RecordBatch,
    ) -> Result<AppendResult, StreamError> {
        // SNAPSHOT_READ → `IllegalStateException`.
        if self.snapshot_read {
            return Err(StreamError::Unexpected(
                "Append operation is not support for readonly stream".into(),
            ));
        }
        let start = std::time::Instant::now();
        let _guard = self.pending_appends.begin();
        let result = self.append0(context, batch).await;
        crate::metrics::record_operation_latency(
            crate::metrics::S3Operation::AppendStream,
            start.elapsed().as_nanos() as i64,
        );
        result
    }

    fn submit_append(
        self: Arc<Self>,
        context: AppendContext,
        batch: RecordBatch,
    ) -> Result<PendingAppend, StreamError> {
        // SNAPSHOT_READ → `IllegalStateException`.
        if self.snapshot_read {
            return Err(StreamError::Unexpected(
                "Append operation is not support for readonly stream".into(),
            ));
        }
        let start = std::time::Instant::now();
        let guard = self.pending_appends.begin();
        let (offset, count, durable) = self.admit_append(context, batch)?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let this = Arc::clone(&self);
        tokio::spawn(async move {
            let result = durable.await;
            let outcome = this.complete_append(offset, count, result);
            crate::metrics::record_operation_latency(
                crate::metrics::S3Operation::AppendStream,
                start.elapsed().as_nanos() as i64,
            );
            drop(guard);
            // Receiver may be gone (caller cancelled). Bookkeeping above
            // already ran, which is the part that must not be lost.
            let _ = tx.send(outcome);
        });
        Ok(PendingAppend::new(offset, rx))
    }

    async fn fetch(
        &self,
        context: FetchContext,
        start_offset: u64,
        end_offset: u64,
        max_bytes_hint: usize,
    ) -> Result<FetchResult, StreamError> {
        let mut context = context;
        if self.snapshot_read {
            context.snapshot_read = true;
        }
        let start = std::time::Instant::now();
        let _guard = self.pending_fetches.begin();
        let result = self
            .fetch0(context, start_offset, end_offset, max_bytes_hint)
            .await;
        crate::metrics::record_operation_latency(
            crate::metrics::S3Operation::FetchStream,
            start.elapsed().as_nanos() as i64,
        );
        result
    }

    async fn trim(&self, new_start_offset: u64) -> Result<(), StreamError> {
        // SNAPSHOT_READ → `IllegalStateException`.
        if self.snapshot_read {
            return Err(StreamError::Unexpected(
                "Trim operation is not support for readonly stream".into(),
            ));
        }
        let _serial = self.trim_lock.lock().await;
        let current = self.start_offset.load(Ordering::Acquire);
        if new_start_offset < current {
            tracing::warn!(
                stream_id = self.stream_id,
                new_start_offset,
                current,
                "trim newStartOffset less than current start offset"
            );
            return Ok(());
        }
        let start = std::time::Instant::now();
        self.start_offset.store(new_start_offset, Ordering::Release);
        self.pending_fetches.wait_idle().await;
        let result = self
            .stream_manager
            .trim_stream(self.stream_id, self.epoch, new_start_offset)
            .await;
        crate::metrics::record_operation_latency(
            crate::metrics::S3Operation::TrimStream,
            start.elapsed().as_nanos() as i64,
        );
        result
    }

    async fn close(&self) -> Result<(), StreamError> {
        self.close_with(false).await
    }

    async fn destroy(&self) -> Result<(), StreamError> {
        // SNAPSHOT_READ → `IllegalStateException`.
        if self.snapshot_read {
            return Err(StreamError::Unexpected(
                "Destroy operation is not support for readonly stream".into(),
            ));
        }
        self.close().await?;
        self.mark(DESTROY_MARK);
        self.start_offset.store(
            self.confirm_offset.load(Ordering::Acquire),
            Ordering::Release,
        );
        self.stream_manager
            .delete_stream(self.stream_id, self.epoch)
            .await
    }
}

impl StreamMetadataListener for S3Stream {
    fn on_new_stream_metadata(&self, metadata: StreamMetadata) {
        self.update_snapshot_read_confirm_offset(metadata.end_offset);
        self.start_offset
            .store(metadata.start_offset, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use bytes::Bytes;

    use super::*;
    use crate::storage::ReadDataBlock;

    /// Storage stub: records appends, serves reads back from memory, and can be told
    /// to fail appends with a given error.
    struct StubStorage {
        records: Mutex<Vec<StreamRecordBatch>>,
        fail_append: Mutex<Option<StreamError>>,
    }

    impl StubStorage {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                records: Mutex::new(Vec::new()),
                fail_append: Mutex::new(None),
            })
        }
    }

    #[async_trait]
    impl Storage for StubStorage {
        async fn startup(&self) -> Result<(), StreamError> {
            Ok(())
        }

        async fn shutdown(&self) {}

        fn submit(
            &self,
            _context: AppendContext,
            record: StreamRecordBatch,
        ) -> futures::future::BoxFuture<'static, Result<(), StreamError>> {
            // Synchronous half mirrors S3Storage: enqueue (here: record) at
            // submit. The future only reports the outcome.
            let result = match self.fail_append.lock().unwrap().take() {
                Some(e) => Err(e),
                None => {
                    self.records.lock().unwrap().push(record);
                    Ok(())
                }
            };
            Box::pin(async move { result })
        }

        async fn read(
            &self,
            _context: FetchContext,
            stream_id: u64,
            start_offset: u64,
            end_offset: u64,
            _max_bytes: usize,
        ) -> Result<ReadDataBlock, StreamError> {
            let records = self
                .records
                .lock()
                .unwrap()
                .iter()
                .filter(|r| {
                    r.stream_id() == stream_id
                        && r.last_offset() > start_offset
                        && r.base_offset() < end_offset
                })
                .cloned()
                .collect();
            Ok(ReadDataBlock {
                records,
                cache_access: CacheAccessType::DeltaWalCacheHit,
            })
        }

        async fn force_upload(&self, _stream_id: u64) -> Result<(), StreamError> {
            Ok(())
        }
    }

    struct StubStreamManager {
        closed: Mutex<Vec<(u64, u64)>>,
        trimmed: Mutex<Vec<(u64, u64, u64)>>,
        deleted: Mutex<Vec<u64>>,
    }

    impl StubStreamManager {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                closed: Mutex::new(Vec::new()),
                trimmed: Mutex::new(Vec::new()),
                deleted: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl StreamManager for StubStreamManager {
        async fn get_opening_streams(
            &self,
        ) -> Result<Vec<crate::manager::StreamMetadata>, StreamError> {
            Ok(Vec::new())
        }

        async fn get_streams(
            &self,
            _stream_ids: &[u64],
        ) -> Result<Vec<crate::manager::StreamMetadata>, StreamError> {
            Ok(Vec::new())
        }

        async fn create_stream(&self, _tags: HashMap<String, String>) -> Result<u64, StreamError> {
            Ok(1)
        }

        async fn open_stream(
            &self,
            stream_id: u64,
            epoch: u64,
            _tags: HashMap<String, String>,
        ) -> Result<crate::manager::StreamMetadata, StreamError> {
            Ok(crate::manager::StreamMetadata {
                stream_id,
                epoch,
                start_offset: 0,
                end_offset: 0,
                state: crate::manager::StreamState::Opened,
                node_id: -1,
            })
        }

        async fn trim_stream(
            &self,
            stream_id: u64,
            epoch: u64,
            new_start_offset: u64,
        ) -> Result<(), StreamError> {
            self.trimmed
                .lock()
                .unwrap()
                .push((stream_id, epoch, new_start_offset));
            Ok(())
        }

        async fn close_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
            self.closed.lock().unwrap().push((stream_id, epoch));
            Ok(())
        }

        async fn delete_stream(&self, stream_id: u64, _epoch: u64) -> Result<(), StreamError> {
            self.deleted.lock().unwrap().push(stream_id);
            Ok(())
        }
    }

    fn stream(storage: Arc<StubStorage>, manager: Arc<StubStreamManager>) -> S3Stream {
        S3Stream::new(42, 3, 0, 0, storage, manager, false)
    }

    fn batch(count: u32, payload: &[u8]) -> RecordBatch {
        RecordBatch::new(count, 0, Bytes::copy_from_slice(payload))
    }

    #[tokio::test]
    async fn append_assigns_offsets_and_advances_confirm() {
        let s = stream(StubStorage::new(), StubStreamManager::new());
        let r1 = s
            .append(AppendContext::default(), batch(10, b"a"))
            .await
            .unwrap();
        let r2 = s
            .append(AppendContext::default(), batch(5, b"b"))
            .await
            .unwrap();
        assert_eq!(r1.base_offset, 0);
        assert_eq!(r2.base_offset, 10);
        assert_eq!(s.next_offset(), 15);
        assert_eq!(s.confirm_offset(), 15);

        let fetched = s
            .fetch(FetchContext::default(), 0, 15, usize::MAX)
            .await
            .unwrap();
        assert_eq!(fetched.records.len(), 2);
        assert_eq!(fetched.records[0].payload, Bytes::from_static(b"a"));
        assert_eq!(fetched.records[1].base_offset, 10);
        assert_eq!(fetched.records[1].last_offset, 15);
    }

    #[tokio::test]
    async fn state_and_bounds_guards() {
        let storage = StubStorage::new();
        let manager = StubStreamManager::new();
        let s = stream(storage.clone(), manager.clone());
        s.append(AppendContext::default(), batch(10, b"x"))
            .await
            .unwrap();

        // Fetch beyond confirm offset => OffsetOutOfRange.
        let err = s
            .fetch(FetchContext::default(), 0, 11, usize::MAX)
            .await
            .unwrap_err();
        assert!(matches!(err, StreamError::OffsetOutOfRange { .. }), "{err}");
        // Fetch below start offset (after trim) => OffsetOutOfRange.
        s.trim(5).await.unwrap();
        let err = s
            .fetch(FetchContext::default(), 0, 10, usize::MAX)
            .await
            .unwrap_err();
        assert!(matches!(err, StreamError::OffsetOutOfRange { .. }), "{err}");
        assert_eq!(*manager.trimmed.lock().unwrap(), vec![(42, 3, 5)]);
        // Backwards trim is a no-op.
        s.trim(1).await.unwrap();
        assert_eq!(manager.trimmed.lock().unwrap().len(), 1);

        // Fenced append marks the stream unwritable.
        *storage.fail_append.lock().unwrap() = Some(StreamError::Fenced {
            stream_id: 42,
            epoch: 3,
        });
        let err = s
            .append(AppendContext::default(), batch(1, b"y"))
            .await
            .unwrap_err();
        assert!(matches!(err, StreamError::Fenced { .. }), "{err}");
        let err = s
            .append(AppendContext::default(), batch(1, b"z"))
            .await
            .unwrap_err();
        assert!(matches!(err, StreamError::Closed { .. }), "{err}");

        // Close is idempotent and reaches the stream manager once.
        let s2 = stream(storage.clone(), manager.clone());
        s2.close().await.unwrap();
        s2.close().await.unwrap();
        assert_eq!(*manager.closed.lock().unwrap(), vec![(42, 3)]);
        let err = s2
            .append(AppendContext::default(), batch(1, b"w"))
            .await
            .unwrap_err();
        assert!(matches!(err, StreamError::Closed { .. }), "{err}");
    }

    #[tokio::test]
    async fn destroy_closes_then_deletes() {
        let manager = StubStreamManager::new();
        let s = stream(StubStorage::new(), manager.clone());
        s.append(AppendContext::default(), batch(3, b"d"))
            .await
            .unwrap();
        s.destroy().await.unwrap();
        assert_eq!(*manager.closed.lock().unwrap(), vec![(42, 3)]);
        assert_eq!(*manager.deleted.lock().unwrap(), vec![42]);
        assert_eq!(s.start_offset(), 3);
    }

    /// SNAPSHOT_READ. Append/trim/destroy fail. ConfirmOffset(long) works.
    /// Fetch sets snapshot_read. Close does not close_stream.
    #[tokio::test]
    async fn snapshot_read_mode_guards() {
        let manager = StubStreamManager::new();
        let s = S3Stream::new(
            7,
            1,
            SNAPSHOT_FAKE_OFFSET,
            SNAPSHOT_FAKE_OFFSET,
            StubStorage::new(),
            manager.clone(),
            true,
        );
        let err = s
            .append(AppendContext::default(), batch(1, b"x"))
            .await
            .unwrap_err();
        assert!(matches!(err, StreamError::Unexpected(_)), "{err}");
        assert!(s.trim(1).await.is_err());
        assert!(s.destroy().await.is_err());
        assert!(s.confirm_offset_set(10).is_ok());
        assert_eq!(s.confirm_offset(), 10);
        assert_eq!(s.next_offset(), 10);
        s.close().await.unwrap();
        assert!(manager.closed.lock().unwrap().is_empty());
    }

    /// Storage stub whose durability is gated on a manual signal: submits are
    /// recorded synchronously, futures resolve only after `release()`.
    struct GatedStorage {
        records: Mutex<Vec<StreamRecordBatch>>,
        release: tokio::sync::watch::Sender<bool>,
    }

    impl GatedStorage {
        fn new() -> Arc<Self> {
            let (release, _) = tokio::sync::watch::channel(false);
            Arc::new(Self {
                records: Mutex::new(Vec::new()),
                release,
            })
        }

        fn release(&self) {
            let _ = self.release.send(true);
        }
    }

    #[async_trait]
    impl Storage for GatedStorage {
        async fn startup(&self) -> Result<(), StreamError> {
            Ok(())
        }

        async fn shutdown(&self) {}

        fn submit(
            &self,
            _context: AppendContext,
            record: StreamRecordBatch,
        ) -> futures::future::BoxFuture<'static, Result<(), StreamError>> {
            self.records.lock().unwrap().push(record);
            let mut gate = self.release.subscribe();
            Box::pin(async move {
                while !*gate.borrow() {
                    if gate.changed().await.is_err() {
                        return Err(StreamError::Unexpected("gate dropped".into()));
                    }
                }
                Ok(())
            })
        }

        async fn read(
            &self,
            _context: FetchContext,
            _stream_id: u64,
            _start_offset: u64,
            _end_offset: u64,
            _max_bytes: usize,
        ) -> Result<ReadDataBlock, StreamError> {
            Ok(ReadDataBlock {
                records: Vec::new(),
                cache_access: CacheAccessType::DeltaWalCacheHit,
            })
        }

        async fn force_upload(&self, _stream_id: u64) -> Result<(), StreamError> {
            Ok(())
        }
    }

    /// The point of `submit_append`: several appends genuinely in flight at
    /// once. Offsets reserved and records submitted in call order *before*
    /// any of them is durable. Then all durable together (one group commit
    /// in real storage).
    #[tokio::test]
    async fn submit_append_pipelines_multiple_inflight() {
        let storage = GatedStorage::new();
        let s = Arc::new(S3Stream::new(
            42,
            3,
            0,
            0,
            storage.clone(),
            StubStreamManager::new(),
            false,
        ));

        let p1 = Arc::clone(&s)
            .submit_append(AppendContext::default(), batch(1, b"a"))
            .unwrap();
        let p2 = Arc::clone(&s)
            .submit_append(AppendContext::default(), batch(2, b"b"))
            .unwrap();
        let p3 = Arc::clone(&s)
            .submit_append(AppendContext::default(), batch(1, b"c"))
            .unwrap();

        // Submitted in order, none durable yet.
        assert_eq!(
            (p1.base_offset(), p2.base_offset(), p3.base_offset()),
            (0, 1, 3)
        );
        assert_eq!(s.next_offset(), 4);
        assert_eq!(s.confirm_offset(), 0);
        {
            let records = storage.records.lock().unwrap();
            assert_eq!(records.len(), 3, "all three submitted while none durable");
            assert_eq!(
                records.iter().map(|r| r.base_offset()).collect::<Vec<_>>(),
                vec![0, 1, 3],
                "storage sees submit order == offset order"
            );
        }

        storage.release();
        assert_eq!(p1.durable().await.unwrap().base_offset, 0);
        assert_eq!(p2.durable().await.unwrap().base_offset, 1);
        assert_eq!(p3.durable().await.unwrap().base_offset, 3);
        assert_eq!(s.confirm_offset(), 4);
    }

    /// Storage must see records in offset order even when appends are admitted
    /// from several threads at once: the log cache rejects a batch whose base
    /// offset is not the previous batch's end, so an inversion here surfaces
    /// downstream as `[FATAL] record batch base offset mismatch` and drops the
    /// append. Guards `S3Stream#appendLock`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_submits_reach_storage_in_offset_order() {
        let storage = GatedStorage::new();
        let s = Arc::new(S3Stream::new(
            42,
            3,
            0,
            0,
            storage.clone(),
            StubStreamManager::new(),
            false,
        ));

        let appends = 4096;
        let mut tasks = Vec::with_capacity(appends);
        for _ in 0..appends {
            let s = Arc::clone(&s);
            tasks.push(tokio::spawn(async move {
                s.submit_append(AppendContext::default(), batch(1, b"x"))
                    .unwrap()
            }));
        }
        let pending: Vec<_> = futures::future::join_all(tasks)
            .await
            .into_iter()
            .map(|t| t.unwrap())
            .collect();

        let offsets: Vec<u64> = storage
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.base_offset())
            .collect();
        assert_eq!(offsets.len(), appends);
        let expected: Vec<u64> = (0..appends as u64).collect();
        assert_eq!(offsets, expected, "storage saw offsets out of order");

        storage.release();
        for p in pending {
            p.durable().await.unwrap();
        }
        assert_eq!(s.confirm_offset(), appends as u64);
    }

    /// Dropping a `PendingAppend` (cancelled caller) must not wedge the
    /// stream: the detached completer still advances the confirm offset and
    /// releases the pending-append guard (so close() does not hang).
    #[tokio::test]
    async fn submit_append_is_cancel_safe() {
        let storage = GatedStorage::new();
        let s = Arc::new(S3Stream::new(
            42,
            3,
            0,
            0,
            storage.clone(),
            StubStreamManager::new(),
            false,
        ));

        let pending = Arc::clone(&s)
            .submit_append(AppendContext::default(), batch(1, b"x"))
            .unwrap();
        drop(pending); // caller cancelled mid-flight

        storage.release();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while s.confirm_offset() != 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "confirm offset wedged"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Pending-append guard released: close() drains instantly.
        s.close().await.unwrap();
    }
}
