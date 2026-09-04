//! Object WAL write path: batch accumulation and pipelined PUTs with in-order confirm.
//!   submission order. After each completed batch the reservation lease is re-verified
//! await), spawned tokio tasks for uploads and per-bulk timers, an mpsc-driven single

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot, watch};

use s3stream_codec::{StreamRecordBatch, WAL_RECORD_HEADER_SIZE, WalRecordHeader, wal_crc32};
use s3stream_object::{ObjectPath, ObjectStorage, WriteOptions};

use crate::{AppendListener, AppendResult, OpenMode, PendingAppend, RecordOffset, WalError};

use super::config::ObjectWalConfig;
use super::header::{TRIM_OFFSET_NONE, WalObjectHeader};
use super::keys::{
    DATA_FILE_ALIGN_SIZE, TRIM_RECORD_SENTINEL, WalObject, ceil_align_offset, node_prefix,
};

fn object_path(object_prefix: &str, start_offset: u64, end_offset: u64) -> String {
    format!("{object_prefix}{start_offset}-{end_offset}")
}

struct PendingRecord {
    record: StreamRecordBatch,
    /// Framed size: encoded body + 24-byte record header.
    size: u64,
    /// Logical WAL offset, assigned when the bulk payload is built.
    offset: u64,
    ack: oneshot::Sender<Result<AppendResult, WalError>>,
}

/// The accumulating (or sealed-but-not-yet-uploading) batch.
struct Bulk {
    seq: u64,
    base_offset: u64,
    size: u64,
    records: Vec<PendingRecord>,
    complete_tx: watch::Sender<bool>,
}

impl Bulk {
    fn end_offset(&self) -> u64 {
        self.base_offset + self.size
    }
}

/// A bulk whose PUT is in flight (or done, awaiting in-order completion).
struct UploadingBulk {
    seq: u64,
    end_offset_aligned: u64,
    complete_tx: watch::Sender<bool>,
    /// Taken by the upload task while it builds/uploads, returned on completion so the
    /// callback loop can ack them.
    records: Option<Vec<PendingRecord>>,
    done: Option<Result<(), WalError>>,
}

struct WriterState {
    previous_objects: Vec<WalObject>,
    last_record_offset_to_object: BTreeMap<u64, WalObject>,
    active: Option<Bulk>,
    bulk_seq: u64,
    last_bulk_force_upload: Instant,
    last_inactive_complete: Option<watch::Receiver<bool>>,
    waiting: VecDeque<Bulk>,
    uploading: VecDeque<UploadingBulk>,
}

struct Inner {
    config: ObjectWalConfig,
    storage: Arc<dyn ObjectStorage>,
    object_prefix: String,
    node_prefix: String,

    state: Mutex<WriterState>,

    buffered_data_bytes: AtomicU64,
    object_data_bytes: AtomicU64,
    next_offset: AtomicU64,
    flushed_offset: AtomicU64,
    trim_offset: AtomicI64,
    closed: AtomicBool,
    fenced: AtomicBool,

    callback_tx: mpsc::UnboundedSender<()>,
    completed_trim_tx: watch::Sender<i64>,

    /// Chosen as min(10 ms, batch interval).
    min_bulk_upload_interval: Duration,

    /// In-order confirm hook, invoked from the callback loop before acks (see
    append_listener: std::sync::RwLock<Option<AppendListener>>,
}

/// The writer half of the object WAL.
pub struct ObjectWalWriter {
    inner: Arc<Inner>,
}

impl ObjectWalWriter {
    pub fn new(storage: Arc<dyn ObjectStorage>, config: ObjectWalConfig) -> Self {
        let prefix = node_prefix(
            &config.cluster_id,
            config.node_id,
            Some(config.wal_type.as_str()),
        );
        let object_prefix = format!("{prefix}{}/wal/", config.epoch);
        let min_bulk_upload_interval = Duration::from_millis(10).min(config.batch_interval);
        let (callback_tx, callback_rx) = mpsc::unbounded_channel();
        let (completed_trim_tx, _) = watch::channel(TRIM_OFFSET_NONE);
        let inner = Arc::new(Inner {
            config,
            storage,
            object_prefix,
            node_prefix: prefix,
            state: Mutex::new(WriterState {
                previous_objects: Vec::new(),
                last_record_offset_to_object: BTreeMap::new(),
                active: None,
                bulk_seq: 0,
                last_bulk_force_upload: Instant::now(),
                last_inactive_complete: None,
                waiting: VecDeque::new(),
                uploading: VecDeque::new(),
            }),
            buffered_data_bytes: AtomicU64::new(0),
            object_data_bytes: AtomicU64::new(0),
            next_offset: AtomicU64::new(0),
            flushed_offset: AtomicU64::new(0),
            trim_offset: AtomicI64::new(TRIM_OFFSET_NONE),
            closed: AtomicBool::new(true),
            fenced: AtomicBool::new(false),
            callback_tx,
            completed_trim_tx,
            min_bulk_upload_interval,
            append_listener: std::sync::RwLock::new(None),
        });
        tokio::spawn(callback_loop(Arc::downgrade(&inner), callback_rx));
        Self { inner }
    }

    pub async fn start(&self) -> Result<(), WalError> {
        let inner = &self.inner;
        let config = &inner.config;
        let failover = config.open_mode == OpenMode::Recovery;
        let verified = config
            .reservation_service
            .verify(config.node_id, config.epoch, failover)
            .await?;
        if !verified {
            inner.fenced.store(true, Ordering::SeqCst);
            return Err(WalError::Fenced {
                node_id: config.node_id,
                our_epoch: config.epoch,
            });
        }

        let listed = inner.storage.list(&inner.node_prefix).await?;
        let mut objects = super::keys::parse_wal_objects(listed);
        let overlap = super::keys::skip_overlap_objects(&mut objects);
        if !overlap.is_empty() {
            let storage = Arc::clone(&inner.storage);
            let paths: Vec<ObjectPath> = overlap
                .iter()
                .map(|o| ObjectPath {
                    bucket_id: o.bucket_id,
                    key: o.key.clone(),
                })
                .collect();
            tokio::spawn(async move {
                if let Err(e) = storage.delete(&paths).await {
                    tracing::error!(error = %e, "delete overlap objects failed");
                } else {
                    tracing::info!(?paths, "deleted overlap objects");
                }
            });
        }

        if let Some(last) = objects.last()
            && last.epoch > config.epoch
        {
            tracing::warn!(
                largest_epoch = last.epoch,
                "detected newer epoch WAL started, exit current WAL start"
            );
            inner.fenced.store(true, Ordering::SeqCst);
            return Ok(());
        }

        let total: u64 = objects.iter().map(|o| o.size).sum();
        inner.object_data_bytes.fetch_add(total, Ordering::SeqCst);
        let flushed = objects.last().map(|o| o.end_offset).unwrap_or(0);
        {
            let mut state = inner.state.lock().expect("writer state poisoned");
            state.previous_objects.extend(objects);
            state.last_bulk_force_upload = Instant::now();
        }
        inner.flushed_offset.store(flushed, Ordering::SeqCst);
        inner.next_offset.store(flushed, Ordering::SeqCst);
        inner.closed.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        let rx = {
            let mut state = self.inner.state.lock().expect("writer state poisoned");
            seal_active(&self.inner, &mut state);
            state.last_inactive_complete.clone()
        };
        try_upload_waiting(&self.inner);
        if let Some(mut rx) = rx {
            while !*rx.borrow() {
                if rx.changed().await.is_err() {
                    tracing::error!("failed to flush records when close");
                    break;
                }
            }
        }
        tracing::info!("S3WAL writer is closed");
    }

    /// Register the in-order confirm callback (invoked from the callback loop before
    /// each append future resolves).
    pub fn set_append_listener(&self, listener: AppendListener) {
        *self
            .inner
            .append_listener
            .write()
            .expect("listener poisoned") = Some(listener);
    }

    pub fn submit(&self, record: StreamRecordBatch) -> Result<PendingAppend, WalError> {
        let inner = &self.inner;
        self.check_write_status()?;

        let buffered = inner.buffered_data_bytes.load(Ordering::SeqCst);
        if buffered > inner.config.max_unflushed_bytes {
            return Err(WalError::OverCapacity {
                unconfirmed_bytes: buffered,
                cap_bytes: inner.config.max_unflushed_bytes,
            });
        }
        let data_size = record.encoded().len() as u64 + WAL_RECORD_HEADER_SIZE as u64;
        if data_size > DATA_FILE_ALIGN_SIZE {
            return Err(WalError::RecordTooLarge {
                size: data_size,
                max: DATA_FILE_ALIGN_SIZE,
            });
        }

        let (ack_tx, ack_rx) = oneshot::channel();
        let sealed = {
            let mut state = inner.state.lock().expect("writer state poisoned");
            if state.active.is_none() {
                new_active_bulk(inner, &mut state);
            }
            if data_size + state.active.as_ref().unwrap().size > DATA_FILE_ALIGN_SIZE {
                seal_active(inner, &mut state);
                new_active_bulk(inner, &mut state);
            }
            inner
                .buffered_data_bytes
                .fetch_add(data_size, Ordering::SeqCst);
            let active = state.active.as_mut().unwrap();
            active.records.push(PendingRecord {
                record,
                size: data_size,
                offset: 0,
                ack: ack_tx,
            });
            active.size += data_size;
            if active.size > inner.config.max_bytes_in_batch
                || inner.config.open_mode == OpenMode::Recovery
            {
                seal_active(inner, &mut state);
                true
            } else {
                false
            }
        };
        if sealed {
            try_upload_waiting(inner);
        }

        // Everything above ran without awaiting, so the record's place in the
        // current bulk is already fixed relative to other callers. Only
        // durability is deferred.
        let inner = Arc::clone(&self.inner);
        Ok(PendingAppend {
            durable: Box::pin(async move {
                let result = ack_rx.await.unwrap_or(Err(WalError::Shutdown));
                inner
                    .buffered_data_bytes
                    .fetch_sub(data_size, Ordering::SeqCst);
                if let Err(e) = &result {
                    tracing::error!(error = %e, "failed to append record to S3 WAL");
                }
                result
            }),
        })
    }

    pub async fn append(&self, record: StreamRecordBatch) -> Result<AppendResult, WalError> {
        self.submit(record)?.durable.await
    }

    pub fn confirm_offset(&self) -> RecordOffset {
        RecordOffset {
            epoch: self.inner.config.epoch,
            offset: self.inner.flushed_offset.load(Ordering::SeqCst),
            size: 0,
        }
    }

    pub async fn flush(&self) -> Result<(), WalError> {
        let rx = {
            let mut state = self.inner.state.lock().expect("writer state poisoned");
            seal_active(&self.inner, &mut state);
            state.last_inactive_complete.clone()
        };
        try_upload_waiting(&self.inner);
        if let Some(mut rx) = rx {
            while !*rx.borrow() {
                rx.changed().await.map_err(|_| WalError::Shutdown)?;
            }
        }
        Ok(())
    }

    pub async fn trim(&self, offset: RecordOffset) -> Result<(), WalError> {
        self.trim0(offset.offset as i64).await
    }

    pub async fn reset(&self) -> Result<(), WalError> {
        let next_offset = self.inner.next_offset.load(Ordering::SeqCst);
        if next_offset == 0 {
            return Ok(());
        }
        self.trim0(next_offset as i64 - 1).await
    }

    pub async fn trim0(&self, inclusive_trim_record_offset: i64) -> Result<(), WalError> {
        let inner = &self.inner;
        self.check_status()?;

        {
            let _state = inner.state.lock().expect("writer state poisoned");
            if inner.trim_offset.load(Ordering::SeqCst) >= inclusive_trim_record_offset {
                drop(_state);
                // (which covers at least this offset) completes.
                let mut rx = inner.completed_trim_tx.subscribe();
                while *rx.borrow() < inclusive_trim_record_offset {
                    rx.changed().await.map_err(|_| WalError::Shutdown)?;
                }
                return Ok(());
            }
            inner
                .trim_offset
                .store(inclusive_trim_record_offset, Ordering::SeqCst);
        }

        let fake = StreamRecordBatch::new(
            TRIM_RECORD_SENTINEL,
            TRIM_RECORD_SENTINEL,
            0,
            0,
            Bytes::new(),
        );
        self.append(fake).await?;

        let (delete_list, deleted_size) = {
            let mut state = inner.state.lock().expect("writer state poisoned");
            let mut delete_list: Vec<ObjectPath> = Vec::new();
            let mut deleted_size: u64 = 0;

            if let Some((&last_flushed, _)) = state.last_record_offset_to_object.iter().next_back()
            {
                let covered: Vec<u64> = state
                    .last_record_offset_to_object
                    .range(..=(inclusive_trim_record_offset.max(0) as u64))
                    .map(|(k, _)| *k)
                    .collect();
                for key in covered {
                    if key == last_flushed {
                        continue;
                    }
                    let object = state
                        .last_record_offset_to_object
                        .remove(&key)
                        .expect("key just enumerated");
                    deleted_size += object.size;
                    delete_list.push(ObjectPath {
                        bucket_id: object.bucket_id,
                        key: object.key,
                    });
                }
            }

            if !state.previous_objects.is_empty() {
                let skip_the_last_object = delete_list.is_empty();
                let bound = state.previous_objects.len() - usize::from(skip_the_last_object);
                let mut count = 0;
                for object in state.previous_objects.iter().take(bound) {
                    if object.end_offset as i64 > inclusive_trim_record_offset {
                        break;
                    }
                    deleted_size += object.size;
                    delete_list.push(ObjectPath {
                        bucket_id: object.bucket_id,
                        key: object.key.clone(),
                    });
                    count += 1;
                }
                state.previous_objects.drain(..count);
            }
            (delete_list, deleted_size)
        };

        if !delete_list.is_empty() {
            let result = inner.storage.delete(&delete_list).await;
            inner
                .object_data_bytes
                .fetch_sub(deleted_size, Ordering::SeqCst);
            if let Err(e) = &result {
                tracing::error!(error = %e, "failed to delete objects when trim S3 WAL");
            }
            let storage = Arc::clone(&inner.storage);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let _ = storage.delete(&delete_list).await;
            });
        }

        inner.completed_trim_tx.send_modify(|v| {
            *v = (*v).max(inclusive_trim_record_offset);
        });
        Ok(())
    }

    pub fn object_list(&self) -> Result<Vec<WalObject>, WalError> {
        self.check_status()?;
        let state = self.inner.state.lock().expect("writer state poisoned");
        let mut list = Vec::with_capacity(
            state.previous_objects.len() + state.last_record_offset_to_object.len(),
        );
        list.extend(state.previous_objects.iter().cloned());
        list.extend(state.last_record_offset_to_object.values().cloned());
        Ok(list)
    }

    pub fn object_data_bytes(&self) -> u64 {
        self.inner.object_data_bytes.load(Ordering::SeqCst)
    }

    fn check_status(&self) -> Result<(), WalError> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(WalError::NotInitialized);
        }
        if self.inner.fenced.load(Ordering::SeqCst) {
            return Err(WalError::Fenced {
                node_id: self.inner.config.node_id,
                our_epoch: self.inner.config.epoch,
            });
        }
        Ok(())
    }

    fn check_write_status(&self) -> Result<(), WalError> {
        self.check_status()
    }
}

fn new_active_bulk(inner: &Arc<Inner>, state: &mut WriterState) {
    state.bulk_seq += 1;
    let seq = state.bulk_seq;
    let base_offset = inner.next_offset.load(Ordering::SeqCst);
    let (complete_tx, _) = watch::channel(false);
    state.active = Some(Bulk {
        seq,
        base_offset,
        size: 0,
        records: Vec::new(),
        complete_tx,
    });

    let now = Instant::now();
    let batch = inner.config.batch_interval;
    let since_last = state
        .last_bulk_force_upload
        .checked_add(batch)
        .and_then(|deadline| deadline.checked_duration_since(now))
        .unwrap_or(Duration::ZERO);
    let delay = since_last.max(inner.min_bulk_upload_interval).min(batch);
    state.last_bulk_force_upload = now + delay;

    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let Some(inner) = weak.upgrade() else { return };
        let sealed = {
            let mut state = inner.state.lock().expect("writer state poisoned");
            if state.active.as_ref().map(|b| b.seq) == Some(seq) {
                seal_active(&inner, &mut state);
                true
            } else {
                false
            }
        };
        if sealed {
            try_upload_waiting(&inner);
        }
    });
}

fn seal_active(inner: &Arc<Inner>, state: &mut WriterState) {
    let Some(bulk) = state.active.take() else {
        return;
    };
    let next = ceil_align_offset(inner.next_offset.load(Ordering::SeqCst) + bulk.size);
    inner.next_offset.store(next, Ordering::SeqCst);
    state.last_inactive_complete = Some(bulk.complete_tx.subscribe());
    state.waiting.push_back(bulk);
}

fn try_upload_waiting(inner: &Arc<Inner>) {
    let mut jobs = Vec::new();
    {
        let mut state = inner.state.lock().expect("writer state poisoned");
        while state.uploading.len() < inner.config.max_inflight_upload_count {
            let Some(bulk) = state.waiting.pop_front() else {
                break;
            };
            let end_offset_aligned = ceil_align_offset(bulk.end_offset());
            state.uploading.push_back(UploadingBulk {
                seq: bulk.seq,
                end_offset_aligned,
                complete_tx: bulk.complete_tx,
                records: None,
                done: None,
            });
            jobs.push((bulk.seq, bulk.base_offset, bulk.records));
        }
    }
    for (seq, base_offset, records) in jobs {
        let inner = Arc::clone(inner);
        tokio::spawn(async move { upload_bulk(inner, seq, base_offset, records).await });
    }
}

async fn upload_bulk(
    inner: Arc<Inner>,
    seq: u64,
    base_offset: u64,
    mut records: Vec<PendingRecord>,
) {
    // Order by <streamId, baseOffset>.
    records.sort_by(|a, b| {
        a.record
            .stream_id()
            .cmp(&b.record.stream_id())
            .then(a.record.base_offset().cmp(&b.record.base_offset()))
    });

    let mut next_offset = base_offset;
    let mut last_record_offset = next_offset;
    let data_length: u64 = records.iter().map(|r| r.size).sum();

    let header = WalObjectHeader::v1(
        base_offset,
        data_length,
        inner.config.node_id,
        inner.config.epoch,
        inner.trim_offset.load(Ordering::SeqCst),
    );
    let mut payload = BytesMut::with_capacity(header.size() + data_length as usize);
    payload.put(header.marshal());
    for record in records.iter_mut() {
        record.offset = next_offset;
        last_record_offset = record.offset;
        let body = record.record.encoded();
        let frame_header =
            WalRecordHeader::data(record.offset, body.len() as u32, wal_crc32(&body));
        payload.put_slice(&frame_header.marshal());
        payload.put_slice(&body);
        next_offset += record.size;
    }

    let end_offset = ceil_align_offset(next_offset);
    let object_length = payload.len() as u64;
    let path = object_path(&inner.object_prefix, base_offset, end_offset);
    let write_options = WriteOptions {
        enable_fast_retry: true,
        ..Default::default()
    };
    let result = inner
        .storage
        .write(&write_options, &path, payload.freeze())
        .await;

    {
        let mut state = inner.state.lock().expect("writer state poisoned");
        let entry = state
            .uploading
            .iter_mut()
            .find(|b| b.seq == seq)
            .expect("uploading bulk present");
        entry.records = Some(records);
        match result {
            Ok(write_result) => {
                entry.done = Some(Ok(()));
                state.last_record_offset_to_object.insert(
                    last_record_offset,
                    WalObject {
                        bucket_id: write_result.bucket_id,
                        key: path,
                        epoch: inner.config.epoch,
                        start_offset: base_offset,
                        end_offset,
                        size: object_length,
                    },
                );
                inner
                    .object_data_bytes
                    .fetch_add(object_length, Ordering::SeqCst);
            }
            Err(e) => {
                inner.fenced.store(true, Ordering::SeqCst);
                tracing::error!(path, error = %e, "S3WAL upload fail");
                entry.done = Some(Err(e.into()));
            }
        }
    }
    let _ = inner.callback_tx.send(());
}

async fn callback_loop(weak: Weak<Inner>, mut rx: mpsc::UnboundedReceiver<()>) {
    while rx.recv().await.is_some() {
        let Some(inner) = weak.upgrade() else { return };
        let completed: Vec<UploadingBulk> = {
            let mut state = inner.state.lock().expect("writer state poisoned");
            let mut completed = Vec::new();
            while state.uploading.front().is_some_and(|b| b.done.is_some()) {
                completed.push(state.uploading.pop_front().unwrap());
            }
            completed
        };
        if completed.is_empty() {
            continue;
        }
        // Inflight count decreased: admit more waiting bulks.
        try_upload_waiting(&inner);

        let failover = inner.config.open_mode == OpenMode::Recovery;
        match inner
            .config
            .reservation_service
            .verify(inner.config.node_id, inner.config.epoch, failover)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!("the S3WAL is fenced by another node; failing appends");
                inner.fenced.store(true, Ordering::SeqCst);
            }
            Err(e) => {
                tracing::error!(error = %e, "unexpected S3WAL lease check fail; fencing");
                inner.fenced.store(true, Ordering::SeqCst);
            }
        }

        let fenced = inner.fenced.load(Ordering::SeqCst);
        for bulk in completed {
            if fenced {
                complete_bulk(&inner, bulk, true);
            } else {
                inner
                    .flushed_offset
                    .store(bulk.end_offset_aligned, Ordering::SeqCst);
                complete_bulk(&inner, bulk, false);
            }
        }
    }
}

fn complete_bulk(inner: &Arc<Inner>, bulk: UploadingBulk, fenced: bool) {
    let records = bulk.records.expect("records returned by upload task");
    let count = records.len();
    let listener = inner
        .append_listener
        .read()
        .expect("listener poisoned")
        .clone();
    for (idx, record) in records.into_iter().enumerate() {
        let result = if fenced {
            Err(WalError::Fenced {
                node_id: inner.config.node_id,
                our_epoch: inner.config.epoch,
            })
        } else {
            let next_offset = if idx == count - 1 {
                ceil_align_offset(record.offset + record.size)
            } else {
                record.offset + record.size
            };
            let result = AppendResult {
                record_offset: RecordOffset {
                    epoch: inner.config.epoch,
                    offset: record.offset,
                    size: record.size as u32,
                },
                next_offset: RecordOffset {
                    epoch: inner.config.epoch,
                    offset: next_offset,
                    size: 0,
                },
            };
            // Skip the trim fake record. It is WAL-internal, not user data.
            if record.record.stream_id() != TRIM_RECORD_SENTINEL
                && let Some(listener) = &listener
            {
                listener(&record.record, result.record_offset, result.next_offset);
            }
            Ok(result)
        };
        let _ = record.ack.send(result);
    }
    let _ = bulk.complete_tx.send(true);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::header::WAL_HEADER_SIZE_V1;
    use crate::{NoopReservationService, ReservationService};
    use async_trait::async_trait;
    use s3stream_object::{MemoryObjectStorage, ReadOptions};
    use std::sync::atomic::AtomicBool as StdAtomicBool;

    fn test_config(batch_ms: u64) -> ObjectWalConfig {
        let mut config = ObjectWalConfig::defaults();
        config.cluster_id = "test".to_string();
        config.node_id = 1;
        config.epoch = 3;
        config.batch_interval = Duration::from_millis(batch_ms);
        config
    }

    fn record(stream_id: u64, base_offset: u64, payload: &[u8]) -> StreamRecordBatch {
        StreamRecordBatch::new(
            stream_id,
            0,
            base_offset,
            1,
            Bytes::copy_from_slice(payload),
        )
    }

    #[tokio::test]
    async fn append_uploads_one_object_per_bulk() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let writer = ObjectWalWriter::new(storage.clone(), test_config(20));
        writer.start().await.unwrap();

        // Concurrent appends land in the same bulk (batch window).
        let r1 = writer.append(record(2, 0, b"bbb"));
        let r2 = writer.append(record(1, 0, b"aaa"));
        let (r1, r2) = tokio::join!(r1, r2);
        let (r1, r2) = (r1.unwrap(), r2.unwrap());

        // Records are sorted by (streamId, baseOffset) inside the object: stream 1
        // gets the lower offset even though it was appended second.
        assert!(r2.record_offset.offset < r1.record_offset.offset);
        // The bulk's last record jumps its next offset to the align boundary.
        assert_eq!(r1.next_offset.offset, DATA_FILE_ALIGN_SIZE);
        assert_eq!(writer.confirm_offset().offset, DATA_FILE_ALIGN_SIZE);

        // Exactly one WAL object, header parses, carries trim -1.
        let objects = writer.object_list().unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].start_offset, 0);
        assert_eq!(objects[0].end_offset, DATA_FILE_ALIGN_SIZE);
        let bytes = storage
            .read(&ReadOptions::default(), &objects[0].key)
            .await
            .unwrap();
        let header = WalObjectHeader::unmarshal(&bytes).unwrap();
        assert_eq!(header.start_offset, 0);
        assert_eq!(header.trim_offset, TRIM_OFFSET_NONE);
        assert_eq!(
            header.body_length as usize,
            bytes.len() - WAL_HEADER_SIZE_V1
        );
    }

    #[tokio::test]
    async fn second_bulk_starts_at_next_align_window() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let writer = ObjectWalWriter::new(storage.clone(), test_config(5));
        writer.start().await.unwrap();

        let r1 = writer.append(record(1, 0, b"first")).await.unwrap();
        let r2 = writer.append(record(1, 1, b"second")).await.unwrap();
        assert_eq!(r1.record_offset.offset, 0);
        // Whether or not the two records shared a bulk, the second object (if any)
        // starts at an align boundary and confirm covers both.
        assert!(
            r2.record_offset.offset == r1.record_offset.offset + r1.record_offset.size as u64
                || r2.record_offset.offset % DATA_FILE_ALIGN_SIZE == 0
        );
        assert!(writer.confirm_offset().offset >= r2.next_offset.offset);
    }

    #[tokio::test]
    async fn over_capacity_backpressure() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let mut config = test_config(10);
        config.max_unflushed_bytes = 1; // any buffered byte trips the cap
        let writer = ObjectWalWriter::new(storage, config);
        writer.start().await.unwrap();

        // First append succeeds (check happens before buffering)...
        writer.append(record(1, 0, b"x")).await.unwrap();
        // ...and once drained (append awaited => buffered back to 0), still succeeds.
        writer.append(record(1, 1, b"y")).await.unwrap();
    }

    #[tokio::test]
    async fn record_too_large_rejected() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let writer = ObjectWalWriter::new(storage, test_config(10));
        writer.start().await.unwrap();
        let huge = vec![0u8; DATA_FILE_ALIGN_SIZE as usize];
        let err = writer.append(record(1, 0, &huge)).await.unwrap_err();
        assert!(matches!(err, WalError::RecordTooLarge { .. }));
    }

    /// A reservation flip after start fences the writer at the next completed batch.
    #[tokio::test]
    async fn fenced_writer_fails_appends() {
        struct FlippableReservation {
            ok: StdAtomicBool,
        }
        #[async_trait]
        impl ReservationService for FlippableReservation {
            async fn acquire(&self, _: u32, _: u64, _: bool) -> Result<(), WalError> {
                Ok(())
            }
            async fn verify(&self, _: u32, _: u64, _: bool) -> Result<bool, WalError> {
                Ok(self.ok.load(Ordering::SeqCst))
            }
        }

        let reservation = Arc::new(FlippableReservation {
            ok: StdAtomicBool::new(true),
        });
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let mut config = test_config(5);
        config.reservation_service = reservation.clone();
        let writer = ObjectWalWriter::new(storage, config);
        writer.start().await.unwrap();

        writer.append(record(1, 0, b"ok")).await.unwrap();

        reservation.ok.store(false, Ordering::SeqCst);
        // The append itself completes with Fenced (post-upload lease check fails).
        let err = writer.append(record(1, 1, b"fenced")).await.unwrap_err();
        assert!(matches!(err, WalError::Fenced { .. }));
        // Subsequent appends fail fast.
        let err = writer.append(record(1, 2, b"after")).await.unwrap_err();
        assert!(matches!(err, WalError::Fenced { .. }));
    }

    #[tokio::test]
    async fn trim_deletes_covered_objects_but_keeps_newest() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let writer = ObjectWalWriter::new(storage.clone(), test_config(1));
        writer.start().await.unwrap();

        // Three separate bulks (batch interval 1ms and sequential awaits).
        let r1 = writer.append(record(1, 0, b"one")).await.unwrap();
        let _r2 = writer.append(record(1, 1, b"two")).await.unwrap();
        let r3 = writer.append(record(1, 2, b"three")).await.unwrap();
        assert_eq!(writer.object_list().unwrap().len(), 3);

        // Trim everything up to r3's offset: the trim itself appends a fake-record
        // object. Every covered object except the newest is deleted.
        writer.trim(r3.record_offset).await.unwrap();
        let objects = writer.object_list().unwrap();
        assert!(
            objects
                .iter()
                .all(|o| o.start_offset > r1.record_offset.offset)
        );
        // The newest data object (containing r3 or the fake record) survives.
        assert!(!objects.is_empty());

        // Idempotent: trimming to the same offset again returns after the completed
        // watermark is reached, without appending another fake record.
        let before = writer.object_list().unwrap().len();
        writer.trim(r3.record_offset).await.unwrap();
        assert_eq!(writer.object_list().unwrap().len(), before);
    }

    #[tokio::test]
    async fn start_fences_on_newer_epoch_objects() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        // Writer at epoch 5 writes one object.
        let mut config = test_config(5);
        config.epoch = 5;
        let writer5 = ObjectWalWriter::new(storage.clone(), config);
        writer5.start().await.unwrap();
        writer5.append(record(1, 0, b"newer")).await.unwrap();
        writer5.close().await;

        // Writer at older epoch 3 starts: sees the epoch-5 object and fences (start
        let writer3 = ObjectWalWriter::new(storage.clone(), test_config(5));
        writer3.start().await.unwrap();
        let err = writer3.append(record(1, 0, b"stale")).await.unwrap_err();
        assert!(matches!(err, WalError::NotInitialized));
    }

    #[tokio::test]
    async fn start_fails_when_reservation_denied() {
        struct DeniedReservation;
        #[async_trait]
        impl ReservationService for DeniedReservation {
            async fn acquire(&self, _: u32, _: u64, _: bool) -> Result<(), WalError> {
                Ok(())
            }
            async fn verify(&self, _: u32, _: u64, _: bool) -> Result<bool, WalError> {
                Ok(false)
            }
        }
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let mut config = test_config(5);
        config.reservation_service = Arc::new(DeniedReservation);
        let writer = ObjectWalWriter::new(storage, config);
        assert!(matches!(
            writer.start().await.unwrap_err(),
            WalError::Fenced { .. }
        ));
    }

    #[tokio::test]
    async fn golden_wal_objects_match_java() {
        use s3stream_object::{ObjectError, ObjectInfo, ObjectPath, WriteResult};

        /// Records every PUT (deletes don't erase the record).
        struct RecordingStorage {
            inner: MemoryObjectStorage,
            puts: Mutex<Vec<(String, Bytes)>>,
        }
        #[async_trait]
        impl s3stream_object::ObjectStorage for RecordingStorage {
            async fn readiness_check(&self) -> Result<(), ObjectError> {
                self.inner.readiness_check().await
            }
            async fn range_read(
                &self,
                options: &ReadOptions,
                key: &str,
                start: u64,
                end: Option<u64>,
            ) -> Result<Bytes, ObjectError> {
                self.inner.range_read(options, key, start, end).await
            }
            async fn write(
                &self,
                options: &WriteOptions,
                key: &str,
                data: Bytes,
            ) -> Result<WriteResult, ObjectError> {
                self.puts
                    .lock()
                    .unwrap()
                    .push((key.to_string(), data.clone()));
                self.inner.write(options, key, data).await
            }
            async fn writer(
                &self,
                options: &WriteOptions,
                key: &str,
            ) -> Result<Box<dyn s3stream_object::MultipartWriter>, ObjectError> {
                self.inner.writer(options, key).await
            }
            async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectError> {
                self.inner.list(prefix).await
            }
            async fn delete(&self, paths: &[ObjectPath]) -> Result<(), ObjectError> {
                self.inner.delete(paths).await
            }
            fn bucket_id(&self) -> i16 {
                self.inner.bucket_id()
            }
        }

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/fixtures/wal_objects/manifest.json");
        let manifest = std::fs::read_to_string(&path).expect("run conformance/generator first");
        let manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        let fixture_dir = path.parent().unwrap().to_path_buf();

        let storage = Arc::new(RecordingStorage {
            inner: MemoryObjectStorage::new(0),
            puts: Mutex::new(Vec::new()),
        });
        let mut config = ObjectWalConfig::defaults();
        config.cluster_id = manifest["cluster_id"].as_str().unwrap().to_string();
        config.node_id = manifest["node_id"].as_u64().unwrap() as u32;
        config.epoch = manifest["epoch"].as_u64().unwrap();
        config.batch_interval = Duration::from_millis(100);
        let writer = ObjectWalWriter::new(storage.clone(), config);
        writer.start().await.unwrap();

        // All appends must share the first bulk: join_all's first poll enqueues each
        // append synchronously (no await before the ack).
        let appends: Vec<_> = manifest["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let record = StreamRecordBatch::new(
                    r["stream_id"].as_u64().unwrap(),
                    r["epoch"].as_u64().unwrap(),
                    r["base_offset"].as_u64().unwrap(),
                    r["count"].as_i64().unwrap() as i32,
                    Bytes::from(hex::decode(r["payload_hex"].as_str().unwrap()).unwrap()),
                );
                writer.append(record)
            })
            .collect();
        let results = futures::future::join_all(appends).await;

        for (result, expected) in results
            .iter()
            .zip(manifest["append_results"].as_array().unwrap())
        {
            let result = result.as_ref().unwrap();
            assert_eq!(
                result.record_offset.epoch,
                expected["epoch"].as_u64().unwrap()
            );
            assert_eq!(
                result.record_offset.offset,
                expected["offset"].as_u64().unwrap()
            );
            assert_eq!(
                result.record_offset.size as u64,
                expected["size"].as_u64().unwrap()
            );
            assert_eq!(
                result.next_offset.offset,
                expected["next_offset"].as_u64().unwrap()
            );
        }

        writer
            .trim0(manifest["trim_at_offset"].as_i64().unwrap())
            .await
            .unwrap();
        writer.close().await;

        let puts = storage.puts.lock().unwrap();
        let expected_objects = manifest["objects"].as_array().unwrap();
        assert_eq!(puts.len(), expected_objects.len(), "uploaded object count");
        for (put, expected) in puts.iter().zip(expected_objects) {
            let name = expected["name"].as_str().unwrap();
            assert_eq!(
                put.0,
                expected["path"].as_str().unwrap(),
                "object key: {name}"
            );
            let golden = std::fs::read(fixture_dir.join(format!("{name}.bin"))).unwrap();
            assert_eq!(put.1.as_ref(), golden.as_slice(), "object bytes: {name}");
        }
    }

    #[tokio::test]
    async fn close_flushes_pending_bulk() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        // Long batch interval: without close() the bulk would linger.
        let writer = ObjectWalWriter::new(storage.clone(), test_config(10_000));
        writer.start().await.unwrap();
        let pending = tokio::spawn({
            let record = record(1, 0, b"pending");
            let writer_inner = Arc::clone(&writer.inner);
            async move {
                let w = ObjectWalWriter {
                    inner: writer_inner,
                };
                w.append(record).await
            }
        });
        // Give the append a moment to enter the active bulk.
        tokio::time::sleep(Duration::from_millis(50)).await;
        writer.close().await;
        let result = pending.await.unwrap().unwrap();
        assert_eq!(result.record_offset.offset, 0);
        let _ = NoopReservationService; // silence unused import in some cfg combos
    }
}
