use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use picomq_common::now_ms;
use picomq_metadata::state::StreamRow;
use picomq_metadata::{MetadataKvClient, ViewPublisher};
use s3stream::{AppendContext, FetchContext, PendingAppend, RecordBatch, Stream, StreamState};

use crate::error::{ErrorKind, ServiceError};
use crate::log::{Log, decode_stream_id, encode_stream_id};
use crate::producers::{self, Admission, IdLease, ProducerDecision, Producers, Session};
use crate::record::{self, BatchHeader, LogRecord};
use crate::retention::Retention;
use crate::streams::Streams;
use crate::types::{
    AppendBatchResult, BatchReadResult, LogOffsets, LogRead, NumericProducer, OffsetToken,
    Producer, StreamBatch, StreamRecord, StreamWatermarks, SubmittedBatchAppend,
};
use crate::waiter::StreamWaiterRegistry;

const TAIL_MAX_BYTES: usize = 4 * 1024 * 1024;
const TAIL_MAX_RECORDS: usize = 4096;

#[derive(Default)]
pub(crate) struct TailCache {
    recent: VecDeque<StreamBatch>,
    recent_bytes: usize,
    recent_records: usize,
}

impl TailCache {
    fn record_append(&mut self, batches: &[StreamBatch]) {
        let Some(first) = batches.first() else {
            return;
        };

        if let Some(last) = self.recent.back() {
            let expected = last.last_offset;
            if first.base_offset < expected {
                return;
            }
            if first.base_offset > expected {
                self.reset();
            }
        }

        for batch in batches {
            self.recent_bytes += batch.payload.len();
            self.recent_records += batch.count as usize;
            self.recent.push_back(batch.clone());
        }

        while self.recent.len() > 1
            && (self.recent_records > TAIL_MAX_RECORDS || self.recent_bytes > TAIL_MAX_BYTES)
        {
            if let Some(dropped) = self.recent.pop_front() {
                self.recent_bytes -= dropped.payload.len();
                self.recent_records -= dropped.count as usize;
            }
        }
    }

    fn tail_batches(&self, start: u64) -> Option<Vec<StreamBatch>> {
        let first = self.recent.front()?.base_offset;
        let last = self.recent.back()?.last_offset;
        if start < first || start >= last {
            return None;
        }

        Some(
            self.recent
                .iter()
                .filter(|b| b.last_offset > start)
                .cloned()
                .collect(),
        )
    }

    fn reset(&mut self) {
        self.recent.clear();
        self.recent_bytes = 0;
        self.recent_records = 0;
    }
}

struct Gate {
    op: Arc<tokio::sync::Mutex<()>>,
    tail: Mutex<TailCache>,
    last_timestamp_ms: Mutex<Option<i64>>,
}

impl Gate {
    fn reset(&self) {
        self.tail.lock().unwrap().reset();
        *self.last_timestamp_ms.lock().unwrap() = None;
    }

    fn note_timestamp(&self, timestamp_ms: i64) {
        let mut last = self.last_timestamp_ms.lock().unwrap();
        *last = Some(last.map_or(timestamp_ms, |l| l.max(timestamp_ms)));
    }
}

pub struct S3StreamService {
    streams: Arc<Streams>,
    kv: MetadataKvClient,
    views: Arc<ViewPublisher>,
    waiters: Arc<StreamWaiterRegistry>,
    gates: Mutex<HashMap<String, Arc<Gate>>>,
    producers: Mutex<HashMap<u64, Arc<tokio::sync::Mutex<Producers>>>>,
    producer_ids: tokio::sync::Mutex<IdLease>,
}

pub struct Append<'a> {
    service: &'a S3StreamService,
    key: String,
    gate: Arc<Gate>,
    op: tokio::sync::OwnedMutexGuard<()>,
    stream: Arc<dyn Stream>,
    producers: Arc<tokio::sync::Mutex<Producers>>,
    held: tokio::sync::OwnedMutexGuard<Producers>,
}

pub struct Committed {
    pub next_offset: OffsetToken,
    pub timestamp_ms: Option<i64>,
}

impl S3StreamService {
    pub fn new(
        streams: Arc<Streams>,
        kv: MetadataKvClient,
        views: Arc<ViewPublisher>,
        waiters: Arc<StreamWaiterRegistry>,
    ) -> Self {
        Self {
            streams,
            kv,
            views,
            waiters,
            gates: Mutex::new(HashMap::new()),
            producers: Mutex::new(HashMap::new()),
            producer_ids: tokio::sync::Mutex::new(IdLease::default()),
        }
    }

    pub fn streams(&self) -> &Arc<Streams> {
        &self.streams
    }

    pub async fn create_placed_log(
        &self,
        log: &Log,
        owner: Option<i32>,
    ) -> Result<(), ServiceError> {
        self.claim_log(log, async |stream_id| {
            self.streams.place(stream_id, owner).await
        })
        .await
    }

    pub async fn create_local_log(&self, log: &Log) -> Result<(), ServiceError> {
        self.claim_log(log, async |stream_id| {
            self.streams.ensure_open(stream_id).await.map(|_| ())
        })
        .await
    }

    pub fn spawn_deleted_cleanup(self: &Arc<Self>, tick: Duration) -> tokio::task::JoinHandle<()> {
        let service = self.clone();

        tokio::spawn(async move {
            loop {
                let view = service.views.load();
                for stream_id in service.streams.owned(&view.state) {
                    if view.state.get_kv(&stream_id_key(stream_id)).is_none() {
                        service.discard(stream_id).await;
                    }
                }

                service.views.wait_past(view.applied_index, tick).await;
            }
        })
    }

    pub async fn destroy_log(&self, log: &Log) {
        let key = log.key();
        let Ok(Some(stream_id)) = self.stream_id(log) else {
            return;
        };

        let _ = self.kv.del_kv(&key).await;
        let _ = self.kv.del_kv(&stream_id_key(stream_id)).await;
        self.forget_producer_rows(stream_id).await;
        self.forget(&key, stream_id).await;
    }

    async fn claim_log(
        &self,
        log: &Log,
        own: impl AsyncFnOnce(u64) -> Result<(), ServiceError>,
    ) -> Result<(), ServiceError> {
        let key = log.key();
        if self.stream_id(log)?.is_some() {
            return Ok(());
        }

        let stream_id = self.streams.register(&key).await?;
        let index = stream_id_key(stream_id);
        let claimed: Result<u64, ServiceError> = async {
            self.kv.put_kv(&index, Bytes::from(key.clone())).await?;
            own(stream_id).await?;
            let stored = self
                .kv
                .put_kv_if_absent(&key, encode_stream_id(stream_id))
                .await?;
            decode_stream_id(&stored)
        }
        .await;

        match claimed {
            Ok(current) if current == stream_id => Ok(()),
            outcome => {
                if let Err(error) = self.streams.destroy(stream_id).await {
                    tracing::warn!(%error, stream_id, "stream of an unclaimed log not destroyed");
                }
                let _ = self.kv.del_kv(&index).await;
                outcome.map(|_| ())
            }
        }
    }

    async fn discard(&self, stream_id: u64) {
        self.producers.lock().unwrap().remove(&stream_id);
        self.forget_producer_rows(stream_id).await;

        if let Err(error) = self.streams.destroy(stream_id).await {
            tracing::warn!(%error, stream_id, "stream not destroyed, retrying");
        }
    }

    async fn forget_producer_rows(&self, stream_id: u64) {
        let _ = self.kv.del_kv(&producers::checkpoint_key(stream_id)).await;
        let _ = self.kv.del_kv(&producers::stream_seq_key(stream_id)).await;
        for (key, _) in self.kv.list_kv(&producers::session_prefix(stream_id)) {
            let _ = self.kv.del_kv(&key).await;
        }
    }

    async fn forget(&self, key: &str, stream_id: u64) {
        let gate = self.gate_of(key);
        let _op = gate.op.lock().await;

        self.producers.lock().unwrap().remove(&stream_id);
        gate.reset();
        self.gates.lock().unwrap().remove(key);
        self.waiters.notify_closed(key);
    }

    pub fn stream_id(&self, log: &Log) -> Result<Option<u64>, ServiceError> {
        self.stream_id_at(&log.key())
    }

    fn stream_id_at(&self, key: &str) -> Result<Option<u64>, ServiceError> {
        self.kv
            .get_kv(key)
            .map(|value| decode_stream_id(&value))
            .transpose()
    }

    fn require_stream_id(&self, key: &str) -> Result<u64, ServiceError> {
        self.stream_id_at(key)?
            .ok_or_else(|| ServiceError::kind(ErrorKind::NotFound))
    }

    pub async fn begin_append(&self, log: &Log) -> Result<Append<'_>, ServiceError> {
        let key = log.key();
        let gate = self.gate_of(&key);
        let op = gate.op.clone().lock_owned().await;

        let stream_id = self.require_stream_id(&key)?;
        let stream = self.streams.ensure_open(stream_id).await?;
        let producers = self.load_producers(&stream).await?;
        let held = producers.clone().lock_owned().await;

        Ok(Append {
            service: self,
            key,
            gate,
            op,
            stream,
            producers,
            held,
        })
    }

    pub async fn append_records(
        &self,
        log: &Log,
        records: Vec<LogRecord>,
    ) -> Result<Committed, ServiceError> {
        self.begin_append(log)
            .await?
            .commit(records, None, None, None)
            .await
    }

    pub async fn submit_batch_append(
        &self,
        log: &Log,
        payload: Bytes,
    ) -> Result<SubmittedBatchAppend, ServiceError> {
        let headers = parse_batches(&payload)?;
        let producer = headers[0].producer;
        let record_count = headers
            .iter()
            .fold(0u32, |acc, h| acc.saturating_add(h.record_count));

        let key = log.key();
        let gate = self.gate_of(&key);
        let _op = gate.op.lock().await;

        let stream_id = self.require_stream_id(&key)?;
        let stream = self.streams.ensure_open(stream_id).await?;
        let shared = self.load_producers(&stream).await?;
        let mut producers = shared.lock().await;
        let log_start_offset = live_start_offset(stream.as_ref());
        let now = now_ms();

        if producers.replay_needed(producer, stream.confirm_offset()) {
            producers.replay(stream.as_ref(), now).await?;
        }
        let expired = producers.expire(now);
        let accepted = match producers.admit_numeric(producer, record_count, now)? {
            Admission::Accepted(accepted) => accepted,
            Admission::Duplicate { base_offset } => {
                return Ok(SubmittedBatchAppend {
                    key,
                    stream_id,
                    base_offset,
                    log_start_offset,
                    notify_offset: 0,
                    duplicate: true,
                    pending: None,
                    batches: Vec::new(),
                    checkpoint: false,
                    expired,
                });
            }
        };

        let base_offset = stream.next_offset();
        let leader_epoch = record::leader_epoch(stream.stream_epoch());
        let (payload, batches) = stamp_batches(&payload, &headers, base_offset, leader_epoch);
        let timestamp_ms = headers
            .iter()
            .map(|h| h.max_timestamp_ms)
            .max()
            .unwrap_or(0);

        let pending = match Arc::clone(&stream).submit_append(
            AppendContext::default(),
            RecordBatch::new(record_count, timestamp_ms, payload),
        ) {
            Ok(pending) => pending,
            Err(e) => {
                drop(producers);
                self.reset(&key, stream_id).await;
                return Err(ServiceError::durability(e));
            }
        };
        debug_assert_eq!(pending.base_offset(), base_offset);
        if headers.iter().any(|h| h.log_append_time) {
            gate.note_timestamp(timestamp_ms);
        }

        producers.record_numeric(accepted, base_offset, now);
        let covered_before = producers.covered();
        producers.note_appended(base_offset, stream.next_offset());
        let checkpoint = producers.checkpoint_due(covered_before);

        let notify_offset = stream.next_offset();
        Ok(SubmittedBatchAppend {
            key,
            stream_id,
            base_offset,
            log_start_offset,
            notify_offset,
            duplicate: false,
            pending: Some(pending),
            batches,
            checkpoint,
            expired,
        })
    }

    pub async fn finish_batch_append(
        &self,
        submitted: SubmittedBatchAppend,
    ) -> Result<AppendBatchResult, ServiceError> {
        if let Some(pending) = submitted.pending {
            let gate = self.gate_of(&submitted.key);
            if let Err(e) = await_durable(vec![pending]).await {
                self.reset(&submitted.key, submitted.stream_id).await;
                return Err(ServiceError::durability(e));
            }

            gate.tail.lock().unwrap().record_append(&submitted.batches);
            self.waiters
                .notify_append(&submitted.key, submitted.notify_offset);
            if submitted.checkpoint
                && let Err(error) = self.checkpoint(submitted.stream_id).await
            {
                tracing::warn!(%error, log = %submitted.key, "producer checkpoint not written, replay covers it");
            }
        }
        self.forget_sessions(submitted.stream_id, &submitted.expired)
            .await;

        Ok(AppendBatchResult {
            base_offset: submitted.base_offset,
            duplicate: submitted.duplicate,
            log_start_offset: submitted.log_start_offset,
        })
    }

    pub async fn read(
        &self,
        log: &Log,
        from: OffsetToken,
        max_bytes: usize,
        max_records: usize,
    ) -> Result<LogRead, ServiceError> {
        let key = log.key();
        let stream_id = self.require_stream_id(&key)?;
        let stream = self.streams.ensure_open(stream_id).await?;

        let start = from.record_offset();
        let end = stream.confirm_offset();
        if start > end {
            return Err(ServiceError::kind(ErrorKind::BadRequest));
        }
        if start == end {
            return Ok(LogRead {
                records: Vec::new(),
                next_offset: OffsetToken::of_record_offset(end),
                up_to_date: true,
            });
        }

        let max_bytes = if max_bytes > 0 { max_bytes } else { usize::MAX };
        let max_records = if max_records > 0 {
            max_records
        } else {
            usize::MAX
        };

        let batches = self
            .batches_from(&key, stream.as_ref(), start, end, max_bytes)
            .await?;
        collect_records(&batches, start, end, max_bytes, max_records)
    }

    pub async fn read_batches(
        &self,
        log: &Log,
        from: u64,
        max_bytes: usize,
    ) -> Result<BatchReadResult, ServiceError> {
        let key = log.key();
        let stream_id = self.require_stream_id(&key)?;
        let stream = self.streams.ensure_open(stream_id).await?;

        let high_watermark = stream.confirm_offset();
        let log_start_offset = live_start_offset(stream.as_ref());
        if from >= high_watermark {
            return Ok(BatchReadResult {
                batches: Vec::new(),
                next_offset: from,
                high_watermark,
                log_start_offset,
            });
        }

        let max_bytes = if max_bytes > 0 { max_bytes } else { usize::MAX };
        let batches = self
            .batches_from(&key, stream.as_ref(), from, high_watermark, max_bytes)
            .await?;

        let mut next = from;
        for batch in &batches {
            next = next.max(batch.last_offset);
        }

        Ok(BatchReadResult {
            batches,
            next_offset: next,
            high_watermark,
            log_start_offset,
        })
    }

    pub async fn watermarks(&self, log: &Log) -> Result<StreamWatermarks, ServiceError> {
        let stream_id = self.require_stream_id(&log.key())?;
        let stream = self.streams.ensure_open(stream_id).await?;

        Ok(StreamWatermarks {
            log_start_offset: live_start_offset(stream.as_ref()),
            high_watermark: stream.confirm_offset(),
            epoch: stream.stream_epoch(),
        })
    }

    pub fn epochs(&self, log: &Log) -> Result<Vec<(u64, u64)>, ServiceError> {
        let stream_id = self.require_stream_id(&log.key())?;
        Ok(self
            .views
            .load()
            .state
            .stream_epochs(stream_id)
            .into_iter()
            .map(|(epoch, start_offset)| (epoch.max(0) as u64, start_offset))
            .collect())
    }

    pub async fn live_offsets(&self, log: &Log) -> Result<LogOffsets, ServiceError> {
        let stream_id = self.require_stream_id(&log.key())?;
        let stream = self.streams.ensure_open(stream_id).await?;

        Ok(stream_offsets(stream_id, stream.as_ref()))
    }

    pub fn offsets(&self, log: &Log) -> Result<LogOffsets, ServiceError> {
        let stream_id = self.require_stream_id(&log.key())?;
        match self.streams.held(stream_id) {
            Some(stream) => Ok(stream_offsets(stream_id, stream.as_ref())),
            None => self.committed_offsets(log),
        }
    }

    fn committed_offsets(&self, log: &Log) -> Result<LogOffsets, ServiceError> {
        let stream_id = self.require_stream_id(&log.key())?;
        let committed = self.views.load().state.get_stream(stream_id);

        let (start, end) = committed
            .map(|m| {
                let start = if m.start_offset == u64::MAX {
                    0
                } else {
                    m.start_offset
                };
                let end = if m.end_offset == u64::MAX {
                    0
                } else {
                    m.end_offset
                };
                (start, end)
            })
            .unwrap_or((0, 0));

        Ok(LogOffsets {
            stream_id,
            start_offset: OffsetToken::of_record_offset(start),
            next_offset: OffsetToken::of_record_offset(end),
            submitted_offset: OffsetToken::of_record_offset(end),
        })
    }

    pub async fn wait_appended(
        &self,
        log: &Log,
        from: OffsetToken,
        timeout: Duration,
    ) -> Result<bool, ServiceError> {
        let key = log.key();
        let Some(stream_id) = self.stream_id_at(&key)? else {
            return Ok(false);
        };

        let stream = self.streams.ensure_open(stream_id).await?;
        if stream.confirm_offset() > from.record_offset() {
            return Ok(true);
        }

        let woken = self.waiters.wait(&key, from, timeout).await;
        if self.waiters.stopped() {
            return Err(ServiceError::with_message(
                ErrorKind::Unavailable,
                None,
                false,
                "this node is stopping",
            ));
        }

        Ok(woken)
    }

    pub async fn trim(&self, log: &Log, new_start_offset: u64) -> Result<u64, ServiceError> {
        let key = log.key();
        let gate = self.gate_of(&key);
        let _op = gate.op.lock().await;

        let stream_id = self.require_stream_id(&key)?;
        let stream = self.streams.ensure_open(stream_id).await?;
        self.trim_open(stream.as_ref(), new_start_offset).await
    }

    pub async fn apply_retention(
        &self,
        log: &Log,
        retention: Retention,
        now_ms: i64,
    ) -> Result<(), ServiceError> {
        let key = log.key();
        let Some(stream_id) = self.stream_id_at(&key)? else {
            return Ok(());
        };
        if self.retention_start(stream_id, retention, now_ms).is_none() {
            return Ok(());
        }

        let gate = self.gate_of(&key);
        let _op = gate.op.lock().await;
        let Some((row, start)) = self.retention_start(stream_id, retention, now_ms) else {
            return Ok(());
        };

        match (
            self.streams.held(row.stream_id),
            row.state,
            u64::try_from(row.epoch),
        ) {
            (Some(stream), _, _) => self.trim_open(stream.as_ref(), start).await.map(drop),
            (None, StreamState::Closed, Ok(epoch)) => {
                self.streams.trim_closed(row.stream_id, epoch, start).await
            }
            (None, _, _) => Ok(()),
        }
    }

    async fn trim_open(
        &self,
        stream: &dyn Stream,
        new_start_offset: u64,
    ) -> Result<u64, ServiceError> {
        let committed_end = self
            .views
            .load()
            .state
            .get_stream(stream.stream_id())
            .map(|m| m.end_offset)
            .unwrap_or(0);

        let lower = live_start_offset(stream);
        let clamped = new_start_offset
            .max(lower)
            .min(stream.confirm_offset().min(committed_end));
        if clamped > lower {
            stream.trim(clamped).await?;
        }

        Ok(live_start_offset(stream))
    }

    fn retention_start(
        &self,
        stream_id: u64,
        retention: Retention,
        now_ms: i64,
    ) -> Option<(StreamRow, u64)> {
        let view = self.views.load();
        let row = view.state.streams.get(&stream_id).copied()?;
        if row.node_id != self.streams.node().node_id() {
            return None;
        }

        let start = retention.start_offset(&view.state.object_spans(stream_id), now_ms)?;
        (start > row.start_offset).then_some((row, start))
    }

    pub fn notify_closed(&self, log: &Log) {
        self.waiters.notify_closed(&log.key());
    }

    pub async fn release_for_transfer(&self, stream_id: u64) -> Result<Option<i64>, ServiceError> {
        let key = self
            .kv
            .get_kv(&stream_id_key(stream_id))
            .and_then(|value| String::from_utf8(value.to_vec()).ok());
        let gate = key.as_deref().map(|key| self.gate_of(key));
        let _op = match &gate {
            Some(gate) => Some(gate.op.lock().await),
            None => None,
        };

        let released = self.streams.release(stream_id).await?;
        self.producers.lock().unwrap().remove(&stream_id);
        if released.is_some()
            && let (Some(key), Some(gate)) = (&key, &gate)
        {
            gate.reset();
            self.waiters.notify_closed(key);
        }

        Ok(released)
    }

    /// Ends long polls and subscriptions, so in-flight requests finish and
    /// their clients reconnect while the node stops.
    pub fn stop_live_reads(&self) {
        self.waiters.stop();
    }

    pub async fn shutdown(&self) {
        self.stop_live_reads();
        self.streams.shutdown().await;
        self.producers.lock().unwrap().clear();
    }

    fn gate_of(&self, key: &str) -> Arc<Gate> {
        self.gates
            .lock()
            .unwrap()
            .entry(key.to_owned())
            .or_insert_with(|| {
                Arc::new(Gate {
                    op: Arc::new(tokio::sync::Mutex::new(())),
                    tail: Mutex::new(TailCache::default()),
                    last_timestamp_ms: Mutex::new(None),
                })
            })
            .clone()
    }

    fn producers_of(&self, stream_id: u64) -> Option<Arc<tokio::sync::Mutex<Producers>>> {
        self.producers.lock().unwrap().get(&stream_id).cloned()
    }

    async fn load_producers(
        &self,
        stream: &Arc<dyn Stream>,
    ) -> Result<Arc<tokio::sync::Mutex<Producers>>, ServiceError> {
        let stream_id = stream.stream_id();
        if let Some(producers) = self.producers_of(stream_id)
            && producers.lock().await.epoch() == stream.stream_epoch()
        {
            return Ok(producers);
        }

        let mut loaded = Producers::load(
            stream.as_ref(),
            self.kv.get_kv(&producers::checkpoint_key(stream_id)),
            self.kv.list_kv(&producers::session_prefix(stream_id)),
            self.kv.get_kv(&producers::stream_seq_key(stream_id)),
        )?;
        if loaded.replay_needed(None, stream.confirm_offset()) {
            loaded.replay(stream.as_ref(), now_ms()).await?;
        }
        let producers = Arc::new(tokio::sync::Mutex::new(loaded));
        self.producers
            .lock()
            .unwrap()
            .insert(stream_id, producers.clone());

        Ok(producers)
    }

    pub async fn allocate_producer_id(&self) -> Result<i64, ServiceError> {
        let mut lease = self.producer_ids.lock().await;
        if let Some(id) = lease.take() {
            return Ok(id);
        }

        let first = self
            .streams
            .node()
            .allocate_producer_ids(producers::ID_BLOCK)
            .await?;
        lease.refill(first);
        lease.take().ok_or_else(|| {
            ServiceError::with_message(ErrorKind::Internal, None, false, "empty producer id lease")
        })
    }

    /// An HTTP producer's session at its current epoch. A new epoch gets a
    /// numeric id and its row before anything is written under it.
    async fn session_for(
        &self,
        stream_id: u64,
        producers: &mut Producers,
        producer: &Producer,
        now_ms: i64,
    ) -> Result<Session, ServiceError> {
        if let Some(session) = producers.session(&producer.producer_id)
            && session.epoch == producer.epoch
        {
            return Ok(session);
        }

        let session = Session {
            epoch: producer.epoch,
            id: self.allocate_producer_id().await?,
        };
        self.kv
            .put_kv(
                &producers::session_key(stream_id, &producer.producer_id),
                producers::encode_session(session),
            )
            .await?;
        producers.establish(producer.producer_id.clone(), session, now_ms);

        Ok(session)
    }

    async fn checkpoint(&self, stream_id: u64) -> Result<(), ServiceError> {
        let Some(producers) = self.producers_of(stream_id) else {
            return Ok(());
        };
        let bytes = producers.lock().await.checkpoint();
        self.kv
            .put_kv(&producers::checkpoint_key(stream_id), bytes)
            .await?;

        Ok(())
    }

    /// Writes the stream sequence row by compare and set. A row from a
    /// higher epoch means the stream moved, one from a lower epoch is folded
    /// in and overwritten.
    async fn write_stream_seq(
        &self,
        stream: &dyn Stream,
        producers: &mut Producers,
        mut row: Bytes,
    ) -> Result<(), ServiceError> {
        let stream_id = stream.stream_id();
        let key = producers::stream_seq_key(stream_id);

        loop {
            let stored = match producers.stream_seq_written() {
                None => {
                    let stored = self.kv.put_kv_if_absent(&key, row.clone()).await?;
                    (stored != row).then_some(stored)
                }
                Some(expected) => match self.kv.put_kv_if(&key, &expected, row.clone()).await? {
                    Some(_) => None,
                    None => Some(
                        self.kv
                            .get_kv(&key)
                            .ok_or_else(|| ServiceError::kind(ErrorKind::NotFound))?,
                    ),
                },
            };
            let Some(stored) = stored else {
                producers.mark_stream_seq_written(row);
                return Ok(());
            };

            row = match producers.merge_stream_seq(stream_id, stored, stream.confirm_offset()) {
                Ok(row) => row,
                Err(error) => {
                    self.producers.lock().unwrap().remove(&stream_id);
                    return Err(error);
                }
            };
        }
    }

    async fn forget_sessions(&self, stream_id: u64, producer_ids: &[String]) {
        for producer_id in producer_ids {
            if let Err(error) = self
                .kv
                .del_kv(&producers::session_key(stream_id, producer_id))
                .await
            {
                tracing::warn!(%error, stream_id, "expired producer session not removed");
            }
        }
    }

    async fn reset(&self, key: &str, stream_id: u64) {
        self.streams.reset(stream_id).await;
        self.producers.lock().unwrap().remove(&stream_id);
        self.gate_of(key).reset();
    }

    async fn batches_from(
        &self,
        key: &str,
        stream: &dyn Stream,
        start: u64,
        end: u64,
        max_bytes: usize,
    ) -> Result<Vec<StreamBatch>, ServiceError> {
        let cached = self.gate_of(key).tail.lock().unwrap().tail_batches(start);
        if let Some(cached) = cached {
            let mut out = Vec::new();
            let mut total = 0usize;
            for batch in cached {
                if batch.base_offset >= end {
                    break;
                }
                if total + batch.payload.len() > max_bytes && !out.is_empty() {
                    break;
                }
                total += batch.payload.len();
                out.push(batch);
            }
            return Ok(out);
        }

        let fetch = stream
            .fetch(FetchContext::default(), start, end, max_bytes)
            .await?;

        Ok(fetch
            .records
            .into_iter()
            .map(|batch| StreamBatch {
                base_offset: batch.base_offset,
                last_offset: batch.last_offset,
                count: batch.count,
                payload: batch.payload,
            })
            .collect())
    }

    async fn next_timestamp(&self, gate: &Gate, stream: &dyn Stream) -> Result<i64, ServiceError> {
        let last = *gate.last_timestamp_ms.lock().unwrap();
        let last = match last {
            Some(last) => last,
            None => {
                let last = read_back_timestamp(stream).await?;
                gate.note_timestamp(last);
                last
            }
        };

        Ok(now_ms().max(last + 1))
    }
}

impl Append<'_> {
    pub fn next_offset(&self) -> OffsetToken {
        OffsetToken::of_record_offset(self.stream.next_offset())
    }

    pub fn producer_decision(&self, producer: &Producer) -> ProducerDecision {
        self.held.decide(producer)
    }

    pub fn check_stream_seq(
        &self,
        stream_seq: Option<&str>,
        decision: Option<ProducerDecision>,
    ) -> Result<(), ServiceError> {
        let Some(stream_seq) = stream_seq else {
            return Ok(());
        };

        let accepted =
            decision.is_none() || matches!(decision, Some(ProducerDecision::Accepted { .. }));
        if accepted && self.held.stream_seq_regressed(stream_seq) {
            return Err(ServiceError::with_message(
                ErrorKind::Conflict,
                Some(self.next_offset()),
                false,
                "Sequence conflict",
            ));
        }

        Ok(())
    }

    pub async fn commit(
        self,
        records: Vec<LogRecord>,
        stream_seq: Option<String>,
        producer: Option<&Producer>,
        decision: Option<ProducerDecision>,
    ) -> Result<Committed, ServiceError> {
        let Append {
            service,
            key,
            gate,
            op,
            stream,
            producers,
            mut held,
        } = self;
        let stream_id = stream.stream_id();
        let now = now_ms();
        let accepted =
            producer.filter(|_| matches!(decision, Some(ProducerDecision::Accepted { .. })));
        let landed = (!records.is_empty()).then(|| stream.next_offset());

        let session = match accepted {
            Some(producer) => Some(
                service
                    .session_for(stream_id, &mut held, producer, now)
                    .await?,
            ),
            None => None,
        };
        let sequenced = stream_seq.is_some();
        if let Some(seq) = stream_seq {
            let row = held.accept_stream_seq(seq, landed);
            service
                .write_stream_seq(stream.as_ref(), &mut held, row)
                .await?;
        }

        let mut pendings = Vec::new();
        let mut batches = Vec::new();
        let mut stamped = None;
        let covered_before = held.covered();
        if let Some(base_offset) = landed {
            let timestamp_ms = service.next_timestamp(&gate, stream.as_ref()).await?;
            stamped = Some(timestamp_ms);

            let leader_epoch = record::leader_epoch(stream.stream_epoch());
            let identity = session
                .zip(accepted)
                .map(|(session, producer)| NumericProducer {
                    id: session.id,
                    epoch: 0,
                    first_seq: producers::header_seq(producer.seq),
                });
            let payload =
                record::encode_batch(base_offset, leader_epoch, timestamp_ms, identity, &records);
            let count = records.len() as u32;
            match Arc::clone(&stream).submit_append(
                AppendContext::default(),
                RecordBatch::new(count, timestamp_ms, payload.clone()),
            ) {
                Ok(pending) => pendings.push(pending),
                Err(e) => {
                    drop(held);
                    service.reset(&key, stream_id).await;
                    return Err(ServiceError::durability(e));
                }
            }

            gate.note_timestamp(timestamp_ms);
            batches.push(StreamBatch {
                base_offset,
                last_offset: stream.next_offset(),
                count,
                payload,
            });
            held.note_appended(base_offset, stream.next_offset());
        }
        if let (Some(session), Some(producer)) = (session, accepted) {
            held.record_http(session, producer.seq, now);
        }

        let expired = match accepted {
            Some(_) => held.expire(now),
            None => Vec::new(),
        };
        let checkpoint = held.checkpoint_due(covered_before);
        let closed_under = landed.is_none() && session.is_some();
        let next_offset = OffsetToken::of_record_offset(stream.next_offset());
        drop(held);
        drop(op);

        if let Err(e) = await_durable(pendings).await {
            service.reset(&key, stream_id).await;
            return Err(ServiceError::durability(e));
        }
        if !batches.is_empty() {
            gate.tail.lock().unwrap().record_append(&batches);
            service
                .waiters
                .notify_append(&key, next_offset.record_offset());
        }
        if sequenced {
            producers
                .lock()
                .await
                .confirm_stream_seq(stream.confirm_offset());
        }
        if closed_under {
            service.checkpoint(stream_id).await?;
        } else if checkpoint && let Err(error) = service.checkpoint(stream_id).await {
            tracing::warn!(%error, log = %key, "producer checkpoint not written, replay covers it");
        }
        service.forget_sessions(stream_id, &expired).await;

        Ok(Committed {
            next_offset,
            timestamp_ms: stamped,
        })
    }
}

pub(crate) fn stream_id_key(stream_id: u64) -> String {
    format!("idx/sid/{stream_id}")
}

fn parse_batches(payload: &Bytes) -> Result<Vec<BatchHeader>, ServiceError> {
    let bad =
        |message: String| ServiceError::with_message(ErrorKind::BadRequest, None, false, message);
    if payload.is_empty() {
        return Err(bad("empty batch payload".into()));
    }

    let headers = record::batch_headers(payload)?;
    if headers.is_empty() {
        return Err(bad("empty batch payload".into()));
    }

    let mut pos = 0;
    for header in &headers {
        record::verify_crc(&payload[pos..pos + header.len])?;
        pos += header.len;
    }

    if headers.iter().any(|h| h.transactional_or_control) {
        return Err(bad("transactional produce is not supported".into()));
    }
    if headers.len() > 1 && headers.iter().any(|h| h.producer.is_some()) {
        return Err(bad("idempotent produce must carry exactly one batch".into()));
    }

    Ok(headers)
}

fn stamp_batches(
    payload: &Bytes,
    headers: &[BatchHeader],
    base_offset: u64,
    leader_epoch: i32,
) -> (Bytes, Vec<StreamBatch>) {
    let mut stamped = payload.to_vec();
    let mut assigned = base_offset;
    let mut pos = 0usize;
    let mut spans = Vec::with_capacity(headers.len());

    for header in headers {
        record::stamp(&mut stamped, pos, assigned, leader_epoch);
        spans.push((pos, pos + header.len, assigned, header.record_count));
        pos += header.len;
        assigned += header.record_count as u64;
    }

    let stamped = Bytes::from(stamped);
    let batches = spans
        .into_iter()
        .map(|(from, to, base, count)| StreamBatch {
            base_offset: base,
            last_offset: base + count as u64,
            count,
            payload: stamped.slice(from..to),
        })
        .collect();

    (stamped, batches)
}

fn collect_records(
    batches: &[StreamBatch],
    start: u64,
    end: u64,
    max_bytes: usize,
    max_records: usize,
) -> Result<LogRead, ServiceError> {
    let mut records: Vec<StreamRecord> = Vec::new();
    let mut total = 0usize;
    let mut next = start;

    'outer: for batch in batches {
        if batch.last_offset <= start {
            continue;
        }
        if batch.base_offset >= end {
            break;
        }

        for stream_record in record::decode_batches(&batch.payload)? {
            let offset = stream_record.offset.record_offset();
            if offset < start {
                continue;
            }
            if offset >= end {
                break 'outer;
            }

            let len = stream_record.record.size_hint();
            if total + len > max_bytes && total > 0 {
                break 'outer;
            }

            total += len;
            next = offset + 1;
            records.push(stream_record);
            if total >= max_bytes || records.len() >= max_records {
                break 'outer;
            }
        }
    }

    Ok(LogRead {
        records,
        next_offset: OffsetToken::of_record_offset(next),
        up_to_date: next >= end,
    })
}

pub(crate) fn live_start_offset(stream: &dyn Stream) -> u64 {
    let start = stream.start_offset();
    if start == u64::MAX { 0 } else { start }
}

async fn read_back_timestamp(stream: &dyn Stream) -> Result<i64, ServiceError> {
    let end = stream.confirm_offset();
    if end <= live_start_offset(stream) {
        return Ok(0);
    }

    let fetch = stream
        .fetch(FetchContext::default(), end - 1, end, TAIL_MAX_BYTES)
        .await?;
    let Some(tail) = fetch.records.last() else {
        return Ok(0);
    };

    let headers = record::batch_headers(&tail.payload)?;
    Ok(headers
        .iter()
        .filter(|h| h.log_append_time)
        .map(|h| h.max_timestamp_ms)
        .max()
        .unwrap_or(0))
}

async fn await_durable(pendings: Vec<PendingAppend>) -> Result<(), s3stream::Error> {
    for pending in pendings {
        pending.durable().await?;
    }
    Ok(())
}

fn stream_offsets(stream_id: u64, stream: &dyn Stream) -> LogOffsets {
    LogOffsets {
        stream_id,
        start_offset: OffsetToken::of_record_offset(live_start_offset(stream)),
        next_offset: OffsetToken::of_record_offset(stream.confirm_offset()),
        submitted_offset: OffsetToken::of_record_offset(stream.next_offset()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(base_offset: u64, values: &[&str]) -> StreamBatch {
        let records: Vec<LogRecord> = values
            .iter()
            .map(|v| LogRecord::value(v.to_string()))
            .collect();
        StreamBatch {
            base_offset,
            last_offset: base_offset + values.len() as u64,
            count: values.len() as u32,
            payload: record::encode_batch(base_offset, -1, 1, None, &records),
        }
    }

    #[test]
    fn tail_cache_contiguity_and_eviction() {
        let mut tail = TailCache::default();
        tail.record_append(&[batch(0, &["a", "b"])]);
        assert_eq!(tail.tail_batches(1).unwrap().len(), 1);
        assert!(tail.tail_batches(2).is_none());

        tail.record_append(&[batch(0, &["x"])]);
        assert_eq!(tail.recent_records, 2);

        tail.record_append(&[batch(2, &["c"])]);
        assert_eq!(tail.tail_batches(0).unwrap().len(), 2);
        assert_eq!(tail.tail_batches(2).unwrap().len(), 1);

        tail.record_append(&[batch(10, &["j"])]);
        assert!(tail.tail_batches(0).is_none());
        assert_eq!(tail.tail_batches(10).unwrap()[0].base_offset, 10);

        let many: Vec<StreamBatch> = (0..TAIL_MAX_RECORDS as u64 + 10)
            .map(|i| batch(11 + i, &["r"]))
            .collect();
        tail.record_append(&many);
        assert!(tail.recent_records <= TAIL_MAX_RECORDS);
        assert!(tail.recent.len() <= TAIL_MAX_RECORDS);
    }

    #[test]
    fn collect_records_skips_mid_batch_and_honours_limits() {
        let batches = [batch(0, &["aa", "bb", "cc"]), batch(3, &["dd", "ee"])];

        let read = collect_records(&batches, 1, 5, usize::MAX, usize::MAX).unwrap();
        let values: Vec<&[u8]> = read.records.iter().map(|r| &r.record.value[..]).collect();
        assert_eq!(values, [b"bb", b"cc", b"dd", b"ee"]);
        assert_eq!(read.next_offset.record_offset(), 5);
        assert!(read.up_to_date);

        let read = collect_records(&batches, 0, 5, 3, usize::MAX).unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.next_offset.record_offset(), 1);
        assert!(!read.up_to_date);

        let read = collect_records(&batches, 0, 4, usize::MAX, 2).unwrap();
        assert_eq!(read.records.len(), 2);

        let read = collect_records(&batches, 2, 4, usize::MAX, usize::MAX).unwrap();
        assert_eq!(read.records.len(), 2);
        assert_eq!(read.next_offset.record_offset(), 4);
        assert!(read.up_to_date);
    }

    #[test]
    fn produce_payloads_are_stamped_per_batch() {
        let one = batch(0, &["a", "b"]).payload;
        let two = batch(0, &["c"]).payload;
        let payload = Bytes::from([&one[..], &two[..]].concat());

        let headers = parse_batches(&payload).unwrap();
        assert_eq!(headers.len(), 2);

        let (stored, stamped) = stamp_batches(&payload, &headers, 100, 7);
        assert_eq!(stored.len(), payload.len());
        assert_eq!(
            stamped
                .iter()
                .map(|b| (b.base_offset, b.last_offset))
                .collect::<Vec<_>>(),
            [(100, 102), (102, 103)]
        );

        assert!(
            record::batch_headers(&stored)
                .unwrap()
                .iter()
                .all(|header| header.leader_epoch == 7)
        );

        let decoded = record::decode_batches(&stamped[1].payload).unwrap();
        assert_eq!(decoded[0].offset.record_offset(), 102);
        assert_eq!(&decoded[0].record.value[..], b"c");

        let all = record::decode_batches(&stored).unwrap();
        let offsets: Vec<u64> = all.iter().map(|r| r.offset.record_offset()).collect();
        assert_eq!(offsets, [100, 101, 102]);

        assert!(parse_batches(&Bytes::new()).is_err());
        assert!(parse_batches(&Bytes::from_static(b"not a batch at all, far too short")).is_err());
    }
}
