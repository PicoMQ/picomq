use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use picomq_common::now_ms;
use picomq_metadata::{MetadataKvClient, ViewPublisher};
use s3stream::{AppendContext, FetchContext, PendingAppend, RecordBatch, Stream};

use crate::error::{ErrorKind, ServiceError};
use crate::log::{Log, LogEntry, ProducerDecision, validate_producer};
use crate::producer::{self, Admission};
use crate::record::{self, BatchHeader, LogRecord};
use crate::streams::Streams;
use crate::types::{
    AppendBatchResult, BatchReadResult, LogOffsets, LogRead, NumericProducer, OffsetToken,
    Producer, StreamBatch, StreamRecord, StreamWatermarks, SubmittedBatchAppend,
};
use crate::waiter::StreamWaiterRegistry;

const TAIL_MAX_BYTES: usize = 4 * 1024 * 1024;
const TAIL_MAX_RECORDS: usize = 4096;
const PRODUCER_CHECKPOINT_INTERVAL: u64 = 4096;

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
    entries: Mutex<HashMap<String, LogEntry>>,
}

pub struct Append<'a> {
    service: &'a S3StreamService,
    key: String,
    gate: Arc<Gate>,
    op: tokio::sync::OwnedMutexGuard<()>,
    entry: LogEntry,
    stream: Arc<dyn Stream>,
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
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn streams(&self) -> &Arc<Streams> {
        &self.streams
    }

    pub async fn create_placed_log(&self, log: &Log) -> Result<(), ServiceError> {
        if let Some(stream_id) = self.claim_log(log).await? {
            self.streams.place(stream_id).await?;
        }

        Ok(())
    }

    pub async fn create_local_log(&self, log: &Log) -> Result<(), ServiceError> {
        if let Some(stream_id) = self.claim_log(log).await? {
            self.streams.ensure_open(stream_id).await?;
        }

        Ok(())
    }

    pub fn spawn_deleted_cleanup(self: &Arc<Self>, tick: Duration) -> tokio::task::JoinHandle<()> {
        let service = self.clone();

        tokio::spawn(async move {
            loop {
                let view = service.views.load();
                for stream_id in service.streams.held_ids() {
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
        let Ok(Some(entry)) = self.entry(&key) else {
            return;
        };

        let _ = self.kv.del_kv(&key).await;
        let _ = self.kv.del_kv(&stream_id_key(entry.stream_id)).await;
        self.discard(entry.stream_id).await;
    }

    async fn claim_log(&self, log: &Log) -> Result<Option<u64>, ServiceError> {
        let key = log.key();
        if self.entry(&key)?.is_some() {
            return Ok(None);
        }

        let stream_id = self.streams.register(&key).await?;
        let index = stream_id_key(stream_id);
        let claimed: Result<LogEntry, ServiceError> = async {
            self.kv.put_kv(&index, Bytes::from(key.clone())).await?;
            let stored = self
                .kv
                .put_kv_if_absent(&key, LogEntry::new(stream_id).encode())
                .await?;
            LogEntry::decode(&stored)
        }
        .await;

        match claimed {
            Ok(current) if current.stream_id == stream_id => {
                self.entries.lock().unwrap().insert(key, current);
                Ok(Some(stream_id))
            }
            outcome => {
                let _ = self.kv.del_kv(&index).await;
                if let Err(error) = self.streams.destroy(stream_id).await {
                    tracing::warn!(%error, stream_id, "unclaimed stream not destroyed");
                }
                outcome.map(|_| None)
            }
        }
    }

    async fn discard(&self, stream_id: u64) {
        let keys: Vec<String> = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, entry)| entry.stream_id == stream_id)
            .map(|(key, _)| key.clone())
            .collect();

        for key in &keys {
            let gate = self.gate_of(key);
            let _op = gate.op.lock().await;

            self.entries.lock().unwrap().remove(key);
            gate.reset();
            self.gates.lock().unwrap().remove(key);
            self.waiters.notify_closed(key);
        }

        if let Err(error) = self.streams.destroy(stream_id).await {
            tracing::warn!(%error, stream_id, "stream not destroyed, retrying");
        }
    }

    pub fn stream_id(&self, log: &Log) -> Result<Option<u64>, ServiceError> {
        Ok(self.entry(&log.key())?.map(|entry| entry.stream_id))
    }

    pub async fn begin_append(&self, log: &Log) -> Result<Append<'_>, ServiceError> {
        let key = log.key();
        let gate = self.gate_of(&key);
        let op = gate.op.clone().lock_owned().await;

        let entry = self.require_entry(&key)?;
        let stream = self.streams.ensure_open(entry.stream_id).await?;

        Ok(Append {
            service: self,
            key,
            gate,
            op,
            entry,
            stream,
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

        let mut entry = self.require_entry(&key)?;
        let stream_id = entry.stream_id;
        let stream = self.streams.ensure_open(stream_id).await?;
        let log_start_offset = live_start_offset(stream.as_ref());
        let now = now_ms();

        self.recover_producers(&key, &mut entry, stream.as_ref(), producer, now)
            .await?;
        producer::expire_producers(&mut entry, now);

        let accepted = match producer::admit(&mut entry, producer, record_count, now) {
            Ok(Admission::Accepted(accepted)) => accepted,
            Ok(Admission::Duplicate { base_offset }) => {
                self.cache_entry(&key, entry);
                return Ok(SubmittedBatchAppend {
                    key,
                    stream_id,
                    base_offset,
                    log_start_offset,
                    notify_offset: 0,
                    duplicate: true,
                    pending: None,
                    batches: Vec::new(),
                });
            }
            Err(error) => {
                self.cache_entry(&key, entry);
                return Err(error);
            }
        };

        let base_offset = stream.next_offset();
        let (payload, batches) = patch_base_offsets(&payload, &headers, base_offset);
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
                self.streams.forget(stream_id);
                return Err(ServiceError::durability(e));
            }
        };
        debug_assert_eq!(pending.base_offset(), base_offset);
        if headers.iter().any(|h| h.log_append_time) {
            gate.note_timestamp(timestamp_ms);
        }

        producer::record(&mut entry, accepted, base_offset, now);
        let prev_offset = entry.producer_state_offset;
        entry.producer_state_offset = stream.next_offset();
        self.cache_entry(&key, entry.clone());

        if !entry.numeric_producers.is_empty()
            && prev_offset / PRODUCER_CHECKPOINT_INTERVAL
                != entry.producer_state_offset / PRODUCER_CHECKPOINT_INTERVAL
            && let Err(error) = self.put_entry(&key, entry).await
        {
            tracing::warn!(%error, log = %key, "producer state checkpoint failed");
        }

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
        })
    }

    pub async fn finish_batch_append(
        &self,
        submitted: SubmittedBatchAppend,
    ) -> Result<AppendBatchResult, ServiceError> {
        if let Some(pending) = submitted.pending {
            if let Err(e) = await_durable(vec![pending]).await {
                self.streams.forget(submitted.stream_id);
                return Err(ServiceError::durability(e));
            }

            let gate = self.gate_of(&submitted.key);
            gate.tail.lock().unwrap().record_append(&submitted.batches);
            self.waiters
                .notify_append(&submitted.key, submitted.notify_offset);
        }

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
        let entry = self.require_entry(&key)?;
        let stream = self.streams.ensure_open(entry.stream_id).await?;

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
        let entry = self.require_entry(&key)?;
        let stream = self.streams.ensure_open(entry.stream_id).await?;

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
        let entry = self.require_entry(&log.key())?;
        let stream = self.streams.ensure_open(entry.stream_id).await?;

        Ok(StreamWatermarks {
            log_start_offset: live_start_offset(stream.as_ref()),
            high_watermark: stream.confirm_offset(),
        })
    }

    pub async fn live_offsets(&self, log: &Log) -> Result<LogOffsets, ServiceError> {
        let entry = self.require_entry(&log.key())?;
        let stream = self.streams.ensure_open(entry.stream_id).await?;

        Ok(stream_offsets(entry.stream_id, stream.as_ref()))
    }

    pub fn offsets(&self, log: &Log) -> Result<LogOffsets, ServiceError> {
        let entry = self.require_entry(&log.key())?;
        match self.streams.held(entry.stream_id) {
            Some(stream) => Ok(stream_offsets(entry.stream_id, stream.as_ref())),
            None => self.committed_offsets(log),
        }
    }

    fn committed_offsets(&self, log: &Log) -> Result<LogOffsets, ServiceError> {
        let entry = self.require_entry(&log.key())?;
        let committed = self.views.load().state.get_stream(entry.stream_id);

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
            stream_id: entry.stream_id,
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
        let Some(entry) = self.entry(&key)? else {
            return Ok(false);
        };

        let stream = self.streams.ensure_open(entry.stream_id).await?;
        if stream.confirm_offset() > from.record_offset() {
            return Ok(true);
        }

        Ok(self.waiters.wait(&key, from, timeout).await)
    }

    pub async fn trim(&self, log: &Log, new_start_offset: u64) -> Result<u64, ServiceError> {
        let key = log.key();
        let gate = self.gate_of(&key);
        let _op = gate.op.lock().await;

        let entry = self.require_entry(&key)?;
        let stream = self.streams.ensure_open(entry.stream_id).await?;
        let committed_end = self
            .views
            .load()
            .state
            .get_stream(entry.stream_id)
            .map(|m| m.end_offset)
            .unwrap_or(0);

        let lower = live_start_offset(stream.as_ref());
        let clamped = new_start_offset
            .max(lower)
            .min(stream.confirm_offset().min(committed_end));
        if clamped > lower {
            stream.trim(clamped).await?;
        }

        Ok(live_start_offset(stream.as_ref()))
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
        if released.is_some()
            && let (Some(key), Some(gate)) = (&key, &gate)
        {
            gate.reset();
            self.waiters.notify_closed(key);
        }

        Ok(released)
    }

    pub async fn shutdown(&self) {
        self.waiters.clear();
        self.streams.shutdown().await;
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

    fn entry(&self, key: &str) -> Result<Option<LogEntry>, ServiceError> {
        if let Some(cached) = self.entries.lock().unwrap().get(key).cloned() {
            return Ok(Some(cached));
        }

        let Some(value) = self.kv.get_kv(key) else {
            return Ok(None);
        };
        let entry = LogEntry::decode(&value)?;
        self.cache_entry(key, entry.clone());

        Ok(Some(entry))
    }

    fn require_entry(&self, key: &str) -> Result<LogEntry, ServiceError> {
        self.entry(key)?
            .ok_or_else(|| ServiceError::kind(ErrorKind::NotFound))
    }

    fn cache_entry(&self, key: &str, entry: LogEntry) {
        self.entries.lock().unwrap().insert(key.to_owned(), entry);
    }

    async fn put_entry(&self, key: &str, entry: LogEntry) -> Result<(), ServiceError> {
        self.kv.put_kv(key, entry.encode()).await?;
        self.cache_entry(key, entry);
        Ok(())
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

    async fn recover_producers(
        &self,
        key: &str,
        entry: &mut LogEntry,
        stream: &dyn Stream,
        producer: Option<NumericProducer>,
        now: i64,
    ) -> Result<(), ServiceError> {
        if producer.is_none() && entry.numeric_producers.is_empty() {
            return Ok(());
        }

        let confirm = stream.confirm_offset();
        let mut cursor = entry.producer_state_offset.max(live_start_offset(stream));
        if cursor >= confirm {
            return Ok(());
        }

        while cursor < confirm {
            let fetch = stream
                .fetch(FetchContext::default(), cursor, confirm, 8 * 1024 * 1024)
                .await?;
            if fetch.records.is_empty() {
                break;
            }
            for batch in fetch.records {
                producer::fold_stored_payload(entry, &batch.payload, now);
                cursor = cursor.max(batch.last_offset);
            }
        }

        entry.producer_state_offset = confirm;
        self.cache_entry(key, entry.clone());
        Ok(())
    }
}

impl Append<'_> {
    pub fn next_offset(&self) -> OffsetToken {
        OffsetToken::of_record_offset(self.stream.next_offset())
    }

    pub fn producer_decision(&self, producer: &Producer) -> ProducerDecision {
        validate_producer(&self.entry, producer)
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
        if accepted
            && let Some(last_seq) = &self.entry.last_seq
            && stream_seq <= last_seq.as_str()
        {
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
            entry,
            stream,
        } = self;

        let mut pendings = Vec::new();
        let mut batches = Vec::new();
        let mut stamped = None;

        if !records.is_empty() {
            let base_offset = stream.next_offset();
            let timestamp_ms = service.next_timestamp(&gate, stream.as_ref()).await?;
            stamped = Some(timestamp_ms);

            let payload = record::encode_batch(base_offset, timestamp_ms, &records);
            let count = records.len() as u32;
            match Arc::clone(&stream).submit_append(
                AppendContext::default(),
                RecordBatch::new(count, timestamp_ms, payload.clone()),
            ) {
                Ok(pending) => pendings.push(pending),
                Err(e) => {
                    service.streams.forget(entry.stream_id);
                    gate.reset();
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
        }

        let updated = apply_append_state(entry.clone(), stream_seq, producer, decision);
        if updated != entry {
            service.put_entry(&key, updated).await?;
        }

        let next_offset = OffsetToken::of_record_offset(stream.next_offset());
        drop(op);

        if let Err(e) = await_durable(pendings).await {
            service.streams.forget(entry.stream_id);
            gate.reset();
            return Err(ServiceError::durability(e));
        }

        if !batches.is_empty() {
            gate.tail.lock().unwrap().record_append(&batches);
            service
                .waiters
                .notify_append(&key, next_offset.record_offset());
        }

        Ok(Committed {
            next_offset,
            timestamp_ms: stamped,
        })
    }
}

pub(crate) fn stream_id_key(stream_id: u64) -> String {
    format!("idx/sid/{stream_id}")
}

fn apply_append_state(
    entry: LogEntry,
    stream_seq: Option<String>,
    producer: Option<&Producer>,
    decision: Option<ProducerDecision>,
) -> LogEntry {
    let mut updated = entry;
    if let Some(seq) = stream_seq {
        updated = updated.with_last_seq(seq);
    }

    if let (Some(ProducerDecision::Accepted { .. }), Some(producer)) = (decision, producer) {
        let now = now_ms();
        updated = updated.with_producer(
            producer.producer_id.clone(),
            producer.epoch,
            producer.seq,
            now,
        );
        producer::expire_producers(&mut updated, now);
    }

    updated
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

fn patch_base_offsets(
    payload: &Bytes,
    headers: &[BatchHeader],
    base_offset: u64,
) -> (Bytes, Vec<StreamBatch>) {
    let mut patched = payload.to_vec();
    let mut assigned = base_offset;
    let mut pos = 0usize;
    let mut spans = Vec::with_capacity(headers.len());

    for header in headers {
        record::patch_base_offset(&mut patched, pos, assigned);
        spans.push((pos, pos + header.len, assigned, header.record_count));
        pos += header.len;
        assigned += header.record_count as u64;
    }

    let patched = Bytes::from(patched);
    let batches = spans
        .into_iter()
        .map(|(from, to, base, count)| StreamBatch {
            base_offset: base,
            last_offset: base + count as u64,
            count,
            payload: patched.slice(from..to),
        })
        .collect();

    (patched, batches)
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
            payload: record::encode_batch(base_offset, 1, &records),
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
    fn produce_payloads_are_patched_per_batch() {
        let one = batch(0, &["a", "b"]).payload;
        let two = batch(0, &["c"]).payload;
        let payload = Bytes::from([&one[..], &two[..]].concat());

        let headers = parse_batches(&payload).unwrap();
        assert_eq!(headers.len(), 2);

        let (stored, patched) = patch_base_offsets(&payload, &headers, 100);
        assert_eq!(stored.len(), payload.len());
        assert_eq!(
            patched
                .iter()
                .map(|b| (b.base_offset, b.last_offset))
                .collect::<Vec<_>>(),
            [(100, 102), (102, 103)]
        );

        let decoded = record::decode_batches(&patched[1].payload).unwrap();
        assert_eq!(decoded[0].offset.record_offset(), 102);
        assert_eq!(&decoded[0].record.value[..], b"c");

        let all = record::decode_batches(&stored).unwrap();
        let offsets: Vec<u64> = all.iter().map(|r| r.offset.record_offset()).collect();
        assert_eq!(offsets, [100, 101, 102]);

        assert!(parse_batches(&Bytes::new()).is_err());
        assert!(parse_batches(&Bytes::from_static(b"not a batch at all, far too short")).is_err());
    }
}
