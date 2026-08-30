//! Named streams over the engine, with registry state in the metadata KV.
//! Appends submit under the per-stream gate, then await durability outside
//! it so pipelined requests share WAL group commits.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use pico_common::now_ms;
use pico_metadata::{MetadataNodeHandle, ViewPublisher};
use pico_schema::Validator as _;
use s3stream::{
    AppendContext, CreateStreamOptions, FetchContext, KVClient, KeyValue, OpenStreamOptions,
    PendingAppend, RecordBatch, Stream, StreamClientTrait as StreamClient,
};

use crate::error::{ErrorKind, ServiceError};
use crate::framing;
use crate::producer::{self, Admission};
use crate::registry::{validate_producer, ClosedBy, ProducerDecision, RegistryEntry};
use crate::types::{
    AppendBatchCommand, AppendBatchResult, AppendCommand, AppendResult, BatchReadResult,
    CloseResult, CreateCommand, CreateResult, OffsetToken, ReadResult, StreamBatch, StreamConfig,
    StreamList, StreamMeta, StreamRecord, StreamWatermarks, UpdateStreamCommand,
};
use crate::waiter::StreamWaiterRegistry;

const DEFAULT_LIST_LIMIT: usize = 1000;
const MAX_LIST_LIMIT: usize = 10_000;
const TAIL_MAX_BYTES: usize = 4 * 1024 * 1024;
const TAIL_MAX_RECORDS: usize = 4096;
/// How long the transfer target holds an open attempt before giving up on a
/// pending transfer settling.
const TRANSFER_SETTLE_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSFER_SETTLE_POLL: Duration = Duration::from_millis(50);
/// Records between durable producer-state checkpoints.
const PRODUCER_CHECKPOINT_INTERVAL: u64 = 4096;
/// Live objects a stream may accumulate before an out-of-schedule compaction.
const COMPACTION_LIVE_OBJECT_THRESHOLD: usize = 64;

/// Recent appended records kept in memory so tail reads skip the engine fetch.
#[derive(Default)]
pub(crate) struct TailCache {
    recent: VecDeque<StreamRecord>,
    recent_bytes: usize,
}

impl TailCache {
    fn record_append(&mut self, base_offset: u64, records: &[Bytes]) {
        if records.is_empty() {
            return;
        }
        if let Some(last) = self.recent.back() {
            let expected = last.offset.record_offset() + 1;
            if base_offset < expected {
                return;
            }
            if base_offset > expected {
                self.recent.clear();
                self.recent_bytes = 0;
            }
        }
        for (i, bytes) in records.iter().enumerate() {
            self.recent.push_back(StreamRecord {
                offset: OffsetToken::of_record_offset(base_offset + i as u64),
                payload: bytes.clone(),
            });
            self.recent_bytes += bytes.len();
        }
        while self.recent.len() > TAIL_MAX_RECORDS
            || (self.recent_bytes > TAIL_MAX_BYTES && self.recent.len() > 1)
        {
            if let Some(dropped) = self.recent.pop_front() {
                self.recent_bytes -= dropped.payload.len();
            }
        }
    }

    fn tail_records(&self, start: u64) -> Option<Vec<StreamRecord>> {
        let first = self.recent.front()?.offset.record_offset();
        let last = self.recent.back()?.offset.record_offset();
        if start < first || start > last {
            return None;
        }
        Some(
            self.recent
                .iter()
                .filter(|r| r.offset.record_offset() >= start)
                .cloned()
                .collect(),
        )
    }

    fn reset(&mut self) {
        self.recent.clear();
        self.recent_bytes = 0;
    }
}

/// Per-name gate: the operation lock plus the tail cache it protects.
struct Gate {
    op: tokio::sync::Mutex<()>,
    tail: Mutex<TailCache>,
}

pub struct S3StreamService {
    stream_client: Arc<dyn StreamClient>,
    kv_client: Arc<dyn KVClient>,
    views: Arc<ViewPublisher>,
    node: MetadataNodeHandle,
    waiters: Arc<StreamWaiterRegistry>,
    open_streams: Mutex<HashMap<u64, Arc<dyn Stream>>>,
    local_epochs: Mutex<HashMap<u64, u64>>,
    gates: Mutex<HashMap<String, Arc<Gate>>>,
    entry_cache: Mutex<HashMap<String, RegistryEntry>>,
    open_lock: tokio::sync::Mutex<()>,
    schema_registry: Option<Arc<dyn pico_schema::SchemaStore>>,
}

/// Stream names under `/_sys/`, `/_schemas/`, and `/_streams/` are reserved.
pub fn is_reserved_name(name: &str) -> bool {
    name == "/_sys"
        || name.starts_with("/_sys/")
        || name == "/_schemas"
        || name.starts_with("/_schemas/")
        || name == "/_streams"
        || name.starts_with("/_streams/")
}

impl S3StreamService {
    pub fn new(
        stream_client: Arc<dyn StreamClient>,
        kv_client: Arc<dyn KVClient>,
        views: Arc<ViewPublisher>,
        node: MetadataNodeHandle,
        waiters: Arc<StreamWaiterRegistry>,
    ) -> Self {
        Self {
            stream_client,
            kv_client,
            views,
            node,
            waiters,
            open_streams: Mutex::new(HashMap::new()),
            local_epochs: Mutex::new(HashMap::new()),
            gates: Mutex::new(HashMap::new()),
            entry_cache: Mutex::new(HashMap::new()),
            open_lock: tokio::sync::Mutex::new(()),
            schema_registry: None,
        }
    }

    pub fn with_schema_registry(mut self, registry: Arc<dyn pico_schema::SchemaStore>) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    pub fn schema_registry(&self) -> Option<&Arc<dyn pico_schema::SchemaStore>> {
        self.schema_registry.as_ref()
    }

    pub fn waiters(&self) -> Arc<StreamWaiterRegistry> {
        self.waiters.clone()
    }

    fn schema_registry_required(&self) -> Result<&Arc<dyn pico_schema::SchemaStore>, ServiceError> {
        self.schema_registry
            .as_ref()
            .ok_or_else(|| schema_error("schema registry is not configured"))
    }

    /// Fail-closed: a bound stream on a node without a registry, or whose
    /// schema is gone from the registry, rejects writes rather than skipping
    /// validation.
    pub async fn validate_schema(
        &self,
        name: &str,
        batch: &pico_schema::Batch,
    ) -> Result<(), ServiceError> {
        let registry = self.schema_registry_required()?;
        let schema = registry
            .schema(name)
            .await
            .map_err(|e| schema_error(e.to_string()))?
            .ok_or_else(|| {
                schema_error(format!("bound schema {name} is missing from the registry"))
            })?;
        schema
            .validate(batch)
            .map_err(|e| schema_error(e.to_string()))
    }

    pub async fn put_schema(
        &self,
        name: &str,
        format: pico_schema::SchemaFormat,
        bytes: bytes::Bytes,
    ) -> Result<(), ServiceError> {
        let registry = self.schema_registry_required()?;
        registry
            .put(name, format, bytes)
            .await
            .map_err(|e| schema_error(e.to_string()))
    }

    pub async fn get_schema(
        &self,
        name: &str,
    ) -> Result<Option<(pico_schema::SchemaFormat, bytes::Bytes)>, ServiceError> {
        let registry = self.schema_registry_required()?;
        registry
            .get(name)
            .await
            .map_err(|e| schema_error(e.to_string()))
    }

    pub async fn delete_schema(&self, name: &str) -> Result<bool, ServiceError> {
        let registry = self.schema_registry_required()?;
        registry
            .delete(name)
            .await
            .map_err(|e| schema_error(e.to_string()))
    }

    pub async fn validation_schema_of(&self, name: &str) -> Result<Option<String>, ServiceError> {
        if self.schema_registry.is_none() {
            return Ok(None);
        }
        Ok(self
            .get_entry(&normalize(name), false)
            .await?
            .filter(|entry| entry.schema_validate)
            .and_then(|entry| entry.schema_name))
    }

    async fn require_schema(&self, schema_name: &str) -> Result<(), ServiceError> {
        let registry = self.schema_registry_required()?;
        registry
            .schema(schema_name)
            .await
            .map_err(|e| schema_error(e.to_string()))?
            .map(|_| ())
            .ok_or_else(|| schema_error(format!("unknown schema {schema_name}")))
    }

    pub async fn stream_config(&self, name: &str) -> Result<Option<StreamConfig>, ServiceError> {
        let name = normalize(name);
        Ok(self
            .get_entry(&name, false)
            .await?
            .map(|entry| stream_config_of(&name, &entry)))
    }

    pub async fn update_stream(
        &self,
        command: UpdateStreamCommand,
    ) -> Result<StreamConfig, ServiceError> {
        command.validate()?;
        let name = normalize(&command.name);
        if is_reserved_name(&name) {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "reserved stream name prefix",
            ));
        }
        let gate = self.gate_of(&name);
        let _op = gate.op.lock().await;
        let mut entry = self
            .get_entry(&name, false)
            .await?
            .ok_or_else(|| ServiceError::kind(ErrorKind::NotFound))?;

        if let Some(schema_name) = &command.schema_name {
            match schema_name {
                Some(schema_name) => {
                    self.require_schema(schema_name).await?;
                    entry.schema_name = Some(schema_name.clone());
                }
                None => {
                    entry.schema_name = None;
                    entry.schema_validate = false;
                }
            }
        }
        if let Some(validate) = command.schema_validate {
            entry.schema_validate = validate;
        }
        if entry.schema_name.is_none() {
            entry.schema_validate = false;
        }

        self.put_entry(&name, entry.clone()).await?;
        Ok(stream_config_of(&name, &entry))
    }

    pub async fn create(&self, mut command: CreateCommand) -> Result<CreateResult, ServiceError> {
        if command.schema_name.is_none() {
            command.schema_validate = false;
        }
        command.validate()?;
        let name = normalize(&command.name);
        if is_reserved_name(&name) && !command.internal {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "reserved stream name prefix",
            ));
        }
        let gate = self.gate_of(&name);
        let _op = gate.op.lock().await;

        if let Some(existing) = self.get_entry(&name, false).await? {
            if !config_matches(&existing, &command) {
                return Err(ServiceError::kind(ErrorKind::Conflict));
            }
            let meta = self.to_meta_live(&name, &existing).await?;
            return Ok(CreateResult {
                created: false,
                meta,
            });
        }

        // Only a real create checks the bind, so replays of an existing
        // matching create stay idempotent even if the schema was deleted.
        if let Some(schema_name) = command.schema_name.as_deref() {
            self.require_schema(schema_name).await?;
        }

        let stream = self.provision_stream(&name).await?;
        let deadline = deadline_of(command.ttl_seconds, command.expires_at_ms);
        let candidate = RegistryEntry {
            stream_id: stream.stream_id(),
            content_type: command.content_type.clone(),
            ttl_seconds: command.ttl_seconds,
            expires_at_ms: command.expires_at_ms,
            closed: command.closed,
            deadline_ms: deadline,
            last_seq: None,
            producers: Default::default(),
            closed_by: None,
            external_id: command.external_id.unwrap_or([0; 16]),
            numeric_producers: Default::default(),
            producer_state_offset: 0,
            schema_name: command.schema_name.clone(),
            schema_validate: command.schema_validate,
        };
        let stored = self
            .kv_client
            .put_kv_if_absent(KeyValue {
                key: name.clone(),
                value: candidate.encode(),
            })
            .await?;
        let mut current = RegistryEntry::decode(&stored)?;
        self.entry_cache
            .lock()
            .unwrap()
            .insert(name.clone(), current.clone());
        self.write_stream_index(&name, &current).await?;

        if current.stream_id != candidate.stream_id {
            return self
                .resolve_lost_race(&name, stream, current, &command)
                .await;
        }

        if self
            .append_initial_payload(&name, &gate, &stream, &command)
            .await?
        {
            current = self.require_entry(&name).await?;
        }
        if command.closed && !current.closed {
            self.put_entry(&name, current.close(None)).await?;
            current = self.require_entry(&name).await?;
        }

        let meta = to_meta_from_stream(&name, &current, stream.as_ref());
        Ok(CreateResult {
            created: true,
            meta,
        })
    }

    pub async fn head(&self, name: &str) -> Result<Option<StreamMeta>, ServiceError> {
        let name = normalize(name);
        match self.get_entry(&name, false).await? {
            None => Ok(None),
            Some(entry) => Ok(Some(self.to_meta_live(&name, &entry).await?)),
        }
    }

    /// Registry-backed metadata without opening the stream, so it answers
    /// for streams owned by other nodes. Offsets are the committed values.
    pub async fn describe(&self, name: &str) -> Result<Option<StreamMeta>, ServiceError> {
        let name = normalize(name);
        let Some(entry) = self.get_entry(&name, false).await? else {
            return Ok(None);
        };
        let committed = {
            let view = self.views.load();
            view.state
                .get_streams(&[entry.stream_id])
                .into_iter()
                .next()
        };
        Ok(Some(to_meta_from_committed(
            &name,
            &entry,
            committed.as_ref(),
        )))
    }

    pub async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<StreamList, ServiceError> {
        let max = if limit > 0 {
            limit.min(MAX_LIST_LIMIT)
        } else {
            DEFAULT_LIST_LIMIT
        };
        let prefix = normalize(prefix);
        let now = now_ms();

        // Bounded pages from one view snapshot: cost tracks the response
        // size, not the registry size.
        let view = self.views.load();
        let mut cursor = start_after
            .filter(|after| !after.is_empty())
            .map(str::to_owned);
        let mut selected: Vec<(String, RegistryEntry)> = Vec::new();
        'pages: loop {
            let page =
                view.state
                    .list_kv_page(&prefix, cursor.as_deref(), max + 1 - selected.len());
            let exhausted = page.len() < max + 1 - selected.len();
            for (key, value) in page {
                cursor = Some(key.clone());
                let Ok(entry) = RegistryEntry::decode(&value) else {
                    continue;
                };
                if entry.deadline_ms > 0 && now > entry.deadline_ms {
                    continue;
                }
                selected.push((key, entry));
                if selected.len() > max {
                    break 'pages;
                }
            }
            if exhausted {
                break;
            }
        }

        let ids: Vec<u64> = selected
            .iter()
            .take(max)
            .map(|(_, e)| e.stream_id)
            .collect();
        let committed: HashMap<u64, s3stream::StreamMetadata> = {
            let view = self.views.load();
            view.state
                .get_streams(&ids)
                .into_iter()
                .map(|m| (m.stream_id, m))
                .collect()
        };
        let has_more = selected.len() > max;
        let streams = selected
            .into_iter()
            .take(max)
            .map(|(name, entry)| {
                let meta = committed.get(&entry.stream_id);
                to_meta_from_committed(&name, &entry, meta)
            })
            .collect();
        Ok(StreamList { streams, has_more })
    }

    pub async fn close(&self, name: &str) -> Result<CloseResult, ServiceError> {
        let result = self
            .append(AppendCommand {
                name: name.to_owned(),
                close_after: true,
                ..Default::default()
            })
            .await?;
        Ok(CloseResult {
            next_offset: result.next_offset,
        })
    }

    pub async fn delete(&self, name: &str) -> Result<bool, ServiceError> {
        let name = normalize(name);
        let gate = self.gate_of(&name);
        let _op = gate.op.lock().await;

        let Some(entry) = self.get_entry(&name, false).await? else {
            return Ok(false);
        };
        let deleted = self.kv_client.del_kv(&name).await?;
        self.entry_cache.lock().unwrap().remove(&name);
        if deleted.is_none() {
            return Ok(false);
        }
        self.remove_stream_index(&entry).await;
        self.destroy_stream(entry.stream_id).await;
        gate.tail.lock().unwrap().reset();
        self.waiters.notify_closed(&name);
        Ok(true)
    }

    pub async fn trim(&self, name: &str, new_start_offset: u64) -> Result<u64, ServiceError> {
        let name = normalize(name);
        let gate = self.gate_of(&name);
        let _op = gate.op.lock().await;

        let Some(entry) = self.get_entry(&name, false).await? else {
            return Err(ServiceError::kind(ErrorKind::NotFound));
        };
        let stream = self.ensure_open(entry.stream_id).await?;
        let committed_end = {
            let view = self.views.load();
            view.state
                .get_stream(entry.stream_id)
                .map(|m| m.end_offset)
                .unwrap_or(0)
        };
        let lower = live_start_offset(stream.as_ref());
        let clamped = new_start_offset
            .max(lower)
            .min(stream.confirm_offset().min(committed_end));
        if clamped > lower {
            stream.trim(clamped).await?;
        }
        Ok(live_start_offset(stream.as_ref()))
    }

    /// Appends records and returns only after they are durable.
    pub async fn append(&self, command: AppendCommand) -> Result<AppendResult, ServiceError> {
        let command = command.normalized();
        let name = normalize(&command.name);
        let gate = self.gate_of(&name);
        let _op = gate.op.lock().await;

        let Some(entry) = self.get_entry(&name, false).await? else {
            return Err(ServiceError::kind(ErrorKind::NotFound));
        };
        let stream = self.ensure_open(entry.stream_id).await?;
        let mut next = OffsetToken::of_record_offset(stream.next_offset());

        let decision = command
            .producer
            .as_ref()
            .map(|p| validate_producer(&entry, p));
        if let Some(decision) = decision {
            if !matches!(decision, ProducerDecision::Accepted { .. }) {
                return self.handle_producer_reject(&entry, next, decision, &command);
            }
        }
        if entry.closed {
            return append_to_closed(&entry, &command, next);
        }
        if let Some(match_seq) = command.match_seq {
            if match_seq != next.record_offset() {
                return Err(ServiceError::with_message(
                    ErrorKind::MatchFailed,
                    Some(next),
                    false,
                    format!(
                        "match failed: expected tail {match_seq}, actual {}",
                        next.record_offset()
                    ),
                ));
            }
        }

        let close_only = command.payload_len() == 0 && command.close_after;
        validate_payload(&entry, &command, decision, next, close_only)?;

        let base_offset = next.record_offset();
        let messages: Vec<Bytes> = if close_only {
            Vec::new()
        } else {
            expand_payloads(&entry.content_type, &command.payloads)?
        };
        if entry.schema_validate && !messages.is_empty() && self.schema_registry.is_some() {
            if let Some(schema_name) = entry.schema_name.as_deref() {
                let batch = schema_batch_from_messages(&entry.content_type, &messages)?;
                self.validate_schema(schema_name, &batch).await?;
            }
        }

        let pendings = match self.submit_messages(&stream, &messages, command.atomic) {
            Ok(pendings) => pendings,
            Err(e) => {
                self.open_streams.lock().unwrap().remove(&entry.stream_id);
                gate.tail.lock().unwrap().reset();
                return Err(ServiceError::durability(e));
            }
        };
        if !messages.is_empty() {
            next = OffsetToken::of_record_offset(stream.next_offset());
        }

        let updated = apply_append_state(entry.clone(), &command, decision);
        let echoed_seq = command.producer.as_ref().map(|p| p.seq);
        let echoed_epoch = command.producer.as_ref().map(|p| p.epoch);
        let result = AppendResult {
            next_offset: next,
            applied: !close_only,
            closed: command.close_after,
            producer_epoch: echoed_epoch,
            producer_seq: echoed_seq,
        };
        let notify_offset = next.record_offset();

        if command.close_after {
            // Rare path: durability must precede the registry close marker, so
            // this stays under the gate.
            if let Err(e) = await_durable(pendings).await {
                self.open_streams.lock().unwrap().remove(&entry.stream_id);
                gate.tail.lock().unwrap().reset();
                return Err(ServiceError::durability(e));
            }
            if !close_only {
                gate.tail
                    .lock()
                    .unwrap()
                    .record_append(base_offset, &messages);
                self.waiters.notify_append(&name, notify_offset);
            }
            self.close_entry(&name, updated, &command).await?;
            return Ok(result);
        }

        // Apply this producer/registry update at submit time, before
        // durability.
        let touched = touch_deadline(updated);
        if touched != entry {
            self.put_entry(&name, touched).await?;
        }

        // Drop the gate before awaiting durability. That pipelining lets
        // queued requests on the same stream share WAL group commits.
        drop(_op);
        if let Err(e) = await_durable(pendings).await {
            self.open_streams.lock().unwrap().remove(&entry.stream_id);
            gate.tail.lock().unwrap().reset();
            return Err(ServiceError::durability(e));
        }

        // Post-durability bookkeeping runs without the gate, so two pipelined
        // requests may reach here out of submit order.`record_append`
        // tolerates the resulting gap (it restarts the window) and waiter
        // notification is monotonic in the offset it publishes.
        gate.tail
            .lock()
            .unwrap()
            .record_append(base_offset, &messages);
        self.waiters.notify_append(&name, notify_offset);
        Ok(result)
    }

    /// Append a batch payload verbatim, patching each contained batch's
    /// base-offset field to the assigned offset. Durable on return.
    pub async fn append_batch(
        &self,
        command: AppendBatchCommand,
    ) -> Result<AppendBatchResult, ServiceError> {
        validate_batch_spans(&command)?;
        let record_count = command.record_count();
        let name = normalize(&command.name);
        let gate = self.gate_of(&name);
        let _op = gate.op.lock().await;

        let Some(mut entry) = self.get_entry(&name, false).await? else {
            return Err(ServiceError::kind(ErrorKind::NotFound));
        };
        if entry.closed {
            return Err(ServiceError::kind(ErrorKind::Closed));
        }
        let stream_id = entry.stream_id;
        let stream = self.ensure_open(stream_id).await?;
        let log_start_offset = live_start_offset(stream.as_ref());
        let now = now_ms();
        self.recover_producers(&name, &mut entry, stream.as_ref(), &command, now)
            .await?;
        producer::expire_producers(&mut entry, now);

        let accepted = match producer::admit(&mut entry, command.producer, record_count, now) {
            Ok(Admission::Accepted(accepted)) => accepted,
            Ok(Admission::Duplicate { base_offset }) => {
                self.cache_entry(&name, touch_deadline(entry));
                return Ok(AppendBatchResult {
                    base_offset,
                    duplicate: true,
                    log_start_offset,
                });
            }
            Err(error) => {
                self.cache_entry(&name, touch_deadline(entry));
                return Err(error);
            }
        };

        // Submits happen only under this gate, so `next_offset` read here is
        // exactly the base offset the submit below will be assigned. Patch
        // the batch headers with it before the payload reaches the engine.
        let base_offset = stream.next_offset();
        let payload = patch_base_offsets(&command, base_offset);
        let pending = match Arc::clone(&stream).submit_append(
            AppendContext::default(),
            RecordBatch::new(record_count, command.base_timestamp_ms, payload),
        ) {
            Ok(pending) => pending,
            Err(e) => {
                self.open_streams.lock().unwrap().remove(&stream_id);
                return Err(ServiceError::durability(e));
            }
        };
        debug_assert_eq!(pending.base_offset(), base_offset);

        producer::record(&mut entry, accepted, base_offset, now);
        // A failed checkpoint only lengthens the takeover rescan, so it
        // must not fail the append.
        let prev_offset = entry.producer_state_offset;
        entry.producer_state_offset = stream.next_offset();
        let entry = touch_deadline(entry);
        self.cache_entry(&name, entry.clone());
        if !entry.numeric_producers.is_empty()
            && prev_offset / PRODUCER_CHECKPOINT_INTERVAL
                != entry.producer_state_offset / PRODUCER_CHECKPOINT_INTERVAL
        {
            if let Err(error) = self.put_entry(&name, entry).await {
                tracing::warn!(%error, stream = %name, "producer state checkpoint failed");
            }
        }

        let notify_offset = stream.next_offset();
        drop(_op);
        if let Err(e) = await_durable(vec![pending]).await {
            self.open_streams.lock().unwrap().remove(&stream_id);
            return Err(ServiceError::durability(e));
        }
        self.waiters.notify_append(&name, notify_offset);
        Ok(AppendBatchResult {
            base_offset,
            duplicate: false,
            log_start_offset,
        })
    }

    /// Read stored batches verbatim starting at the batch covering `from`.
    /// No per-record decode and no gate: payloads stream out as the zero-copy
    /// `Bytes` the engine returned.
    pub async fn read_batches(
        &self,
        name: &str,
        from: u64,
        max_bytes: usize,
    ) -> Result<BatchReadResult, ServiceError> {
        let name = normalize(name);
        let Some(entry) = self.get_entry(&name, false).await? else {
            return Err(ServiceError::kind(ErrorKind::NotFound));
        };
        let stream = self.ensure_open(entry.stream_id).await?;
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
        // The engine returns the batch covering `from` even when `from`
        // falls mid-batch. Clients skip leading records themselves.
        let fetch = stream
            .fetch(FetchContext::default(), from, high_watermark, max_bytes)
            .await?;
        let batches: Vec<StreamBatch> = fetch
            .records
            .into_iter()
            .map(|batch| StreamBatch {
                base_offset: batch.base_offset,
                last_offset: batch.last_offset,
                count: batch.count,
                payload: batch.payload,
            })
            .collect();
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

    /// Trim watermark and confirm offset, for Fetch and ListOffsets replies.
    pub async fn watermarks(&self, name: &str) -> Result<StreamWatermarks, ServiceError> {
        let name = normalize(name);
        let Some(entry) = self.get_entry(&name, false).await? else {
            return Err(ServiceError::kind(ErrorKind::NotFound));
        };
        let stream = self.ensure_open(entry.stream_id).await?;
        Ok(StreamWatermarks {
            log_start_offset: live_start_offset(stream.as_ref()),
            high_watermark: stream.confirm_offset(),
        })
    }

    pub async fn read(
        &self,
        name: &str,
        from: OffsetToken,
        max_bytes: usize,
        max_records: usize,
    ) -> Result<ReadResult, ServiceError> {
        let name = normalize(name);
        let gate = self.gate_of(&name);
        let _op = gate.op.lock().await;

        let Some(entry) = self.get_entry(&name, true).await? else {
            return Err(ServiceError::kind(ErrorKind::NotFound));
        };
        let stream = self.ensure_open(entry.stream_id).await?;
        let start = from.record_offset();
        let end = stream.confirm_offset();
        if start > end {
            return Err(ServiceError::kind(ErrorKind::BadRequest));
        }
        if start == end {
            return Ok(ReadResult {
                records: Vec::new(),
                content_type: entry.content_type,
                next_offset: OffsetToken::of_record_offset(end),
                up_to_date: true,
                closed: entry.closed,
            });
        }
        let max_bytes = if max_bytes > 0 { max_bytes } else { usize::MAX };
        let max_records = if max_records > 0 {
            max_records
        } else {
            usize::MAX
        };

        let cached = gate.tail.lock().unwrap().tail_records(start);
        if let Some(cached) = cached {
            return Ok(tail_read(&entry, &cached, end, max_bytes, max_records));
        }
        self.fetch_records(&entry, stream.as_ref(), start, end, max_bytes, max_records)
            .await
    }

    /// Park until data past `from` is durable, the stream closes, or `timeout`
    /// lapses.
    ///
    /// (named `wait_appended`, `await` is a Rust
    /// fn already is one.
    pub async fn wait_appended(
        &self,
        name: &str,
        from: OffsetToken,
        timeout: Duration,
    ) -> Result<bool, ServiceError> {
        let name = normalize(name);
        let Some(entry) = self.get_entry(&name, false).await? else {
            return Ok(false);
        };
        if entry.closed {
            return Ok(true);
        }
        let stream = self.ensure_open(entry.stream_id).await?;
        if stream.confirm_offset() > from.record_offset() {
            return Ok(true);
        }
        Ok(self.waiters.wait(&name, from, timeout).await)
    }

    pub async fn lookup_stream_id(&self, name: &str) -> Result<Option<u64>, ServiceError> {
        Ok(self
            .get_entry(&normalize(name), false)
            .await?
            .map(|e| e.stream_id))
    }

    /// Resolve a caller-assigned external id to its stream name via the
    /// `idx/extid/` record: one point read plus one entry verification.
    pub async fn lookup_by_external_id(
        &self,
        external_id: [u8; 16],
    ) -> Result<Option<String>, ServiceError> {
        if external_id == [0u8; 16] {
            return Ok(None);
        }
        let Some(value) = self
            .kv_client
            .get_kv(&external_id_key(&external_id))
            .await?
        else {
            return Ok(None);
        };
        let Ok(name) = String::from_utf8(value.to_vec()) else {
            return Ok(None);
        };
        match self.get_entry(&name, false).await? {
            Some(entry) if entry.external_id == external_id => Ok(Some(name)),
            _ => Ok(None),
        }
    }

    pub fn open_stream_snapshot(&self) -> Vec<Arc<dyn Stream>> {
        self.open_streams
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub async fn shutdown(&self) {
        self.waiters.clear();
        // Each close may wait a WAL upload cycle, so a serial pass over many
        // open streams would take hours.
        let mut streams = self.open_stream_snapshot().into_iter();
        let mut inflight = tokio::task::JoinSet::new();
        loop {
            while inflight.len() < 1024 {
                let Some(stream) = streams.next() else { break };
                inflight.spawn(async move {
                    let _ = stream.close().await;
                });
            }
            if inflight.join_next().await.is_none() {
                break;
            }
        }
        self.open_streams.lock().unwrap().clear();
    }

    /// Walks the object catalog one page per tick and compacts streams past
    /// [`COMPACTION_LIVE_OBJECT_THRESHOLD`]. The engine no-ops for streams
    /// not open on this node. One trigger per tick, cursor skips a triggered
    /// stream until the next full rotation.
    pub fn spawn_compaction_check(
        self: &Arc<Self>,
        tick: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        const PAGE: usize = 4096;
        let service = self.clone();
        tokio::spawn(async move {
            let mut cursor: Option<pico_metadata::StreamOffsetKey> = None;
            let mut run: (u64, usize) = (u64::MAX, 0);
            loop {
                tokio::time::sleep(tick).await;
                let page = service
                    .views
                    .load()
                    .state
                    .stream_object_keys_page(cursor, PAGE);
                let exhausted = page.len() < PAGE;
                for key in page {
                    let (stream_id, _, _) = key;
                    if stream_id != run.0 {
                        run = (stream_id, 0);
                    }
                    run.1 += 1;
                    cursor = Some(key);
                    if run.1 >= COMPACTION_LIVE_OBJECT_THRESHOLD {
                        if let Err(error) = service
                            .stream_client
                            .compact_stream(stream_id, s3stream::CompactionLevel::MinorV1)
                            .await
                        {
                            tracing::debug!(%error, stream_id, "triggered compaction failed");
                        }
                        cursor = Some((stream_id, u64::MAX, u64::MAX));
                        run = (u64::MAX, 0);
                        break;
                    }
                }
                if exhausted {
                    cursor = None;
                    run = (u64::MAX, 0);
                }
            }
        })
    }

    /// Lease-holder backstop for TTL'd streams nothing touches: walks the
    /// registry one bounded page per tick and expires lapsed entries. Lazy
    /// expiry on access stays the fast path; this reclaims the idle rest at
    /// flat per-tick cost regardless of registry size.
    pub fn spawn_ttl_sweep(
        self: &Arc<Self>,
        mut leadership: tokio::sync::watch::Receiver<bool>,
        tick: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        const PAGE: usize = 256;
        let service = self.clone();
        tokio::spawn(async move {
            let mut cursor: Option<String> = None;
            loop {
                tokio::time::sleep(tick).await;
                if leadership.has_changed().is_err() {
                    return;
                }
                if !*leadership.borrow_and_update() {
                    continue;
                }
                let page = service
                    .views
                    .load()
                    .state
                    .list_kv_page("/", cursor.as_deref(), PAGE);
                // An empty or short page wraps the walk to the start.
                cursor = page.last().map(|(key, _)| key.clone());
                let now = now_ms();
                for (name, value) in page {
                    let Ok(entry) = RegistryEntry::decode(&value) else {
                        continue;
                    };
                    if entry.deadline_ms > 0 && now > entry.deadline_ms {
                        service.expire(&name).await;
                    }
                }
            }
        })
    }

    fn gate_of(&self, name: &str) -> Arc<Gate> {
        self.gates
            .lock()
            .unwrap()
            .entry(name.to_owned())
            .or_insert_with(|| {
                Arc::new(Gate {
                    op: tokio::sync::Mutex::new(()),
                    tail: Mutex::new(TailCache::default()),
                })
            })
            .clone()
    }

    async fn provision_stream(&self, name: &str) -> Result<Arc<dyn Stream>, ServiceError> {
        let stream = self
            .stream_client
            .create_and_open_stream(CreateStreamOptions {
                epoch: 1,
                tags: HashMap::from([("path".to_owned(), name.to_owned())]),
            })
            .await?;
        self.open_streams
            .lock()
            .unwrap()
            .insert(stream.stream_id(), stream.clone());
        self.local_epochs
            .lock()
            .unwrap()
            .insert(stream.stream_id(), stream.stream_epoch());
        Ok(stream)
    }

    async fn resolve_lost_race(
        &self,
        name: &str,
        stream: Arc<dyn Stream>,
        current: RegistryEntry,
        command: &CreateCommand,
    ) -> Result<CreateResult, ServiceError> {
        let _ = stream.destroy().await;
        self.open_streams
            .lock()
            .unwrap()
            .remove(&stream.stream_id());
        self.local_epochs
            .lock()
            .unwrap()
            .remove(&stream.stream_id());
        if !config_matches(&current, command) {
            return Err(ServiceError::kind(ErrorKind::Conflict));
        }
        let meta = self.to_meta_live(name, &current).await?;
        Ok(CreateResult {
            created: false,
            meta,
        })
    }

    async fn append_initial_payload(
        &self,
        name: &str,
        gate: &Gate,
        stream: &Arc<dyn Stream>,
        command: &CreateCommand,
    ) -> Result<bool, ServiceError> {
        if command.initial_payload.is_empty() {
            return Ok(false);
        }
        let messages = split_messages(&command.content_type, &command.initial_payload, true)?;
        if messages.is_empty() {
            return Ok(false);
        }
        let base_offset = stream.next_offset();
        let pendings = self
            .submit_messages(stream, &messages, false)
            .map_err(ServiceError::durability)?;
        await_durable(pendings)
            .await
            .map_err(ServiceError::durability)?;
        gate.tail
            .lock()
            .unwrap()
            .record_append(base_offset, &messages);
        self.waiters.notify_append(name, stream.next_offset());
        Ok(true)
    }

    fn submit_messages(
        &self,
        stream: &Arc<dyn Stream>,
        messages: &[Bytes],
        atomic: bool,
    ) -> Result<Vec<PendingAppend>, s3stream::Error> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        if atomic && messages.len() > 1 {
            let framed = framing::encode_frames(messages);
            let pending = Arc::clone(stream).submit_append(
                AppendContext::default(),
                RecordBatch::new(messages.len() as u32, now_ms(), framed),
            )?;
            return Ok(vec![pending]);
        }
        messages
            .iter()
            .map(|message| {
                Arc::clone(stream).submit_append(
                    AppendContext::default(),
                    RecordBatch::new(1, now_ms(), message.clone()),
                )
            })
            .collect()
    }

    fn handle_producer_reject(
        &self,
        entry: &RegistryEntry,
        next: OffsetToken,
        decision: ProducerDecision,
        command: &AppendCommand,
    ) -> Result<AppendResult, ServiceError> {
        match decision {
            ProducerDecision::Duplicate { last_seq } => Ok(AppendResult {
                next_offset: next,
                applied: false,
                closed: entry.closed,
                producer_epoch: command.producer.as_ref().map(|p| p.epoch),
                producer_seq: Some(last_seq),
            }),
            ProducerDecision::StaleEpoch { current_epoch } => {
                Err(ServiceError::fenced(current_epoch))
            }
            ProducerDecision::InvalidEpochSeq => Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                Some(next),
                false,
                "New epoch must start with sequence 0",
            )),
            ProducerDecision::SequenceGap { expected, received } => {
                Err(ServiceError::sequence_gap(expected, received))
            }
            ProducerDecision::Accepted { .. } => Err(ServiceError::kind(ErrorKind::BadRequest)),
        }
    }

    async fn close_entry(
        &self,
        name: &str,
        entry: RegistryEntry,
        command: &AppendCommand,
    ) -> Result<(), ServiceError> {
        let closed_by = command.producer.as_ref().map(|p| ClosedBy {
            producer_id: p.producer_id.clone(),
            epoch: p.epoch,
            seq: p.seq,
        });
        self.put_entry(name, touch_deadline(entry.close(closed_by)))
            .await?;
        self.waiters.notify_closed(name);
        Ok(())
    }

    async fn fetch_records(
        &self,
        entry: &RegistryEntry,
        stream: &dyn Stream,
        start: u64,
        end: u64,
        max_bytes: usize,
        max_records: usize,
    ) -> Result<ReadResult, ServiceError> {
        let fetch = stream
            .fetch(FetchContext::default(), start, end, max_bytes)
            .await?;
        let mut records: Vec<StreamRecord> = Vec::new();
        let mut total = 0usize;
        let mut next = start;

        'outer: for batch in &fetch.records {
            if batch.count > 1 {
                let frames = framing::decode_frames(&batch.payload, batch.count)?;
                for (i, frame) in frames.into_iter().enumerate() {
                    let offset = batch.base_offset + i as u64;
                    if offset < start {
                        continue;
                    }
                    if total + frame.len() > max_bytes && total > 0 {
                        break 'outer;
                    }
                    total += frame.len();
                    next = offset + 1;
                    records.push(StreamRecord {
                        offset: OffsetToken::of_record_offset(offset),
                        payload: frame,
                    });
                    if total >= max_bytes || records.len() >= max_records {
                        break 'outer;
                    }
                }
                continue;
            }

            let bytes = batch.payload.clone();
            if total + bytes.len() > max_bytes && total > 0 {
                break;
            }
            total += bytes.len();
            next = batch.last_offset;
            records.push(StreamRecord {
                offset: OffsetToken::of_record_offset(batch.base_offset),
                payload: bytes,
            });
            if total >= max_bytes || records.len() >= max_records {
                break;
            }
        }

        Ok(ReadResult {
            records,
            content_type: entry.content_type.clone(),
            next_offset: OffsetToken::of_record_offset(next),
            up_to_date: next >= end,
            closed: entry.closed,
        })
    }

    pub(crate) async fn ensure_open(
        &self,
        stream_id: u64,
    ) -> Result<Arc<dyn Stream>, ServiceError> {
        if let Some(existing) = self.open_streams.lock().unwrap().get(&stream_id) {
            return Ok(existing.clone());
        }
        let _open = self.open_lock.lock().await;
        if let Some(existing) = self.open_streams.lock().unwrap().get(&stream_id) {
            return Ok(existing.clone());
        }
        self.await_transfer_settled(stream_id).await?;
        let epoch = self.next_epoch(stream_id);
        let opened = self
            .stream_client
            .open_stream(
                stream_id,
                OpenStreamOptions {
                    epoch,
                    ..Default::default()
                },
            )
            .await?;
        self.open_streams
            .lock()
            .unwrap()
            .insert(stream_id, opened.clone());
        self.local_epochs
            .lock()
            .unwrap()
            .insert(stream_id, opened.stream_epoch());
        Ok(opened)
    }

    /// Hold back a local open while the stream has a pending transfer. The
    /// transfer target waits for the completion to land so its first request
    /// stalls instead of failing. Any other node refuses immediately.
    async fn await_transfer_settled(&self, stream_id: u64) -> Result<(), ServiceError> {
        let deadline = tokio::time::Instant::now() + TRANSFER_SETTLE_TIMEOUT;
        loop {
            let view = self.views.load();
            let Some(pending) = view.state.pending_transfers.get(&stream_id).copied() else {
                return Ok(());
            };
            if pending.to_node != self.node.node_id() {
                return Err(ServiceError::with_message(
                    ErrorKind::Conflict,
                    None,
                    false,
                    format!(
                        "stream {stream_id} is transferring to node {}",
                        pending.to_node
                    ),
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ServiceError::with_message(
                    ErrorKind::Conflict,
                    None,
                    false,
                    format!("stream {stream_id} transfer did not settle in time"),
                ));
            }
            let _ = tokio::time::timeout(
                TRANSFER_SETTLE_POLL,
                self.views.wait_applied(view.applied_index + 1),
            )
            .await;
        }
    }

    /// Drain and close a locally held stream so a pending transfer can
    /// complete. Returns the epoch the stream closed at, or `None` when this
    /// process does not hold it open.
    pub async fn release_for_transfer(&self, stream_id: u64) -> Result<Option<i64>, ServiceError> {
        let name = self.name_of(stream_id).await;
        let gate = name.as_deref().map(|n| self.gate_of(n));
        let _op = match &gate {
            Some(gate) => Some(gate.op.lock().await),
            None => None,
        };

        let held = self.open_streams.lock().unwrap().remove(&stream_id);
        let Some(stream) = held else {
            return Ok(None);
        };
        self.local_epochs.lock().unwrap().remove(&stream_id);
        let epoch = stream.stream_epoch() as i64;
        stream.close().await?;
        if let (Some(name), Some(gate)) = (&name, &gate) {
            gate.tail.lock().unwrap().reset();
            self.waiters.notify_closed(name);
        }
        Ok(Some(epoch))
    }

    /// Reverse registry lookup via the `idx/sid/` record, verified against
    /// the entry it names.
    async fn name_of(&self, stream_id: u64) -> Option<String> {
        let value = self
            .kv_client
            .get_kv(&stream_id_key(stream_id))
            .await
            .ok()??;
        let name = String::from_utf8(value.to_vec()).ok()?;
        let entry = self.get_entry(&name, false).await.ok()??;
        (entry.stream_id == stream_id).then_some(name)
    }

    fn next_epoch(&self, stream_id: u64) -> u64 {
        let mut epochs = self.local_epochs.lock().unwrap();
        if let Some(epoch) = epochs.get_mut(&stream_id) {
            *epoch += 1;
            return *epoch;
        }
        let current = {
            let view = self.views.load();
            view.state
                .get_stream(stream_id)
                .map(|m| m.epoch)
                .unwrap_or(0)
        };
        let next = current + 1;
        epochs.insert(stream_id, next);
        next
    }

    async fn get_entry(
        &self,
        name: &str,
        touch: bool,
    ) -> Result<Option<RegistryEntry>, ServiceError> {
        let cached = self.entry_cache.lock().unwrap().get(name).cloned();
        let entry = match cached {
            Some(entry) => entry,
            None => {
                let Some(value) = self.kv_client.get_kv(name).await? else {
                    return Ok(None);
                };
                let entry = RegistryEntry::decode(&value)?;
                self.entry_cache
                    .lock()
                    .unwrap()
                    .insert(name.to_owned(), entry.clone());
                entry
            }
        };
        if entry.deadline_ms > 0 && now_ms() > entry.deadline_ms {
            self.expire(name).await;
            return Ok(None);
        }
        if touch {
            let refreshed = touch_deadline(entry.clone());
            if refreshed != entry {
                self.put_entry(name, refreshed.clone()).await?;
                return Ok(Some(refreshed));
            }
        }
        Ok(Some(entry))
    }

    /// TTL lapsed: conditionally drop the KV entry, destroy the stream,
    /// reset the tail, wake waiters. Rechecks the stored bytes and deletes
    /// only if they still match, so a concurrent touch (which rewrites the
    /// entry with a newer deadline) always wins over a stale observation.
    async fn expire(&self, name: &str) {
        self.entry_cache.lock().unwrap().remove(name);
        let Ok(Some(stored)) = self.kv_client.get_kv(name).await else {
            return;
        };
        let Ok(entry) = RegistryEntry::decode(&stored) else {
            return;
        };
        if entry.deadline_ms == 0 || now_ms() <= entry.deadline_ms {
            return;
        }
        let Ok(Some(_)) = self.kv_client.del_kv_if(name, &stored).await else {
            return;
        };
        self.remove_stream_index(&entry).await;
        self.destroy_stream(entry.stream_id).await;
        if let Some(gate) = self.gates.lock().unwrap().get(name) {
            gate.tail.lock().unwrap().reset();
        }
        self.waiters.notify_closed(name);
    }

    async fn destroy_stream(&self, stream_id: u64) {
        let held = self.open_streams.lock().unwrap().remove(&stream_id);
        self.local_epochs.lock().unwrap().remove(&stream_id);
        let stream = match held {
            Some(stream) => stream,
            None => {
                let epoch = self.next_epoch(stream_id);
                match self
                    .stream_client
                    .open_stream(
                        stream_id,
                        OpenStreamOptions {
                            epoch,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(stream) => stream,
                    Err(_) => return,
                }
            }
        };
        let _ = stream.destroy().await;
        self.local_epochs.lock().unwrap().remove(&stream_id);
    }

    async fn require_entry(&self, name: &str) -> Result<RegistryEntry, ServiceError> {
        self.get_entry(name, false).await?.ok_or_else(|| {
            ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                format!("missing registry entry for {name}"),
            )
        })
    }

    /// Update the in-memory registry view without a durable write.
    fn cache_entry(&self, name: &str, entry: RegistryEntry) {
        self.entry_cache
            .lock()
            .unwrap()
            .insert(name.to_owned(), entry);
    }

    /// Rebuild producer spans from stored batch headers between the last
    /// durable checkpoint and the confirmed tail.
    async fn recover_producers(
        &self,
        name: &str,
        entry: &mut RegistryEntry,
        stream: &dyn Stream,
        command: &AppendBatchCommand,
        now: i64,
    ) -> Result<(), ServiceError> {
        if command.producer.is_none() && entry.numeric_producers.is_empty() {
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
                producer::fold_stored_batch(entry, &batch.payload, batch.base_offset, now);
                cursor = cursor.max(batch.last_offset);
            }
        }
        entry.producer_state_offset = confirm;
        self.cache_entry(name, entry.clone());
        Ok(())
    }

    async fn put_entry(&self, name: &str, entry: RegistryEntry) -> Result<(), ServiceError> {
        self.kv_client
            .put_kv(KeyValue {
                key: name.to_owned(),
                value: entry.encode(),
            })
            .await?;
        self.entry_cache
            .lock()
            .unwrap()
            .insert(name.to_owned(), entry);
        Ok(())
    }

    /// Write the reverse-lookup records for a settled registry entry. Puts
    /// are idempotent, so racing creators that adopted the same winner write
    /// the same records.
    async fn write_stream_index(
        &self,
        name: &str,
        entry: &RegistryEntry,
    ) -> Result<(), ServiceError> {
        let value = Bytes::copy_from_slice(name.as_bytes());
        self.kv_client
            .put_kv(KeyValue {
                key: stream_id_key(entry.stream_id),
                value: value.clone(),
            })
            .await?;
        if entry.external_id != [0u8; 16] {
            self.kv_client
                .put_kv(KeyValue {
                    key: external_id_key(&entry.external_id),
                    value,
                })
                .await?;
        }
        Ok(())
    }

    /// Best-effort: a record left behind fails verification on read, so it
    /// is a miss, never a wrong answer.
    async fn remove_stream_index(&self, entry: &RegistryEntry) {
        let _ = self.kv_client.del_kv(&stream_id_key(entry.stream_id)).await;
        if entry.external_id != [0u8; 16] {
            let _ = self
                .kv_client
                .del_kv(&external_id_key(&entry.external_id))
                .await;
        }
    }

    async fn to_meta_live(
        &self,
        name: &str,
        entry: &RegistryEntry,
    ) -> Result<StreamMeta, ServiceError> {
        let stream = self.ensure_open(entry.stream_id).await?;
        Ok(to_meta_from_stream(name, entry, stream.as_ref()))
    }

    /// The metadata node identity this service runs as.
    pub fn node(&self) -> &MetadataNodeHandle {
        &self.node
    }
}

pub(crate) fn normalize(name: &str) -> String {
    if name.is_empty() || name == "/" {
        "/".to_owned()
    } else if name.starts_with('/') {
        name.to_owned()
    } else {
        format!("/{name}")
    }
}

// Reverse-lookup index records live beside registry entries in the KV plane:
// `idx/sid/<stream id>` and `idx/extid/<hex>` map back to the stream name.
// Stream names start with `/` and auth state with `auth/`, so `idx/` cannot
// collide. Readers verify the resolved entry before trusting a record.

fn stream_id_key(stream_id: u64) -> String {
    format!("idx/sid/{stream_id}")
}

fn external_id_key(id: &[u8; 16]) -> String {
    use std::fmt::Write;
    let mut key = String::with_capacity(42);
    key.push_str("idx/extid/");
    for byte in id {
        write!(key, "{byte:02x}").expect("write to String");
    }
    key
}

fn config_matches(entry: &RegistryEntry, command: &CreateCommand) -> bool {
    framing::mime_equals(Some(&entry.content_type), Some(&command.content_type))
        && entry.ttl_seconds == command.ttl_seconds
        && entry.expires_at_ms == command.expires_at_ms
        && entry.closed == command.closed
        && entry.schema_name == command.schema_name
        && entry.schema_validate == command.schema_validate
}

fn deadline_of(ttl_seconds: Option<u64>, expires_at_ms: Option<i64>) -> i64 {
    if let Some(expires) = expires_at_ms {
        return expires;
    }
    if let Some(ttl) = ttl_seconds {
        return now_ms() + ttl as i64 * 1000;
    }
    0
}

fn touch_deadline(entry: RegistryEntry) -> RegistryEntry {
    let Some(ttl) = entry.ttl_seconds else {
        return entry;
    };
    if entry.expires_at_ms.is_some() {
        return entry;
    }
    let next = now_ms() + ttl as i64 * 1000;
    let coarsen = (ttl as i64 * 100).max(1000);
    if entry.deadline_ms > 0 && next - entry.deadline_ms < coarsen {
        return entry;
    }
    entry.with_deadline(next)
}

fn validate_payload(
    entry: &RegistryEntry,
    command: &AppendCommand,
    decision: Option<ProducerDecision>,
    next: OffsetToken,
    close_only: bool,
) -> Result<(), ServiceError> {
    if !close_only {
        let ct = command.content_type.as_deref().unwrap_or("");
        if ct.is_empty() {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                Some(next),
                false,
                "missing Content-Type",
            ));
        }
        if !framing::mime_equals(Some(&entry.content_type), Some(ct)) {
            return Err(ServiceError::at(ErrorKind::Conflict, next, false));
        }
        if command.payload_len() == 0 {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                Some(next),
                false,
                "Empty body",
            ));
        }
    }
    if let Some(stream_seq) = &command.stream_seq {
        let accepted =
            decision.is_none() || matches!(decision, Some(ProducerDecision::Accepted { .. }));
        if accepted {
            if let Some(last_seq) = &entry.last_seq {
                if stream_seq.as_str() <= last_seq.as_str() {
                    return Err(ServiceError::with_message(
                        ErrorKind::Conflict,
                        Some(next),
                        false,
                        "Sequence conflict",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn append_to_closed(
    entry: &RegistryEntry,
    command: &AppendCommand,
    next: OffsetToken,
) -> Result<AppendResult, ServiceError> {
    let has_producer = command.producer.is_some();
    if command.close_after && command.payload_len() == 0 {
        if has_producer && !matches_closed_by(entry, command) && entry.closed_by.is_some() {
            return Err(ServiceError::at(ErrorKind::Closed, next, true));
        }
        let echoed_seq = if has_producer {
            Some(
                entry
                    .closed_by
                    .as_ref()
                    .map(|c| c.seq)
                    .unwrap_or_else(|| command.producer.as_ref().unwrap().seq),
            )
        } else {
            None
        };
        let echoed_epoch = command.producer.as_ref().map(|p| p.epoch);
        return Ok(AppendResult {
            next_offset: next,
            applied: false,
            closed: true,
            producer_epoch: echoed_epoch,
            producer_seq: echoed_seq,
        });
    }
    if has_producer && matches_closed_by(entry, command) {
        return Ok(AppendResult {
            next_offset: next,
            applied: false,
            closed: true,
            producer_epoch: command.producer.as_ref().map(|p| p.epoch),
            producer_seq: entry.closed_by.as_ref().map(|c| c.seq),
        });
    }
    Err(ServiceError::at(ErrorKind::Closed, next, true))
}

fn matches_closed_by(entry: &RegistryEntry, command: &AppendCommand) -> bool {
    match (&entry.closed_by, &command.producer) {
        (Some(closed_by), Some(producer)) => {
            closed_by.producer_id == producer.producer_id
                && closed_by.epoch == producer.epoch
                && closed_by.seq == producer.seq
        }
        _ => false,
    }
}

fn apply_append_state(
    entry: RegistryEntry,
    command: &AppendCommand,
    decision: Option<ProducerDecision>,
) -> RegistryEntry {
    let mut updated = entry;
    if let Some(seq) = &command.stream_seq {
        updated = updated.with_last_seq(seq.clone());
    }
    if let (Some(ProducerDecision::Accepted { .. }), Some(producer)) = (decision, &command.producer)
    {
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

/// JSON bodies split into one record per element.
fn expand_payloads(content_type: &str, payloads: &[Bytes]) -> Result<Vec<Bytes>, ServiceError> {
    if payloads.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for payload in payloads {
        out.extend(split_messages(content_type, payload, false)?);
    }
    Ok(out)
}

fn schema_error(message: impl Into<String>) -> ServiceError {
    ServiceError::with_message(ErrorKind::BadRequest, None, false, message.into())
}

fn stream_config_of(name: &str, entry: &RegistryEntry) -> StreamConfig {
    StreamConfig {
        name: name.to_owned(),
        schema_name: entry.schema_name.clone(),
        schema_validate: entry.schema_validate,
    }
}

fn schema_batch_from_messages(
    content_type: &str,
    messages: &[Bytes],
) -> Result<pico_schema::Batch, ServiceError> {
    let pico = framing::mime_of(Some(content_type)) == "application/x-picomq";
    let mut records = Vec::with_capacity(messages.len());
    let mut base_timestamp = 0i64;
    for (i, message) in messages.iter().enumerate() {
        if pico {
            let envelope = pico_protocol::envelope::decode_envelope(message).map_err(|e| {
                ServiceError::with_message(ErrorKind::BadRequest, None, false, e.to_string())
            })?;
            if i == 0 {
                base_timestamp = envelope.timestamp;
            }
            records.push(
                pico_schema::Record::builder()
                    .value(envelope.body)
                    .timestamp_delta(envelope.timestamp.saturating_sub(base_timestamp))
                    .build(),
            );
        } else {
            records.push(
                pico_schema::Record::builder()
                    .value(message.clone())
                    .build(),
            );
        }
    }
    Ok(pico_schema::Batch {
        base_timestamp,
        records,
    })
}

fn split_messages(
    content_type: &str,
    body: &Bytes,
    create: bool,
) -> Result<Vec<Bytes>, ServiceError> {
    if !framing::is_json(&framing::mime_of(Some(content_type))) {
        return Ok(vec![body.clone()]);
    }
    let node: serde_json::Value = serde_json::from_slice(body).map_err(|_| {
        ServiceError::with_message(ErrorKind::BadRequest, None, false, "invalid JSON")
    })?;
    match node {
        serde_json::Value::Array(items) => {
            if items.is_empty() {
                if create {
                    return Ok(Vec::new());
                }
                return Err(ServiceError::with_message(
                    ErrorKind::BadRequest,
                    None,
                    false,
                    "empty JSON array not allowed",
                ));
            }
            items
                .into_iter()
                .map(|item| {
                    serde_json::to_vec(&item).map(Bytes::from).map_err(|_| {
                        ServiceError::with_message(
                            ErrorKind::BadRequest,
                            None,
                            false,
                            "invalid JSON",
                        )
                    })
                })
                .collect()
        }
        other => Ok(vec![Bytes::from(serde_json::to_vec(&other).map_err(
            |_| ServiceError::with_message(ErrorKind::BadRequest, None, false, "invalid JSON"),
        )?)]),
    }
}

fn tail_read(
    entry: &RegistryEntry,
    cached: &[StreamRecord],
    end: u64,
    max_bytes: usize,
    max_records: usize,
) -> ReadResult {
    let mut records = Vec::new();
    let mut total = 0usize;
    let mut next = cached
        .first()
        .map(|r| r.offset.record_offset())
        .unwrap_or(end);
    for record in cached {
        if record.offset.record_offset() >= end {
            break;
        }
        let len = record.payload.len();
        if total + len > max_bytes && total > 0 {
            break;
        }
        records.push(record.clone());
        total += len;
        next = record.offset.record_offset() + 1;
        if total >= max_bytes || records.len() >= max_records {
            break;
        }
    }
    ReadResult {
        records,
        content_type: entry.content_type.clone(),
        next_offset: OffsetToken::of_record_offset(next),
        up_to_date: next >= end,
        closed: entry.closed,
    }
}

fn validate_batch_spans(command: &AppendBatchCommand) -> Result<(), ServiceError> {
    let bad = |message: &str| {
        Err(ServiceError::with_message(
            ErrorKind::BadRequest,
            None,
            false,
            message,
        ))
    };
    if command.payload.is_empty() || command.batches.is_empty() {
        return bad("empty batch payload");
    }
    if command.producer.is_some() && command.batches.len() != 1 {
        return bad("idempotent produce must carry exactly one batch");
    }
    let mut previous_end = 0usize;
    for (i, span) in command.batches.iter().enumerate() {
        if span.record_count == 0 {
            return bad("batch with zero records");
        }
        if i == 0 && span.patch_at != 0 {
            return bad("first batch must start at payload byte 0");
        }
        if i > 0 && span.patch_at < previous_end {
            return bad("batch spans out of order");
        }
        let Some(end) = span.patch_at.checked_add(8) else {
            return bad("batch span out of bounds");
        };
        if end > command.payload.len() {
            return bad("batch span out of bounds");
        }
        previous_end = end;
    }
    Ok(())
}

/// Rewrite each contained batch's base-offset field to the assigned engine
/// offsets. One copy of the payload, same as a real Kafka broker's rewrite.
fn patch_base_offsets(command: &AppendBatchCommand, base_offset: u64) -> Bytes {
    let mut payload = command.payload.to_vec();
    let mut assigned = base_offset;
    for span in &command.batches {
        payload[span.patch_at..span.patch_at + 8].copy_from_slice(&(assigned as i64).to_be_bytes());
        assigned += span.record_count as u64;
    }
    Bytes::from(payload)
}

/// `-1` sentinel as `u64::MAX` (snapshot-read fake opens).
fn live_start_offset(stream: &dyn Stream) -> u64 {
    let start = stream.start_offset();
    if start == u64::MAX {
        0
    } else {
        start
    }
}

fn to_meta_from_stream(name: &str, entry: &RegistryEntry, stream: &dyn Stream) -> StreamMeta {
    to_meta(
        name,
        entry,
        live_start_offset(stream),
        stream.confirm_offset(),
        stream.next_offset(),
    )
}

fn to_meta_from_committed(
    name: &str,
    entry: &RegistryEntry,
    committed: Option<&s3stream::StreamMetadata>,
) -> StreamMeta {
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
    to_meta(name, entry, start, end, end)
}

fn to_meta(name: &str, entry: &RegistryEntry, start: u64, next: u64, submitted: u64) -> StreamMeta {
    StreamMeta {
        name: name.to_owned(),
        stream_id: entry.stream_id,
        content_type: entry.content_type.clone(),
        ttl_seconds: entry.ttl_seconds,
        expires_at_ms: entry.expires_at_ms,
        start_offset: OffsetToken::of_record_offset(start),
        next_offset: OffsetToken::of_record_offset(next),
        submitted_offset: OffsetToken::of_record_offset(submitted),
        closed: entry.closed,
        external_id: entry.external_id,
        schema_name: entry.schema_name.clone(),
    }
}

async fn await_durable(pendings: Vec<PendingAppend>) -> Result<(), s3stream::Error> {
    for pending in pendings {
        pending.durable().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(offset: u64, payload: &[u8]) -> StreamRecord {
        StreamRecord {
            offset: OffsetToken::of_record_offset(offset),
            payload: Bytes::copy_from_slice(payload),
        }
    }

    #[test]
    fn tail_cache_contiguity_and_eviction() {
        let mut tail = TailCache::default();
        tail.record_append(0, &[Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
        assert_eq!(tail.tail_records(1).unwrap(), vec![rec(1, b"b")],);
        // Older-than-tail ignored.
        tail.record_append(0, &[Bytes::from_static(b"x")]);
        assert_eq!(tail.tail_records(0).unwrap().len(), 2);
        // Gap restarts the window.
        tail.record_append(10, &[Bytes::from_static(b"j")]);
        assert!(tail.tail_records(0).is_none());
        assert_eq!(tail.tail_records(10).unwrap(), vec![rec(10, b"j")]);
        // Record-count cap.
        let batch: Vec<Bytes> = (0..TAIL_MAX_RECORDS + 10)
            .map(|_| Bytes::from_static(b"r"))
            .collect();
        tail.record_append(11, &batch);
        assert!(tail.recent.len() <= TAIL_MAX_RECORDS);
    }

    #[test]
    fn split_messages_json_semantics() {
        let split = split_messages(
            "application/json",
            &Bytes::from_static(br#"[{"a":1}, 2, "x"]"#),
            false,
        )
        .unwrap();
        assert_eq!(split.len(), 3);
        assert_eq!(&split[0][..], br#"{"a":1}"#);
        assert_eq!(&split[1][..], b"2");

        // Non-array: one compacted record.
        let one = split_messages(
            "application/json",
            &Bytes::from_static(br#" {"b": 2} "#),
            false,
        )
        .unwrap();
        assert_eq!(one, vec![Bytes::from_static(br#"{"b":2}"#)]);

        // Empty array: legal at create only.
        assert!(
            split_messages("application/json", &Bytes::from_static(b"[]"), true)
                .unwrap()
                .is_empty()
        );
        assert!(split_messages("application/json", &Bytes::from_static(b"[]"), false).is_err());
        assert!(split_messages("application/json", &Bytes::from_static(b"{oops"), false).is_err());

        // Non-JSON passes through untouched.
        let raw = split_messages("text/plain", &Bytes::from_static(b"[1,2]"), false).unwrap();
        assert_eq!(raw, vec![Bytes::from_static(b"[1,2]")]);
    }

    #[test]
    fn touch_deadline_coarsens() {
        let entry = RegistryEntry {
            stream_id: 1,
            content_type: "text/plain".into(),
            ttl_seconds: Some(60),
            expires_at_ms: None,
            closed: false,
            deadline_ms: 0,
            last_seq: None,
            producers: Default::default(),
            closed_by: None,
            external_id: [0; 16],
            numeric_producers: Default::default(),
            producer_state_offset: 0,
            schema_name: None,
            schema_validate: false,
        };
        let touched = touch_deadline(entry.clone());
        assert!(touched.deadline_ms > 0);
        // Touching again immediately is within the coarsening window: no-op.
        let again = touch_deadline(touched.clone());
        assert_eq!(again.deadline_ms, touched.deadline_ms);
        let fixed = RegistryEntry {
            ttl_seconds: None,
            expires_at_ms: Some(123),
            deadline_ms: 123,
            ..entry
        };
        assert_eq!(touch_deadline(fixed.clone()), fixed);
    }
}
