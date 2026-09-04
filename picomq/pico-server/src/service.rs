//! Named streams over the engine, with registry state in the metadata KV.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use picomq_common::now_ms;
use picomq_metadata::{MetadataNodeHandle, ViewPublisher};
use picomq_protocol::mime::mime_equals;
use picomq_schema::Validator as _;
use s3stream::{
    AppendContext, CreateStreamOptions, FetchContext, KVClient, KeyValue, OpenStreamOptions,
    PendingAppend, RecordBatch, Stream, StreamClientTrait as StreamClient,
};

use crate::alias;
use crate::error::{ErrorKind, ServiceError};
use crate::producer::{self, Admission};
use crate::record::{self, BatchHeader, LogRecord};
use crate::registry::{ClosedBy, ProducerDecision, RegistryEntry, validate_producer};
use crate::types::{
    AppendBatchCommand, AppendBatchResult, AppendCommand, AppendResult, BatchReadResult,
    CloseResult, CreateCommand, CreateResult, NumericProducer, OffsetToken, ReadResult,
    StreamBatch, StreamConfig, StreamList, StreamMeta, StreamRecord, StreamWatermarks,
    SubmittedBatchAppend, UpdateStreamCommand,
};
use crate::waiter::StreamWaiterRegistry;

const DEFAULT_LIST_LIMIT: usize = 1000;
const MAX_LIST_LIMIT: usize = 10_000;
const TAIL_MAX_BYTES: usize = 4 * 1024 * 1024;
const TAIL_MAX_RECORDS: usize = 4096;
const TRANSFER_SETTLE_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSFER_SETTLE_POLL: Duration = Duration::from_millis(50);
const PRODUCER_CHECKPOINT_INTERVAL: u64 = 4096;
const COMPACTION_LIVE_OBJECT_THRESHOLD: usize = 64;

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
    op: tokio::sync::Mutex<()>,
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
    schema_registry: Option<Arc<dyn picomq_schema::SchemaStore>>,
}

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

    pub fn with_schema_registry(mut self, registry: Arc<dyn picomq_schema::SchemaStore>) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    pub fn schema_registry(&self) -> Option<&Arc<dyn picomq_schema::SchemaStore>> {
        self.schema_registry.as_ref()
    }

    pub fn waiters(&self) -> Arc<StreamWaiterRegistry> {
        self.waiters.clone()
    }

    fn schema_registry_required(
        &self,
    ) -> Result<&Arc<dyn picomq_schema::SchemaStore>, ServiceError> {
        self.schema_registry
            .as_ref()
            .ok_or_else(|| schema_error("schema registry is not configured"))
    }

    pub async fn validate_schema(
        &self,
        name: &str,
        batch: &picomq_schema::Batch,
    ) -> Result<(), ServiceError> {
        let registry = self.schema_registry_required()?;
        let schema = registry
            .schema(name)
            .await
            .map_err(|e| schema_error(e.to_string()))?
            .ok_or_else(|| {
                schema_error(format!("bound schema {name} is missing from the registry"))
            })?;
        schema.validate(batch).map_err(|e| {
            ServiceError::with_message(ErrorKind::SchemaViolation, None, false, e.to_string())
        })
    }

    pub async fn put_schema(
        &self,
        name: &str,
        format: picomq_schema::SchemaFormat,
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
    ) -> Result<Option<(picomq_schema::SchemaFormat, bytes::Bytes)>, ServiceError> {
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
            return Err(reserved_name());
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
        if let Some(change) = &command.kafka_topic {
            match change {
                Some(topic) if entry.kafka_topic.as_deref() == Some(topic) => {}
                Some(topic) => {
                    alias::validate_topic(topic)?;
                    if !self.claim_topic(topic, &name).await? {
                        return Err(topic_taken(topic));
                    }
                    if let Some(old) = entry.kafka_topic.replace(topic.clone()) {
                        self.release_topic(&old, &name).await;
                    }
                }
                None => {
                    if let Some(old) = entry.kafka_topic.take() {
                        self.release_topic(&old, &name).await;
                    }
                }
            }
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
            return Err(reserved_name());
        }
        if let Some(topic) = &command.kafka_topic {
            alias::validate_topic(topic)?;
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

        if let Some(schema_name) = command.schema_name.as_deref() {
            self.require_schema(schema_name).await?;
        }

        let topic = match &command.kafka_topic {
            Some(topic) => {
                if !self.claim_topic(topic, &name).await? {
                    return Err(topic_taken(topic));
                }
                Some(topic.clone())
            }
            None => match alias::derive_topic(&name) {
                Some(topic) => self.claim_topic(&topic, &name).await?.then_some(topic),
                None => None,
            },
        };

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
            kafka_topic: topic.clone(),
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
        if let Some(topic) = &topic
            && current.kafka_topic.as_deref() != Some(topic)
        {
            self.release_topic(topic, &name).await;
        }

        if current.stream_id != candidate.stream_id {
            return self
                .resolve_lost_race(&name, stream, current, &command)
                .await;
        }

        if self
            .append_initial_records(&name, &gate, &stream, &command)
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
        self.remove_stream_index(&name, &entry).await;
        self.destroy_stream(entry.stream_id).await;
        gate.reset();
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
        if let Some(decision) = decision
            && !matches!(decision, ProducerDecision::Accepted { .. })
        {
            return self.handle_producer_reject(&entry, next, decision, &command);
        }
        if entry.closed {
            return append_to_closed(&entry, &command, next);
        }
        if let Some(match_seq) = command.match_seq
            && match_seq != next.record_offset()
        {
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

        let close_only = command.records.is_empty() && command.close_after;
        validate_append(&entry, &command, decision, next, close_only)?;
        if !close_only
            && entry.schema_validate
            && self.schema_registry.is_some()
            && let Some(schema_name) = entry.schema_name.as_deref()
        {
            self.validate_schema(schema_name, &schema_batch(&command.records))
                .await?;
        }

        let mut pendings = Vec::new();
        let mut batches = Vec::new();
        let mut stamped = None;
        if !close_only {
            let base_offset = next.record_offset();
            let timestamp_ms = self.next_timestamp(&gate, stream.as_ref()).await?;
            stamped = Some(timestamp_ms);
            let payload = record::encode_batch(base_offset, timestamp_ms, &command.records);
            let count = command.records.len() as u32;
            match Arc::clone(&stream).submit_append(
                AppendContext::default(),
                RecordBatch::new(count, timestamp_ms, payload.clone()),
            ) {
                Ok(pending) => pendings.push(pending),
                Err(e) => {
                    self.open_streams.lock().unwrap().remove(&entry.stream_id);
                    gate.reset();
                    return Err(ServiceError::durability(e));
                }
            }
            gate.note_timestamp(timestamp_ms);
            next = OffsetToken::of_record_offset(stream.next_offset());
            batches.push(StreamBatch {
                base_offset,
                last_offset: next.record_offset(),
                count,
                payload,
            });
        }

        let updated = apply_append_state(entry.clone(), &command, decision);
        let echoed_seq = command.producer.as_ref().map(|p| p.seq);
        let echoed_epoch = command.producer.as_ref().map(|p| p.epoch);
        let result = AppendResult {
            next_offset: next,
            applied: !close_only,
            timestamp_ms: stamped,
            closed: command.close_after,
            producer_epoch: echoed_epoch,
            producer_seq: echoed_seq,
        };
        let notify_offset = next.record_offset();

        if command.close_after {
            if let Err(e) = await_durable(pendings).await {
                self.open_streams.lock().unwrap().remove(&entry.stream_id);
                gate.reset();
                return Err(ServiceError::durability(e));
            }
            if !close_only {
                gate.tail.lock().unwrap().record_append(&batches);
                self.waiters.notify_append(&name, notify_offset);
            }
            self.close_entry(&name, updated, &command).await?;
            return Ok(result);
        }

        let touched = touch_deadline(updated);
        if touched != entry {
            self.put_entry(&name, touched).await?;
        }

        drop(_op);
        if let Err(e) = await_durable(pendings).await {
            self.open_streams.lock().unwrap().remove(&entry.stream_id);
            gate.reset();
            return Err(ServiceError::durability(e));
        }

        gate.tail.lock().unwrap().record_append(&batches);
        self.waiters.notify_append(&name, notify_offset);
        Ok(result)
    }

    pub async fn append_batch(
        &self,
        command: AppendBatchCommand,
    ) -> Result<AppendBatchResult, ServiceError> {
        let submitted = self.submit_batch_append(command).await?;
        self.finish_batch_append(submitted).await
    }

    pub async fn submit_batch_append(
        &self,
        command: AppendBatchCommand,
    ) -> Result<SubmittedBatchAppend, ServiceError> {
        let headers = parse_batches(&command.payload)?;
        let producer = headers[0].producer;
        let record_count = headers
            .iter()
            .fold(0u32, |acc, h| acc.saturating_add(h.record_count));
        let name = normalize(&command.name);
        let gate = self.gate_of(&name);
        let _op = gate.op.lock().await;

        let Some(mut entry) = self.get_entry(&name, false).await? else {
            return Err(ServiceError::kind(ErrorKind::NotFound));
        };
        if entry.closed {
            return Err(ServiceError::kind(ErrorKind::Closed));
        }
        if entry.schema_validate
            && let Some(schema_name) = entry.schema_name.as_deref()
        {
            let records = record::decode_batches(&command.payload).map_err(corrupt_batch)?;
            let batch = schema_batch(&records.into_iter().map(|r| r.record).collect::<Vec<_>>());
            self.validate_schema(schema_name, &batch).await?;
        }
        let stream_id = entry.stream_id;
        let stream = self.ensure_open(stream_id).await?;
        let log_start_offset = live_start_offset(stream.as_ref());
        let now = now_ms();
        self.recover_producers(&name, &mut entry, stream.as_ref(), producer, now)
            .await?;
        producer::expire_producers(&mut entry, now);

        let accepted = match producer::admit(&mut entry, producer, record_count, now) {
            Ok(Admission::Accepted(accepted)) => accepted,
            Ok(Admission::Duplicate { base_offset }) => {
                self.cache_entry(&name, touch_deadline(entry));
                return Ok(SubmittedBatchAppend {
                    name,
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
                self.cache_entry(&name, touch_deadline(entry));
                return Err(error);
            }
        };

        let base_offset = stream.next_offset();
        let (payload, batches) = patch_base_offsets(&command.payload, &headers, base_offset);
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
                self.open_streams.lock().unwrap().remove(&stream_id);
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
        let entry = touch_deadline(entry);
        self.cache_entry(&name, entry.clone());
        if !entry.numeric_producers.is_empty()
            && prev_offset / PRODUCER_CHECKPOINT_INTERVAL
                != entry.producer_state_offset / PRODUCER_CHECKPOINT_INTERVAL
            && let Err(error) = self.put_entry(&name, entry).await
        {
            tracing::warn!(%error, stream = %name, "producer state checkpoint failed");
        }

        let notify_offset = stream.next_offset();
        drop(_op);
        Ok(SubmittedBatchAppend {
            name,
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
                self.open_streams
                    .lock()
                    .unwrap()
                    .remove(&submitted.stream_id);
                return Err(ServiceError::durability(e));
            }
            let gate = self.gate_of(&submitted.name);
            gate.tail.lock().unwrap().record_append(&submitted.batches);
            self.waiters
                .notify_append(&submitted.name, submitted.notify_offset);
        }
        Ok(AppendBatchResult {
            base_offset: submitted.base_offset,
            duplicate: submitted.duplicate,
            log_start_offset: submitted.log_start_offset,
        })
    }

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
        let batches = self
            .batches_from(&name, stream.as_ref(), from, high_watermark, max_bytes)
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

        let batches = self
            .batches_from(&name, stream.as_ref(), start, end, max_bytes)
            .await?;
        collect_records(&entry, &batches, start, end, max_bytes, max_records)
    }

    async fn batches_from(
        &self,
        name: &str,
        stream: &dyn Stream,
        start: u64,
        end: u64,
        max_bytes: usize,
    ) -> Result<Vec<StreamBatch>, ServiceError> {
        let cached = self.gate_of(name).tail.lock().unwrap().tail_batches(start);
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

    pub async fn lookup_by_topic(&self, topic: &str) -> Result<Option<String>, ServiceError> {
        if !alias::is_valid_topic(topic) {
            return Ok(None);
        }
        let Some(value) = self.kv_client.get_kv(&topic_key(topic)).await? else {
            return Ok(None);
        };
        let Ok(name) = String::from_utf8(value.to_vec()) else {
            return Ok(None);
        };
        match self.get_entry(&name, false).await? {
            Some(entry) if entry.kafka_topic.as_deref() == Some(topic) => Ok(Some(name)),
            _ => Ok(None),
        }
    }

    pub fn list_topics(&self) -> Vec<(String, String)> {
        let view = self.views.load();
        let now = now_ms();
        view.state
            .list_kv(TOPIC_INDEX_PREFIX)
            .into_iter()
            .filter_map(|(key, value)| {
                let topic = key.strip_prefix(TOPIC_INDEX_PREFIX)?.to_owned();
                let name = String::from_utf8(value.to_vec()).ok()?;
                let entry = RegistryEntry::decode(&view.state.get_kv(&name)?).ok()?;
                if entry.deadline_ms > 0 && now > entry.deadline_ms {
                    return None;
                }
                (entry.kafka_topic.as_deref() == Some(topic.as_str())).then_some((topic, name))
            })
            .collect()
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

    pub fn spawn_compaction_check(
        self: &Arc<Self>,
        tick: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        const PAGE: usize = 4096;
        let service = self.clone();
        tokio::spawn(async move {
            let mut cursor: Option<picomq_metadata::StreamOffsetKey> = None;
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
                    last_timestamp_ms: Mutex::new(None),
                })
            })
            .clone()
    }

    async fn next_timestamp(&self, gate: &Gate, stream: &dyn Stream) -> Result<i64, ServiceError> {
        let last = *gate.last_timestamp_ms.lock().unwrap();
        let last = match last {
            Some(last) => last,
            None => {
                let last = self.read_back_timestamp(stream).await?;
                gate.note_timestamp(last);
                last
            }
        };
        Ok(now_ms().max(last + 1))
    }

    async fn read_back_timestamp(&self, stream: &dyn Stream) -> Result<i64, ServiceError> {
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
        let headers = record::batch_headers(&tail.payload).map_err(corrupt_batch)?;
        Ok(headers
            .iter()
            .filter(|h| h.log_append_time)
            .map(|h| h.max_timestamp_ms)
            .max()
            .unwrap_or(0))
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

    async fn append_initial_records(
        &self,
        name: &str,
        gate: &Gate,
        stream: &Arc<dyn Stream>,
        command: &CreateCommand,
    ) -> Result<bool, ServiceError> {
        if command.initial_records.is_empty() {
            return Ok(false);
        }
        if command.schema_validate
            && self.schema_registry.is_some()
            && let Some(schema_name) = command.schema_name.as_deref()
        {
            self.validate_schema(schema_name, &schema_batch(&command.initial_records))
                .await?;
        }
        let base_offset = stream.next_offset();
        let timestamp_ms = self.next_timestamp(gate, stream.as_ref()).await?;
        let payload = record::encode_batch(base_offset, timestamp_ms, &command.initial_records);
        let count = command.initial_records.len() as u32;
        let pending = Arc::clone(stream)
            .submit_append(
                AppendContext::default(),
                RecordBatch::new(count, timestamp_ms, payload.clone()),
            )
            .map_err(ServiceError::durability)?;
        gate.note_timestamp(timestamp_ms);
        await_durable(vec![pending])
            .await
            .map_err(ServiceError::durability)?;
        gate.tail.lock().unwrap().record_append(&[StreamBatch {
            base_offset,
            last_offset: stream.next_offset(),
            count,
            payload,
        }]);
        self.waiters.notify_append(name, stream.next_offset());
        Ok(true)
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
                timestamp_ms: None,
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
            gate.reset();
            self.waiters.notify_closed(name);
        }
        Ok(Some(epoch))
    }

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
        self.remove_stream_index(name, &entry).await;
        self.destroy_stream(entry.stream_id).await;
        if let Some(gate) = self.gates.lock().unwrap().get(name) {
            gate.reset();
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

    fn cache_entry(&self, name: &str, entry: RegistryEntry) {
        self.entry_cache
            .lock()
            .unwrap()
            .insert(name.to_owned(), entry);
    }

    async fn recover_producers(
        &self,
        name: &str,
        entry: &mut RegistryEntry,
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

    async fn remove_stream_index(&self, name: &str, entry: &RegistryEntry) {
        let _ = self.kv_client.del_kv(&stream_id_key(entry.stream_id)).await;
        if entry.external_id != [0u8; 16] {
            let _ = self
                .kv_client
                .del_kv(&external_id_key(&entry.external_id))
                .await;
        }
        if let Some(topic) = &entry.kafka_topic {
            self.release_topic(topic, name).await;
        }
    }

    async fn claim_topic(&self, topic: &str, name: &str) -> Result<bool, ServiceError> {
        let key = topic_key(topic);
        let value = Bytes::copy_from_slice(name.as_bytes());
        for _ in 0..2 {
            let stored = self
                .kv_client
                .put_kv_if_absent(KeyValue {
                    key: key.clone(),
                    value: value.clone(),
                })
                .await?;
            if stored == value {
                return Ok(true);
            }
            let live = match String::from_utf8(stored.to_vec()) {
                Ok(holder) => self
                    .get_entry(&holder, false)
                    .await?
                    .is_some_and(|entry| entry.kafka_topic.as_deref() == Some(topic)),
                Err(_) => false,
            };
            if live {
                return Ok(false);
            }
            if self.kv_client.del_kv_if(&key, &stored).await?.is_none() {
                return Ok(false);
            }
        }
        Ok(false)
    }

    async fn release_topic(&self, topic: &str, name: &str) {
        let _ = self
            .kv_client
            .del_kv_if(&topic_key(topic), &Bytes::copy_from_slice(name.as_bytes()))
            .await;
    }

    async fn to_meta_live(
        &self,
        name: &str,
        entry: &RegistryEntry,
    ) -> Result<StreamMeta, ServiceError> {
        let stream = self.ensure_open(entry.stream_id).await?;
        Ok(to_meta_from_stream(name, entry, stream.as_ref()))
    }

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

const TOPIC_INDEX_PREFIX: &str = "idx/topic/";

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

fn topic_key(topic: &str) -> String {
    format!("{TOPIC_INDEX_PREFIX}{topic}")
}

fn reserved_name() -> ServiceError {
    ServiceError::with_message(
        ErrorKind::BadRequest,
        None,
        false,
        "reserved stream name prefix",
    )
}

fn topic_taken(topic: &str) -> ServiceError {
    ServiceError::with_message(
        ErrorKind::Conflict,
        None,
        false,
        format!("Kafka topic {topic:?} belongs to another stream"),
    )
}

fn corrupt_batch(error: record::RecordError) -> ServiceError {
    ServiceError::with_message(
        ErrorKind::CorruptBatch,
        None,
        false,
        format!("stored record batch: {error}"),
    )
}

fn config_matches(entry: &RegistryEntry, command: &CreateCommand) -> bool {
    mime_equals(Some(&entry.content_type), Some(&command.content_type))
        && entry.ttl_seconds == command.ttl_seconds
        && entry.expires_at_ms == command.expires_at_ms
        && entry.closed == command.closed
        && entry.schema_name == command.schema_name
        && entry.schema_validate == command.schema_validate
        && (command.kafka_topic.is_none() || command.kafka_topic == entry.kafka_topic)
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

fn validate_append(
    entry: &RegistryEntry,
    command: &AppendCommand,
    decision: Option<ProducerDecision>,
    next: OffsetToken,
    close_only: bool,
) -> Result<(), ServiceError> {
    if !close_only {
        if let Some(ct) = command.content_type.as_deref()
            && !mime_equals(Some(&entry.content_type), Some(ct))
        {
            return Err(ServiceError::at(ErrorKind::Conflict, next, false));
        }
        if command.records.is_empty() {
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
        if accepted
            && let Some(last_seq) = &entry.last_seq
            && stream_seq.as_str() <= last_seq.as_str()
        {
            return Err(ServiceError::with_message(
                ErrorKind::Conflict,
                Some(next),
                false,
                "Sequence conflict",
            ));
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
    if command.close_after && command.records.is_empty() {
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
            timestamp_ms: None,
            closed: true,
            producer_epoch: echoed_epoch,
            producer_seq: echoed_seq,
        });
    }
    if has_producer && matches_closed_by(entry, command) {
        return Ok(AppendResult {
            next_offset: next,
            applied: false,
            timestamp_ms: None,
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

fn schema_error(message: impl Into<String>) -> ServiceError {
    ServiceError::with_message(ErrorKind::BadRequest, None, false, message.into())
}

fn stream_config_of(name: &str, entry: &RegistryEntry) -> StreamConfig {
    StreamConfig {
        name: name.to_owned(),
        schema_name: entry.schema_name.clone(),
        schema_validate: entry.schema_validate,
        kafka_topic: entry.kafka_topic.clone(),
    }
}

fn schema_batch(records: &[LogRecord]) -> picomq_schema::Batch {
    picomq_schema::Batch {
        records: records
            .iter()
            .map(|record| {
                let mut builder = picomq_schema::Record::builder().value(record.value.clone());
                if let Some(key) = &record.key {
                    builder = builder.key(key.clone());
                }
                builder.build()
            })
            .collect(),
    }
}

fn parse_batches(payload: &Bytes) -> Result<Vec<BatchHeader>, ServiceError> {
    let bad =
        |message: String| ServiceError::with_message(ErrorKind::BadRequest, None, false, message);
    if payload.is_empty() {
        return Err(bad("empty batch payload".into()));
    }
    let headers = record::batch_headers(payload).map_err(corrupt_batch)?;
    if headers.is_empty() {
        return Err(bad("empty batch payload".into()));
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
    entry: &RegistryEntry,
    batches: &[StreamBatch],
    start: u64,
    end: u64,
    max_bytes: usize,
    max_records: usize,
) -> Result<ReadResult, ServiceError> {
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
        for stream_record in record::decode_batches(&batch.payload).map_err(corrupt_batch)? {
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
    Ok(ReadResult {
        records,
        content_type: entry.content_type.clone(),
        next_offset: OffsetToken::of_record_offset(next),
        up_to_date: next >= end,
        closed: entry.closed,
    })
}

fn live_start_offset(stream: &dyn Stream) -> u64 {
    let start = stream.start_offset();
    if start == u64::MAX { 0 } else { start }
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
        kafka_topic: entry.kafka_topic.clone(),
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

    fn entry() -> RegistryEntry {
        RegistryEntry {
            stream_id: 1,
            content_type: "text/plain".into(),
            ttl_seconds: None,
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
            kafka_topic: None,
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
        let read = collect_records(&entry(), &batches, 1, 5, usize::MAX, usize::MAX).unwrap();
        let values: Vec<&[u8]> = read.records.iter().map(|r| &r.record.value[..]).collect();
        assert_eq!(values, [b"bb", b"cc", b"dd", b"ee"]);
        assert_eq!(read.next_offset.record_offset(), 5);
        assert!(read.up_to_date);

        let read = collect_records(&entry(), &batches, 0, 5, 3, usize::MAX).unwrap();
        assert_eq!(read.records.len(), 1);
        assert_eq!(read.next_offset.record_offset(), 1);
        assert!(!read.up_to_date);

        let read = collect_records(&entry(), &batches, 0, 4, usize::MAX, 2).unwrap();
        assert_eq!(read.records.len(), 2);
        let read = collect_records(&entry(), &batches, 2, 4, usize::MAX, usize::MAX).unwrap();
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

    #[test]
    fn touch_deadline_coarsens() {
        let entry = RegistryEntry {
            ttl_seconds: Some(60),
            ..entry()
        };
        let touched = touch_deadline(entry.clone());
        assert!(touched.deadline_ms > 0);
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

    #[test]
    fn create_replay_matches_ignore_derived_topic() {
        let existing = RegistryEntry {
            kafka_topic: Some("a".into()),
            ..entry()
        };
        assert!(config_matches(
            &existing,
            &CreateCommand::new("/a", "text/plain")
        ));
        assert!(config_matches(
            &existing,
            &CreateCommand::new("/a", "text/plain").with_kafka_topic("a")
        ));
        assert!(!config_matches(
            &existing,
            &CreateCommand::new("/a", "text/plain").with_kafka_topic("b")
        ));
    }
}
