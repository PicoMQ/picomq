use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use picomq_common::name::canonical;
use picomq_common::now_ms;
use picomq_metadata::{MetadataKvClient, MetadataState, ViewPublisher};
use picomq_protocol::mime::mime_equals;
use tokio::sync::RwLock;

use crate::error::{ErrorKind, ServiceError};
use crate::log::Log;
use crate::page::{self, Page};
use crate::partition::Partition;
use crate::producers::ProducerDecision;
use crate::record::{LogRecord, StreamRecord};
use crate::service::S3StreamService;
use crate::topic::{ClosedBy, Topic, TopicId, TopicName, deadline_of};
use crate::types::{
    AppendBatchResult, AppendCommand, AppendResult, BatchReadResult, CloseResult, CreateCommand,
    CreateResult, LogOffsets, MAX_NEW_PARTITIONS, OffsetToken, ReadResult, StreamWatermarks,
    SubmittedBatchAppend, TopicMeta, UpdateTopicCommand,
};

const MAX_WRITE_ATTEMPTS: usize = 16;
const SYNC_TIMEOUT: Duration = Duration::from_secs(5);
const TOPIC_PREFIX: &str = "topic/";
const TOPIC_ID_PREFIX: &str = "topic-id/";

pub struct Topics {
    service: Arc<S3StreamService>,
    kv: MetadataKvClient,
    views: Arc<ViewPublisher>,
    locks: Mutex<HashMap<String, Arc<RwLock<()>>>>,
}

impl Topics {
    pub fn new(
        service: Arc<S3StreamService>,
        kv: MetadataKvClient,
        views: Arc<ViewPublisher>,
    ) -> Self {
        Self {
            service,
            kv,
            views,
            locks: Mutex::new(HashMap::new()),
        }
    }

    pub async fn create(&self, command: CreateCommand) -> Result<CreateResult, ServiceError> {
        command.validate()?;

        let name = TopicName::parse(&command.name)?;
        let lock = self.lock(name.canonical());
        let _exclusive = lock.write().await;

        if let Some((existing, stored)) = self.record(name.canonical())? {
            if !existing.expired(now_ms()) {
                return self.existing(existing, &command);
            }
            self.remove(&existing, &stored).await;
        }
        let id = TopicId::random();
        self.kv
            .put_kv(&id_key(&id), Bytes::from(name.canonical().to_owned()))
            .await?;
        let local = (!command.initial_records.is_empty()).then_some(0);
        if let Err(error) = self
            .provision(id, 0..command.partitions, &command.owners, local)
            .await
        {
            let _ = self.kv.del_kv(&id_key(&id)).await;
            return Err(error);
        }

        let candidate = Topic {
            id,
            name: name.clone(),
            partitions: command.partitions,
            content_type: command.content_type.clone(),
            ttl_seconds: command.ttl_seconds,
            expires_at_ms: command.expires_at_ms,
            deadline_ms: deadline_of(command.ttl_seconds, command.expires_at_ms),
            closed: false,
            closed_by: None,
            retention: command.retention,
        };

        let stored = self
            .kv
            .put_kv_if_absent(&topic_key(name.canonical()), candidate.encode())
            .await;
        let current = match stored
            .map_err(ServiceError::from)
            .and_then(|stored| Topic::decode(&stored))
        {
            Ok(current) if current.id == id => current,
            outcome => {
                self.unprovision(id, 0..command.partitions).await;
                let _ = self.kv.del_kv(&id_key(&id)).await;
                return self.existing(outcome?, &command);
            }
        };

        if !command.initial_records.is_empty() {
            self.append_initial_records(&current, command.initial_records)
                .await?;
        }
        let current = if command.closed {
            self.seal(&current, None).await?
        } else {
            current
        };

        Ok(CreateResult {
            created: true,
            meta: self.meta(current)?,
        })
    }

    pub async fn get(&self, name: &str) -> Result<Option<Topic>, ServiceError> {
        let name = TopicName::parse(name)?;
        let Some((topic, stored)) = self.load(name.canonical()).await? else {
            return Ok(None);
        };

        if topic.expired(now_ms()) {
            self.expire(&topic, &stored).await;
            return Ok(None);
        }

        Ok(Some(topic))
    }

    pub async fn get_kafka(&self, kafka_name: &str) -> Result<Option<Topic>, ServiceError> {
        if !crate::topic::is_kafka_name(kafka_name) {
            return Ok(None);
        }
        self.get(kafka_name).await
    }

    pub async fn by_id(&self, id: &TopicId) -> Result<Option<Topic>, ServiceError> {
        let Some((topic, stored)) = self.find(id).await? else {
            return Ok(None);
        };

        if topic.expired(now_ms()) {
            self.expire(&topic, &stored).await;
            return Ok(None);
        }

        Ok(Some(topic))
    }

    pub async fn exists(&self, id: &TopicId) -> Result<bool, ServiceError> {
        Ok(self
            .find(id)
            .await?
            .is_some_and(|(topic, _)| !topic.expired(now_ms())))
    }

    pub fn kafka_topics(&self) -> Vec<Topic> {
        let view = self.views.load();
        live_topics(&view.state, now_ms())
            .filter(|topic| topic.name.kafka().is_some())
            .collect()
    }

    pub async fn live_offsets(
        &self,
        topic: &Topic,
        partition: u32,
    ) -> Result<LogOffsets, ServiceError> {
        let log = partition_log(topic, partition)?;
        self.service.live_offsets(&log).await
    }

    pub fn offsets(&self, topic: &Topic, partition: u32) -> Result<LogOffsets, ServiceError> {
        let log = partition_log(topic, partition)?;
        self.service.offsets(&log)
    }

    pub async fn watermarks(
        &self,
        topic: &Topic,
        partition: u32,
    ) -> Result<StreamWatermarks, ServiceError> {
        let log = partition_log(topic, partition)?;
        self.service.watermarks(&log).await
    }

    pub fn epochs(&self, topic: &Topic, partition: u32) -> Result<Vec<(u64, u64)>, ServiceError> {
        let log = partition_log(topic, partition)?;
        self.service.epochs(&log)
    }

    pub async fn record_at_or_after(
        &self,
        topic: &Topic,
        partition: u32,
        timestamp_ms: i64,
    ) -> Result<Option<StreamRecord>, ServiceError> {
        let log = partition_log(topic, partition)?;
        self.service.record_at_or_after(&log, timestamp_ms).await
    }

    pub async fn record_with_max_timestamp(
        &self,
        topic: &Topic,
        partition: u32,
    ) -> Result<Option<StreamRecord>, ServiceError> {
        let log = partition_log(topic, partition)?;
        self.service.record_with_max_timestamp(&log).await
    }

    pub fn meta(&self, topic: Topic) -> Result<TopicMeta, ServiceError> {
        let partitions = partition_logs(&topic)
            .iter()
            .map(|log| self.service.offsets(log))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TopicMeta { topic, partitions })
    }

    pub fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
        mut keep: impl FnMut(&Topic) -> bool,
    ) -> Page<String, Topic> {
        let now = now_ms();
        let prefix = topic_key(&canonical(prefix));
        let after = after.map(topic_key);

        let entries = self
            .views
            .load()
            .state
            .list_kv_page(&prefix, after.as_deref(), page::fetch_count(limit))
            .into_iter()
            .map(|(key, value)| {
                let name = key[TOPIC_PREFIX.len()..].to_owned();
                (name, Topic::decode(&value).ok())
            });
        let page = Page::scan(entries, limit, |_, topic| {
            topic
                .as_ref()
                .is_some_and(|topic| !topic.expired(now) && keep(topic))
        });

        Page {
            items: page
                .items
                .into_iter()
                .filter_map(|(name, topic)| Some((name, topic?)))
                .collect(),
            next: page.next,
        }
    }

    pub async fn update(&self, command: UpdateTopicCommand) -> Result<Topic, ServiceError> {
        command.validate()?;
        let name = TopicName::parse(&command.name)?;

        let lock = self.lock(name.canonical());
        let _exclusive = lock.write().await;
        let topic = self.require_live(name.canonical()).await?;

        let added = command
            .partitions
            .map(|partitions| added_partitions(&topic, partitions, &command.owners))
            .transpose()?
            .unwrap_or_default();
        self.provision(topic.id, added.clone(), &command.owners, None)
            .await?;

        let updated = self.modify(&topic, |topic| command.apply(topic)).await;
        if updated.is_err() {
            self.unprovision(topic.id, added).await;
        }

        updated
    }

    pub async fn delete(&self, name: &str) -> Result<bool, ServiceError> {
        let name = TopicName::parse(name)?;
        let lock = self.lock(name.canonical());
        let _exclusive = lock.write().await;

        let Some((topic, stored)) = self.load(name.canonical()).await? else {
            return Ok(false);
        };
        Ok(self.remove(&topic, &stored).await)
    }

    pub async fn close(&self, name: &str) -> Result<CloseResult, ServiceError> {
        let result = self
            .append(
                name,
                0,
                AppendCommand {
                    close_after: true,
                    ..Default::default()
                },
            )
            .await?;

        Ok(CloseResult {
            next_offset: result.next_offset,
        })
    }

    pub async fn append(
        &self,
        name: &str,
        partition: u32,
        command: AppendCommand,
    ) -> Result<AppendResult, ServiceError> {
        let command = command.normalized();
        let name = TopicName::parse(name)?;
        let lock = self.lock(name.canonical());
        let (_shared, _exclusive) = if command.close_after {
            (None, Some(lock.write().await))
        } else {
            (Some(lock.read().await), None)
        };

        let topic = self.require_live(name.canonical()).await?;
        if command.close_after && topic.partitions > 1 {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "a topic with more than one partition cannot be closed",
            ));
        }
        let log = partition_log(&topic, partition)?;
        let append = self.service.begin_append(&log).await?;
        let next = append.next_offset();

        let decision = command
            .producer
            .as_ref()
            .map(|producer| append.producer_decision(producer));
        if let Some(decision) = decision
            && !matches!(decision, ProducerDecision::Accepted { .. })
        {
            let topic = match decision {
                ProducerDecision::Duplicate { .. } if command.close_after && !topic.closed => {
                    self.seal(&topic, closed_by(&command)).await?
                }
                _ => topic,
            };
            return producer_reject(&topic, next, decision, &command);
        }
        if topic.closed {
            return append_to_closed(&topic, &command, next);
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
        if !close_only {
            if let Some(content_type) = command.content_type.as_deref()
                && !mime_equals(Some(&topic.content_type), Some(content_type))
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
        append.check_stream_seq(command.stream_seq.as_deref(), decision)?;
        if !command.close_after {
            self.touch(&topic).await?;
        }

        let closer = closed_by(&command);
        let written = (
            command.records.len(),
            command.records.iter().map(LogRecord::size_hint).sum(),
        );
        let committed = append
            .commit(
                command.records,
                command.stream_seq,
                command.producer.as_ref(),
                decision,
            )
            .await?;

        if !close_only {
            picomq_metrics::written(written.0, written.1);
        }
        if command.close_after {
            self.seal(&topic, closer).await?;
        }

        Ok(AppendResult {
            next_offset: committed.next_offset,
            applied: !close_only,
            timestamp_ms: committed.timestamp_ms,
            closed: command.close_after,
            producer_epoch: command.producer.as_ref().map(|p| p.epoch),
            producer_seq: command.producer.as_ref().map(|p| p.seq),
        })
    }

    pub async fn submit_batch(
        &self,
        topic: &Topic,
        partition: u32,
        payload: Bytes,
    ) -> Result<SubmittedBatchAppend, ServiceError> {
        let lock = self.lock(topic.name.canonical());
        let _shared = lock.read().await;

        let topic = self.require_live(topic.name.canonical()).await?;
        if topic.closed {
            return Err(ServiceError::kind(ErrorKind::Closed));
        }
        let log = partition_log(&topic, partition)?;
        self.touch(&topic).await?;

        self.service.submit_batch_append(&log, payload).await
    }

    pub async fn finish_batch(
        &self,
        submitted: SubmittedBatchAppend,
    ) -> Result<AppendBatchResult, ServiceError> {
        let written = (!submitted.duplicate).then(|| {
            submitted
                .batches
                .iter()
                .fold((0, 0), |(records, bytes), batch| {
                    (records + batch.count as usize, bytes + batch.payload.len())
                })
        });

        let result = self.service.finish_batch_append(submitted).await?;
        if let Some((records, bytes)) = written {
            picomq_metrics::written(records, bytes);
        }
        Ok(result)
    }

    pub async fn read(
        &self,
        name: &str,
        partition: u32,
        from: OffsetToken,
        max_bytes: usize,
        max_records: usize,
    ) -> Result<ReadResult, ServiceError> {
        let topic = self.require(name).await?;
        let log = partition_log(&topic, partition)?;
        let topic = self.touch(&topic).await?;

        let read = self
            .service
            .read(&log, from, max_bytes, max_records)
            .await?;
        picomq_metrics::read(
            read.records.len(),
            read.records
                .iter()
                .map(|stored| stored.record.size_hint())
                .sum(),
        );

        Ok(ReadResult {
            records: read.records,
            content_type: topic.content_type,
            next_offset: read.next_offset,
            up_to_date: read.up_to_date,
            closed: topic.closed,
        })
    }

    pub async fn read_batches(
        &self,
        topic: &Topic,
        partition: u32,
        from: u64,
        max_bytes: usize,
    ) -> Result<BatchReadResult, ServiceError> {
        let log = partition_log(topic, partition)?;
        self.touch(topic).await?;

        let read = self.service.read_batches(&log, from, max_bytes).await?;
        picomq_metrics::read(
            read.batches.iter().map(|batch| batch.count as usize).sum(),
            read.batches.iter().map(|batch| batch.payload.len()).sum(),
        );
        Ok(read)
    }

    pub async fn wait_appended(
        &self,
        name: &str,
        partition: u32,
        from: OffsetToken,
        timeout: Duration,
    ) -> Result<bool, ServiceError> {
        let Some(topic) = self.get(name).await? else {
            return Ok(false);
        };
        if topic.closed {
            return Ok(true);
        }

        self.wait_partition(&topic, partition, from, timeout).await
    }

    pub async fn wait_partition(
        &self,
        topic: &Topic,
        partition: u32,
        from: OffsetToken,
        timeout: Duration,
    ) -> Result<bool, ServiceError> {
        let log = partition_log(topic, partition)?;
        self.service.wait_appended(&log, from, timeout).await
    }

    pub async fn trim(
        &self,
        name: &str,
        partition: u32,
        new_start_offset: u64,
    ) -> Result<u64, ServiceError> {
        let topic = self.require(name).await?;
        let log = partition_log(&topic, partition)?;
        self.service.trim(&log, new_start_offset).await
    }

    pub async fn delete_records(
        &self,
        topic: &Topic,
        partition: u32,
        offset: u64,
        timeout: Duration,
    ) -> Result<u64, ServiceError> {
        let log = partition_log(topic, partition)?;
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let view = self.views.load();
            let start = self.service.trim(&log, offset).await?;
            if start >= offset {
                return Ok(start);
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(ServiceError::with_message(
                    ErrorKind::Timeout,
                    None,
                    false,
                    format!("records below {offset} were not uploaded in time to delete"),
                ));
            }
            self.views.wait_past(view.applied_index, remaining).await;
        }
    }

    pub fn spawn_ttl_sweep(
        self: &Arc<Self>,
        mut leadership: tokio::sync::watch::Receiver<bool>,
        tick: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let topics = self.clone();

        tokio::spawn(async move {
            let mut cursor: Option<String> = None;
            while let Some(leading) = crate::leadership::tick(&mut leadership, tick).await {
                if !leading {
                    continue;
                }

                let now = now_ms();
                for (topic, value) in topics.next_page(&mut cursor) {
                    if topic.expired(now) {
                        topics.expire(&topic, &value).await;
                    }
                }
            }
        })
    }

    pub fn spawn_retention_sweep(self: &Arc<Self>, tick: Duration) -> tokio::task::JoinHandle<()> {
        let topics = self.clone();

        tokio::spawn(async move {
            let mut cursor: Option<String> = None;
            loop {
                tokio::time::sleep(tick).await;

                let now = now_ms();
                for (topic, _) in topics.next_page(&mut cursor) {
                    if topic.retention.is_unlimited() {
                        continue;
                    }
                    for index in 0..topic.partitions {
                        let Ok(log) = partition_log(&topic, index) else {
                            continue;
                        };
                        if let Err(error) = topics
                            .service
                            .apply_retention(&log, topic.retention, now)
                            .await
                        {
                            tracing::debug!(%error, topic = %topic.name, index, "retention trim failed");
                        }
                    }
                }
            }
        })
    }

    fn next_page(&self, cursor: &mut Option<String>) -> Vec<(Topic, Bytes)> {
        const PAGE: usize = 256;

        let page = self
            .views
            .load()
            .state
            .list_kv_page(TOPIC_PREFIX, cursor.as_deref(), PAGE);
        *cursor = page.last().map(|(key, _)| key.clone());

        page.into_iter()
            .filter_map(|(_, value)| Some((Topic::decode(&value).ok()?, value)))
            .collect()
    }

    fn existing(
        &self,
        existing: Topic,
        command: &CreateCommand,
    ) -> Result<CreateResult, ServiceError> {
        if !config_matches(&existing, command) {
            return Err(ServiceError::kind(ErrorKind::Conflict));
        }

        Ok(CreateResult {
            created: false,
            meta: self.meta(existing)?,
        })
    }

    async fn append_initial_records(
        &self,
        topic: &Topic,
        records: Vec<LogRecord>,
    ) -> Result<(), ServiceError> {
        let log = partition_log(topic, 0)?;
        self.service.append_records(&log, records).await?;

        Ok(())
    }

    async fn provision(
        &self,
        id: TopicId,
        indexes: Range<u32>,
        owners: &[i32],
        local: Option<u32>,
    ) -> Result<(), ServiceError> {
        for index in indexes.clone() {
            let log = Log::Partition(Partition::new(id, index));
            let owner = owners.get((index - indexes.start) as usize).copied();
            let created = if local == Some(index) {
                self.service.create_local_log(&log).await
            } else {
                self.service.create_placed_log(&log, owner).await
            };

            if let Err(error) = created {
                self.unprovision(id, indexes.start..index + 1).await;
                return Err(error);
            }
        }

        Ok(())
    }

    async fn unprovision(&self, id: TopicId, indexes: Range<u32>) {
        for index in indexes {
            let log = Log::Partition(Partition::new(id, index));
            self.service.destroy_log(&log).await;
        }
    }

    async fn seal(
        &self,
        topic: &Topic,
        closed_by: Option<ClosedBy>,
    ) -> Result<Topic, ServiceError> {
        let sealed = self
            .modify(topic, |mut topic| {
                topic.closed = true;
                if closed_by.is_some() {
                    topic.closed_by = closed_by.clone();
                }
                topic.touched()
            })
            .await?;

        for log in partition_logs(&sealed) {
            self.service.notify_closed(&log);
        }

        Ok(sealed)
    }

    async fn touch(&self, topic: &Topic) -> Result<Topic, ServiceError> {
        if topic.clone().touched() == *topic {
            return Ok(topic.clone());
        }

        self.modify(topic, Topic::touched).await
    }

    async fn modify(
        &self,
        topic: &Topic,
        change: impl Fn(Topic) -> Topic,
    ) -> Result<Topic, ServiceError> {
        let canonical = topic.name.canonical();
        let key = topic_key(canonical);

        for _ in 0..MAX_WRITE_ATTEMPTS {
            let Some((current, stored)) = self
                .load(canonical)
                .await?
                .filter(|(current, _)| current.id == topic.id)
            else {
                return Err(ServiceError::kind(ErrorKind::NotFound));
            };

            let next = change(current.clone());
            if next == current {
                return Ok(current);
            }

            if self
                .kv
                .put_kv_if(&key, &stored, next.encode())
                .await?
                .is_some()
            {
                return Ok(next);
            }
        }

        Err(ServiceError::with_message(
            ErrorKind::Conflict,
            None,
            false,
            "topic changed concurrently",
        ))
    }

    async fn expire(&self, topic: &Topic, stored: &Bytes) {
        let lock = self.lock(topic.name.canonical());
        let _exclusive = lock.write().await;

        let Ok(Some((current, current_stored))) = self.load(topic.name.canonical()).await else {
            return;
        };
        if current_stored != *stored || !current.expired(now_ms()) {
            return;
        }

        self.remove(&current, &current_stored).await;
    }

    async fn remove(&self, topic: &Topic, stored: &Bytes) -> bool {
        let removed = self
            .kv
            .del_kv_if(&topic_key(topic.name.canonical()), stored)
            .await;
        if !matches!(removed, Ok(Some(_))) {
            return false;
        }

        let _ = self
            .kv
            .del_kv_if(
                &id_key(&topic.id),
                &Bytes::from(topic.name.canonical().to_owned()),
            )
            .await;
        for log in partition_logs(topic) {
            self.service.destroy_log(&log).await;
        }

        true
    }

    async fn require(&self, name: &str) -> Result<Topic, ServiceError> {
        self.get(name)
            .await?
            .ok_or_else(|| ServiceError::kind(ErrorKind::NotFound))
    }

    async fn require_live(&self, canonical: &str) -> Result<Topic, ServiceError> {
        self.load(canonical)
            .await?
            .map(|(topic, _)| topic)
            .filter(|topic| !topic.expired(now_ms()))
            .ok_or_else(|| ServiceError::kind(ErrorKind::NotFound))
    }

    async fn find(&self, id: &TopicId) -> Result<Option<(Topic, Bytes)>, ServiceError> {
        self.fresh(|| self.record_by_id(id)).await
    }

    async fn load(&self, canonical: &str) -> Result<Option<(Topic, Bytes)>, ServiceError> {
        self.fresh(|| self.record(canonical)).await
    }

    async fn fresh<T>(
        &self,
        read: impl Fn() -> Result<Option<T>, ServiceError>,
    ) -> Result<Option<T>, ServiceError> {
        if let Some(found) = read()? {
            return Ok(Some(found));
        }

        tokio::time::timeout(SYNC_TIMEOUT, self.kv.sync())
            .await
            .map_err(|_| {
                ServiceError::with_message(
                    ErrorKind::Timeout,
                    None,
                    false,
                    "metadata did not catch up in time",
                )
            })??;
        read()
    }

    fn record_by_id(&self, id: &TopicId) -> Result<Option<(Topic, Bytes)>, ServiceError> {
        let Some(canonical) = self
            .kv
            .get_kv(&id_key(id))
            .and_then(|value| String::from_utf8(value.to_vec()).ok())
        else {
            return Ok(None);
        };

        Ok(self
            .record(&canonical)?
            .filter(|(topic, _)| topic.id == *id))
    }

    fn record(&self, canonical: &str) -> Result<Option<(Topic, Bytes)>, ServiceError> {
        let Some(stored) = self.kv.get_kv(&topic_key(canonical)) else {
            return Ok(None);
        };
        let topic = Topic::decode(&stored)?;

        Ok(Some((topic, stored)))
    }

    fn lock(&self, canonical: &str) -> Arc<RwLock<()>> {
        let mut locks = self.locks.lock().unwrap();
        if let Some(lock) = locks.get(canonical) {
            return lock.clone();
        }

        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        locks.entry(canonical.to_owned()).or_default().clone()
    }
}

pub fn live_topics(state: &MetadataState, now_ms: i64) -> impl Iterator<Item = Topic> + '_ {
    state
        .kv()
        .range(TOPIC_PREFIX.to_owned()..)
        .take_while(|(key, _)| key.starts_with(TOPIC_PREFIX))
        .filter_map(|(_, value)| Topic::decode(value).ok())
        .filter(move |topic| !topic.expired(now_ms))
}

pub fn partition_log(topic: &Topic, index: u32) -> Result<Log, ServiceError> {
    if index >= topic.partitions {
        return Err(ServiceError::kind(ErrorKind::NotFound));
    }

    Ok(Log::Partition(Partition::new(topic.id, index)))
}

fn partition_logs(topic: &Topic) -> Vec<Log> {
    (0..topic.partitions)
        .map(|index| Log::Partition(Partition::new(topic.id, index)))
        .collect()
}

fn topic_key(canonical: &str) -> String {
    format!("{TOPIC_PREFIX}{canonical}")
}

fn id_key(id: &TopicId) -> String {
    format!("{TOPIC_ID_PREFIX}{}", id.hex())
}

fn config_matches(topic: &Topic, command: &CreateCommand) -> bool {
    mime_equals(Some(&topic.content_type), Some(&command.content_type))
        && topic.partitions == command.partitions
        && topic.ttl_seconds == command.ttl_seconds
        && topic.expires_at_ms == command.expires_at_ms
        && topic.closed == command.closed
        && topic.retention == command.retention
}

fn producer_reject(
    topic: &Topic,
    next: OffsetToken,
    decision: ProducerDecision,
    command: &AppendCommand,
) -> Result<AppendResult, ServiceError> {
    match decision {
        ProducerDecision::Duplicate { last_seq } => Ok(AppendResult {
            next_offset: next,
            applied: false,
            timestamp_ms: None,
            closed: topic.closed,
            producer_epoch: command.producer.as_ref().map(|p| p.epoch),
            producer_seq: Some(last_seq),
        }),
        ProducerDecision::StaleEpoch { current_epoch } => Err(ServiceError::fenced(current_epoch)),
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

fn append_to_closed(
    topic: &Topic,
    command: &AppendCommand,
    next: OffsetToken,
) -> Result<AppendResult, ServiceError> {
    let has_producer = command.producer.is_some();

    if command.close_after && command.records.is_empty() {
        if has_producer && !matches_closed_by(topic, command) && topic.closed_by.is_some() {
            return Err(ServiceError::at(ErrorKind::Closed, next, true));
        }

        let echoed_seq = command.producer.as_ref().map(|producer| {
            topic
                .closed_by
                .as_ref()
                .map(|c| c.seq)
                .unwrap_or(producer.seq)
        });
        return Ok(AppendResult {
            next_offset: next,
            applied: false,
            timestamp_ms: None,
            closed: true,
            producer_epoch: command.producer.as_ref().map(|p| p.epoch),
            producer_seq: echoed_seq,
        });
    }

    if has_producer && matches_closed_by(topic, command) {
        return Ok(AppendResult {
            next_offset: next,
            applied: false,
            timestamp_ms: None,
            closed: true,
            producer_epoch: command.producer.as_ref().map(|p| p.epoch),
            producer_seq: topic.closed_by.as_ref().map(|c| c.seq),
        });
    }

    Err(ServiceError::at(ErrorKind::Closed, next, true))
}

fn closed_by(command: &AppendCommand) -> Option<ClosedBy> {
    command.producer.as_ref().map(|producer| ClosedBy {
        producer_id: producer.producer_id.clone(),
        epoch: producer.epoch,
        seq: producer.seq,
    })
}

fn matches_closed_by(topic: &Topic, command: &AppendCommand) -> bool {
    match (&topic.closed_by, &command.producer) {
        (Some(closed_by), Some(producer)) => {
            closed_by.producer_id == producer.producer_id
                && closed_by.epoch == producer.epoch
                && closed_by.seq == producer.seq
        }
        _ => false,
    }
}

fn bad_request(message: impl Into<String>) -> ServiceError {
    ServiceError::with_message(ErrorKind::BadRequest, None, false, message)
}

fn added_partitions(
    topic: &Topic,
    partitions: u32,
    owners: &[i32],
) -> Result<Range<u32>, ServiceError> {
    if topic.closed {
        return Err(ServiceError::kind(ErrorKind::Closed));
    }
    if partitions <= topic.partitions {
        return Err(bad_request(format!(
            "topic already has {} partitions, it can only grow",
            topic.partitions
        )));
    }
    if partitions - topic.partitions > MAX_NEW_PARTITIONS {
        return Err(bad_request(format!(
            "at most {MAX_NEW_PARTITIONS} partitions can be added at once"
        )));
    }

    let added = topic.partitions..partitions;
    if !owners.is_empty() && owners.len() != added.len() {
        return Err(bad_request(
            "owners must name a node for every added partition",
        ));
    }

    Ok(added)
}
