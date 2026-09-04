//! Compaction: merges small committed objects into fewer, larger, stream-major ones.
//!
//! `CompactionAnalyzer` (plan builder), `CompactionUploader`, plus
//! `s3.StreamObjectCompactor` (per-stream object compaction, including composite
//! objects).
//!
//! Two independent compaction paths, both preserved:
//! 1. **Stream set compaction** (`CompactionManager`): periodically rewrites this

pub mod executor;
pub mod plan;
pub mod stream_compactor;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;

use s3stream_object::{NOOP_OBJECT_ID, ObjectAttributes, ObjectStorage, S3ObjectMetadata};

use crate::api::StreamError;
use crate::manager::{
    CommitStreamSetObjectRequest, ObjectManager, StreamManager, StreamMetadata, StreamObject,
};
use crate::storage::upload::AsyncRateLimiter;

pub use executor::{
    CompactionUploader, DataBlockReader, DataBlockWriter, FetchedBlock, S3_OBJECT_MAX_READ_BATCH,
    S3_OBJECT_TTL_MINUTES, build_data_block_indices_from_group,
    build_object_stream_ranges_from_group,
};
pub use plan::{
    CompactOperations, CompactResult, CompactedObject, CompactionAnalyzer, CompactionPlan,
    CompactionType, GroupByLimitPredicate, GroupByOffsetPredicate, StreamDataBlock,
    filter_blocks_to_compact, group_stream_data_blocks, sort_stream_range_positions,
};
pub use stream_compactor::{CompactionLevel, StreamObjectCompactor, StreamView};

const MIN_COMPACTION_DELAY_MS: u64 = 10_000;
const MAX_THROTTLE_BYTES_PER_SEC: u64 = 1_000_000_000;

/// Stream set compaction configuration, consumed by `CompactionManager`.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub compaction_interval_min: u64,
    pub compaction_cache_size: u64,
    pub stream_split_size: u64,
    pub force_split_period_min: u64,
    pub max_object_num_to_compact: usize,
    pub max_stream_num_per_stream_set_object: usize,
    pub max_stream_object_num_per_commit: usize,
    pub network_bandwidth: u64,
    pub object_part_size: usize,
}

impl CompactionConfig {
    pub fn defaults() -> Self {
        Self {
            compaction_interval_min: 20,
            compaction_cache_size: 200 * 1024 * 1024,
            stream_split_size: 8 * 1024 * 1024,
            force_split_period_min: 120,
            max_object_num_to_compact: 500,
            max_stream_num_per_stream_set_object: 100_000,
            max_stream_object_num_per_commit: 10_000,
            network_bandwidth: 100 * 1024 * 1024,
            object_part_size: 16 * 1024 * 1024,
        }
    }
}

/// Stream set compaction scheduler + executor.
pub struct CompactionManager {
    config: CompactionConfig,
    object_manager: Arc<dyn ObjectManager>,
    stream_manager: Arc<dyn StreamManager>,
    object_storage: Arc<dyn ObjectStorage>,
    analyzer: CompactionAnalyzer,
    running: Arc<AtomicBool>,
}

impl CompactionManager {
    pub fn new(
        config: CompactionConfig,
        object_manager: Arc<dyn ObjectManager>,
        stream_manager: Arc<dyn StreamManager>,
        object_storage: Arc<dyn ObjectStorage>,
    ) -> Arc<Self> {
        let analyzer = CompactionAnalyzer::new(
            config.compaction_cache_size,
            config.stream_split_size,
            config.max_stream_num_per_stream_set_object,
            config.max_stream_object_num_per_commit,
        );
        Arc::new(Self {
            config,
            object_manager,
            stream_manager,
            object_storage,
            analyzer,
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Start the periodic scheduler.
    ///
    /// Reschedules with `max(MIN_COMPACTION_DELAY, interval - elapsed)`, or
    /// after only 10s when a round left remaining objects.
    pub fn start(self: &Arc<Self>) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let interval_ms = manager.config.compaction_interval_min * 60 * 1000;
            let mut delay_ms = interval_ms;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                if !manager.running.load(Ordering::Acquire) {
                    return;
                }
                let started = std::time::Instant::now();
                let remaining = match manager.compact_once().await {
                    Ok(remaining) => {
                        tracing::info!(
                            cost_ms = started.elapsed().as_millis() as u64,
                            "compaction complete"
                        );
                        remaining
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "compaction failed");
                        false
                    }
                };
                delay_ms = if remaining {
                    MIN_COMPACTION_DELAY_MS
                } else {
                    MIN_COMPACTION_DELAY_MS
                        .max(interval_ms.saturating_sub(started.elapsed().as_millis() as u64))
                };
            }
        });
    }

    /// (cooperative: in-flight round observes the
    /// flag between stages).
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Run one compaction round over this node's stream set objects. Returns whether
    /// `hasRemainingObjects`, which shortens the next delay).
    ///
    /// (the `getServerObjects → indices → streams →
    /// filter → force-split + compact` pipeline).
    pub async fn compact_once(&self) -> Result<bool, StreamError> {
        let objects = self.object_manager.get_server_objects().await?;
        if objects.is_empty() {
            return Ok(false);
        }
        let objects = deduplicate_objects_by_id(objects);
        let mut block_map = self.build_stream_data_block_map(&objects).await?;

        let mut stream_ids: Vec<u64> = Vec::new();
        for blocks in block_map.values() {
            for block in blocks {
                if block.block_size() as u64 > self.config.compaction_cache_size {
                    return Err(StreamError::Unexpected(format!(
                        "block size {} exceeds compaction cache size {}",
                        block.block_size(),
                        self.config.compaction_cache_size
                    )));
                }
                if !stream_ids.contains(&block.stream_id()) {
                    stream_ids.push(block.stream_id());
                }
            }
        }
        let streams = self.stream_manager.get_streams(&stream_ids).await?;
        filter_invalid_stream_data_blocks(&streams, &mut block_map);

        let now_ms = current_time_ms();
        let force_split_before =
            now_ms.saturating_sub(self.config.force_split_period_min as i64 * 60 * 1000);
        let (to_force_split, to_compact): (Vec<_>, Vec<_>) = objects
            .into_iter()
            .partition(|o| o.data_timestamp_ms <= force_split_before);

        let total_size: u64 = to_force_split
            .iter()
            .chain(to_compact.iter())
            .map(|o| o.object_size)
            .sum();
        let expect_complete_min = self.config.compaction_interval_min.max(2) - 1;
        let expect_read_bytes_per_sec =
            (expect_complete_min * 60).max(total_size / expect_complete_min / 60);
        let throttle = if expect_read_bytes_per_sec < MAX_THROTTLE_BYTES_PER_SEC {
            Some(Arc::new(AsyncRateLimiter::new(
                expect_read_bytes_per_sec as f64,
            )))
        } else {
            None
        };

        if !to_force_split.is_empty() {
            self.force_split_objects(&streams, &to_force_split, &block_map, throttle.clone())
                .await?;
        }
        self.compact_objects(&streams, to_compact, &block_map, throttle)
            .await
    }

    /// Force split every stream set object into per-stream objects.
    pub async fn force_split_all(&self) -> Result<(), StreamError> {
        let objects = self.object_manager.get_server_objects().await?;
        if objects.is_empty() {
            return Ok(());
        }
        let objects = deduplicate_objects_by_id(objects);
        let mut block_map = self.build_stream_data_block_map(&objects).await?;
        let stream_ids: Vec<u64> = {
            let mut ids: Vec<u64> = block_map
                .values()
                .flatten()
                .map(StreamDataBlock::stream_id)
                .collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };
        let streams = self.stream_manager.get_streams(&stream_ids).await?;
        filter_invalid_stream_data_blocks(&streams, &mut block_map);
        self.force_split_objects(&streams, &objects, &block_map, None)
            .await
    }

    /// Objects whose index cannot be read (e.g. deleted) are skipped.
    async fn build_stream_data_block_map(
        &self,
        objects: &[S3ObjectMetadata],
    ) -> Result<HashMap<u64, Vec<StreamDataBlock>>, StreamError> {
        let mut map = HashMap::new();
        for metadata in objects {
            let reader = DataBlockReader::new(metadata.clone(), self.object_storage.clone(), None);
            match reader.parse_data_block_index().await {
                Ok(blocks) => {
                    map.insert(metadata.object_id, blocks);
                }
                Err(e) => {
                    tracing::warn!(
                        object_id = metadata.object_id,
                        error = %e,
                        "failed to get data block index, skipping object"
                    );
                }
            }
        }
        Ok(map)
    }

    /// Each object splits into one stream object per continuous run, batched
    /// under the compaction cache size.
    async fn force_split_objects(
        &self,
        streams: &[StreamMetadata],
        objects: &[S3ObjectMetadata],
        block_map: &HashMap<u64, Vec<StreamDataBlock>>,
        throttle: Option<Arc<AsyncRateLimiter>>,
    ) -> Result<(), StreamError> {
        for metadata in objects {
            if !self.running.load(Ordering::Acquire) {
                return Ok(());
            }
            let Some(blocks) = block_map.get(&metadata.object_id) else {
                continue;
            };
            let mut request = CommitStreamSetObjectRequest {
                object_id: NOOP_OBJECT_ID,
                compacted_object_ids: vec![metadata.object_id],
                ..Default::default()
            };
            if !blocks.is_empty() {
                let stream_objects = self
                    .group_and_split(metadata, blocks, throttle.clone())
                    .await?;
                request.stream_objects = stream_objects;
            }
            if is_sanity_check_failed(streams, block_map, &request) {
                tracing::error!(
                    object_id = metadata.object_id,
                    "sanity check failed, force split result is illegal"
                );
                continue;
            }
            self.object_manager
                .commit_stream_set_object(request)
                .await?;
        }
        Ok(())
    }

    async fn group_and_split(
        &self,
        metadata: &S3ObjectMetadata,
        blocks: &[StreamDataBlock],
        throttle: Option<Arc<AsyncRateLimiter>>,
    ) -> Result<Vec<StreamObject>, StreamError> {
        let mut predicate = GroupByOffsetPredicate::new();
        let groups = group_stream_data_blocks(blocks, |b| predicate.test(b));
        let mut stream_objects = Vec::with_capacity(groups.len());

        let mut index = 0;
        while index < groups.len() {
            // Batch groups under the cache budget (measured as source byte span).
            let mut batch = Vec::new();
            let mut read_size = 0u64;
            while index < groups.len() {
                let group = &groups[index];
                let span =
                    group[group.len() - 1].index.end_position() - group[0].block_start_position();
                if !batch.is_empty() && read_size + span > self.config.compaction_cache_size {
                    break;
                }
                read_size += span;
                batch.push(group.clone());
                index += 1;
            }
            if batch.is_empty() {
                return Err(StreamError::Unexpected(
                    "force split failed: compaction cache size too small for one group".into(),
                ));
            }
            let object_id = self
                .object_manager
                .prepare_object(batch.len(), S3_OBJECT_TTL_MINUTES * 60 * 1000)
                .await?;
            let blocks_to_read: Vec<StreamDataBlock> = batch.iter().flatten().copied().collect();
            let reader = DataBlockReader::new(
                metadata.clone(),
                self.object_storage.clone(),
                throttle.clone(),
            );
            let max_batch = S3_OBJECT_MAX_READ_BATCH.min(self.config.network_bandwidth);
            let fetched = reader.read_blocks(&blocks_to_read, max_batch).await?;
            let data: HashMap<(u64, u64), Bytes> = fetched
                .into_iter()
                .map(|f| ((f.block.object_id, f.block.block_start_position()), f.data))
                .collect();

            for (i, group) in batch.iter().enumerate() {
                let group_object_id = object_id + i as u64;
                let mut writer = DataBlockWriter::open(
                    group_object_id,
                    self.object_storage.as_ref(),
                    self.config.object_part_size,
                )
                .await?;
                for block in group {
                    let bytes = data[&(block.object_id, block.block_start_position())].clone();
                    writer
                        .write(&FetchedBlock {
                            block: *block,
                            data: bytes,
                        })
                        .await?;
                }
                let (size, bucket_id) = writer.close().await?;
                stream_objects.push(StreamObject {
                    object_id: group_object_id,
                    object_size: size,
                    stream_id: group[0].stream_id(),
                    start_offset: group[0].start_offset(),
                    end_offset: group[group.len() - 1].end_offset(),
                    attributes: ObjectAttributes::new(bucket_id, false, false).0,
                });
            }
        }
        Ok(stream_objects)
    }

    /// Returns `true` when objects remained beyond the per-round cap.
    async fn compact_objects(
        &self,
        streams: &[StreamMetadata],
        mut objects: Vec<S3ObjectMetadata>,
        block_map: &HashMap<u64, Vec<StreamDataBlock>>,
        throttle: Option<Arc<AsyncRateLimiter>>,
    ) -> Result<bool, StreamError> {
        if objects.is_empty() {
            return Ok(false);
        }
        // Sort by data time descending. Compact the newest first when capped.
        objects.sort_by_key(|o| std::cmp::Reverse(o.data_timestamp_ms));
        let mut has_remaining = false;
        if objects.len() > self.config.max_object_num_to_compact {
            objects.truncate(self.config.max_object_num_to_compact);
            has_remaining = true;
        }

        let mut to_compact: HashMap<u64, Vec<StreamDataBlock>> = HashMap::new();
        for metadata in &objects {
            if let Some(blocks) = block_map.get(&metadata.object_id) {
                to_compact.insert(metadata.object_id, blocks.clone());
            }
        }

        let mut excluded = HashSet::new();
        let plans = self.analyzer.analyze(to_compact.clone(), &mut excluded);
        let objects: Vec<S3ObjectMetadata> = objects
            .into_iter()
            .filter(|o| !excluded.contains(&o.object_id))
            .collect();

        let mut request = CommitStreamSetObjectRequest {
            object_id: NOOP_OBJECT_ID,
            ..Default::default()
        };
        let mut compacted_object_ids: Vec<u64> = Vec::new();

        if !plans.is_empty() {
            let metadata_by_id: HashMap<u64, &S3ObjectMetadata> =
                objects.iter().map(|o| (o.object_id, o)).collect();
            let mut uploader = CompactionUploader::new(
                self.object_manager.clone(),
                self.object_storage.clone(),
                self.config.object_part_size,
            );
            // Blocks written to the stream set object across all plans, in order
            let mut stream_set_blocks: Vec<StreamDataBlock> = Vec::new();
            let max_batch = S3_OBJECT_MAX_READ_BATCH.min(self.config.network_bandwidth);
            for plan in &plans {
                if !self.running.load(Ordering::Acquire) {
                    uploader.release().await;
                    return Ok(false);
                }
                // Stage reads: one reader per source object.
                let mut data: HashMap<(u64, u64), Bytes> = HashMap::new();
                for (object_id, blocks) in &plan.stream_data_blocks_map {
                    let metadata = metadata_by_id
                        .get(object_id)
                        .unwrap_or_else(|| panic!("[BUG] object {object_id} not in metadata"));
                    let reader = DataBlockReader::new(
                        (*metadata).clone(),
                        self.object_storage.clone(),
                        throttle.clone(),
                    );
                    for fetched in reader.read_blocks(blocks, max_batch).await? {
                        data.insert(
                            (
                                fetched.block.object_id,
                                fetched.block.block_start_position(),
                            ),
                            fetched.data,
                        );
                    }
                }
                // Stage writes.
                for compacted in &plan.compacted_objects {
                    match compacted.compaction_type {
                        CompactionType::Compact => {
                            stream_set_blocks.extend(compacted.blocks.iter().copied());
                            uploader.write_stream_set_object(compacted, &data).await?;
                        }
                        CompactionType::Split => {
                            if let Some(stream_object) =
                                uploader.write_stream_object(compacted, &data).await?
                            {
                                request.stream_objects.push(stream_object);
                            }
                        }
                    }
                }
            }
            let mut predicate = GroupByOffsetPredicate::new();
            let groups = group_stream_data_blocks(&stream_set_blocks, |b| predicate.test(b));
            request.stream_ranges = build_object_stream_ranges_from_group(&groups);
            request.object_size = uploader.complete().await?;
            request.object_id = uploader.stream_set_object_id().unwrap_or(NOOP_OBJECT_ID);
            request.attributes = ObjectAttributes::new(uploader.bucket_id(), false, false).0;

            for plan in &plans {
                for blocks in plan.stream_data_blocks_map.values() {
                    for block in blocks {
                        if !compacted_object_ids.contains(&block.object_id) {
                            compacted_object_ids.push(block.object_id);
                        }
                    }
                }
            }
        }

        for (object_id, blocks) in &to_compact {
            if blocks.is_empty() && !compacted_object_ids.contains(object_id) {
                compacted_object_ids.push(*object_id);
            }
        }
        if compacted_object_ids.is_empty() {
            return Ok(has_remaining);
        }
        compacted_object_ids.sort_unstable();
        request.compacted_object_ids = compacted_object_ids;

        if is_sanity_check_failed(streams, block_map, &request) {
            tracing::error!("sanity check failed, compaction result is illegal");
            return Ok(has_remaining);
        }
        self.object_manager
            .commit_stream_set_object(request)
            .await?;
        Ok(has_remaining)
    }
}

fn deduplicate_objects_by_id(objects: Vec<S3ObjectMetadata>) -> Vec<S3ObjectMetadata> {
    let mut seen = HashSet::new();
    objects
        .into_iter()
        .filter(|o| seen.insert(o.object_id))
        .collect()
}

pub fn filter_invalid_stream_data_blocks(
    streams: &[StreamMetadata],
    block_map: &mut HashMap<u64, Vec<StreamDataBlock>>,
) {
    let start_offsets: HashMap<u64, u64> = streams
        .iter()
        .map(|s| (s.stream_id, s.start_offset))
        .collect();
    for blocks in block_map.values_mut() {
        blocks.retain(|block| {
            start_offsets
                .get(&block.stream_id())
                .is_some_and(|&start| block.end_offset() > start)
        });
    }
}

/// Every untrimmed source block must be covered by the request's output ranges.
fn is_sanity_check_failed(
    streams: &[StreamMetadata],
    block_map: &HashMap<u64, Vec<StreamDataBlock>>,
    request: &CommitStreamSetObjectRequest,
) -> bool {
    let stream_start: HashMap<u64, u64> = streams
        .iter()
        .map(|s| (s.stream_id, s.start_offset))
        .collect();
    // Merge output ranges per stream.
    let mut by_stream: BTreeMap<u64, Vec<(u64, u64)>> = BTreeMap::new();
    for range in &request.stream_ranges {
        by_stream
            .entry(range.stream_id)
            .or_default()
            .push((range.start_offset, range.end_offset));
    }
    for object in &request.stream_objects {
        by_stream
            .entry(object.stream_id)
            .or_default()
            .push((object.start_offset, object.end_offset));
    }
    let merged: HashMap<u64, Vec<(u64, u64)>> = by_stream
        .into_iter()
        .map(|(stream_id, mut ranges)| {
            ranges.sort_unstable();
            let mut out: Vec<(u64, u64)> = Vec::new();
            for (start, end) in ranges {
                match out.last_mut() {
                    Some((_, last_end)) if start <= *last_end => *last_end = (*last_end).max(end),
                    _ => out.push((start, end)),
                }
            }
            (stream_id, out)
        })
        .collect();

    for object_id in &request.compacted_object_ids {
        let Some(blocks) = block_map.get(object_id) else {
            continue;
        };
        for block in blocks {
            let Some(&start) = stream_start.get(&block.stream_id()) else {
                continue; // non-existent stream: skip
            };
            if block.end_offset() <= start {
                continue; // trimmed
            }
            let covered = merged.get(&block.stream_id()).is_some_and(|ranges| {
                ranges
                    .iter()
                    .any(|&(s, e)| s <= block.start_offset() && block.end_offset() <= e)
            });
            if !covered {
                tracing::error!(
                    object_id,
                    stream_id = block.stream_id(),
                    start_offset = block.start_offset(),
                    "sanity check failed: block missing after compact"
                );
                return true;
            }
        }
    }
    false
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::CommitStreamSetObjectRequest;
    use crate::memory::MemoryMetadataManager;
    use s3stream_codec::StreamRecordBatch;
    use s3stream_object::{MemoryObjectStorage, ObjectWriter, WriteOptions};

    struct Harness {
        manager: Arc<MemoryMetadataManager>,
        storage: Arc<MemoryObjectStorage>,
    }

    impl Harness {
        async fn new() -> Self {
            Self {
                manager: MemoryMetadataManager::new(),
                storage: Arc::new(MemoryObjectStorage::new(0)),
            }
        }

        /// Write a stream set object holding `(stream, start, count)` ranges and
        /// commit it as a server (stream set) object.
        async fn put_stream_set_object(
            &self,
            ranges: &[(u64, u64, u64)],
        ) -> Result<u64, StreamError> {
            let object_id = self.manager.prepare_object(1, 60_000).await?;
            let mut writer = ObjectWriter::open(
                object_id,
                self.storage.as_ref(),
                1024,
                16 << 20,
                WriteOptions::default(),
            )
            .await
            .unwrap();
            let mut stream_ranges = Vec::new();
            for &(stream_id, start, count) in ranges {
                let records: Vec<StreamRecordBatch> = (start..start + count)
                    .map(|o| StreamRecordBatch::new(stream_id, 1, o, 1, vec![o as u8; 128].into()))
                    .collect();
                writer.write(stream_id, &records).await.unwrap();
                stream_ranges.push(s3stream_object::ObjectStreamRange {
                    stream_id,
                    epoch: 1,
                    start_offset: start,
                    end_offset: start + count,
                    size: records.iter().map(|r| r.size() as u64).sum(),
                });
            }
            let size = writer.close().await.unwrap();
            self.manager
                .commit_stream_set_object(CommitStreamSetObjectRequest {
                    object_id,
                    object_size: size,
                    stream_ranges,
                    ..Default::default()
                })
                .await?;
            Ok(object_id)
        }

        fn compaction_manager(&self) -> Arc<CompactionManager> {
            let mut config = CompactionConfig::defaults();
            config.stream_split_size = 256; // small so split paths trigger
            CompactionManager::new(
                config,
                self.manager.clone() as Arc<dyn ObjectManager>,
                self.manager.clone() as Arc<dyn StreamManager>,
                self.storage.clone() as Arc<dyn ObjectStorage>,
            )
        }

        async fn read_all(&self, stream_id: u64, end: u64) -> Vec<u64> {
            use crate::cache::block_cache::S3BlockCache;
            let cache = crate::cache::blockcache::StreamReaders::new(
                64 << 20,
                self.manager.clone() as Arc<dyn ObjectManager>,
                self.storage.clone() as Arc<dyn ObjectStorage>,
                1,
            );
            let mut offsets = Vec::new();
            let mut next = 0u64;
            while next < end {
                let read = cache.read(stream_id, next, end, 1 << 20).await.unwrap();
                for record in &read.records {
                    for o in record.base_offset()..record.last_offset() {
                        offsets.push(o);
                    }
                }
                next = read.records.last().unwrap().last_offset();
            }
            offsets
        }
    }

    /// After compaction, every previously readable offset range is still readable.
    /// Source objects are consumed (compacted_object_ids) from metadata.
    #[tokio::test]
    async fn compaction_preserves_readability() {
        let h = Harness::new().await;
        let s1 = h.manager.create_stream(Default::default()).await.unwrap();
        let s2 = h.manager.create_stream(Default::default()).await.unwrap();
        h.manager
            .open_stream(s1, 1, Default::default())
            .await
            .unwrap();
        h.manager
            .open_stream(s2, 1, Default::default())
            .await
            .unwrap();

        // Two stream set objects interleaving both streams (compactable: shared
        // streams across objects).
        h.put_stream_set_object(&[(s1, 0, 4), (s2, 0, 4)])
            .await
            .unwrap();
        h.put_stream_set_object(&[(s1, 4, 4), (s2, 4, 4)])
            .await
            .unwrap();

        let manager = h.compaction_manager();
        let remaining = manager.compact_once().await.unwrap();
        assert!(!remaining);

        // Old stream set objects are gone. Data still fully readable.
        let server_objects = h.manager.get_server_objects().await.unwrap();
        assert!(
            server_objects.len() <= 1,
            "sources should be compacted away, got {}",
            server_objects.len()
        );
        assert_eq!(h.read_all(s1, 8).await, (0..8).collect::<Vec<_>>());
        assert_eq!(h.read_all(s2, 8).await, (0..8).collect::<Vec<_>>());
    }

    /// Force split rewrites a stream set object into one stream object per
    /// continuous run and deletes the source.
    #[tokio::test]
    async fn force_split_all_splits_into_stream_objects() {
        let h = Harness::new().await;
        let s1 = h.manager.create_stream(Default::default()).await.unwrap();
        let s2 = h.manager.create_stream(Default::default()).await.unwrap();
        h.manager
            .open_stream(s1, 1, Default::default())
            .await
            .unwrap();
        h.manager
            .open_stream(s2, 1, Default::default())
            .await
            .unwrap();
        h.put_stream_set_object(&[(s1, 0, 4), (s2, 0, 4)])
            .await
            .unwrap();

        let manager = h.compaction_manager();
        manager.force_split_all().await.unwrap();

        let server_objects = h.manager.get_server_objects().await.unwrap();
        assert!(
            server_objects.is_empty(),
            "stream set object should be split away"
        );
        assert_eq!(h.read_all(s1, 4).await, (0..4).collect::<Vec<_>>());
        assert_eq!(h.read_all(s2, 4).await, (0..4).collect::<Vec<_>>());
    }
}
