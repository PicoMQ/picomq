//! Per-stream object compaction: cleanup of expired objects + merging a stream's
//! small objects into bigger ones (physically, or logically via composite
//! objects). Levels:
//! - `CLEANUP`: delete objects fully below the stream start offset (trimmed).
//! - `MINOR` / `MAJOR` / `MINOR_V1`: physical merge. Copy the live data region of each
//!   group member into one new stream object, regrouping the index at 1 MiB.
//! - `MAJOR_V1`: composite merge. Link component objects into a composite object

use std::sync::Arc;

use bytes::{BufMut, BytesMut};

use s3stream_object::composite::{CompositeObjectReader, CompositeObjectWriter};
use s3stream_object::{
    DataBlockIndex, FOOTER_MAGIC, FOOTER_SIZE, NOOP_OBJECT_ID, NOOP_OFFSET, ObjectPath,
    ObjectReader, ObjectStorage, ReadOptions, S3ObjectMetadata, ThrottleStrategy, WriteOptions,
    gen_object_key,
};

use crate::api::StreamError;
use crate::manager::{CompactStreamObjectRequest, ObjectManager};

use super::plan::CompactOperations;

pub const EXPIRED_OBJECTS_CLEAN_UP_STEP: usize = 1000;
pub const MINOR_COMPACTION_SIZE_THRESHOLD: u64 = 128 * 1024 * 1024;
pub const MINOR_V1_COMPACTION_SIZE_THRESHOLD: u64 = 4 * 1024 * 1024;
pub const DEFAULT_DATA_BLOCK_GROUP_SIZE_THRESHOLD: u64 = 1024 * 1024;
const MAX_DIRTY_BYTES: u64 = 512 * 1024 * 1024;
const MAX_PART_COUNT: u64 = 10_000;
const MAX_PART_SIZE: u64 = 5u64 * 1024 * 1024 * 1024;
const MAX_OBJECT_SIZE: u64 = MAX_PART_COUNT * MAX_PART_SIZE;
/// Chosen as min(5000, MAX_PART_COUNT / 2).
const MAX_OBJECT_GROUP_COUNT: usize = 5000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionLevel {
    Cleanup,
    Minor,
    Major,
    CleanupV1,
    MinorV1,
    MajorV1,
}

impl CompactionLevel {
    fn skip_single_object_group(self) -> bool {
        matches!(
            self,
            CompactionLevel::Minor
                | CompactionLevel::Major
                | CompactionLevel::MinorV1
                | CompactionLevel::MajorV1
        )
    }
}

/// Snapshot of the stream state the compactor operates against.
///
/// `startOffset`, `confirmOffset`).
#[derive(Debug, Clone, Copy)]
pub struct StreamView {
    pub stream_id: u64,
    pub stream_epoch: u64,
    pub start_offset: u64,
    pub confirm_offset: u64,
}

/// Per-stream object compaction task.
pub struct StreamObjectCompactor {
    object_manager: Arc<dyn ObjectManager>,
    object_storage: Arc<dyn ObjectStorage>,
    max_stream_object_size: u64,
    data_block_group_size_threshold: u64,
    minor_v1_compaction_threshold: u64,
    major_v1_skip_small_object: bool,
}

impl StreamObjectCompactor {
    pub fn new(
        object_manager: Arc<dyn ObjectManager>,
        object_storage: Arc<dyn ObjectStorage>,
        max_stream_object_size: u64,
    ) -> Self {
        Self {
            object_manager,
            object_storage,
            max_stream_object_size: max_stream_object_size.min(MAX_OBJECT_SIZE),
            data_block_group_size_threshold: DEFAULT_DATA_BLOCK_GROUP_SIZE_THRESHOLD,
            minor_v1_compaction_threshold: MINOR_V1_COMPACTION_SIZE_THRESHOLD,
            major_v1_skip_small_object: false,
        }
    }

    pub fn with_major_v1_skip_small_object(mut self, skip: bool) -> Self {
        self.major_v1_skip_small_object = skip;
        self
    }

    /// Run one compaction pass at `level` for `stream`.
    pub async fn compact(
        &self,
        stream: StreamView,
        level: CompactionLevel,
    ) -> Result<(), StreamError> {
        let objects = self
            .object_manager
            .get_stream_objects(stream.stream_id, 0, stream.confirm_offset, usize::MAX)
            .await?;
        let objects = deduplicate_objects_by_id(objects);
        let (expired, living): (Vec<_>, Vec<_>) = objects
            .into_iter()
            .partition(|o| end_offset_of(o) <= stream.start_offset);

        self.cleanup_expired(&stream, expired).await?;
        if level == CompactionLevel::Cleanup {
            return Ok(());
        }

        let groups: Vec<Vec<S3ObjectMetadata>> = if level == CompactionLevel::CleanupV1 {
            cleanup_v1_groups(&living, stream.start_offset)
        } else {
            group0(
                &living,
                self.max_group_size(level),
                level,
                if self.major_v1_skip_small_object {
                    self.minor_v1_compaction_threshold
                } else {
                    0
                },
            )
        };

        for group in groups {
            if group.len() == 1 && level.skip_single_object_group() {
                continue;
            }
            let object_id = self
                .object_manager
                .prepare_object(1, 60 * 60 * 1000)
                .await?;
            let request = match level {
                CompactionLevel::Minor | CompactionLevel::Major | CompactionLevel::MinorV1 => {
                    self.compact_by_physical_merge(&stream, &group, object_id)
                        .await?
                }
                CompactionLevel::MajorV1 | CompactionLevel::CleanupV1 => {
                    self.compact_by_composite_object(&stream, &group, object_id)
                        .await?
                }
                CompactionLevel::Cleanup => unreachable!(),
            };
            if let Some(request) = request {
                self.object_manager.compact_stream_object(request).await?;
            }
        }
        Ok(())
    }

    fn max_group_size(&self, level: CompactionLevel) -> u64 {
        match level {
            CompactionLevel::Minor => MINOR_COMPACTION_SIZE_THRESHOLD,
            CompactionLevel::Major | CompactionLevel::MajorV1 => self.max_stream_object_size,
            CompactionLevel::MinorV1 => self.minor_v1_compaction_threshold,
            _ => unreachable!("no group size for {level:?}"),
        }
    }

    /// Delete objects fully below the stream start offset, in steps of 1000.
    async fn cleanup_expired(
        &self,
        stream: &StreamView,
        expired: Vec<S3ObjectMetadata>,
    ) -> Result<(), StreamError> {
        for chunk in expired.chunks(EXPIRED_OBJECTS_CLEAN_UP_STEP) {
            if chunk.is_empty() {
                break;
            }
            let ids: Vec<u64> = chunk.iter().map(|o| o.object_id).collect();
            let operations = vec![CompactOperations::DeepDelete; ids.len()];
            self.object_manager
                .compact_stream_object(CompactStreamObjectRequest {
                    object_id: NOOP_OBJECT_ID,
                    object_size: 0,
                    stream_id: stream.stream_id,
                    stream_epoch: stream.stream_epoch,
                    start_offset: NOOP_OFFSET,
                    end_offset: NOOP_OFFSET,
                    source_object_ids: ids,
                    operations,
                    attributes: 0,
                })
                .await?;
        }
        Ok(())
    }

    /// Physically merge a group: copy live data regions, regroup the index at 1 MiB.
    async fn compact_by_physical_merge(
        &self,
        stream: &StreamView,
        group: &[S3ObjectMetadata],
        object_id: u64,
    ) -> Result<Option<CompactStreamObjectRequest>, StreamError> {
        let start_offset = stream.start_offset;
        let mut next_block_position = 0u64;
        let mut object_size = 0u64;
        let mut compacted_start_offset = start_offset_of(&group[0]);
        let compacted_end_offset = end_offset_of(&group[group.len() - 1]);
        let mut compacted_object_ids = Vec::with_capacity(group.len());
        let mut indexes = BytesMut::new();

        let write_options = WriteOptions {
            throttle: ThrottleStrategy::Compaction,
            ..Default::default()
        };
        let mut writer = self
            .object_storage
            .writer(&write_options, &gen_object_key(0, object_id))
            .await?;

        let mut group_start_offset = 0u64;
        let mut group_start_position = 0u64;
        let mut group_size = 0u64;
        let mut group_record_count = 0u64;
        let mut last_index: Option<DataBlockIndex> = None;

        for object in group {
            let reader = ObjectReader::new(object.clone(), self.object_storage.clone());
            let info = reader.basic_object_info().await?;
            let mut valid_data_start = 0u64;
            for data_block in info.index_block.entries() {
                if data_block.end_offset() <= start_offset {
                    valid_data_start = data_block.end_position();
                    compacted_start_offset = data_block.end_offset();
                    continue;
                }
                if group_size == 0
                    || group_size + data_block.size as u64 > self.data_block_group_size_threshold
                    || group_record_count + data_block.record_count as u64 > i32::MAX as u64
                    || data_block.end_offset() - group_start_offset > i32::MAX as u64
                {
                    if group_size != 0 {
                        let last = last_index.unwrap();
                        DataBlockIndex {
                            block_id: -1,
                            stream_id: stream.stream_id,
                            start_offset: group_start_offset,
                            end_offset_delta: (last.end_offset() - group_start_offset) as u32,
                            record_count: group_record_count as u32,
                            start_position: group_start_position,
                            size: group_size as u32,
                        }
                        .encode(&mut indexes);
                    }
                    group_start_offset = data_block.start_offset;
                    group_start_position = next_block_position;
                    group_size = 0;
                    group_record_count = 0;
                }
                group_size += data_block.size as u64;
                group_record_count += data_block.record_count as u64;
                next_block_position += data_block.size as u64;
                last_index = Some(*data_block);
            }
            // (server-side copy). We range-read + rewrite. Byte-identical output.
            let data_block_size = info.data_block_size;
            if valid_data_start < data_block_size {
                let read_options = ReadOptions {
                    throttle: ThrottleStrategy::Compaction,
                    ..Default::default()
                };
                let data = self
                    .object_storage
                    .range_read(
                        &read_options,
                        &object.key(),
                        valid_data_start,
                        Some(data_block_size),
                    )
                    .await?;
                writer.write(data).await?;
            }
            object_size += data_block_size - valid_data_start;
            compacted_object_ids.push(object.object_id);
        }
        if let (Some(last), true) = (last_index, group_size != 0) {
            DataBlockIndex {
                block_id: -1,
                stream_id: stream.stream_id,
                start_offset: group_start_offset,
                end_offset_delta: (last.end_offset() - group_start_offset) as u32,
                record_count: group_record_count as u32,
                start_position: group_start_position,
                size: group_size as u32,
            }
            .encode(&mut indexes);
        }

        let index_size = indexes.len();
        let mut tail = indexes;
        tail.reserve(FOOTER_SIZE);
        tail.put_u64(next_block_position);
        tail.put_u32(index_size as u32);
        tail.put_bytes(0, 40 - 8 - 4);
        tail.put_u64(FOOTER_MAGIC);
        object_size += tail.len() as u64;
        writer.write(tail.freeze()).await?;
        let result = writer.finish().await?;

        let operations = vec![CompactOperations::Delete; compacted_object_ids.len()];
        Ok(Some(CompactStreamObjectRequest {
            object_id,
            object_size,
            stream_id: stream.stream_id,
            stream_epoch: stream.stream_epoch,
            start_offset: compacted_start_offset,
            end_offset: compacted_end_offset,
            source_object_ids: compacted_object_ids,
            operations,
            attributes: s3stream_object::ObjectAttributes::new(result.bucket_id, false, false).0,
        }))
    }

    /// Logically merge a group into a composite object (no data copy).
    async fn compact_by_composite_object(
        &self,
        stream: &StreamView,
        group: &[S3ObjectMetadata],
        object_id: u64,
    ) -> Result<Option<CompactStreamObjectRequest>, StreamError> {
        let start_offset = stream.start_offset;
        let mut writer = CompositeObjectWriter::new(object_id, WriteOptions::default());
        let mut compacted_object_ids: Vec<u64> = Vec::new();
        let mut operations: Vec<CompactOperations> = Vec::new();

        for object in group {
            if object.attributes.is_composite() {
                let reader =
                    CompositeObjectReader::new(object.clone(), self.object_storage.clone());
                let info = reader.info().await?;
                let mut to_delete: Vec<ObjectPath> = Vec::new();
                for linked in &info.objects {
                    let blocks = &info.index_block.entries()
                        [linked.block_start_index as usize..linked.block_end_index as usize];
                    let has_live = blocks.iter().any(|b| b.end_offset() > start_offset);
                    let linked_metadata = S3ObjectMetadata {
                        object_id: linked.object_id,
                        object_type: s3stream_object::S3ObjectType::Stream,
                        offset_ranges: vec![],
                        object_size: 0,
                        attributes: s3stream_object::ObjectAttributes::new(
                            linked.bucket_id,
                            false,
                            false,
                        ),
                        committed_timestamp_ms: 0,
                        data_timestamp_ms: 0,
                    };
                    if has_live {
                        writer
                            .add_component(&linked_metadata, blocks.to_vec())
                            .map_err(StreamError::Object)?;
                    } else {
                        to_delete.push(ObjectPath {
                            bucket_id: linked.bucket_id,
                            key: linked_metadata.key(),
                        });
                    }
                }
                if !to_delete.is_empty() {
                    self.object_storage.delete(&to_delete).await?;
                }
                compacted_object_ids.push(object.object_id);
                operations.push(CompactOperations::Delete);
            } else {
                let reader = ObjectReader::new(object.clone(), self.object_storage.clone());
                let info = reader.basic_object_info().await?;
                writer
                    .add_component(object, info.index_block.entries().to_vec())
                    .map_err(StreamError::Object)?;
                compacted_object_ids.push(object.object_id);
                operations.push(CompactOperations::KeepData);
            }
        }

        match writer.stream_range() {
            None => {
                // All data blocks expired: delete the prepared object id too.
                compacted_object_ids.push(object_id);
                operations.push(CompactOperations::Delete);
                Ok(Some(CompactStreamObjectRequest {
                    object_id: NOOP_OBJECT_ID,
                    object_size: 0,
                    stream_id: stream.stream_id,
                    stream_epoch: stream.stream_epoch,
                    start_offset: NOOP_OFFSET,
                    end_offset: NOOP_OFFSET,
                    source_object_ids: compacted_object_ids,
                    operations,
                    attributes: 0,
                }))
            }
            Some((_, range_start, range_end)) => {
                writer.close(self.object_storage.as_ref()).await?;
                let attributes = s3stream_object::ObjectAttributes::new(
                    self.object_storage.bucket_id(),
                    true,
                    false,
                );
                Ok(Some(CompactStreamObjectRequest {
                    object_id,
                    object_size: writer.size(),
                    stream_id: stream.stream_id,
                    stream_epoch: stream.stream_epoch,
                    start_offset: range_start,
                    end_offset: range_end,
                    source_object_ids: compacted_object_ids,
                    operations,
                    attributes: attributes.0,
                }))
            }
        }
    }
}

fn start_offset_of(object: &S3ObjectMetadata) -> u64 {
    object
        .offset_ranges
        .first()
        .map(|r| r.start_offset)
        .unwrap_or(0)
}

fn end_offset_of(object: &S3ObjectMetadata) -> u64 {
    object
        .offset_ranges
        .last()
        .map(|r| r.end_offset)
        .unwrap_or(0)
}

fn deduplicate_objects_by_id(objects: Vec<S3ObjectMetadata>) -> Vec<S3ObjectMetadata> {
    let mut seen = std::collections::HashSet::new();
    objects
        .into_iter()
        .filter(|o| seen.insert(o.object_id))
        .collect()
}

fn object_filter(
    level: CompactionLevel,
    min_major_v1_size: u64,
    object: &S3ObjectMetadata,
) -> bool {
    let is_composite = object.attributes.is_composite();
    if level != CompactionLevel::MajorV1 && is_composite {
        return false;
    }
    if level == CompactionLevel::MajorV1 && !is_composite && object.object_size < min_major_v1_size
    {
        return false;
    }
    true
}

/// Group offset-continuous objects under size/count/part limits.
fn group0(
    objects: &[S3ObjectMetadata],
    max_stream_object_size: u64,
    level: CompactionLevel,
    min_major_v1_size: u64,
) -> Vec<Vec<S3ObjectMetadata>> {
    let mut groups: Vec<Vec<S3ObjectMetadata>> = Vec::new();
    let mut group: Vec<S3ObjectMetadata> = Vec::new();
    let mut group_size = 0u64;
    let mut group_start_offset = 0u64;
    let mut group_next_offset: Option<u64> = None;
    let mut part_count = 0u64;
    for object in objects {
        if !object_filter(level, min_major_v1_size, object) {
            continue;
        }
        let object_part_count = object.object_size.div_ceil(MAX_PART_SIZE);
        if object_part_count >= MAX_PART_COUNT {
            continue;
        }
        let start = start_offset_of(object);
        let end = end_offset_of(object);
        if group_next_offset.is_none() {
            group_start_offset = start;
            group_next_offset = Some(start);
        }
        if group_next_offset != Some(start)
            || (group_size + object.object_size > max_stream_object_size && !group.is_empty())
            || group.len() >= MAX_OBJECT_GROUP_COUNT
            || part_count + object_part_count > MAX_PART_COUNT
            || end - group_start_offset > i32::MAX as u64
        {
            if !group.is_empty() {
                groups.push(std::mem::take(&mut group));
            }
            group_size = 0;
            group_start_offset = start;
            part_count = 0;
        }
        group.push(object.clone());
        group_size += object.object_size;
        group_next_offset = Some(end);
        part_count += object_part_count;
    }
    if !group.is_empty() {
        groups.push(group);
    }
    groups
}

fn cleanup_v1_groups(living: &[S3ObjectMetadata], start_offset: u64) -> Vec<Vec<S3ObjectMetadata>> {
    let Some(first) = living.first() else {
        return Vec::new();
    };
    if !first.attributes.is_composite() {
        return Vec::new();
    }
    let first_start = start_offset_of(first);
    let first_end = end_offset_of(first);
    if first_end <= first_start {
        return Vec::new();
    }
    let dirty = (start_offset.saturating_sub(first_start)) as f64
        / (first_end - first_start) as f64
        * first.object_size as f64;
    if dirty > MAX_DIRTY_BYTES as f64 {
        vec![vec![first.clone()]]
    } else {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::{CommitStreamSetObjectRequest, StreamManager, StreamObject};
    use crate::memory::MemoryMetadataManager;
    use s3stream_codec::StreamRecordBatch;
    use s3stream_object::{MemoryObjectStorage, ObjectWriter};

    struct Harness {
        manager: Arc<MemoryMetadataManager>,
        storage: Arc<MemoryObjectStorage>,
    }

    impl Harness {
        async fn put_stream_object(&self, stream_id: u64, start: u64, count: u64) -> u64 {
            let object_id = self.manager.prepare_object(1, 60_000).await.unwrap();
            let mut writer = ObjectWriter::open(
                object_id,
                self.storage.as_ref(),
                1024,
                16 << 20,
                WriteOptions::default(),
            )
            .await
            .unwrap();
            let records: Vec<StreamRecordBatch> = (start..start + count)
                .map(|o| StreamRecordBatch::new(stream_id, 1, o, 1, vec![o as u8; 64].into()))
                .collect();
            writer.write(stream_id, &records).await.unwrap();
            let size = writer.close().await.unwrap();
            self.manager
                .commit_stream_set_object(CommitStreamSetObjectRequest {
                    object_id: NOOP_OBJECT_ID,
                    stream_objects: vec![StreamObject {
                        object_id,
                        object_size: size,
                        stream_id,
                        start_offset: start,
                        end_offset: start + count,
                        attributes: 0,
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            object_id
        }

        async fn stream_objects(&self, stream_id: u64) -> Vec<S3ObjectMetadata> {
            self.manager
                .get_stream_objects(stream_id, 0, u64::MAX, usize::MAX)
                .await
                .unwrap()
        }
    }

    async fn harness() -> Harness {
        Harness {
            manager: MemoryMetadataManager::new(),
            storage: Arc::new(MemoryObjectStorage::new(0)),
        }
    }

    /// MINOR merges adjacent small stream objects into one. The merged object serves
    /// all records and sources are removed.
    #[tokio::test]
    async fn minor_compaction_merges_adjacent_objects() {
        let h = harness().await;
        let stream_id = h.manager.create_stream(Default::default()).await.unwrap();
        h.manager
            .open_stream(stream_id, 1, Default::default())
            .await
            .unwrap();
        h.put_stream_object(stream_id, 0, 4).await;
        h.put_stream_object(stream_id, 4, 4).await;
        h.put_stream_object(stream_id, 8, 4).await;

        let compactor = StreamObjectCompactor::new(
            h.manager.clone() as Arc<dyn ObjectManager>,
            h.storage.clone() as Arc<dyn ObjectStorage>,
            1 << 30,
        );
        let view = StreamView {
            stream_id,
            stream_epoch: 1,
            start_offset: 0,
            confirm_offset: 12,
        };
        compactor
            .compact(view, CompactionLevel::Minor)
            .await
            .unwrap();

        let objects = h.stream_objects(stream_id).await;
        assert_eq!(objects.len(), 1, "three objects merged into one");
        let merged = &objects[0];
        assert_eq!(start_offset_of(merged), 0);
        assert_eq!(end_offset_of(merged), 12);

        // Merged object parses and serves all 12 records.
        let reader = ObjectReader::new(merged.clone(), h.storage.clone() as Arc<dyn ObjectStorage>);
        let info = reader.basic_object_info().await.unwrap();
        let records: u32 = info
            .index_block
            .entries()
            .iter()
            .map(|e| e.record_count)
            .sum();
        assert_eq!(records, 12);
    }

    /// CLEANUP deletes objects fully below the stream start offset and leaves the
    /// rest untouched.
    #[tokio::test]
    async fn cleanup_removes_expired_objects() {
        let h = harness().await;
        let stream_id = h.manager.create_stream(Default::default()).await.unwrap();
        h.manager
            .open_stream(stream_id, 1, Default::default())
            .await
            .unwrap();
        h.put_stream_object(stream_id, 0, 4).await;
        let live = h.put_stream_object(stream_id, 4, 4).await;

        let compactor = StreamObjectCompactor::new(
            h.manager.clone() as Arc<dyn ObjectManager>,
            h.storage.clone() as Arc<dyn ObjectStorage>,
            1 << 30,
        );
        let view = StreamView {
            stream_id,
            stream_epoch: 1,
            start_offset: 4,
            confirm_offset: 8,
        };
        compactor
            .compact(view, CompactionLevel::Cleanup)
            .await
            .unwrap();

        let objects = h.stream_objects(stream_id).await;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].object_id, live);
    }

    /// MAJOR_V1 links normal objects into a composite object without copying data.
    /// The composite reads back with all component blocks.
    #[tokio::test]
    async fn major_v1_builds_composite_object() {
        let h = harness().await;
        let stream_id = h.manager.create_stream(Default::default()).await.unwrap();
        h.manager
            .open_stream(stream_id, 1, Default::default())
            .await
            .unwrap();
        h.put_stream_object(stream_id, 0, 4).await;
        h.put_stream_object(stream_id, 4, 4).await;

        let compactor = StreamObjectCompactor::new(
            h.manager.clone() as Arc<dyn ObjectManager>,
            h.storage.clone() as Arc<dyn ObjectStorage>,
            1 << 30,
        );
        let view = StreamView {
            stream_id,
            stream_epoch: 1,
            start_offset: 0,
            confirm_offset: 8,
        };
        compactor
            .compact(view, CompactionLevel::MajorV1)
            .await
            .unwrap();

        let objects = h.stream_objects(stream_id).await;
        assert_eq!(objects.len(), 1);
        let composite = &objects[0];
        assert!(composite.attributes.is_composite());
        assert_eq!(start_offset_of(composite), 0);
        assert_eq!(end_offset_of(composite), 8);

        let reader = CompositeObjectReader::new(
            composite.clone(),
            h.storage.clone() as Arc<dyn ObjectStorage>,
        );
        let info = reader.info().await.unwrap();
        assert_eq!(info.objects.len(), 2);
        let records: u32 = info
            .index_block
            .entries()
            .iter()
            .map(|e| e.record_count)
            .sum();
        assert_eq!(records, 8);
    }
}
