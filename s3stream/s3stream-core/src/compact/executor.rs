//! Compaction execution: stream blocks from source objects into planned new
//! objects.
//!
//! `DataBlockReader` batches adjacent blocks into single range-GETs, capped
//! at `S3_OBJECT_MAX_READ_BATCH` and throttled at
//! `ThrottleStrategy::Compaction`. `DataBlockWriter` forwards block bytes
//! **verbatim** into the planned objects and builds a fresh index.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};

use s3stream_object::{
    BLOCK_INDEX_SIZE, DataBlockIndex, FOOTER_MAGIC, FOOTER_SIZE, MultipartWriter, ObjectReader,
    ObjectStorage, ReadOptions, S3ObjectMetadata, ThrottleStrategy, WriteOptions, gen_object_key,
};

use crate::api::StreamError;
use crate::manager::{ObjectManager, StreamObject};
use crate::storage::upload::AsyncRateLimiter;

use super::plan::{
    CompactedObject, CompactionType, GroupByLimitPredicate, StreamDataBlock,
    group_stream_data_blocks,
};

pub const S3_OBJECT_TTL_MINUTES: u64 = 24 * 60;
pub const S3_OBJECT_MAX_READ_BATCH: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct FetchedBlock {
    pub block: StreamDataBlock,
    pub data: Bytes,
}

/// Batched, throttled reader of source-object blocks. Merges
/// position-adjacent blocks of one source object into single range-GETs,
/// splitting GETs above `max_read_batch_size`.
pub struct DataBlockReader {
    metadata: S3ObjectMetadata,
    storage: Arc<dyn ObjectStorage>,
    throttle: Option<Arc<AsyncRateLimiter>>,
}

impl DataBlockReader {
    pub fn new(
        metadata: S3ObjectMetadata,
        storage: Arc<dyn ObjectStorage>,
        throttle: Option<Arc<AsyncRateLimiter>>,
    ) -> Self {
        Self {
            metadata,
            storage,
            throttle,
        }
    }

    /// Load this object's index and explode it into `StreamDataBlock`s.
    pub async fn parse_data_block_index(&self) -> Result<Vec<StreamDataBlock>, StreamError> {
        let reader = ObjectReader::new(self.metadata.clone(), self.storage.clone());
        let info = reader.basic_object_info().await?;
        Ok(info
            .index_block
            .entries()
            .iter()
            .map(|index| StreamDataBlock {
                object_id: self.metadata.object_id,
                index: *index,
            })
            .collect())
    }

    /// Read the given blocks (position-ordered), coalescing adjacent ranges.
    ///
    /// Returns blocks
    /// paired with their bytes, in input order.
    pub async fn read_blocks(
        &self,
        blocks: &[StreamDataBlock],
        max_read_batch_size: u64,
    ) -> Result<Vec<FetchedBlock>, StreamError> {
        let mut fetched = Vec::with_capacity(blocks.len());
        let mut start = 0;
        let mut offset: Option<u64> = None;
        for end in 0..blocks.len() {
            if let Some(expected) = offset
                && blocks[end].block_start_position() != expected
            {
                self.read_continuous(&blocks[start..end], max_read_batch_size, &mut fetched)
                    .await?;
                start = end;
            }
            offset = Some(blocks[end].index.end_position());
        }
        if start < blocks.len() {
            self.read_continuous(&blocks[start..], max_read_batch_size, &mut fetched)
                .await?;
        }
        Ok(fetched)
    }

    async fn read_continuous(
        &self,
        blocks: &[StreamDataBlock],
        max_read_batch_size: u64,
        out: &mut Vec<FetchedBlock>,
    ) -> Result<(), StreamError> {
        if blocks.is_empty() {
            return Ok(());
        }
        let mut start = 0;
        let mut current = 0u64;
        let mut i = 0;
        while i < blocks.len() {
            current += blocks[i].block_size() as u64;
            if max_read_batch_size > 0 && current >= max_read_batch_size {
                if start == i {
                    // Single oversized block: read it in max-batch chunks.
                    let block = &blocks[i];
                    let mut chunks = BytesMut::with_capacity(block.block_size() as usize);
                    let mut position = block.block_start_position();
                    let block_end = block.index.end_position();
                    while position < block_end {
                        let end = block_end.min(position + max_read_batch_size);
                        chunks.extend_from_slice(&self.range_read(position, end).await?);
                        position = end;
                    }
                    out.push(FetchedBlock {
                        block: *block,
                        data: chunks.freeze(),
                    });
                    i += 1;
                } else {
                    self.read_run(&blocks[start..i], out).await?;
                }
                start = i;
                current = 0;
            } else {
                i += 1;
            }
        }
        if start < blocks.len() {
            self.read_run(&blocks[start..], out).await?;
        }
        Ok(())
    }

    /// One GET covering `blocks` (continuous), sliced zero-copy per block.
    async fn read_run(
        &self,
        blocks: &[StreamDataBlock],
        out: &mut Vec<FetchedBlock>,
    ) -> Result<(), StreamError> {
        let start = blocks[0].block_start_position();
        let end = blocks[blocks.len() - 1].index.end_position();
        let data = self.range_read(start, end).await?;
        let mut cursor = 0usize;
        for block in blocks {
            let size = block.block_size() as usize;
            out.push(FetchedBlock {
                block: *block,
                data: data.slice(cursor..cursor + size),
            });
            cursor += size;
        }
        Ok(())
    }

    async fn range_read(&self, start: u64, end: u64) -> Result<Bytes, StreamError> {
        if let Some(throttle) = &self.throttle {
            throttle.acquire((end - start) as usize).await;
        }
        let options = ReadOptions {
            throttle: ThrottleStrategy::Compaction,
            ..Default::default()
        };
        Ok(self
            .storage
            .range_read(&options, &self.metadata.key(), start, Some(end))
            .await?)
    }
}

/// Multipart writer that forwards block bytes verbatim and appends a fresh index
/// block + footer, byte-compatible with `ObjectWriter` output.
///
/// (part batching, `GroupByLimitPredicate`
/// index grouping at 1 MiB, 48-byte footer with the data-object magic).
pub struct DataBlockWriter {
    object_id: u64,
    part_size_threshold: usize,
    writer: Box<dyn MultipartWriter>,
    waiting: BytesMut,
    completed_blocks: Vec<StreamDataBlock>,
    next_data_block_position: u64,
    size: u64,
}

const DATA_BLOCK_GROUP_SIZE_THRESHOLD: u64 = 1024 * 1024;

impl DataBlockWriter {
    pub async fn open(
        object_id: u64,
        storage: &dyn ObjectStorage,
        part_size_threshold: usize,
    ) -> Result<Self, StreamError> {
        let key = gen_object_key(0, object_id);
        let options = WriteOptions {
            throttle: ThrottleStrategy::Compaction,
            ..Default::default()
        };
        let writer = storage.writer(&options, &key).await?;
        Ok(Self {
            object_id,
            part_size_threshold,
            writer,
            waiting: BytesMut::new(),
            completed_blocks: Vec::new(),
            next_data_block_position: 0,
            size: 0,
        })
    }

    pub fn object_id(&self) -> u64 {
        self.object_id
    }

    pub async fn write(&mut self, fetched: &FetchedBlock) -> Result<(), StreamError> {
        debug_assert_eq!(fetched.data.len(), fetched.block.block_size() as usize);
        self.waiting.extend_from_slice(&fetched.data);
        self.completed_blocks.push(fetched.block);
        self.next_data_block_position += fetched.block.block_size() as u64;
        if self.waiting.len() >= self.part_size_threshold {
            let part = self.waiting.split().freeze();
            self.writer.write(part).await?;
        }
        Ok(())
    }

    /// Finish the object: flush data, then index block + footer.
    ///
    /// (index via `buildDataBlockIndicesFromGroup` over
    /// `GroupByLimitPredicate(1MiB)` groups. Footer identical to `ObjectWriter`).
    pub async fn close(mut self) -> Result<(u64, i16), StreamError> {
        let index_position = self.next_data_block_position;
        let mut predicate = GroupByLimitPredicate::new(DATA_BLOCK_GROUP_SIZE_THRESHOLD);
        let groups = group_stream_data_blocks(&self.completed_blocks, |b| predicate.test(b));
        let indices = build_data_block_indices_from_group(&groups);
        let index_size = indices.len() * BLOCK_INDEX_SIZE;

        let mut tail = self.waiting.split();
        tail.reserve(index_size + FOOTER_SIZE);
        for index in &indices {
            index.encode(&mut tail);
        }
        tail.put_u64(index_position);
        tail.put_u32(index_size as u32);
        tail.put_bytes(0, 40 - 8 - 4);
        tail.put_u64(FOOTER_MAGIC);
        self.writer.write(tail.freeze()).await?;
        let result = self.writer.finish().await?;
        self.size = index_position + index_size as u64 + FOOTER_SIZE as u64;
        Ok((self.size, result.bucket_id))
    }

    pub async fn release(mut self) {
        let _ = self.writer.abort().await;
    }
}

pub fn build_data_block_indices_from_group(groups: &[Vec<StreamDataBlock>]) -> Vec<DataBlockIndex> {
    let mut indices = Vec::with_capacity(groups.len());
    let mut position = 0u64;
    for group in groups {
        if group.is_empty() {
            continue;
        }
        let first = &group[0];
        let last = &group[group.len() - 1];
        let group_size: u64 = group.iter().map(|b| b.block_size() as u64).sum();
        indices.push(DataBlockIndex {
            block_id: -1,
            stream_id: first.stream_id(),
            start_offset: first.start_offset(),
            end_offset_delta: (last.end_offset() - first.start_offset()) as u32,
            record_count: group.iter().map(|b| b.index.record_count).sum(),
            start_position: position,
            size: group_size as u32,
        });
        position += group_size;
    }
    indices
}

pub fn build_object_stream_ranges_from_group(
    groups: &[Vec<StreamDataBlock>],
) -> Vec<s3stream_object::ObjectStreamRange> {
    groups
        .iter()
        .filter(|g| !g.is_empty())
        .map(|g| s3stream_object::ObjectStreamRange {
            stream_id: g[0].stream_id(),
            epoch: u64::MAX, // epoch unknown at compaction
            start_offset: g[0].start_offset(),
            end_offset: g[g.len() - 1].end_offset(),
            size: g.iter().map(|b| b.block_size() as u64).sum(),
        })
        .collect()
}

/// Uploads planned objects: one chained stream set object + N stream objects.
pub struct CompactionUploader {
    object_manager: Arc<dyn ObjectManager>,
    storage: Arc<dyn ObjectStorage>,
    part_size: usize,
    stream_set_object_id: Option<u64>,
    stream_set_writer: Option<DataBlockWriter>,
    bucket_id: i16,
}

impl CompactionUploader {
    pub fn new(
        object_manager: Arc<dyn ObjectManager>,
        storage: Arc<dyn ObjectStorage>,
        part_size: usize,
    ) -> Self {
        Self {
            object_manager,
            storage,
            part_size,
            stream_set_object_id: None,
            stream_set_writer: None,
            bucket_id: 0,
        }
    }

    /// Append a COMPACT object's blocks to the (single) new stream set object.
    pub async fn write_stream_set_object(
        &mut self,
        compacted: &CompactedObject,
        data: &HashMap<(u64, u64), Bytes>,
    ) -> Result<(), StreamError> {
        assert_eq!(compacted.compaction_type, CompactionType::Compact);
        if compacted.blocks.is_empty() {
            return Ok(());
        }
        if self.stream_set_writer.is_none() {
            let object_id = self
                .object_manager
                .prepare_object(1, S3_OBJECT_TTL_MINUTES * 60 * 1000)
                .await?;
            self.stream_set_object_id = Some(object_id);
            self.stream_set_writer = Some(
                DataBlockWriter::open(object_id, self.storage.as_ref(), self.part_size).await?,
            );
        }
        let writer = self.stream_set_writer.as_mut().unwrap();
        for block in &compacted.blocks {
            let bytes = data
                .get(&(block.object_id, block.block_start_position()))
                .expect("block data fetched by plan reader")
                .clone();
            writer
                .write(&FetchedBlock {
                    block: *block,
                    data: bytes,
                })
                .await?;
        }
        Ok(())
    }

    /// Write one SPLIT object as a standalone stream object.
    pub async fn write_stream_object(
        &self,
        compacted: &CompactedObject,
        data: &HashMap<(u64, u64), Bytes>,
    ) -> Result<Option<StreamObject>, StreamError> {
        assert_eq!(compacted.compaction_type, CompactionType::Split);
        if compacted.blocks.is_empty() {
            return Ok(None);
        }
        let object_id = self
            .object_manager
            .prepare_object(1, S3_OBJECT_TTL_MINUTES * 60 * 1000)
            .await?;
        let mut writer =
            DataBlockWriter::open(object_id, self.storage.as_ref(), self.part_size).await?;
        for block in &compacted.blocks {
            let bytes = data
                .get(&(block.object_id, block.block_start_position()))
                .expect("block data fetched by plan reader")
                .clone();
            writer
                .write(&FetchedBlock {
                    block: *block,
                    data: bytes,
                })
                .await?;
        }
        let (size, bucket_id) = writer.close().await?;
        Ok(Some(StreamObject {
            object_id,
            object_size: size,
            stream_id: compacted.blocks[0].stream_id(),
            start_offset: compacted.blocks[0].start_offset(),
            end_offset: compacted.blocks[compacted.blocks.len() - 1].end_offset(),
            attributes: s3stream_object::ObjectAttributes::new(bucket_id, false, false).0,
        }))
    }

    /// Close the stream set object and return its size (0 when none was written).
    pub async fn complete(&mut self) -> Result<u64, StreamError> {
        let Some(writer) = self.stream_set_writer.take() else {
            return Ok(0);
        };
        let (size, bucket_id) = writer.close().await?;
        self.bucket_id = bucket_id;
        Ok(size)
    }

    pub async fn release(&mut self) {
        if let Some(writer) = self.stream_set_writer.take() {
            writer.release().await;
        }
        self.stream_set_object_id = None;
    }

    pub fn stream_set_object_id(&self) -> Option<u64> {
        self.stream_set_object_id
    }

    pub fn bucket_id(&self) -> i16 {
        self.bucket_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3stream_codec::StreamRecordBatch;
    use s3stream_object::{MemoryObjectStorage, ObjectWriter, WriteOptions};

    fn source_metadata(object_id: u64, object_size: u64) -> S3ObjectMetadata {
        S3ObjectMetadata {
            object_id,
            object_type: s3stream_object::S3ObjectType::StreamSet,
            offset_ranges: vec![],
            object_size,
            attributes: Default::default(),
            committed_timestamp_ms: 0,
            data_timestamp_ms: 0,
        }
    }

    async fn write_source_object(
        storage: &MemoryObjectStorage,
        object_id: u64,
        stream_id: u64,
        start: u64,
        count: u64,
    ) -> (Vec<StreamDataBlock>, u64) {
        let records: Vec<StreamRecordBatch> = (start..start + count)
            .map(|o| StreamRecordBatch::new(stream_id, 1, o, 1, vec![o as u8; 64].into()))
            .collect();
        let mut writer =
            ObjectWriter::open(object_id, storage, 1, 16 << 20, WriteOptions::default())
                .await
                .unwrap();
        writer.write(stream_id, &records).await.unwrap();
        let size = writer.close().await.unwrap();
        let reader = DataBlockReader::new(
            source_metadata(object_id, size),
            Arc::new(storage.clone()),
            None,
        );
        (reader.parse_data_block_index().await.unwrap(), size)
    }

    /// Block bytes are forwarded verbatim: output object data blocks are
    /// byte-identical to their sources.
    #[tokio::test]
    async fn blocks_forwarded_verbatim() {
        let storage = MemoryObjectStorage::new(0);
        let (blocks, src_size) = write_source_object(&storage, 1, 10, 0, 4).await;
        assert_eq!(blocks.len(), 4);

        let storage = Arc::new(storage);
        let reader = DataBlockReader::new(source_metadata(1, src_size), storage.clone(), None);
        let fetched = reader
            .read_blocks(&blocks, S3_OBJECT_MAX_READ_BATCH)
            .await
            .unwrap();

        let mut writer = DataBlockWriter::open(99, storage.as_ref(), 16 << 20)
            .await
            .unwrap();
        for f in &fetched {
            writer.write(f).await.unwrap();
        }
        let (size, _) = writer.close().await.unwrap();
        assert!(size > 0);

        // Source data region and destination data region are byte-identical.
        let options = ReadOptions::default();
        let src = storage
            .range_read(&options, &gen_object_key(0, 1), 0, None)
            .await
            .unwrap();
        let dst = storage
            .range_read(&options, &gen_object_key(0, 99), 0, None)
            .await
            .unwrap();
        let data_len: usize = blocks.iter().map(|b| b.block_size() as usize).sum();
        assert_eq!(&src[..data_len], &dst[..data_len]);

        // The rewritten object parses with the standard reader and serves records.
        let dst_metadata = S3ObjectMetadata {
            object_id: 99,
            object_type: s3stream_object::S3ObjectType::Stream,
            offset_ranges: vec![],
            object_size: size,
            attributes: Default::default(),
            committed_timestamp_ms: 0,
            data_timestamp_ms: 0,
        };
        let dst_reader = ObjectReader::new(dst_metadata, storage.clone());
        let info = dst_reader.basic_object_info().await.unwrap();
        let total_records: u32 = info
            .index_block
            .entries()
            .iter()
            .map(|e| e.record_count)
            .sum();
        assert_eq!(total_records, 4);
    }

    /// Adjacent blocks coalesce into a single range-GET. Discontinuous runs split.
    #[tokio::test]
    async fn adjacent_ranges_coalesce() {
        let storage = MemoryObjectStorage::new(0);
        let (blocks, src_size) = write_source_object(&storage, 1, 10, 0, 8).await;
        // Drop block 3 to create a positional gap: [0..3), [4..8).
        let mut sparse: Vec<StreamDataBlock> = blocks.clone();
        sparse.remove(3);

        let storage = Arc::new(storage);
        let reader = DataBlockReader::new(source_metadata(1, src_size), storage.clone(), None);
        let fetched = reader
            .read_blocks(&sparse, S3_OBJECT_MAX_READ_BATCH)
            .await
            .unwrap();
        assert_eq!(fetched.len(), 7);
        for (f, b) in fetched.iter().zip(&sparse) {
            assert_eq!(f.block.index, b.index);
            assert_eq!(f.data.len(), b.block_size() as usize);
        }
    }
}
