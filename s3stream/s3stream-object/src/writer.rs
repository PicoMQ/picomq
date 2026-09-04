//! ObjectWriter: writes stream records into a single object
//! (data blocks + index block + footer).
//!
//! `IndexBlock`, `Footer`).
//! Specification: `specification/object-format.md`.

use bytes::{BufMut, Bytes, BytesMut};

use s3stream_codec::StreamRecordBatch;

use crate::error::ObjectError;
use crate::index::{BLOCK_INDEX_SIZE, DataBlockIndex};
use crate::metadata::gen_object_key;
use crate::storage::{MultipartWriter, ObjectStorage, WriteOptions};

/// Data block magic / default flag bytes.
pub const DATA_BLOCK_MAGIC: u8 = 0x5A;
pub const DATA_BLOCK_DEFAULT_FLAG: u8 = 0x02;
/// Layout: magic(1) + flag(1) + count(4) + len(4).
pub const BLOCK_HEADER_SIZE: usize = 10;
pub const FOOTER_SIZE: usize = 48;
pub const FOOTER_MAGIC: u64 = 0x88E2_41B7_85F4_CFF7;
pub const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// A stream's contiguous range covered by an object being written. Reported to the
/// metadata plane at commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStreamRange {
    pub stream_id: u64,
    pub epoch: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub size: u64,
}

/// One sealed data block awaiting upload.
struct DataBlock {
    encoded: Bytes,
    stream_id: u64,
    epoch: u64,
    start_offset: u64,
    end_offset: u64,
    record_count: u32,
}

impl DataBlock {
    fn new(stream_id: u64, records: &[StreamRecordBatch]) -> Self {
        let encoded = encode_data_block(records);
        Self {
            encoded,
            stream_id,
            epoch: records[0].epoch(),
            start_offset: records[0].base_offset(),
            end_offset: records[records.len() - 1].last_offset(),
            record_count: records.len() as u32,
        }
    }

    fn size(&self) -> usize {
        self.encoded.len()
    }
}

/// Writes records of one or more streams into a single object.
///
/// - `write` calls must present stream ids in non-decreasing order. Within a stream,
///   record ranges must not overlap or go backwards (`#check`).
/// - Records are grouped into data blocks of ~`block_size_threshold` payload bytes
///   quirk kept for byte parity: the grouping accumulator persists across `write`
///   calls and only resets when it crosses the threshold.
/// - Accumulated blocks are flushed as multipart parts once `part_size_threshold`
///   is reached (`#tryUploadPart`). The threshold is clamped up to `MIN_PART_SIZE`.
/// - `close` writes remaining blocks + index block + footer, then completes the
///   multipart upload.
pub struct ObjectWriter {
    writer: Box<dyn MultipartWriter>,
    block_size_threshold: usize,
    part_size_threshold: usize,
    waiting_upload_blocks: Vec<DataBlock>,
    waiting_upload_size: usize,
    completed_blocks: Vec<DataBlock>,
    group_acc: usize,
    object_size: u64,
    /// Ordering-check cursor. `None` until the first non-empty write.
    last: Option<(u64, u64)>, // (stream_id, end_offset)
}

impl ObjectWriter {
    /// Open a writer for object `object_id`.
    ///
    /// `ObjectWriter.writer(objectId, objectStorage, blockSizeThreshold,
    /// partSizeThreshold, writeOptions)`; the key is `gen_object_key(0, object_id)`.
    pub async fn open(
        object_id: u64,
        storage: &dyn ObjectStorage,
        block_size_threshold: usize,
        part_size_threshold: usize,
        options: WriteOptions,
    ) -> Result<Self, ObjectError> {
        let key = gen_object_key(0, object_id);
        let writer = storage.writer(&options, &key).await?;
        Ok(Self {
            writer,
            block_size_threshold,
            part_size_threshold: part_size_threshold.max(MIN_PART_SIZE),
            waiting_upload_blocks: Vec::new(),
            waiting_upload_size: 0,
            completed_blocks: Vec::new(),
            group_acc: 0,
            object_size: 0,
            last: None,
        })
    }

    /// Append records of `stream_id`. Ordering violations (stream id or
    /// offset going backwards) are an error.
    pub async fn write(
        &mut self,
        stream_id: u64,
        records: &[StreamRecordBatch],
    ) -> Result<(), ObjectError> {
        self.check(stream_id, records)?;
        for block_records in self.group_by_block(records) {
            let block = DataBlock::new(stream_id, &block_records);
            self.waiting_upload_size += block.size();
            self.waiting_upload_blocks.push(block);
        }
        if self.waiting_upload_size >= self.part_size_threshold {
            self.try_upload_part().await?;
        }
        Ok(())
    }

    fn check(&mut self, stream_id: u64, records: &[StreamRecordBatch]) -> Result<(), ObjectError> {
        if records.is_empty() {
            return Ok(());
        }
        let records_end = records[records.len() - 1].last_offset();
        match self.last {
            None => {
                self.last = Some((stream_id, records_end));
                Ok(())
            }
            Some((last_stream_id, last_end_offset)) => {
                if last_stream_id > stream_id {
                    return Err(ObjectError::OrderingViolation {
                        reason: format!(
                            "incoming streamId={stream_id} is less than last streamId={last_stream_id}"
                        ),
                    });
                }
                if last_stream_id == stream_id {
                    let records_start = records[0].base_offset();
                    if records_start < last_end_offset {
                        return Err(ObjectError::OrderingViolation {
                            reason: format!(
                                "streamId={stream_id} startOffset={records_start} is less than lastEndOffset={last_end_offset}"
                            ),
                        });
                    }
                }
                self.last = Some((stream_id, records_end));
                Ok(())
            }
        }
    }

    fn group_by_block(&mut self, records: &[StreamRecordBatch]) -> Vec<Vec<StreamRecordBatch>> {
        let mut blocks = Vec::new();
        let mut block_records = Vec::with_capacity(records.len());
        for record in records {
            self.group_acc += record.size();
            block_records.push(record.clone());
            if self.group_acc >= self.block_size_threshold {
                blocks.push(std::mem::take(&mut block_records));
                self.group_acc = 0;
            }
        }
        if !block_records.is_empty() {
            blocks.push(block_records);
        }
        blocks
    }

    async fn try_upload_part(&mut self) -> Result<(), ObjectError> {
        loop {
            let mut part_size = 0;
            let mut take = 0;
            for block in &self.waiting_upload_blocks {
                part_size += block.size();
                take += 1;
                if part_size >= self.part_size_threshold {
                    break;
                }
            }
            if part_size < self.part_size_threshold {
                return Ok(());
            }
            let mut part = BytesMut::with_capacity(part_size);
            for block in self.waiting_upload_blocks.drain(..take) {
                self.waiting_upload_size -= block.size();
                part.put_slice(&block.encoded);
                self.completed_blocks.push(block);
            }
            self.writer.write(part.freeze()).await?;
        }
    }

    /// Finish: flush residual blocks, write index block + footer, complete multipart.
    /// Returns the final object size. `stream_ranges`/`block_indexes`/`size` remain
    pub async fn close(&mut self) -> Result<u64, ObjectError> {
        let mut tail = BytesMut::new();
        for block in self.waiting_upload_blocks.drain(..) {
            tail.put_slice(&block.encoded);
            self.completed_blocks.push(block);
        }
        self.waiting_upload_size = 0;

        let mut index_position = 0u64;
        let mut index_buf = BytesMut::with_capacity(BLOCK_INDEX_SIZE * self.completed_blocks.len());
        for block in &self.completed_blocks {
            DataBlockIndex {
                block_id: -1,
                stream_id: block.stream_id,
                start_offset: block.start_offset,
                end_offset_delta: (block.end_offset - block.start_offset) as u32,
                record_count: block.record_count,
                start_position: index_position,
                size: block.size() as u32,
            }
            .encode(&mut index_buf);
            index_position += block.size() as u64;
        }
        let index_size = index_buf.len();
        tail.put_slice(&index_buf);

        tail.put_u64(index_position);
        tail.put_u32(index_size as u32);
        tail.put_bytes(0, 40 - 8 - 4);
        tail.put_u64(FOOTER_MAGIC);

        self.writer.write(tail.freeze()).await?;
        self.object_size = index_position + index_size as u64 + FOOTER_SIZE as u64;
        self.writer.finish().await?;
        Ok(self.object_size)
    }

    /// Stream ranges covered by everything written. Consecutive blocks of
    /// the same stream merge into one range.
    pub fn stream_ranges(&self) -> Vec<ObjectStreamRange> {
        let mut ranges: Vec<ObjectStreamRange> = Vec::new();
        for block in &self.completed_blocks {
            match ranges.last_mut() {
                Some(last) if last.stream_id == block.stream_id => {
                    last.end_offset = block.end_offset;
                    last.size += block.size() as u64;
                }
                _ => ranges.push(ObjectStreamRange {
                    stream_id: block.stream_id,
                    epoch: block.epoch,
                    start_offset: block.start_offset,
                    end_offset: block.end_offset,
                    size: block.size() as u64,
                }),
            }
        }
        ranges
    }

    /// Index entries of all completed blocks (needed by composite-object writers and
    /// compaction). Positions match what `close` writes.
    pub fn block_indexes(&self) -> Vec<DataBlockIndex> {
        let mut position = 0u64;
        self.completed_blocks
            .iter()
            .enumerate()
            .map(|(i, block)| {
                let index = DataBlockIndex {
                    block_id: i as i32,
                    stream_id: block.stream_id,
                    start_offset: block.start_offset,
                    end_offset_delta: (block.end_offset - block.start_offset) as u32,
                    record_count: block.record_count,
                    start_position: position,
                    size: block.size() as u32,
                };
                position += block.size() as u64;
                index
            })
            .collect()
    }

    /// Total object size (final after `close`).
    pub fn size(&self) -> u64 {
        self.object_size
    }

    /// Bucket the object is being written to.
    pub fn bucket_id(&self) -> i16 {
        self.writer.bucket_id()
    }
}

/// Encode one data block (header + concatenated encoded records) for one
/// stream. Exposed for tests and the WAL-side recovery upload, which builds
/// blocks directly.
pub fn encode_data_block(records: &[StreamRecordBatch]) -> Bytes {
    assert!(!records.is_empty(), "data block must contain records");
    let data_len: usize = records.iter().map(|r| r.encoded().len()).sum();
    let mut buf = BytesMut::with_capacity(BLOCK_HEADER_SIZE + data_len);
    buf.put_u8(DATA_BLOCK_MAGIC);
    buf.put_u8(DATA_BLOCK_DEFAULT_FLAG);
    buf.put_u32(records.len() as u32);
    buf.put_u32(data_len as u32);
    for record in records {
        buf.put_slice(&record.encoded());
    }
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryObjectStorage;
    use crate::storage::ReadOptions;

    #[test]
    fn constants_match_java() {
        assert_eq!(BLOCK_HEADER_SIZE, 10);
        assert_eq!(FOOTER_SIZE, 48);
        assert_eq!(FOOTER_MAGIC, 0x88E2_41B7_85F4_CFF7);
    }

    fn record(
        stream_id: u64,
        epoch: u64,
        base_offset: u64,
        payload_len: usize,
    ) -> StreamRecordBatch {
        StreamRecordBatch::new(
            stream_id,
            epoch,
            base_offset,
            1,
            vec![0xEEu8; payload_len].into(),
        )
    }

    /// Stream ids must be non-decreasing. Same-stream offsets must not overlap.
    #[tokio::test]
    async fn writer_enforces_stream_ordering() {
        let storage = MemoryObjectStorage::new(0);
        let mut writer = ObjectWriter::open(1, &storage, 1024, 16 << 20, WriteOptions::default())
            .await
            .unwrap();
        writer.write(5, &[record(5, 1, 0, 10)]).await.unwrap();

        // Descending stream id.
        let err = writer.write(4, &[record(4, 1, 0, 10)]).await.unwrap_err();
        assert!(matches!(err, ObjectError::OrderingViolation { .. }));

        // Overlapping range within the stream.
        let err = writer.write(5, &[record(5, 1, 0, 10)]).await.unwrap_err();
        assert!(matches!(err, ObjectError::OrderingViolation { .. }));

        // Forward progress is fine.
        writer.write(5, &[record(5, 1, 1, 10)]).await.unwrap();
        writer.write(6, &[record(6, 1, 0, 10)]).await.unwrap();
    }

    /// Golden vectors: objects written by the Java ObjectWriter. For each fixture,
    /// re-write the same records with the same thresholds and require byte equality.
    /// Also check stream ranges against the manifest.
    #[tokio::test]
    async fn golden_objects_match_java() {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/fixtures/object");
        let manifest = std::fs::read_to_string(dir.join("manifest.json"))
            .expect("run conformance/generator first");
        let cases: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        for case in cases.as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let golden = std::fs::read(dir.join(format!("{name}.bin"))).unwrap();
            let object_id = case["object_id"].as_u64().unwrap();
            let threshold = case["block_size_threshold"].as_u64().unwrap() as usize;

            let storage = MemoryObjectStorage::new(0);
            let mut writer = ObjectWriter::open(
                object_id,
                &storage,
                threshold,
                16 << 20,
                WriteOptions::default(),
            )
            .await
            .unwrap();

            // Replay records grouped by stream id, in manifest order.
            let records = case["records"].as_array().unwrap();
            let mut pending: Vec<StreamRecordBatch> = Vec::new();
            let mut pending_stream: Option<u64> = None;
            for r in records {
                let stream_id = r["stream_id"].as_u64().unwrap();
                let batch = StreamRecordBatch::new(
                    stream_id,
                    r["epoch"].as_u64().unwrap(),
                    r["base_offset"].as_u64().unwrap(),
                    r["count"].as_i64().unwrap() as i32,
                    hex::decode(r["payload_hex"].as_str().unwrap())
                        .unwrap()
                        .into(),
                );
                if pending_stream != Some(stream_id) {
                    if let Some(prev) = pending_stream {
                        writer.write(prev, &pending).await.unwrap();
                        pending.clear();
                    }
                    pending_stream = Some(stream_id);
                }
                pending.push(batch);
            }
            if let Some(prev) = pending_stream {
                writer.write(prev, &pending).await.unwrap();
            }

            let size = writer.close().await.unwrap();
            let written = storage
                .read(&ReadOptions::default(), &gen_object_key(0, object_id))
                .await
                .unwrap();
            assert_eq!(
                written.as_ref(),
                golden.as_slice(),
                "object bytes mismatch: {name}"
            );
            assert_eq!(
                size,
                case["size"].as_u64().unwrap(),
                "manifest size mismatch: {name}"
            );

            // Stream ranges must match what Java reported for the same input.
            let expected: Vec<(u64, u64, u64, u64)> = case["stream_ranges"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r["stream_id"].as_u64().unwrap(),
                        r["epoch"].as_u64().unwrap(),
                        r["start_offset"].as_u64().unwrap(),
                        r["end_offset"].as_u64().unwrap(),
                    )
                })
                .collect();
            let actual: Vec<(u64, u64, u64, u64)> = writer
                .stream_ranges()
                .iter()
                .map(|r| (r.stream_id, r.epoch, r.start_offset, r.end_offset))
                .collect();
            assert_eq!(actual, expected, "stream ranges mismatch: {name}");
        }
    }
}
