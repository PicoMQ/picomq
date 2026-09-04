//! Data block index entry: one fixed-size record per data block in an object.
//!
//! `utils.biniarysearch.IndexBlockOrderedBytes`.
//! Specification: `specification/object-format.md` (index block section).

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ObjectError;
use crate::metadata::NOOP_OFFSET;

/// Encoded size: streamId(8) + startOffset(8) + endOffsetDelta(4) + recordCount(4)
/// + startPosition(8) + blockSize(4).
pub const BLOCK_INDEX_SIZE: usize = 36;

/// Index entry describing one data block.
///
/// `block_id` is a reader-side ordinal (position in the index
/// block), not wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataBlockIndex {
    pub block_id: i32,
    pub stream_id: u64,
    pub start_offset: u64,
    pub end_offset_delta: u32,
    pub record_count: u32,
    /// Byte position of the block within the object.
    pub start_position: u64,
    /// Byte size of the block.
    pub size: u32,
}

impl DataBlockIndex {
    pub fn end_offset(&self) -> u64 {
        self.start_offset + self.end_offset_delta as u64
    }

    pub fn end_position(&self) -> u64 {
        self.start_position + self.size as u64
    }

    /// Append the 36-byte wire encoding to `buf`.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.stream_id);
        buf.put_u64(self.start_offset);
        buf.put_u32(self.end_offset_delta);
        buf.put_u32(self.record_count);
        buf.put_u64(self.start_position);
        buf.put_u32(self.size);
    }

    /// Decode one entry. `block_id` is the ordinal supplied by the caller.
    pub fn decode(block_id: i32, buf: &[u8]) -> Result<Self, ObjectError> {
        if buf.len() < BLOCK_INDEX_SIZE {
            return Err(ObjectError::InvalidFormat {
                reason: format!(
                    "index entry needs {BLOCK_INDEX_SIZE} bytes, have {}",
                    buf.len()
                ),
            });
        }
        Ok(Self {
            block_id,
            stream_id: u64::from_be_bytes(buf[0..8].try_into().unwrap()),
            start_offset: u64::from_be_bytes(buf[8..16].try_into().unwrap()),
            end_offset_delta: u32::from_be_bytes(buf[16..20].try_into().unwrap()),
            record_count: u32::from_be_bytes(buf[20..24].try_into().unwrap()),
            start_position: u64::from_be_bytes(buf[24..32].try_into().unwrap()),
            size: u32::from_be_bytes(buf[32..36].try_into().unwrap()),
        })
    }
}

/// Result of an index lookup.
///
/// `ObjectReader.FindIndexResult`. `fulfilled == false` means the caller must
/// continue in the next object (or the request missed this object entirely when
/// `blocks` is empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindIndexResult {
    pub fulfilled: bool,
    pub next_start_offset: u64,
    pub next_max_bytes: usize,
    pub blocks: Vec<DataBlockIndex>,
}

/// A parsed index block: all entries of an object, ordered as written
/// (sorted by streamId then startOffset).
#[derive(Debug, Clone)]
pub struct IndexBlock {
    entries: Vec<DataBlockIndex>,
}

impl IndexBlock {
    /// Parse a full index block payload (`len % 36 == 0`).
    pub fn parse(data: &Bytes) -> Result<Self, ObjectError> {
        if !data.len().is_multiple_of(BLOCK_INDEX_SIZE) {
            return Err(ObjectError::InvalidFormat {
                reason: format!(
                    "index block length {} not a multiple of {BLOCK_INDEX_SIZE}",
                    data.len()
                ),
            });
        }
        let count = data.len() / BLOCK_INDEX_SIZE;
        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            entries.push(DataBlockIndex::decode(
                i as i32,
                &data[i * BLOCK_INDEX_SIZE..],
            )?);
        }
        Ok(Self { entries })
    }

    /// Serialize all entries (writer side / composite objects).
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.entries.len() * BLOCK_INDEX_SIZE);
        for entry in &self.entries {
            entry.encode(&mut buf);
        }
        buf.freeze()
    }

    pub fn from_entries(entries: Vec<DataBlockIndex>) -> Self {
        Self { entries }
    }

    /// Binary search for the block containing `(stream_id, offset)`.
    ///
    /// Comparison rules: a block is "less" when its stream is lower or its endOffset
    /// <= target offset. "greater" when its stream is higher or its startOffset >
    /// target offset. Returns `Err(insertion_point)` when no block contains the target.
    fn search(&self, stream_id: u64, offset: u64) -> Result<usize, usize> {
        let mut lo: isize = 0;
        let mut hi: isize = self.entries.len() as isize - 1;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let e = &self.entries[mid as usize];
            let is_less =
                e.stream_id < stream_id || (e.stream_id == stream_id && e.end_offset() <= offset);
            let is_greater =
                e.stream_id > stream_id || (e.stream_id == stream_id && e.start_offset > offset);
            if is_less {
                lo = mid + 1;
            } else if is_greater {
                hi = mid - 1;
            } else {
                return Ok(mid as usize);
            }
        }
        Err(lo as usize)
    }

    /// Find the blocks covering `[start_offset, end_offset)` of `stream_id`, capped so
    /// that total record payload bytes stay near `max_bytes` (always at least one
    /// block). `end_offset == NOOP_OFFSET` means unbounded. A partial first
    /// block is not charged against `max_bytes` (its in-range byte count is
    /// unknown before decoding).
    pub fn find(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> FindIndexResult {
        const RECORD_HEADER_SIZE: usize = 33; // StreamRecordBatchCodec.HEADER_SIZE
        const BLOCK_HEADER_SIZE: usize = 10; // ObjectWriter.DataBlock.BLOCK_HEADER_SIZE

        let mut next_start_offset = start_offset;
        let mut next_max_bytes = max_bytes;
        let mut matched = false;
        let mut fulfilled = false;
        let mut blocks = Vec::new();

        let start_index = match self.search(stream_id, start_offset) {
            Ok(i) => i,
            Err(_) => {
                return FindIndexResult {
                    fulfilled: false,
                    next_start_offset,
                    next_max_bytes,
                    blocks,
                };
            }
        };
        for e in &self.entries[start_index..] {
            if e.stream_id == stream_id {
                if next_start_offset < e.start_offset {
                    break;
                }
                if e.end_offset() <= next_start_offset {
                    continue;
                }
                matched = next_start_offset == e.start_offset;
                next_start_offset = e.end_offset();
                blocks.push(*e);
                // First block is not counted against max_bytes (see doc above).
                if matched {
                    let record_payload_size = (e.size as usize)
                        .saturating_sub(e.record_count as usize * RECORD_HEADER_SIZE)
                        .saturating_sub(BLOCK_HEADER_SIZE);
                    next_max_bytes -= next_max_bytes.min(record_payload_size);
                }
                if (end_offset != NOOP_OFFSET && next_start_offset >= end_offset)
                    || next_max_bytes == 0
                {
                    fulfilled = true;
                    break;
                }
            } else if matched {
                break;
            }
        }
        FindIndexResult {
            fulfilled,
            next_start_offset,
            next_max_bytes,
            blocks,
        }
    }

    pub fn entries(&self) -> &[DataBlockIndex] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_size_matches_java() {
        assert_eq!(BLOCK_INDEX_SIZE, 36);
    }

    fn entry(
        block_id: i32,
        stream_id: u64,
        start_offset: u64,
        end_offset_delta: u32,
        start_position: u64,
        size: u32,
    ) -> DataBlockIndex {
        DataBlockIndex {
            block_id,
            stream_id,
            start_offset,
            end_offset_delta,
            record_count: 1,
            start_position,
            size,
        }
    }

    #[test]
    fn entry_round_trip() {
        let e = DataBlockIndex {
            block_id: 0,
            stream_id: 9,
            start_offset: 100,
            end_offset_delta: 50,
            record_count: 5,
            start_position: 4096,
            size: 1024,
        };
        let mut buf = BytesMut::new();
        e.encode(&mut buf);
        assert_eq!(buf.len(), BLOCK_INDEX_SIZE);
        let d = DataBlockIndex::decode(0, &buf).unwrap();
        assert_eq!(d, e);
        assert_eq!(d.end_offset(), 150);
        assert_eq!(d.end_position(), 5120);
    }

    #[test]
    fn find_covers_requested_range() {
        // Stream 1: [0,10) [10,20) [20,30). Stream 2: [5,15).
        let index = IndexBlock::from_entries(vec![
            entry(0, 1, 0, 10, 0, 100),
            entry(1, 1, 10, 10, 100, 100),
            entry(2, 1, 20, 10, 200, 100),
            entry(3, 2, 5, 10, 300, 100),
        ]);

        // Exact cover of [0,30).
        let r = index.find(1, 0, 30, usize::MAX);
        assert!(r.fulfilled);
        assert_eq!(
            r.blocks.iter().map(|b| b.block_id).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(r.next_start_offset, 30);

        // Mid-block start: [15,25) needs blocks 1 and 2.
        let r = index.find(1, 15, 25, usize::MAX);
        assert!(r.fulfilled);
        assert_eq!(
            r.blocks.iter().map(|b| b.block_id).collect::<Vec<_>>(),
            vec![1, 2]
        );

        // Unbounded end: runs to the end of the stream, not fulfilled.
        let r = index.find(1, 0, NOOP_OFFSET, usize::MAX);
        assert!(!r.fulfilled);
        assert_eq!(r.blocks.len(), 3);
        assert_eq!(r.next_start_offset, 30);

        // Miss: offset beyond the stream.
        let r = index.find(1, 40, 50, usize::MAX);
        assert!(!r.fulfilled);
        assert!(r.blocks.is_empty());

        // Other stream unaffected.
        let r = index.find(2, 5, 15, usize::MAX);
        assert!(r.fulfilled);
        assert_eq!(
            r.blocks.iter().map(|b| b.block_id).collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[test]
    fn find_respects_max_bytes() {
        // Each block: size 100, 1 record -> payload = 100 - 33 - 10 = 57 bytes.
        let index = IndexBlock::from_entries(vec![
            entry(0, 1, 0, 10, 0, 100),
            entry(1, 1, 10, 10, 100, 100),
            entry(2, 1, 20, 10, 200, 100),
        ]);
        // Budget 60: block 0 aligned start consumes 57, block 1 consumes the rest -> stop.
        let r = index.find(1, 0, NOOP_OFFSET, 60);
        assert!(r.fulfilled);
        assert_eq!(r.blocks.len(), 2);
        assert_eq!(r.next_max_bytes, 0);
    }
}
