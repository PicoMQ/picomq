//! The unit of append: an encoded stream record batch.
//!
//! Specification: `specification/record-format.md`.
//!
//! `retain()`/`release()`. Here the encoded form is a `bytes::Bytes` (cheaply clonable,
//! dropped automatically), which deletes the entire manual-refcount error class.

use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::{
    BASE_OFFSET_POS, EPOCH_POS, HEADER_SIZE, LAST_OFFSET_DELTA_POS, MAGIC_POS, MAGIC_V0,
    PAYLOAD_LENGTH_POS, PAYLOAD_POS, STREAM_ID_POS,
};
use crate::error::CodecError;

/// Approximate per-batch struct overhead for cache accounting. NOT wire format.
///
/// Rust struct is one `Bytes` handle, so the constant differs.
const OBJECT_OVERHEAD: usize = 48;

/// An immutable, encoded stream record batch.
///
/// Stored in encoded form everywhere (WAL body, LogCache, object data blocks) so
/// encoding happens exactly once at append time. Accessors read directly from the
/// encoded buffer.
#[derive(Clone)]
pub struct StreamRecordBatch {
    /// The full encoded bytes: header (33 bytes) + payload.
    encoded: Bytes,
}

#[inline]
fn read_u64(buf: &[u8], pos: usize) -> u64 {
    u64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap())
}

#[inline]
fn read_i32(buf: &[u8], pos: usize) -> i32 {
    i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap())
}

impl StreamRecordBatch {
    /// Encode a new record batch from parts.
    pub fn new(stream_id: u64, epoch: u64, base_offset: u64, count: i32, payload: Bytes) -> Self {
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
        buf.put_u8(MAGIC_V0);
        buf.put_u64(stream_id);
        buf.put_u64(epoch);
        buf.put_u64(base_offset);
        buf.put_i32(count);
        buf.put_u32(payload.len() as u32);
        buf.put_slice(&payload);
        Self {
            encoded: buf.freeze(),
        }
    }

    /// Parse an encoded batch from the front of `buf`, advancing past it.
    /// Slicing `Bytes` is always zero-copy and safe.
    pub fn parse(buf: &mut Bytes) -> Result<Self, CodecError> {
        if buf.len() < HEADER_SIZE {
            return Err(CodecError::BufferTooShort {
                need: HEADER_SIZE,
                have: buf.len(),
            });
        }
        let magic = buf[MAGIC_POS];
        if magic != MAGIC_V0 {
            return Err(CodecError::InvalidMagic {
                expected: MAGIC_V0 as u64,
                actual: magic as u64,
            });
        }
        let payload_len = read_i32(buf, PAYLOAD_LENGTH_POS);
        if payload_len < 0 {
            return Err(CodecError::BufferTooShort {
                need: HEADER_SIZE,
                have: buf.len(),
            });
        }
        let encoded_size = PAYLOAD_POS + payload_len as usize;
        if buf.len() < encoded_size {
            return Err(CodecError::BufferTooShort {
                need: encoded_size,
                have: buf.len(),
            });
        }
        let encoded = buf.split_to(encoded_size);
        Ok(Self { encoded })
    }

    /// Wrap already-validated encoded bytes without copying.
    ///
    /// Used by readers that have just CRC-validated a WAL record body or a data block
    /// region and know it contains exactly one encoded batch.
    pub fn from_encoded_unchecked(encoded: Bytes) -> Self {
        Self { encoded }
    }

    /// The full encoded form (header + payload), zero-copy.
    pub fn encoded(&self) -> Bytes {
        self.encoded.clone()
    }

    pub fn stream_id(&self) -> u64 {
        read_u64(&self.encoded, STREAM_ID_POS)
    }

    pub fn epoch(&self) -> u64 {
        read_u64(&self.encoded, EPOCH_POS)
    }

    pub fn base_offset(&self) -> u64 {
        read_u64(&self.encoded, BASE_OFFSET_POS)
    }

    /// Number of logical records in the batch (`lastOffsetDelta`).
    ///
    /// Negative marks a link record.
    pub fn count(&self) -> i32 {
        read_i32(&self.encoded, LAST_OFFSET_DELTA_POS)
    }

    /// Exclusive last offset.
    ///
    /// `count > 0` => `baseOffset + count`.`count <= 0` (link record) =>
    /// `baseOffset - count`.
    pub fn last_offset(&self) -> u64 {
        let count = self.count() as i64;
        let base = self.base_offset() as i64;
        if count > 0 {
            base.wrapping_add(count) as u64
        } else {
            base.wrapping_sub(count) as u64
        }
    }

    /// The opaque payload, zero-copy.
    pub fn payload(&self) -> Bytes {
        self.encoded.slice(PAYLOAD_POS..)
    }

    /// Payload length in bytes (not the encoded length). Cache accounting
    /// depends on this.
    pub fn size(&self) -> usize {
        read_i32(&self.encoded, PAYLOAD_LENGTH_POS) as usize
    }

    /// Approximate in-memory footprint used for cache accounting.
    pub fn occupied_size(&self) -> usize {
        self.size() + OBJECT_OVERHEAD
    }
}

impl std::fmt::Debug for StreamRecordBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRecordBatch")
            .field("stream_id", &self.stream_id())
            .field("epoch", &self.epoch())
            .field("base_offset", &self.base_offset())
            .field("count", &self.count())
            .field("size", &self.size())
            .finish()
    }
}

/// Ordering: by `(stream_id, base_offset)`.
impl PartialEq for StreamRecordBatch {
    fn eq(&self, other: &Self) -> bool {
        self.encoded == other.encoded
    }
}
impl Eq for StreamRecordBatch {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use proptest::prelude::*;

    /// Round-trip: new -> accessors -> parse(encoded) -> identical.
    #[test]
    fn round_trip() {
        let payload = Bytes::from_static(b"hello stream");
        let batch = StreamRecordBatch::new(42, 7, 1000, 3, payload.clone());
        assert_eq!(batch.stream_id(), 42);
        assert_eq!(batch.epoch(), 7);
        assert_eq!(batch.base_offset(), 1000);
        assert_eq!(batch.count(), 3);
        assert_eq!(batch.last_offset(), 1003);
        assert_eq!(batch.payload(), payload);

        let mut buf = batch.encoded();
        let reparsed = StreamRecordBatch::parse(&mut buf).unwrap();
        assert_eq!(reparsed, batch);
        assert!(buf.is_empty());
    }

    /// Link records: negative count, lastOffset = baseOffset - count.
    #[test]
    fn link_record_last_offset() {
        let batch = StreamRecordBatch::new(1, 1, 500, -5, Bytes::new());
        assert_eq!(batch.last_offset(), 505);
    }

    /// Parse must reject a wrong magic byte.
    #[test]
    fn parse_rejects_bad_magic() {
        let mut buf = Bytes::from_static(&[0xFFu8; 64]);
        assert!(StreamRecordBatch::parse(&mut buf).is_err());
    }

    /// Parse of a truncated buffer must fail, not panic.
    #[test]
    fn parse_rejects_truncated() {
        let batch = StreamRecordBatch::new(1, 1, 0, 1, Bytes::from_static(b"0123456789"));
        let encoded = batch.encoded();
        for cut in [0, 1, HEADER_SIZE - 1, HEADER_SIZE, encoded.len() - 1] {
            let mut buf = encoded.slice(..cut);
            assert!(StreamRecordBatch::parse(&mut buf).is_err(), "cut at {cut}");
        }
    }

    /// Parsing is zero-copy: the payload slice points into the input buffer.
    #[test]
    fn parse_is_zero_copy() {
        let batch = StreamRecordBatch::new(9, 9, 9, 1, Bytes::from(vec![7u8; 256]));
        let original = batch.encoded();
        let range = original.as_ptr() as usize..original.as_ptr() as usize + original.len();
        let mut buf = original.clone();
        let parsed = StreamRecordBatch::parse(&mut buf).unwrap();
        let payload = parsed.payload();
        assert!(
            range.contains(&(payload.as_ptr() as usize)),
            "payload was copied"
        );
    }

    /// Golden vectors
    /// Fixtures: conformance/fixtures/record/*.bin + manifest.json.
    #[test]
    fn golden_vectors_match_java() {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/fixtures/record");
        let manifest = std::fs::read_to_string(dir.join("manifest.json"))
            .expect("run conformance/generator first");
        let cases: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        let cases = cases.as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let golden = Bytes::from(std::fs::read(dir.join(format!("{name}.bin"))).unwrap());
            let stream_id = case["stream_id"].as_u64().unwrap();
            let epoch = case["epoch"].as_u64().unwrap();
            let base_offset = case["base_offset"].as_u64().unwrap();
            let count = case["count"].as_i64().unwrap() as i32;
            let payload = Bytes::from(hex::decode(case["payload_hex"].as_str().unwrap()).unwrap());

            let batch =
                StreamRecordBatch::new(stream_id, epoch, base_offset, count, payload.clone());
            assert_eq!(batch.encoded(), golden, "encode mismatch: {name}");

            let mut buf = golden.clone();
            let parsed = StreamRecordBatch::parse(&mut buf).unwrap();
            assert!(buf.is_empty(), "parse did not consume all of {name}");
            assert_eq!(parsed.stream_id(), stream_id, "{name}");
            assert_eq!(parsed.epoch(), epoch, "{name}");
            assert_eq!(parsed.base_offset(), base_offset, "{name}");
            assert_eq!(parsed.count(), count, "{name}");
            assert_eq!(parsed.payload(), payload, "{name}");
            assert_eq!(
                parsed.last_offset() as i64,
                case["last_offset"].as_i64().unwrap(),
                "{name}"
            );
            assert_eq!(
                parsed.size() as i64,
                case["size"].as_i64().unwrap(),
                "{name}"
            );
        }
    }

    proptest! {
        #[test]
        fn prop_round_trip(
            stream_id in any::<u64>(),
            epoch in any::<u64>(),
            base_offset in 0u64..=i64::MAX as u64,
            count in any::<i32>(),
            payload in proptest::collection::vec(any::<u8>(), 0..1024),
        ) {
            let payload = Bytes::from(payload);
            let batch = StreamRecordBatch::new(stream_id, epoch, base_offset, count, payload.clone());
            let mut buf = batch.encoded();
            let parsed = StreamRecordBatch::parse(&mut buf).unwrap();
            prop_assert!(buf.is_empty());
            prop_assert_eq!(parsed.stream_id(), stream_id);
            prop_assert_eq!(parsed.epoch(), epoch);
            prop_assert_eq!(parsed.base_offset(), base_offset);
            prop_assert_eq!(parsed.count(), count);
            prop_assert_eq!(parsed.payload(), payload);
        }
    }
}
