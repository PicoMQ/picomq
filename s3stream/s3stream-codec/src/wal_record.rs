//! WAL record framing: the 24-byte header wrapping each record body in the WAL.
//!
//! Specification: `specification/wal-protocol.md` (record framing section).

use bytes::{BufMut, Bytes};

use crate::crc::wal_crc32;
use crate::error::CodecError;

/// Header size: magic(4) + bodyLength(4) + bodyOffset(8) + bodyCRC(4) + headerCRC(4).
pub const WAL_RECORD_HEADER_SIZE: usize = 24;
/// Header bytes covered by the trailing header CRC.
pub const WAL_RECORD_HEADER_WITHOUT_CRC_SIZE: usize = WAL_RECORD_HEADER_SIZE - 4;

/// Magic for a data record.
pub const RECORD_DATA_MAGIC: u32 = 0x8765_4321;
/// Magic for an empty/padding record (body is filler, skip it).
pub const RECORD_EMPTY_MAGIC: u32 = 0x7654_3210;

/// The frame header preceding every WAL record body.
///
/// `RecordHeader`. Field order on the wire:
/// magic, bodyLength, bodyOffset, bodyCRC, headerCRC (all big-endian).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRecordHeader {
    pub magic: u32,
    /// Byte length of the record body following this header.
    pub body_length: u32,
    /// Logical WAL offset of the body = header's logical offset + 24.
    pub body_offset: u64,
    /// CRC of the body (see `crate::crc::wal_crc32`).
    pub body_crc: u32,
    /// CRC of the preceding 20 header bytes (computed on marshal).
    pub header_crc: u32,
}

impl WalRecordHeader {
    /// Build a data-record header for a body at logical WAL offset `offset`.
    ///
    /// `bodyOffset = offset + RECORD_HEADER_SIZE`.
    pub fn data(offset: u64, body_length: u32, body_crc: u32) -> Self {
        Self {
            magic: RECORD_DATA_MAGIC,
            body_length,
            body_offset: offset + WAL_RECORD_HEADER_SIZE as u64,
            body_crc,
            header_crc: 0, // computed on marshal
        }
    }

    /// Build a padding-record header (empty body of `body_length` filler
    /// bytes), tagged with the empty magic code.
    pub fn padding(offset: u64, body_length: u32) -> Self {
        Self {
            magic: RECORD_EMPTY_MAGIC,
            body_length,
            body_offset: offset + WAL_RECORD_HEADER_SIZE as u64,
            body_crc: 0,
            header_crc: 0,
        }
    }

    /// Serialize to 24 bytes, computing and appending the header CRC. The
    /// CRC covers the first 20 bytes.
    pub fn marshal(&self) -> [u8; WAL_RECORD_HEADER_SIZE] {
        let mut buf = [0u8; WAL_RECORD_HEADER_SIZE];
        {
            let mut cursor = &mut buf[..];
            cursor.put_u32(self.magic);
            cursor.put_u32(self.body_length);
            cursor.put_u64(self.body_offset);
            cursor.put_u32(self.body_crc);
        }
        let header_crc = wal_crc32(&buf[..WAL_RECORD_HEADER_WITHOUT_CRC_SIZE]);
        buf[WAL_RECORD_HEADER_WITHOUT_CRC_SIZE..].copy_from_slice(&header_crc.to_be_bytes());
        buf
    }

    /// Parse and validate 24 header bytes.
    ///
    /// (`ObjectUtils#decodeRecordBuf` checks the data magic, recovery checks header CRC).
    /// Body CRC validation is the caller's job (it owns the body bytes).
    pub fn unmarshal(buf: &[u8]) -> Result<Self, CodecError> {
        if buf.len() < WAL_RECORD_HEADER_SIZE {
            return Err(CodecError::BufferTooShort {
                need: WAL_RECORD_HEADER_SIZE,
                have: buf.len(),
            });
        }
        let stored_crc = u32::from_be_bytes(buf[20..24].try_into().unwrap());
        let computed_crc = wal_crc32(&buf[..WAL_RECORD_HEADER_WITHOUT_CRC_SIZE]);
        if stored_crc != computed_crc {
            return Err(CodecError::CrcMismatch {
                expected: stored_crc,
                computed: computed_crc,
            });
        }
        let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        if magic != RECORD_DATA_MAGIC && magic != RECORD_EMPTY_MAGIC {
            return Err(CodecError::InvalidMagic {
                expected: RECORD_DATA_MAGIC as u64,
                actual: magic as u64,
            });
        }
        Ok(Self {
            magic,
            body_length: u32::from_be_bytes(buf[4..8].try_into().unwrap()),
            body_offset: u64::from_be_bytes(buf[8..16].try_into().unwrap()),
            body_crc: u32::from_be_bytes(buf[16..20].try_into().unwrap()),
            header_crc: stored_crc,
        })
    }

    /// True if this frames a padding record whose body should be skipped.
    pub fn is_padding(&self) -> bool {
        self.magic == RECORD_EMPTY_MAGIC
    }
}

/// Frame a record body: header + body concatenated, ready to append to a WAL object.
pub fn frame_record(offset: u64, body: &Bytes) -> Vec<u8> {
    let header = WalRecordHeader::data(offset, body.len() as u32, wal_crc32(body));
    let mut framed = Vec::with_capacity(WAL_RECORD_HEADER_SIZE + body.len());
    framed.extend_from_slice(&header.marshal());
    framed.extend_from_slice(body);
    framed
}

/// Frame a padding record of `total_length` bytes (header + zero body).
pub fn frame_padding_record(offset: u64, total_length: usize) -> Vec<u8> {
    assert!(total_length >= WAL_RECORD_HEADER_SIZE);
    let body_length = total_length - WAL_RECORD_HEADER_SIZE;
    let header = WalRecordHeader::padding(offset, body_length as u32);
    let mut framed = vec![0u8; total_length];
    framed[..WAL_RECORD_HEADER_SIZE].copy_from_slice(&header.marshal());
    framed
}

/// Decode the record at the front of `buf` into an encoded `StreamRecordBatch`,
/// advancing `buf` past header+body. Fails on padding records (callers skip those
/// via `WalRecordHeader::is_padding` before decoding).
pub fn decode_record(buf: &mut Bytes) -> Result<crate::record::StreamRecordBatch, CodecError> {
    let header = WalRecordHeader::unmarshal(buf)?;
    if header.magic != RECORD_DATA_MAGIC {
        return Err(CodecError::InvalidMagic {
            expected: RECORD_DATA_MAGIC as u64,
            actual: header.magic as u64,
        });
    }
    let total = WAL_RECORD_HEADER_SIZE + header.body_length as usize;
    if buf.len() < total {
        return Err(CodecError::BufferTooShort {
            need: total,
            have: buf.len(),
        });
    }
    let body = buf.slice(WAL_RECORD_HEADER_SIZE..total);
    let computed = wal_crc32(&body);
    if computed != header.body_crc {
        return Err(CodecError::CrcMismatch {
            expected: header.body_crc,
            computed,
        });
    }
    let mut body_buf = body;
    let record = crate::record::StreamRecordBatch::parse(&mut body_buf)?;
    if !body_buf.is_empty() {
        return Err(CodecError::BufferTooShort {
            need: header.body_length as usize,
            have: header.body_length as usize - body_buf.len(),
        });
    }
    let _ = buf.split_to(total);
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn header_size_matches_java() {
        assert_eq!(WAL_RECORD_HEADER_SIZE, 24);
        assert_eq!(WAL_RECORD_HEADER_WITHOUT_CRC_SIZE, 20);
    }

    #[test]
    fn header_round_trip() {
        let h = WalRecordHeader::data(4096, 128, 0x5EAD_BEEF);
        let bytes = h.marshal();
        let parsed = WalRecordHeader::unmarshal(&bytes).unwrap();
        assert_eq!(parsed.body_offset, 4096 + WAL_RECORD_HEADER_SIZE as u64);
        assert_eq!(parsed.body_length, 128);
        assert_eq!(parsed.body_crc, 0x5EAD_BEEF);
        assert!(!parsed.is_padding());
    }

    /// A flipped bit in the header must fail the header CRC.
    #[test]
    fn corrupt_header_fails_crc() {
        let h = WalRecordHeader::data(0, 10, 0);
        let mut bytes = h.marshal();
        bytes[5] ^= 0x01;
        assert!(WalRecordHeader::unmarshal(&bytes).is_err());
    }

    #[test]
    fn frame_decode_round_trip() {
        let record =
            crate::record::StreamRecordBatch::new(3, 1, 77, 2, Bytes::from_static(b"body"));
        let framed = frame_record(512, &record.encoded());
        let mut buf = Bytes::from(framed);
        let decoded = decode_record(&mut buf).unwrap();
        assert!(buf.is_empty());
        assert_eq!(decoded, record);
    }

    /// Golden vectors: framed records
    #[test]
    fn golden_vectors_match_java() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/fixtures/wal_record");
        let manifest = std::fs::read_to_string(dir.join("manifest.json"))
            .expect("run conformance/generator first");
        let cases: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        let cases = cases.as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let golden = std::fs::read(dir.join(format!("{name}.bin"))).unwrap();
            match case["kind"].as_str().unwrap() {
                "data" => {
                    let start = case["start_offset"].as_u64().unwrap();
                    let body =
                        Bytes::from(hex::decode(case["body_hex"].as_str().unwrap()).unwrap());
                    assert_eq!(frame_record(start, &body), golden, "frame mismatch: {name}");
                    // Header parses and validates against the body.
                    let header = WalRecordHeader::unmarshal(&golden).unwrap();
                    assert_eq!(header.body_length as usize, body.len(), "{name}");
                    assert_eq!(
                        header.body_offset,
                        start + WAL_RECORD_HEADER_SIZE as u64,
                        "{name}"
                    );
                    assert_eq!(header.body_crc, wal_crc32(&body), "{name}");
                }
                "padding" => {
                    let start = case["start_offset"].as_u64().unwrap();
                    let total = case["total_length"].as_u64().unwrap() as usize;
                    assert_eq!(
                        frame_padding_record(start, total),
                        golden,
                        "padding mismatch: {name}"
                    );
                    let header = WalRecordHeader::unmarshal(&golden).unwrap();
                    assert!(header.is_padding(), "{name}");
                }
                other => panic!("unknown fixture kind {other}"),
            }
        }
    }

    proptest! {
        /// Any single-byte corruption of the header is rejected.
        #[test]
        fn prop_header_corruption_rejected(
            offset in 0u64..=u32::MAX as u64,
            body_length in 0u32..=u32::MAX / 2,
            crc in 0u32..=0x7FFF_FFFF,
            pos in 0usize..WAL_RECORD_HEADER_SIZE,
            flip in 1u8..=255,
        ) {
            let bytes = WalRecordHeader::data(offset, body_length, crc).marshal();
            let mut corrupted = bytes;
            corrupted[pos] ^= flip;
            prop_assert!(WalRecordHeader::unmarshal(&corrupted).is_err());
        }
    }
}
