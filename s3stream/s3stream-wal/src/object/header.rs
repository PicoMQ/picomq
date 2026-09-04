//! WAL object header: prefix of every WAL object.
//!
//! Specification: `specification/wal-protocol.md` (object layout section).

use bytes::{BufMut, Bytes, BytesMut};

use crate::WalError;

pub const WAL_HEADER_MAGIC_V0: u32 = 0x1234_5678;
pub const WAL_HEADER_SIZE_V0: usize = 4 + 8 + 8 + 8 + 4 + 8; // 40
pub const WAL_HEADER_MAGIC_V1: u32 = 0xEDCB_A987;
pub const WAL_HEADER_SIZE_V1: usize = WAL_HEADER_SIZE_V0 + 8; // 48: + trimOffset
pub const MAX_WAL_HEADER_SIZE: usize = WAL_HEADER_SIZE_V1;

/// Trim offset value meaning "never trimmed". V0 headers always report `-1`.
pub const TRIM_OFFSET_NONE: i64 = -1;

/// Header of one WAL object. New objects are written v1. V0 is read-compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalObjectHeader {
    pub magic: u32,
    /// Logical WAL offset of the first record body in this object.
    pub start_offset: u64,
    /// Bytes of record data in this object.
    pub body_length: u64,
    /// Deprecated (always 0 in new objects).
    pub sticky_record_length: u64,
    pub node_id: u32,
    pub epoch: u64,
    /// WAL trim watermark piggybacked at write time.`-1` before the first trim.
    pub trim_offset: i64,
}

impl WalObjectHeader {
    /// Build a v1 header.
    pub fn v1(
        start_offset: u64,
        body_length: u64,
        node_id: u32,
        epoch: u64,
        trim_offset: i64,
    ) -> Self {
        Self {
            magic: WAL_HEADER_MAGIC_V1,
            start_offset,
            body_length,
            sticky_record_length: 0,
            node_id,
            epoch,
            trim_offset,
        }
    }

    /// Serialized size for this header's version.
    pub fn size(&self) -> usize {
        match self.magic {
            WAL_HEADER_MAGIC_V1 => WAL_HEADER_SIZE_V1,
            _ => WAL_HEADER_SIZE_V0,
        }
    }

    /// Serialize (field order per the specification, big-endian).
    pub fn marshal(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.size());
        buf.put_u32(self.magic);
        buf.put_u64(self.start_offset);
        buf.put_u64(self.body_length);
        buf.put_u64(self.sticky_record_length);
        buf.put_u32(self.node_id);
        buf.put_u64(self.epoch);
        if self.magic == WAL_HEADER_MAGIC_V1 {
            buf.put_i64(self.trim_offset);
        }
        buf.freeze()
    }

    /// Parse + validate a header from the front of an object (does not
    /// consume). Dispatches on magic and verifies the buffer length.
    pub fn unmarshal(buf: &[u8]) -> Result<Self, WalError> {
        if buf.len() < 4 {
            return Err(WalError::Recovery(format!(
                "Insufficient bytes to read magic code, Recovered: [{}] expect: [4]",
                buf.len()
            )));
        }
        let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let size = match magic {
            WAL_HEADER_MAGIC_V0 => WAL_HEADER_SIZE_V0,
            WAL_HEADER_MAGIC_V1 => WAL_HEADER_SIZE_V1,
            other => {
                return Err(WalError::Recovery(format!(
                    "WALHeader magic code not match, Recovered: [{other:#x}]"
                )));
            }
        };
        if buf.len() < size {
            return Err(WalError::Recovery(format!(
                "WALHeader does not have enough bytes, Recovered: [{}] expect: [{size}]",
                buf.len()
            )));
        }
        let start_offset = u64::from_be_bytes(buf[4..12].try_into().unwrap());
        let body_length = u64::from_be_bytes(buf[12..20].try_into().unwrap());
        let sticky_record_length = u64::from_be_bytes(buf[20..28].try_into().unwrap());
        let node_id = u32::from_be_bytes(buf[28..32].try_into().unwrap());
        let epoch = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        let trim_offset = if magic == WAL_HEADER_MAGIC_V1 {
            i64::from_be_bytes(buf[40..48].try_into().unwrap())
        } else {
            TRIM_OFFSET_NONE
        };
        Ok(Self {
            magic,
            start_offset,
            body_length,
            sticky_record_length,
            node_id,
            epoch,
            trim_offset,
        })
    }
}

pub fn calculate_end_offset_v0(start_offset: u64, object_size: u64) -> u64 {
    start_offset + object_size - WAL_HEADER_SIZE_V0 as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_match_java() {
        assert_eq!(WAL_HEADER_SIZE_V0, 40);
        assert_eq!(WAL_HEADER_SIZE_V1, 48);
    }

    #[test]
    fn v1_round_trip() {
        let h = WalObjectHeader::v1(1024, 8192, 3, 7, 512);
        let bytes = h.marshal();
        assert_eq!(bytes.len(), WAL_HEADER_SIZE_V1);
        let parsed = WalObjectHeader::unmarshal(&bytes).unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn v1_never_trimmed_writes_minus_one() {
        let h = WalObjectHeader::v1(0, 100, 1, 1, TRIM_OFFSET_NONE);
        let bytes = h.marshal();
        assert_eq!(&bytes[40..48], &(-1i64).to_be_bytes());
        assert_eq!(WalObjectHeader::unmarshal(&bytes).unwrap().trim_offset, -1);
    }

    #[test]
    fn v0_parses_with_no_trim_offset() {
        // Hand-build a v0 header.
        let mut buf = Vec::new();
        buf.extend_from_slice(&WAL_HEADER_MAGIC_V0.to_be_bytes());
        buf.extend_from_slice(&123u64.to_be_bytes()); // start offset
        buf.extend_from_slice(&456u64.to_be_bytes()); // body length
        buf.extend_from_slice(&0u64.to_be_bytes()); // sticky
        buf.extend_from_slice(&9u32.to_be_bytes()); // node id
        buf.extend_from_slice(&2u64.to_be_bytes()); // epoch
        let parsed = WalObjectHeader::unmarshal(&buf).unwrap();
        assert_eq!(parsed.magic, WAL_HEADER_MAGIC_V0);
        assert_eq!(parsed.size(), WAL_HEADER_SIZE_V0);
        assert_eq!(parsed.start_offset, 123);
        assert_eq!(parsed.trim_offset, TRIM_OFFSET_NONE);
    }

    #[test]
    fn bad_magic_and_truncation_rejected() {
        assert!(WalObjectHeader::unmarshal(&[0u8; 2]).is_err());
        let mut bytes = WalObjectHeader::v1(0, 0, 0, 0, -1).marshal().to_vec();
        bytes[0] ^= 0xFF;
        assert!(WalObjectHeader::unmarshal(&bytes).is_err());
        let short = &WalObjectHeader::v1(0, 0, 0, 0, -1).marshal()[..20];
        assert!(WalObjectHeader::unmarshal(short).is_err());
    }

    /// Golden vectors from `conformance/fixtures/wal_object/headers.json`.
    #[test]
    fn golden_headers_match_java() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/fixtures/wal_object/headers.json");
        let manifest = std::fs::read_to_string(path).expect("run conformance/generator first");
        let cases: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        let cases = cases.as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let golden = hex::decode(case["bytes_hex"].as_str().unwrap()).unwrap();
            let version = case["version"].as_u64().unwrap();
            let parsed = WalObjectHeader::unmarshal(&golden).unwrap();
            assert_eq!(parsed.start_offset, case["start_offset"].as_u64().unwrap());
            assert_eq!(parsed.body_length, case["length"].as_u64().unwrap());
            assert_eq!(parsed.node_id, case["node_id"].as_u64().unwrap() as u32);
            assert_eq!(parsed.epoch, case["epoch"].as_u64().unwrap());
            match version {
                0 => {
                    assert_eq!(parsed.magic, WAL_HEADER_MAGIC_V0);
                    assert_eq!(parsed.trim_offset, TRIM_OFFSET_NONE);
                }
                1 => {
                    assert_eq!(parsed.magic, WAL_HEADER_MAGIC_V1);
                    assert_eq!(parsed.trim_offset, case["trim_offset"].as_i64().unwrap());
                    // Re-marshal must be byte-identical to Java for v1 (the write path).
                    assert_eq!(parsed.marshal().as_ref(), golden.as_slice());
                }
                other => panic!("unknown version {other}"),
            }
        }
    }
}
