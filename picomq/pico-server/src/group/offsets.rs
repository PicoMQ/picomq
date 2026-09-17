//! Record layout, big endian:
//! "PGS1" u8 kind (0 delta, 1 snapshot) u32 count { string stream, u64 position, u8 has_metadata, [string metadata] }
//! string: u32 length, bytes

use std::collections::BTreeMap;

use super::StreamName;
use super::state::Names;
use bytes::{Buf, BufMut, Bytes, BytesMut};

const MAGIC: &[u8; 4] = b"PGS1";
const KIND_DELTA: u8 = 0;
const KIND_SNAPSHOT: u8 = 1;
const MAX_ENTRIES_PER_RECORD: u32 = 1 << 20;
const MAX_STRING_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub position: u64,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommit {
    pub stream: String,
    pub value: CommittedOffset,
}

pub(super) type OffsetTable = BTreeMap<StreamName, CommittedOffset>;

pub(super) fn encode_commits(commits: &[OffsetCommit]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(MAGIC);
    buf.put_u8(KIND_DELTA);
    buf.put_u32(commits.len() as u32);
    for commit in commits {
        put_entry(&mut buf, &commit.stream, &commit.value);
    }
    buf.freeze()
}

pub(super) fn encode_snapshot(offsets: &OffsetTable) -> Bytes {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(MAGIC);
    buf.put_u8(KIND_SNAPSHOT);
    buf.put_u32(offsets.len() as u32);
    for (stream, value) in offsets {
        put_entry(&mut buf, stream, value);
    }
    buf.freeze()
}

pub(super) fn decode_into(
    payload: &[u8],
    offsets: &mut OffsetTable,
    names: &mut Names,
) -> Result<(), ()> {
    let mut buf = payload;
    if buf.remaining() < MAGIC.len() || &buf[..MAGIC.len()] != MAGIC {
        return Err(());
    }
    buf.advance(MAGIC.len());
    let kind = take_u8(&mut buf)?;
    if kind == KIND_SNAPSHOT {
        offsets.clear();
    } else if kind != KIND_DELTA {
        return Err(());
    }
    let count = take_u32(&mut buf)?;
    if count > MAX_ENTRIES_PER_RECORD {
        return Err(());
    }
    for _ in 0..count {
        let stream = take_string(&mut buf)?;
        let position = take_u64(&mut buf)?;
        let metadata = match take_u8(&mut buf)? {
            0 => None,
            1 => Some(take_string(&mut buf)?),
            _ => return Err(()),
        };
        offsets.insert(
            names.intern(&stream),
            CommittedOffset { position, metadata },
        );
    }
    if buf.has_remaining() {
        return Err(());
    }
    Ok(())
}

fn put_entry(buf: &mut BytesMut, stream: &str, value: &CommittedOffset) {
    put_string(buf, stream);
    buf.put_u64(value.position);
    match &value.metadata {
        Some(metadata) => {
            buf.put_u8(1);
            put_string(buf, metadata);
        }
        None => buf.put_u8(0),
    }
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_u32(value.len() as u32);
    buf.extend_from_slice(value.as_bytes());
}

fn take_string(buf: &mut &[u8]) -> Result<String, ()> {
    let len = take_u32(buf)? as usize;
    if len > MAX_STRING_BYTES || buf.remaining() < len {
        return Err(());
    }
    let value = std::str::from_utf8(&buf[..len]).map_err(|_| ())?.to_owned();
    buf.advance(len);
    Ok(value)
}

fn take_u8(buf: &mut &[u8]) -> Result<u8, ()> {
    (buf.remaining() >= 1).then(|| buf.get_u8()).ok_or(())
}

fn take_u32(buf: &mut &[u8]) -> Result<u32, ()> {
    (buf.remaining() >= 4).then(|| buf.get_u32()).ok_or(())
}

fn take_u64(buf: &mut &[u8]) -> Result<u64, ()> {
    (buf.remaining() >= 8).then(|| buf.get_u64()).ok_or(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn committed(position: u64, metadata: Option<&str>) -> CommittedOffset {
        CommittedOffset {
            position,
            metadata: metadata.map(str::to_owned),
        }
    }

    #[test]
    fn snapshot_round_trip() {
        let mut names = Names::default();
        let offsets = OffsetTable::from([
            (names.intern("a"), committed(12, Some("m"))),
            (names.intern("b"), committed(7, None)),
        ]);
        let mut decoded = OffsetTable::new();
        decode_into(&encode_snapshot(&offsets), &mut decoded, &mut names).unwrap();
        assert_eq!(decoded, offsets);
    }

    #[test]
    fn deltas_fold_in_order() {
        let mut names = Names::default();
        let mut table = OffsetTable::new();
        let first = encode_commits(&[OffsetCommit {
            stream: "t".into(),
            value: committed(5, None),
        }]);
        let second = encode_commits(&[OffsetCommit {
            stream: "t".into(),
            value: committed(9, None),
        }]);
        decode_into(&first, &mut table, &mut names).unwrap();
        decode_into(&second, &mut table, &mut names).unwrap();
        assert_eq!(table[&names.intern("t")].position, 9);
        assert_eq!(names.len(), 1);
    }

    #[test]
    fn snapshot_replaces_the_table() {
        let mut names = Names::default();
        let mut table = OffsetTable::from([(names.intern("old"), committed(1, None))]);
        let snapshot = OffsetTable::from([(names.intern("new"), committed(2, None))]);
        decode_into(&encode_snapshot(&snapshot), &mut table, &mut names).unwrap();
        assert_eq!(table, snapshot);
    }

    #[test]
    fn rejects_garbage() {
        let mut names = Names::default();
        let mut table = OffsetTable::new();
        assert!(decode_into(b"nope", &mut table, &mut names).is_err());
        assert!(decode_into(b"PGS1\x00\xff\xff\xff\xff", &mut table, &mut names).is_err());
        assert!(decode_into(b"PGS1\x07\x00\x00\x00\x00", &mut table, &mut names).is_err());
        let mut trailing = encode_commits(&[]).to_vec();
        trailing.push(0);
        assert!(decode_into(&trailing, &mut table, &mut names).is_err());
    }
}
