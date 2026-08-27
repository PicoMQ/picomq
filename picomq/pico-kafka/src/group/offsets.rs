//! Committed-offset records on the group stream: delta commits with periodic
//! snapshot+trim. Replay is a fold where later entries win, so the same
//! decoder serves deltas and snapshots.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(super) const RECORD_MAGIC: &[u8; 4] = b"PKG1";
pub(super) const MAX_OFFSETS_PER_GROUP: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
}

impl CommittedOffset {
    pub fn none() -> Self {
        Self {
            offset: -1,
            leader_epoch: -1,
            metadata: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OffsetCommit {
    pub topic: String,
    pub partition: i32,
    pub value: CommittedOffset,
}

pub(super) type OffsetTable = BTreeMap<(String, i32), CommittedOffset>;

pub(super) fn encode_commits(commits: &[OffsetCommit]) -> Bytes {
    let mut buf = record_header(commits.len());
    for commit in commits {
        put_entry(&mut buf, &commit.topic, commit.partition, &commit.value);
    }
    buf.freeze()
}

pub(super) fn encode_snapshot(offsets: &OffsetTable) -> Bytes {
    let mut buf = record_header(offsets.len());
    for ((topic, partition), value) in offsets {
        put_entry(&mut buf, topic, *partition, value);
    }
    buf.freeze()
}

pub(super) fn decode_into(payload: &[u8], offsets: &mut OffsetTable) -> Result<(), ()> {
    let mut buf = payload;
    if buf.remaining() < RECORD_MAGIC.len() || &buf[..4] != RECORD_MAGIC {
        return Err(());
    }
    buf.advance(4);
    let count = take_u32(&mut buf)? as usize;
    if count > MAX_OFFSETS_PER_GROUP {
        return Err(());
    }
    for _ in 0..count {
        let topic = take_string(&mut buf)?;
        let partition = take_i32(&mut buf)?;
        let offset = take_i64(&mut buf)?;
        let leader_epoch = take_i32(&mut buf)?;
        let metadata = match take_u8(&mut buf)? {
            0 => None,
            1 => Some(take_string(&mut buf)?),
            _ => return Err(()),
        };
        offsets.insert(
            (topic, partition),
            CommittedOffset {
                offset,
                leader_epoch,
                metadata,
            },
        );
    }
    if buf.has_remaining() {
        return Err(());
    }
    Ok(())
}

/// The all-unset response for a group that has never committed.
pub(super) fn empty_offset_fetch(
    requested: Option<&[(String, Vec<i32>)]>,
) -> BTreeMap<String, Vec<(i32, CommittedOffset)>> {
    let mut result = BTreeMap::new();
    if let Some(topics) = requested {
        for (topic, partitions) in topics {
            result.insert(
                topic.clone(),
                partitions
                    .iter()
                    .map(|partition| (*partition, CommittedOffset::none()))
                    .collect(),
            );
        }
    }
    result
}

fn record_header(count: usize) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(RECORD_MAGIC);
    buf.put_u32(count as u32);
    buf
}

fn put_entry(buf: &mut BytesMut, topic: &str, partition: i32, value: &CommittedOffset) {
    put_string(buf, topic);
    buf.put_i32(partition);
    buf.put_i64(value.offset);
    buf.put_i32(value.leader_epoch);
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
    if len > 1024 * 1024 || buf.remaining() < len {
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

fn take_i32(buf: &mut &[u8]) -> Result<i32, ()> {
    (buf.remaining() >= 4).then(|| buf.get_i32()).ok_or(())
}

fn take_i64(buf: &mut &[u8]) -> Result<i64, ()> {
    (buf.remaining() >= 8).then(|| buf.get_i64()).ok_or(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_round_trip() {
        let offsets = OffsetTable::from([
            (
                ("a".to_owned(), 0),
                CommittedOffset {
                    offset: 12,
                    leader_epoch: 3,
                    metadata: Some("m".to_owned()),
                },
            ),
            (
                ("b".to_owned(), 1),
                CommittedOffset {
                    offset: 7,
                    leader_epoch: -1,
                    metadata: None,
                },
            ),
        ]);
        let mut decoded = OffsetTable::new();
        decode_into(&encode_snapshot(&offsets), &mut decoded).unwrap();
        assert_eq!(decoded, offsets);
    }

    #[test]
    fn deltas_fold_in_order() {
        let mut table = OffsetTable::new();
        let first = encode_commits(&[OffsetCommit {
            topic: "t".into(),
            partition: 0,
            value: CommittedOffset {
                offset: 5,
                leader_epoch: 1,
                metadata: None,
            },
        }]);
        let second = encode_commits(&[OffsetCommit {
            topic: "t".into(),
            partition: 0,
            value: CommittedOffset {
                offset: 9,
                leader_epoch: 1,
                metadata: None,
            },
        }]);
        decode_into(&first, &mut table).unwrap();
        decode_into(&second, &mut table).unwrap();
        assert_eq!(table[&("t".to_owned(), 0)].offset, 9);
    }

    #[test]
    fn rejects_garbage() {
        let mut table = OffsetTable::new();
        assert!(decode_into(b"nope", &mut table).is_err());
        assert!(decode_into(b"PKG1\xff\xff\xff\xff", &mut table).is_err());
    }
}
