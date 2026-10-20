//! Record layout, big endian:
//! "PGS3" u8 kind (0 delta, 1 snapshot) u32 count { [16] topic id, u32 partition, u64 position, i32 leader epoch (-1 none), u8 has_metadata, [string metadata] }
//! "PGS3" u8 kind (2 delete) u32 count { [16] topic id, u32 partition }
//! string: u32 length, bytes

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::partition::Partition;
use crate::topic::TopicId;

const MAGIC: &[u8; 4] = b"PGS3";
const KIND_DELTA: u8 = 0;
const KIND_SNAPSHOT: u8 = 1;
const KIND_DELETE: u8 = 2;
const MAX_ENTRIES_PER_RECORD: u32 = 1 << 20;
const MAX_STRING_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub position: u64,
    pub leader_epoch: Option<i32>,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommit {
    pub partition: Partition,
    pub value: CommittedOffset,
}

pub(super) type OffsetTable = BTreeMap<Partition, CommittedOffset>;

pub(super) fn encode_commits(commits: &[OffsetCommit]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(MAGIC);
    buf.put_u8(KIND_DELTA);
    buf.put_u32(commits.len() as u32);
    for commit in commits {
        put_entry(&mut buf, &commit.partition, &commit.value);
    }
    buf.freeze()
}

pub(super) fn encode_snapshot(offsets: &OffsetTable) -> Bytes {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(MAGIC);
    buf.put_u8(KIND_SNAPSHOT);
    buf.put_u32(offsets.len() as u32);
    for (partition, value) in offsets {
        put_entry(&mut buf, partition, value);
    }
    buf.freeze()
}

pub(super) fn encode_deletes(partitions: &[Partition]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(MAGIC);
    buf.put_u8(KIND_DELETE);
    buf.put_u32(partitions.len() as u32);
    for partition in partitions {
        buf.put_slice(&partition.topic.0);
        buf.put_u32(partition.index);
    }
    buf.freeze()
}

pub(super) fn decode_into(payload: &[u8], offsets: &mut OffsetTable) -> Result<(), ()> {
    let mut buf = payload;
    if buf.remaining() < MAGIC.len() || &buf[..MAGIC.len()] != MAGIC {
        return Err(());
    }
    buf.advance(MAGIC.len());

    let kind = take_u8(&mut buf)?;
    let count = take_u32(&mut buf)?;
    if count > MAX_ENTRIES_PER_RECORD {
        return Err(());
    }

    match kind {
        KIND_SNAPSHOT => offsets.clear(),
        KIND_DELTA => {}
        KIND_DELETE => {
            for _ in 0..count {
                let topic = TopicId(take_id(&mut buf)?);
                let index = take_u32(&mut buf)?;
                offsets.remove(&Partition::new(topic, index));
            }
            return if buf.has_remaining() { Err(()) } else { Ok(()) };
        }
        _ => return Err(()),
    }

    for _ in 0..count {
        let topic = TopicId(take_id(&mut buf)?);
        let index = take_u32(&mut buf)?;
        let position = take_u64(&mut buf)?;
        let leader_epoch = Some(take_i32(&mut buf)?).filter(|epoch| *epoch >= 0);
        let metadata = match take_u8(&mut buf)? {
            0 => None,
            1 => Some(take_string(&mut buf)?),
            _ => return Err(()),
        };
        offsets.insert(
            Partition::new(topic, index),
            CommittedOffset {
                position,
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

fn put_entry(buf: &mut BytesMut, partition: &Partition, value: &CommittedOffset) {
    buf.put_slice(&partition.topic.0);
    buf.put_u32(partition.index);
    buf.put_u64(value.position);
    buf.put_i32(value.leader_epoch.unwrap_or(-1));
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

fn take_id(buf: &mut &[u8]) -> Result<[u8; 16], ()> {
    if buf.remaining() < 16 {
        return Err(());
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&buf[..16]);
    buf.advance(16);
    Ok(id)
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

fn take_i32(buf: &mut &[u8]) -> Result<i32, ()> {
    (buf.remaining() >= 4).then(|| buf.get_i32()).ok_or(())
}

fn take_u64(buf: &mut &[u8]) -> Result<u64, ()> {
    (buf.remaining() >= 8).then(|| buf.get_u64()).ok_or(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(byte: u8, index: u32) -> Partition {
        Partition::new(TopicId([byte; 16]), index)
    }

    fn committed(position: u64, metadata: Option<&str>) -> CommittedOffset {
        CommittedOffset {
            position,
            leader_epoch: None,
            metadata: metadata.map(str::to_owned),
        }
    }

    #[test]
    fn snapshot_round_trip() {
        let offsets = OffsetTable::from([
            (partition(1, 0), committed(12, Some("m"))),
            (partition(1, 3), committed(7, None)),
            (partition(2, 0), committed(1, None)),
        ]);

        let mut decoded = OffsetTable::new();
        decode_into(&encode_snapshot(&offsets), &mut decoded).unwrap();
        assert_eq!(decoded, offsets);
    }

    #[test]
    fn deltas_fold_in_order() {
        let mut table = OffsetTable::new();
        let first = encode_commits(&[OffsetCommit {
            partition: partition(1, 0),
            value: committed(5, None),
        }]);
        let second = encode_commits(&[OffsetCommit {
            partition: partition(1, 0),
            value: committed(9, None),
        }]);

        decode_into(&first, &mut table).unwrap();
        decode_into(&second, &mut table).unwrap();
        assert_eq!(table[&partition(1, 0)].position, 9);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn snapshot_replaces_the_table() {
        let mut table = OffsetTable::from([(partition(1, 0), committed(1, None))]);
        let snapshot = OffsetTable::from([(partition(2, 0), committed(2, None))]);

        decode_into(&encode_snapshot(&snapshot), &mut table).unwrap();
        assert_eq!(table, snapshot);
    }

    #[test]
    fn leader_epochs_round_trip() {
        let offsets = OffsetTable::from([
            (
                partition(1, 0),
                CommittedOffset {
                    leader_epoch: Some(4),
                    ..committed(12, None)
                },
            ),
            (partition(1, 1), committed(3, Some("m"))),
        ]);

        let mut decoded = OffsetTable::new();
        decode_into(&encode_snapshot(&offsets), &mut decoded).unwrap();
        assert_eq!(decoded, offsets);
    }

    #[test]
    fn deletes_remove_entries() {
        let mut table = OffsetTable::from([
            (partition(1, 0), committed(1, None)),
            (partition(2, 0), committed(2, None)),
        ]);

        decode_into(
            &encode_deletes(&[partition(1, 0), partition(3, 0)]),
            &mut table,
        )
        .unwrap();
        assert_eq!(
            table,
            OffsetTable::from([(partition(2, 0), committed(2, None))])
        );
    }

    #[test]
    fn rejects_garbage_and_the_previous_format() {
        let mut table = OffsetTable::new();
        assert!(decode_into(b"nope", &mut table).is_err());
        assert!(decode_into(b"PGS2\x00\x00\x00\x00\x00", &mut table).is_err());
        assert!(decode_into(b"PGS3\x00\xff\xff\xff\xff", &mut table).is_err());
        assert!(decode_into(b"PGS3\x07\x00\x00\x00\x00", &mut table).is_err());

        let mut trailing = encode_commits(&[]).to_vec();
        trailing.push(0);
        assert!(decode_into(&trailing, &mut table).is_err());
    }
}
