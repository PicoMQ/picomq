use bytes::{Buf, BufMut, Bytes, BytesMut};
use s3stream::{
    CompactOperations, ObjectAttributes, S3ObjectMetadata, S3ObjectType, StreamOffsetRange,
    StreamState,
};

use crate::state::{
    MetadataState, NodeRow, PendingTransfer, StreamObjectRow, StreamRow, StreamSetObjectRow,
};

/// Current snapshot format version. Pre-release: no compatibility shims for
/// older layouts. Bump only once real deployments have snapshots to migrate.
pub const SNAPSHOT_VERSION: u8 = 0;

/// Snapshot decode failures (corrupt archive, unknown version).
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("unsupported snapshot version {0}")]
    UnsupportedVersion(u8),
    #[error("corrupt snapshot: {0}")]
    Corrupt(String),
}

/// Serialize the full state (version byte + body).
///
/// Deterministic: equal states produce
/// equal bytes (all maps iterate in key order, the destroyed FIFO in sequence
/// order). Replicas can compare snapshot hashes.
pub fn encode(state: &MetadataState) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(SNAPSHOT_VERSION);

    buf.put_u64_le(state.next_stream_id);
    buf.put_u64_le(state.streams.len() as u64);
    for (_, row) in state.streams.iter() {
        buf.put_u64_le(row.stream_id);
        buf.put_i64_le(row.epoch);
        buf.put_u64_le(row.start_offset);
        buf.put_u64_le(row.end_offset);
        buf.put_u8(match row.state {
            StreamState::Closed => 0,
            StreamState::Opened => 1,
        });
        buf.put_i32_le(row.node_id);
    }

    buf.put_u64_le(state.nodes.len() as u64);
    for (_, node) in state.nodes.iter() {
        buf.put_i32_le(node.node_id);
        buf.put_i64_le(node.epoch);
        put_str(&mut buf, &node.http_address);
        buf.put_u32_le(node.slots);
        put_str_map(&mut buf, &node.protocol_addresses);
    }
    buf.put_u64_le(state.next_producer_id);

    buf.put_u64_le(state.next_object_id);
    buf.put_u64_le(state.prepared.len() as u64);
    for (id, deadline) in state.prepared.iter() {
        buf.put_u64_le(*id);
        buf.put_i64_le(*deadline);
    }

    buf.put_u64_le(state.stream_set_objects.len() as u64);
    for (_, row) in state.stream_set_objects.iter() {
        buf.put_i32_le(row.node_id);
        put_object(&mut buf, &row.object);
    }

    buf.put_u64_le(state.stream_objects.len() as u64);
    for (_, row) in state.stream_objects.iter() {
        put_object(&mut buf, &row.object);
    }

    // Sequences are stored verbatim (they may be sparse after cleans) so the
    // round trip is exact identity. Replicas can compare snapshot hashes.
    buf.put_u64_le(state.mark_destroyed.len() as u64);
    for (seq, (object_id, op)) in state.mark_destroyed.iter() {
        buf.put_u64_le(*seq);
        buf.put_u64_le(*object_id);
        buf.put_u8(*op as u8);
    }
    buf.put_u64_le(state.next_destroyed_seq);

    buf.put_u64_le(state.kv.len() as u64);
    for (key, value) in state.kv.iter() {
        put_str(&mut buf, key);
        buf.put_u32_le(value.len() as u32);
        buf.put_slice(value);
    }

    buf.put_u64_le(state.pending_transfers.len() as u64);
    for (stream_id, pending) in state.pending_transfers.iter() {
        buf.put_u64_le(*stream_id);
        buf.put_i32_le(pending.from_node);
        buf.put_i32_le(pending.to_node);
    }

    buf.freeze()
}

/// Restore a state from snapshot bytes, rebuilding all secondary indexes.
pub fn decode(bytes: &[u8]) -> Result<MetadataState, SnapshotError> {
    let mut buf = bytes;
    let version = get_u8(&mut buf)?;
    if version != SNAPSHOT_VERSION {
        return Err(SnapshotError::UnsupportedVersion(version));
    }

    let mut state = MetadataState::new();
    state.next_stream_id = get_u64(&mut buf)?;
    for _ in 0..get_u64(&mut buf)? {
        let stream_id = get_u64(&mut buf)?;
        let epoch = get_i64(&mut buf)?;
        let start_offset = get_u64(&mut buf)?;
        let end_offset = get_u64(&mut buf)?;
        let stream_state = match get_u8(&mut buf)? {
            0 => StreamState::Closed,
            1 => StreamState::Opened,
            other => return Err(SnapshotError::Corrupt(format!("stream state {other}"))),
        };
        let node_id = get_i32(&mut buf)?;
        let row = StreamRow {
            stream_id,
            epoch,
            start_offset,
            end_offset,
            state: stream_state,
            node_id,
        };
        state.streams.insert(stream_id, row);
        if stream_state == StreamState::Opened {
            state.opening_by_node.insert((node_id, stream_id), ());
        } else if stream_state == StreamState::Closed && epoch == -1 && node_id != -1 {
            state.placed_by_node.insert((node_id, stream_id), ());
        }
    }

    for _ in 0..get_u64(&mut buf)? {
        let node_id = get_i32(&mut buf)?;
        let epoch = get_i64(&mut buf)?;
        let http_address = get_str(&mut buf)?;
        let slots = get_u32(&mut buf)?;
        let protocol_addresses = get_str_map(&mut buf)?;
        state.nodes.insert(
            node_id,
            NodeRow {
                node_id,
                epoch,
                http_address,
                slots,
                protocol_addresses,
            },
        );
    }
    state.next_producer_id = get_u64(&mut buf)?;

    state.next_object_id = get_u64(&mut buf)?;
    for _ in 0..get_u64(&mut buf)? {
        let id = get_u64(&mut buf)?;
        let deadline = get_i64(&mut buf)?;
        state.prepared.insert(id, deadline);
        state.prepared_by_deadline.insert((deadline, id), ());
    }

    for _ in 0..get_u64(&mut buf)? {
        let node_id = get_i32(&mut buf)?;
        let object = get_object(&mut buf)?;
        for range in &object.offset_ranges {
            state.sso_ranges.insert(
                (range.stream_id, range.start_offset, object.object_id),
                range.end_offset,
            );
        }
        state.sso_by_node.insert((node_id, object.object_id), ());
        state
            .stream_set_objects
            .insert(object.object_id, StreamSetObjectRow { node_id, object });
    }

    for _ in 0..get_u64(&mut buf)? {
        let object = get_object(&mut buf)?;
        let [range] = object.offset_ranges.as_slice() else {
            return Err(SnapshotError::Corrupt(
                "stream object must have exactly one range".into(),
            ));
        };
        let key = (range.stream_id, range.start_offset, object.object_id);
        state.stream_object_ids.insert(object.object_id, key);
        state.stream_objects.insert(key, StreamObjectRow { object });
    }

    for _ in 0..get_u64(&mut buf)? {
        let seq = get_u64(&mut buf)?;
        let object_id = get_u64(&mut buf)?;
        let op = match get_u8(&mut buf)? {
            0 => CompactOperations::Delete,
            1 => CompactOperations::KeepData,
            2 => CompactOperations::DeepDelete,
            other => return Err(SnapshotError::Corrupt(format!("compact operation {other}"))),
        };
        state.mark_destroyed.insert(seq, (object_id, op));
        state.destroyed_by_id.insert(object_id, seq);
    }
    state.next_destroyed_seq = get_u64(&mut buf)?;

    for _ in 0..get_u64(&mut buf)? {
        let key = get_str(&mut buf)?;
        let len = get_u32(&mut buf)? as usize;
        if buf.remaining() < len {
            return Err(SnapshotError::Corrupt("kv value truncated".into()));
        }
        state.kv_bytes += (key.len() + len) as u64;
        state.kv.insert(key, Bytes::copy_from_slice(&buf[..len]));
        buf.advance(len);
    }

    for _ in 0..get_u64(&mut buf)? {
        let stream_id = get_u64(&mut buf)?;
        let from_node = get_i32(&mut buf)?;
        let to_node = get_i32(&mut buf)?;
        state
            .pending_transfers
            .insert(stream_id, PendingTransfer { from_node, to_node });
    }

    if buf.has_remaining() {
        return Err(SnapshotError::Corrupt(format!(
            "{} trailing bytes",
            buf.remaining()
        )));
    }
    Ok(state)
}

fn put_object(buf: &mut BytesMut, object: &S3ObjectMetadata) {
    buf.put_u64_le(object.object_id);
    buf.put_u8(match object.object_type {
        S3ObjectType::StreamSet => 0,
        S3ObjectType::Stream => 1,
    });
    buf.put_u64_le(object.object_size);
    buf.put_u32_le(object.attributes.0);
    buf.put_i64_le(object.committed_timestamp_ms);
    buf.put_i64_le(object.data_timestamp_ms);
    buf.put_u32_le(object.offset_ranges.len() as u32);
    for range in &object.offset_ranges {
        buf.put_u64_le(range.stream_id);
        buf.put_u64_le(range.start_offset);
        buf.put_u64_le(range.end_offset);
    }
}

fn get_object(buf: &mut &[u8]) -> Result<S3ObjectMetadata, SnapshotError> {
    let object_id = get_u64(buf)?;
    let object_type = match get_u8(buf)? {
        0 => S3ObjectType::StreamSet,
        1 => S3ObjectType::Stream,
        other => return Err(SnapshotError::Corrupt(format!("object type {other}"))),
    };
    let object_size = get_u64(buf)?;
    let attributes = ObjectAttributes(get_u32(buf)?);
    let committed_timestamp_ms = get_i64(buf)?;
    let data_timestamp_ms = get_i64(buf)?;
    let range_count = get_u32(buf)?;
    let mut offset_ranges = Vec::with_capacity(range_count as usize);
    for _ in 0..range_count {
        offset_ranges.push(StreamOffsetRange {
            stream_id: get_u64(buf)?,
            start_offset: get_u64(buf)?,
            end_offset: get_u64(buf)?,
        });
    }
    Ok(S3ObjectMetadata {
        object_id,
        object_type,
        offset_ranges,
        object_size,
        attributes,
        committed_timestamp_ms,
        data_timestamp_ms,
    })
}

fn put_str(buf: &mut BytesMut, s: &str) {
    buf.put_u32_le(s.len() as u32);
    buf.put_slice(s.as_bytes());
}

fn put_str_map(buf: &mut BytesMut, map: &std::collections::BTreeMap<String, String>) {
    buf.put_u32_le(map.len() as u32);
    for (key, value) in map {
        put_str(buf, key);
        put_str(buf, value);
    }
}

fn get_str_map(
    buf: &mut &[u8],
) -> Result<std::collections::BTreeMap<String, String>, SnapshotError> {
    let len = get_u32(buf)? as usize;
    let mut map = std::collections::BTreeMap::new();
    for _ in 0..len {
        let key = get_str(buf)?;
        let value = get_str(buf)?;
        map.insert(key, value);
    }
    Ok(map)
}

fn get_str(buf: &mut &[u8]) -> Result<String, SnapshotError> {
    let len = get_u32(buf)? as usize;
    if buf.remaining() < len {
        return Err(SnapshotError::Corrupt("string truncated".into()));
    }
    let s = std::str::from_utf8(&buf[..len])
        .map_err(|e| SnapshotError::Corrupt(format!("invalid utf-8: {e}")))?
        .to_owned();
    buf.advance(len);
    Ok(s)
}

macro_rules! checked_get {
    ($name:ident, $ty:ty, $get:ident, $size:expr) => {
        fn $name(buf: &mut &[u8]) -> Result<$ty, SnapshotError> {
            if buf.remaining() < $size {
                return Err(SnapshotError::Corrupt(
                    concat!("truncated reading ", stringify!($ty)).into(),
                ));
            }
            Ok(buf.$get())
        }
    };
}

checked_get!(get_u8, u8, get_u8, 1);
checked_get!(get_u32, u32, get_u32_le, 4);
checked_get!(get_i32, i32, get_i32_le, 4);
checked_get!(get_u64, u64, get_u64_le, 8);
checked_get!(get_i64, i64, get_i64_le, 8);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply::apply;
    use crate::command::MetadataCommand;
    use s3stream::{CommitStreamSetObjectRequest, CompactStreamObjectRequest, ObjectStreamRange};

    /// A state exercising every section: streams (opened + closed), nodes with
    /// and without addresses, prepared leases, both object kinds, a sparse
    /// destroyed FIFO (after a clean), and KV entries.
    fn rich_state() -> MetadataState {
        let mut state = MetadataState::new();
        for (node_id, node_epoch, addr, kafka) in [
            (1, 10i64, "http://n1:9090", Some("n1:9092")),
            (2, 20, "", None),
        ] {
            apply(
                &mut state,
                &MetadataCommand::RegisterNode {
                    node_id,
                    node_epoch,
                    http_address: addr.into(),
                    slots: 1,
                    protocol_addresses: kafka
                        .map(|address| {
                            std::collections::BTreeMap::from([(
                                "kafka".to_owned(),
                                address.to_owned(),
                            )])
                        })
                        .unwrap_or_default(),
                },
            )
            .unwrap();
        }
        apply(
            &mut state,
            &MetadataCommand::AllocateProducerIds {
                node_id: 1,
                node_epoch: 10,
                count: 5,
            },
        )
        .unwrap();
        for _ in 0..3 {
            apply(
                &mut state,
                &MetadataCommand::CreateStream {
                    node_id: 1,
                    node_epoch: 10,
                },
            )
            .unwrap();
        }
        apply(
            &mut state,
            &MetadataCommand::OpenStream {
                node_id: 1,
                node_epoch: 10,
                stream_id: 0,
                epoch: 1,
            },
        )
        .unwrap();
        apply(
            &mut state,
            &MetadataCommand::PrepareObject {
                node_id: 1,
                node_epoch: 10,
                count: 4,
                ttl_ms: 60_000,
                now_ms: 7,
            },
        )
        .unwrap();
        apply(
            &mut state,
            &MetadataCommand::CommitStreamSetObject {
                node_id: 1,
                node_epoch: 10,
                request: CommitStreamSetObjectRequest {
                    object_id: 0,
                    object_size: 64,
                    attributes: 5,
                    stream_ranges: vec![ObjectStreamRange {
                        stream_id: 0,
                        epoch: 1,
                        start_offset: 0,
                        end_offset: 8,
                        size: 64,
                    }],
                    stream_objects: vec![],
                    compacted_object_ids: vec![],
                },
                now_ms: 11,
            },
        )
        .unwrap();
        // Stream object + a destroyed FIFO with three entries...
        apply(
            &mut state,
            &MetadataCommand::CompactStreamObject {
                node_id: 1,
                node_epoch: 10,
                request: CompactStreamObjectRequest {
                    object_id: 1,
                    object_size: 32,
                    stream_id: 0,
                    stream_epoch: 1,
                    start_offset: 0,
                    end_offset: 4,
                    source_object_ids: vec![100, 101, 102],
                    operations: vec![
                        CompactOperations::Delete,
                        CompactOperations::KeepData,
                        CompactOperations::DeepDelete,
                    ],
                    attributes: 0,
                },
                now_ms: 13,
            },
        )
        .unwrap();
        // ...made sparse by a clean (seq 0 removed): verbatim-seq encoding must
        // round-trip this exactly.
        apply(
            &mut state,
            &MetadataCommand::CleanDestroyedObjects {
                object_ids: vec![100],
            },
        )
        .unwrap();
        for (key, value) in [("meta/a", "one"), ("meta/b", "two")] {
            apply(
                &mut state,
                &MetadataCommand::PutKv {
                    key: key.into(),
                    value: Bytes::from(value),
                },
            )
            .unwrap();
        }
        apply(
            &mut state,
            &MetadataCommand::TransferStream {
                stream_id: 0,
                from_node: 1,
                to_node: 2,
            },
        )
        .unwrap();
        state
    }

    #[test]
    fn roundtrip_is_identity() {
        let state = rich_state();
        let encoded = encode(&state);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, state, "decode(encode(s)) == s, indexes included");
        assert_eq!(encode(&decoded), encoded, "re-encode is byte-identical");
    }

    #[test]
    fn empty_state_roundtrips() {
        let state = MetadataState::new();
        assert_eq!(decode(&encode(&state)).unwrap(), state);
    }

    #[test]
    fn encoding_is_deterministic() {
        assert_eq!(encode(&rich_state()), encode(&rich_state()));
    }

    #[test]
    fn rejects_unknown_version() {
        let mut bytes = encode(&rich_state()).to_vec();
        bytes[0] = SNAPSHOT_VERSION + 1;
        assert!(
            matches!(decode(&bytes), Err(SnapshotError::UnsupportedVersion(v)) if v == SNAPSHOT_VERSION + 1)
        );
    }

    #[test]
    fn rejects_truncation_and_trailing_bytes() {
        let bytes = encode(&rich_state());
        // Any strict prefix must fail (never panic, never half-decode).
        for len in 0..bytes.len() {
            assert!(
                decode(&bytes[..len]).is_err(),
                "prefix of {len} bytes must be rejected"
            );
        }
        let mut extended = bytes.to_vec();
        extended.push(0);
        assert!(matches!(decode(&extended), Err(SnapshotError::Corrupt(_))));
    }
}
