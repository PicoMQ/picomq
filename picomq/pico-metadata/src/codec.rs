//! Command wire codec: version prefix, type byte, then fields in declaration
//! order. All integers are little-endian. Strings and blobs are
//! u32-length-prefixed.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use s3stream::{
    CommitStreamSetObjectRequest, CompactOperations, CompactStreamObjectRequest, ObjectStreamRange,
    StreamMetadata, StreamObject, StreamState,
};

use crate::command::{MetadataCommand, MetadataResult};

/// Current command/result wire version. Bumped on any layout change. Decoders
/// Pre-release: no compatibility shims for older layouts. Bump only once
/// there are real deployments with logs to migrate.
pub const CODEC_VERSION: u8 = 0;

/// Decode failures (corrupt entry, unknown version/type).
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("unsupported codec version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown command type {0}")]
    UnknownCommand(u8),
    #[error("unknown result tag {0}")]
    UnknownResult(u8),
    #[error("corrupt encoding: {0}")]
    Corrupt(String),
}

/// Encode one command (version byte + body).
pub fn encode_command(command: &MetadataCommand) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(CODEC_VERSION);
    put_command_body(&mut buf, command);
    buf.freeze()
}

/// Decode one command. Rejects unknown versions, unknown types, truncation,
/// and trailing bytes.
pub fn decode_command(bytes: &[u8]) -> Result<MetadataCommand, CodecError> {
    let mut buf = bytes;
    check_version(&mut buf)?;
    let command = get_command_body(&mut buf)?;
    ensure_drained(buf)?;
    Ok(command)
}

/// Encode a command batch: one replicated-log entry. Deterministic, so
/// replicas replay the batch in order.
pub fn encode_batch(commands: &[MetadataCommand]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(CODEC_VERSION);
    buf.put_u32_le(commands.len() as u32);
    for command in commands {
        put_command_body(&mut buf, command);
    }
    buf.freeze()
}

/// Decode a command batch (a full log entry payload).
pub fn decode_batch(bytes: &[u8]) -> Result<Vec<MetadataCommand>, CodecError> {
    let mut buf = bytes;
    check_version(&mut buf)?;
    let count = get_u32(&mut buf)? as usize;
    let mut commands = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        commands.push(get_command_body(&mut buf)?);
    }
    ensure_drained(buf)?;
    Ok(commands)
}

/// Encode a result for the leader-forwarding RPC response.
///
/// Type tags: 0 = Unit, 1 = Id, 2 = Count, 3 = Stream, 5 = Value(Some),
/// 6 = Value(None). Tag 4 is reserved. The typed enum must round-trip.
pub fn encode_result(result: &MetadataResult) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(CODEC_VERSION);
    match result {
        MetadataResult::Unit => buf.put_u8(0),
        MetadataResult::Id(id) => {
            buf.put_u8(1);
            buf.put_u64_le(*id);
        }
        MetadataResult::Count(count) => {
            buf.put_u8(2);
            buf.put_u64_le(*count);
        }
        MetadataResult::Stream(metadata) => {
            buf.put_u8(3);
            buf.put_u64_le(metadata.stream_id);
            buf.put_u64_le(metadata.epoch);
            buf.put_u64_le(metadata.start_offset);
            buf.put_u64_le(metadata.end_offset);
            buf.put_u8(match metadata.state {
                StreamState::Closed => 0,
                StreamState::Opened => 1,
            });
            buf.put_i32_le(metadata.node_id);
        }
        MetadataResult::Value(Some(value)) => {
            buf.put_u8(5);
            buf.put_u32_le(value.len() as u32);
            buf.put_slice(value);
        }
        MetadataResult::Value(None) => buf.put_u8(6),
    }
    buf.freeze()
}

/// Decode a result.
pub fn decode_result(bytes: &[u8]) -> Result<MetadataResult, CodecError> {
    let mut buf = bytes;
    check_version(&mut buf)?;
    let tag = get_u8(&mut buf)?;
    let result = match tag {
        0 => MetadataResult::Unit,
        1 => MetadataResult::Id(get_u64(&mut buf)?),
        2 => MetadataResult::Count(get_u64(&mut buf)?),
        3 => {
            let stream_id = get_u64(&mut buf)?;
            let epoch = get_u64(&mut buf)?;
            let start_offset = get_u64(&mut buf)?;
            let end_offset = get_u64(&mut buf)?;
            let state = match get_u8(&mut buf)? {
                0 => StreamState::Closed,
                1 => StreamState::Opened,
                other => return Err(CodecError::Corrupt(format!("stream state {other}"))),
            };
            let node_id = get_i32(&mut buf)?;
            MetadataResult::Stream(StreamMetadata {
                stream_id,
                epoch,
                start_offset,
                end_offset,
                state,
                node_id,
            })
        }
        5 => {
            let len = get_u32(&mut buf)? as usize;
            if buf.remaining() < len {
                return Err(CodecError::Corrupt("value truncated".into()));
            }
            let value = Bytes::copy_from_slice(&buf[..len]);
            buf.advance(len);
            MetadataResult::Value(Some(value))
        }
        6 => MetadataResult::Value(None),
        other => return Err(CodecError::UnknownResult(other)),
    };
    ensure_drained(buf)?;
    Ok(result)
}

// ---------------------------------------------------------------------------
// Command bodies (type byte + fields, no version prefix, shared by the single
// and batch encodings).
// ---------------------------------------------------------------------------

fn put_command_body(buf: &mut BytesMut, command: &MetadataCommand) {
    buf.put_u8(command.type_code());
    match command {
        MetadataCommand::RegisterNode {
            node_id,
            node_epoch,
            http_address,
            slots,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            put_str(buf, http_address);
            buf.put_u32_le(*slots);
        }
        MetadataCommand::CreateStream {
            node_id,
            node_epoch,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
        }
        MetadataCommand::OpenStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        }
        | MetadataCommand::CloseStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        }
        | MetadataCommand::DeleteStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_u64_le(*stream_id);
            buf.put_i64_le(*epoch);
        }
        MetadataCommand::TrimStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
            new_start_offset,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_u64_le(*stream_id);
            buf.put_i64_le(*epoch);
            buf.put_u64_le(*new_start_offset);
        }
        MetadataCommand::PrepareObject {
            node_id,
            node_epoch,
            count,
            ttl_ms,
            now_ms,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_u32_le(*count);
            buf.put_i64_le(*ttl_ms);
            buf.put_i64_le(*now_ms);
        }
        MetadataCommand::CommitStreamSetObject {
            node_id,
            node_epoch,
            request,
            now_ms,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_i64_le(*now_ms);
            put_commit_request(buf, request);
        }
        MetadataCommand::CompactStreamObject {
            node_id,
            node_epoch,
            request,
            now_ms,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_i64_le(*now_ms);
            put_compact_request(buf, request);
        }
        MetadataCommand::ExpirePreparedObjects { now_ms } => {
            buf.put_i64_le(*now_ms);
        }
        MetadataCommand::CleanDestroyedObjects { object_ids } => {
            buf.put_u32_le(object_ids.len() as u32);
            for id in object_ids {
                buf.put_u64_le(*id);
            }
        }
        MetadataCommand::PutKv { key, value } | MetadataCommand::PutKvIfAbsent { key, value } => {
            put_str(buf, key);
            buf.put_u32_le(value.len() as u32);
            buf.put_slice(value);
        }
        MetadataCommand::DeleteKv { key } => {
            put_str(buf, key);
        }
        MetadataCommand::DeleteKvIfMatches { key, expected } => {
            put_str(buf, key);
            buf.put_u32_le(expected.len() as u32);
            buf.put_slice(expected);
        }
        MetadataCommand::TransferStream {
            stream_id,
            from_node,
            to_node,
        } => {
            buf.put_u64_le(*stream_id);
            buf.put_i32_le(*from_node);
            buf.put_i32_le(*to_node);
        }
        MetadataCommand::CompleteTransfer { stream_id, epoch } => {
            buf.put_u64_le(*stream_id);
            buf.put_i64_le(*epoch);
        }
        MetadataCommand::CreateStreams {
            node_id,
            node_epoch,
            count,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_u32_le(*count);
        }
        MetadataCommand::PlaceStream { stream_id } => {
            buf.put_u64_le(*stream_id);
        }
    }
}

fn get_command_body(buf: &mut &[u8]) -> Result<MetadataCommand, CodecError> {
    let type_code = get_u8(buf)?;
    Ok(match type_code {
        1 => MetadataCommand::CreateStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
        },
        2 => MetadataCommand::OpenStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
        },
        3 => MetadataCommand::TrimStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
            new_start_offset: get_u64(buf)?,
        },
        4 => MetadataCommand::CloseStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
        },
        5 => MetadataCommand::DeleteStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
        },
        6 => MetadataCommand::PrepareObject {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            count: get_u32(buf)?,
            ttl_ms: get_i64(buf)?,
            now_ms: get_i64(buf)?,
        },
        7 => {
            let node_id = get_i32(buf)?;
            let node_epoch = get_i64(buf)?;
            let now_ms = get_i64(buf)?;
            let request = get_commit_request(buf)?;
            MetadataCommand::CommitStreamSetObject {
                node_id,
                node_epoch,
                request,
                now_ms,
            }
        }
        8 => {
            let node_id = get_i32(buf)?;
            let node_epoch = get_i64(buf)?;
            let now_ms = get_i64(buf)?;
            let request = get_compact_request(buf)?;
            MetadataCommand::CompactStreamObject {
                node_id,
                node_epoch,
                request,
                now_ms,
            }
        }
        9 => MetadataCommand::ExpirePreparedObjects {
            now_ms: get_i64(buf)?,
        },
        10 => {
            let node_id = get_i32(buf)?;
            let node_epoch = get_i64(buf)?;
            let http_address = get_str(buf)?;
            let slots = get_u32(buf)?;
            MetadataCommand::RegisterNode {
                node_id,
                node_epoch,
                http_address,
                slots,
            }
        }
        11 => {
            let count = get_u32(buf)? as usize;
            let mut object_ids = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                object_ids.push(get_u64(buf)?);
            }
            MetadataCommand::CleanDestroyedObjects { object_ids }
        }
        12 => MetadataCommand::PutKv {
            key: get_str(buf)?,
            value: get_blob(buf)?,
        },
        13 => MetadataCommand::PutKvIfAbsent {
            key: get_str(buf)?,
            value: get_blob(buf)?,
        },
        14 => MetadataCommand::DeleteKv { key: get_str(buf)? },
        15 => MetadataCommand::TransferStream {
            stream_id: get_u64(buf)?,
            from_node: get_i32(buf)?,
            to_node: get_i32(buf)?,
        },
        16 => MetadataCommand::CompleteTransfer {
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
        },
        17 => MetadataCommand::CreateStreams {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            count: get_u32(buf)?,
        },
        18 => MetadataCommand::PlaceStream {
            stream_id: get_u64(buf)?,
        },
        19 => MetadataCommand::DeleteKvIfMatches {
            key: get_str(buf)?,
            expected: get_blob(buf)?,
        },
        other => return Err(CodecError::UnknownCommand(other)),
    })
}

/// Commit request wire layout. There is no `order_id` field.
fn put_commit_request(buf: &mut BytesMut, request: &CommitStreamSetObjectRequest) {
    buf.put_u64_le(request.object_id);
    buf.put_u64_le(request.object_size);
    buf.put_u32_le(request.attributes);
    buf.put_u32_le(request.stream_ranges.len() as u32);
    for range in &request.stream_ranges {
        buf.put_u64_le(range.stream_id);
        buf.put_u64_le(range.epoch);
        buf.put_u64_le(range.start_offset);
        buf.put_u64_le(range.end_offset);
        buf.put_u64_le(range.size);
    }
    buf.put_u32_le(request.stream_objects.len() as u32);
    for object in &request.stream_objects {
        buf.put_u64_le(object.object_id);
        buf.put_u64_le(object.object_size);
        buf.put_u64_le(object.stream_id);
        buf.put_u64_le(object.start_offset);
        buf.put_u64_le(object.end_offset);
        buf.put_u32_le(object.attributes);
    }
    buf.put_u32_le(request.compacted_object_ids.len() as u32);
    for id in &request.compacted_object_ids {
        buf.put_u64_le(*id);
    }
}

fn get_commit_request(buf: &mut &[u8]) -> Result<CommitStreamSetObjectRequest, CodecError> {
    let object_id = get_u64(buf)?;
    let object_size = get_u64(buf)?;
    let attributes = get_u32(buf)?;
    let range_count = get_u32(buf)? as usize;
    let mut stream_ranges = Vec::with_capacity(range_count.min(4096));
    for _ in 0..range_count {
        stream_ranges.push(ObjectStreamRange {
            stream_id: get_u64(buf)?,
            epoch: get_u64(buf)?,
            start_offset: get_u64(buf)?,
            end_offset: get_u64(buf)?,
            size: get_u64(buf)?,
        });
    }
    let object_count = get_u32(buf)? as usize;
    let mut stream_objects = Vec::with_capacity(object_count.min(4096));
    for _ in 0..object_count {
        stream_objects.push(StreamObject {
            object_id: get_u64(buf)?,
            object_size: get_u64(buf)?,
            stream_id: get_u64(buf)?,
            start_offset: get_u64(buf)?,
            end_offset: get_u64(buf)?,
            attributes: get_u32(buf)?,
        });
    }
    let compacted_count = get_u32(buf)? as usize;
    let mut compacted_object_ids = Vec::with_capacity(compacted_count.min(4096));
    for _ in 0..compacted_count {
        compacted_object_ids.push(get_u64(buf)?);
    }
    Ok(CommitStreamSetObjectRequest {
        object_id,
        object_size,
        attributes,
        stream_ranges,
        stream_objects,
        compacted_object_ids,
    })
}

/// `CompactOperations` bytes
/// match the snapshot codec: Delete=0, KeepData=1, DeepDelete=2.
fn put_compact_request(buf: &mut BytesMut, request: &CompactStreamObjectRequest) {
    buf.put_u64_le(request.object_id);
    buf.put_u64_le(request.object_size);
    buf.put_u64_le(request.stream_id);
    buf.put_u64_le(request.stream_epoch);
    buf.put_u64_le(request.start_offset);
    buf.put_u64_le(request.end_offset);
    buf.put_u32_le(request.attributes);
    buf.put_u32_le(request.source_object_ids.len() as u32);
    for id in &request.source_object_ids {
        buf.put_u64_le(*id);
    }
    buf.put_u32_le(request.operations.len() as u32);
    for op in &request.operations {
        buf.put_u8(*op as u8);
    }
}

fn get_compact_request(buf: &mut &[u8]) -> Result<CompactStreamObjectRequest, CodecError> {
    let object_id = get_u64(buf)?;
    let object_size = get_u64(buf)?;
    let stream_id = get_u64(buf)?;
    let stream_epoch = get_u64(buf)?;
    let start_offset = get_u64(buf)?;
    let end_offset = get_u64(buf)?;
    let attributes = get_u32(buf)?;
    let source_count = get_u32(buf)? as usize;
    let mut source_object_ids = Vec::with_capacity(source_count.min(4096));
    for _ in 0..source_count {
        source_object_ids.push(get_u64(buf)?);
    }
    let op_count = get_u32(buf)? as usize;
    let mut operations = Vec::with_capacity(op_count.min(4096));
    for _ in 0..op_count {
        operations.push(match get_u8(buf)? {
            0 => CompactOperations::Delete,
            1 => CompactOperations::KeepData,
            2 => CompactOperations::DeepDelete,
            other => return Err(CodecError::Corrupt(format!("compact operation {other}"))),
        });
    }
    Ok(CompactStreamObjectRequest {
        object_id,
        object_size,
        stream_id,
        stream_epoch,
        start_offset,
        end_offset,
        source_object_ids,
        operations,
        attributes,
    })
}

// ---------------------------------------------------------------------------
// Primitive helpers (same posture as the snapshot codec: every read is
// bounds-checked. Truncation is an error, never a panic).
// ---------------------------------------------------------------------------

fn check_version(buf: &mut &[u8]) -> Result<(), CodecError> {
    let version = get_u8(buf)?;
    if version != CODEC_VERSION {
        return Err(CodecError::UnsupportedVersion(version));
    }
    Ok(())
}

fn ensure_drained(buf: &[u8]) -> Result<(), CodecError> {
    if buf.has_remaining() {
        return Err(CodecError::Corrupt(format!(
            "{} trailing bytes",
            buf.remaining()
        )));
    }
    Ok(())
}

fn put_str(buf: &mut BytesMut, s: &str) {
    buf.put_u32_le(s.len() as u32);
    buf.put_slice(s.as_bytes());
}

fn get_str(buf: &mut &[u8]) -> Result<String, CodecError> {
    let len = get_u32(buf)? as usize;
    if buf.remaining() < len {
        return Err(CodecError::Corrupt("string truncated".into()));
    }
    let s = std::str::from_utf8(&buf[..len])
        .map_err(|e| CodecError::Corrupt(format!("invalid utf-8: {e}")))?
        .to_owned();
    buf.advance(len);
    Ok(s)
}

fn get_blob(buf: &mut &[u8]) -> Result<Bytes, CodecError> {
    let len = get_u32(buf)? as usize;
    if buf.remaining() < len {
        return Err(CodecError::Corrupt("blob truncated".into()));
    }
    let value = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(value)
}

macro_rules! checked_get {
    ($name:ident, $ty:ty, $get:ident, $size:expr) => {
        fn $name(buf: &mut &[u8]) -> Result<$ty, CodecError> {
            if buf.remaining() < $size {
                return Err(CodecError::Corrupt(
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
    use proptest::prelude::*;

    fn all_commands() -> Vec<MetadataCommand> {
        vec![
            MetadataCommand::CreateStream {
                node_id: 7,
                node_epoch: 100,
            },
            MetadataCommand::OpenStream {
                node_id: 7,
                node_epoch: 100,
                stream_id: 3,
                epoch: 9,
            },
            MetadataCommand::TrimStream {
                node_id: 7,
                node_epoch: 100,
                stream_id: 3,
                epoch: 9,
                new_start_offset: 4,
            },
            MetadataCommand::CloseStream {
                node_id: 7,
                node_epoch: 100,
                stream_id: 3,
                epoch: 9,
            },
            MetadataCommand::DeleteStream {
                node_id: 7,
                node_epoch: 100,
                stream_id: 3,
                epoch: 9,
            },
            MetadataCommand::PrepareObject {
                node_id: 7,
                node_epoch: 100,
                count: 2,
                ttl_ms: 1000,
                now_ms: 50,
            },
            MetadataCommand::CommitStreamSetObject {
                node_id: 7,
                node_epoch: 100,
                request: CommitStreamSetObjectRequest {
                    object_id: 11,
                    object_size: 128,
                    attributes: 1,
                    stream_ranges: vec![ObjectStreamRange {
                        stream_id: 3,
                        epoch: 1,
                        start_offset: 0,
                        end_offset: 10,
                        size: 128,
                    }],
                    stream_objects: vec![StreamObject {
                        object_id: 12,
                        object_size: 64,
                        stream_id: 3,
                        start_offset: 10,
                        end_offset: 20,
                        attributes: 2,
                    }],
                    compacted_object_ids: vec![1, 2],
                },
                now_ms: 123,
            },
            MetadataCommand::CompactStreamObject {
                node_id: 7,
                node_epoch: 100,
                request: CompactStreamObjectRequest {
                    object_id: 20,
                    object_size: 64,
                    stream_id: 3,
                    stream_epoch: 9,
                    start_offset: 0,
                    end_offset: 20,
                    source_object_ids: vec![12, 13, 14],
                    operations: vec![
                        CompactOperations::Delete,
                        CompactOperations::KeepData,
                        CompactOperations::DeepDelete,
                    ],
                    attributes: 3,
                },
                now_ms: 456,
            },
            MetadataCommand::ExpirePreparedObjects { now_ms: 99 },
            MetadataCommand::RegisterNode {
                node_id: 7,
                node_epoch: 100,
                http_address: "http://127.0.0.1:8080".into(),
                slots: 4,
            },
            MetadataCommand::RegisterNode {
                node_id: 7,
                node_epoch: 100,
                http_address: "".into(),
                slots: 1,
            },
            MetadataCommand::PlaceStream { stream_id: 42 },
            MetadataCommand::TransferStream {
                stream_id: 3,
                from_node: 7,
                to_node: 8,
            },
            MetadataCommand::CompleteTransfer {
                stream_id: 3,
                epoch: 9,
            },
            MetadataCommand::CreateStreams {
                node_id: 7,
                node_epoch: 100,
                count: 16,
            },
            MetadataCommand::CleanDestroyedObjects {
                object_ids: vec![1, 2, 3],
            },
            MetadataCommand::PutKv {
                key: "path/a".into(),
                value: Bytes::from_static(&[1, 2, 3]),
            },
            MetadataCommand::PutKvIfAbsent {
                key: "path/b".into(),
                value: Bytes::from_static(&[4, 5]),
            },
            MetadataCommand::DeleteKv {
                key: "path/c".into(),
            },
            MetadataCommand::DeleteKvIfMatches {
                key: "path/d".into(),
                expected: Bytes::from_static(&[6, 7, 8]),
            },
        ]
    }

    fn all_results() -> Vec<MetadataResult> {
        vec![
            MetadataResult::Unit,
            MetadataResult::Id(42),
            MetadataResult::Count(7),
            MetadataResult::Stream(StreamMetadata {
                stream_id: 3,
                epoch: 9,
                start_offset: 0,
                end_offset: 20,
                state: StreamState::Opened,
                node_id: 7,
            }),
            MetadataResult::Value(Some(Bytes::from_static(&[1, 2, 3]))),
            MetadataResult::Value(None),
        ]
    }

    #[test]
    fn every_command_roundtrips() {
        for command in all_commands() {
            let encoded = encode_command(&command);
            let decoded = decode_command(&encoded).unwrap();
            assert_eq!(decoded, command);
            assert_eq!(encode_command(&decoded), encoded);
        }
    }

    #[test]
    fn every_result_roundtrips() {
        for result in all_results() {
            let encoded = encode_result(&result);
            let decoded = decode_result(&encoded).unwrap();
            assert_eq!(decoded, result);
            assert_eq!(encode_result(&decoded), encoded);
        }
    }

    #[test]
    fn batch_roundtrips_in_order() {
        let commands = all_commands();
        let encoded = encode_batch(&commands);
        assert_eq!(decode_batch(&encoded).unwrap(), commands);

        let empty = encode_batch(&[]);
        assert_eq!(decode_batch(&empty).unwrap(), Vec::<MetadataCommand>::new());
    }

    #[test]
    fn rejects_unknown_version_type_and_tag() {
        let mut bytes = encode_command(&all_commands()[0]).to_vec();
        bytes[0] = CODEC_VERSION + 1;
        assert!(matches!(
            decode_command(&bytes),
            Err(CodecError::UnsupportedVersion(v)) if v == CODEC_VERSION + 1
        ));

        // Unknown codes must be rejected, never misparsed.
        for type_code in [0u8, 20, 200] {
            let bytes = [CODEC_VERSION, type_code];
            assert!(matches!(
                decode_command(&bytes),
                Err(CodecError::UnknownCommand(t)) if t == type_code
            ));
        }

        let bytes = [CODEC_VERSION, 4u8];
        assert!(matches!(
            decode_result(&bytes),
            Err(CodecError::UnknownResult(4))
        ));
    }

    #[test]
    fn rejects_truncation_and_trailing_bytes() {
        for command in all_commands() {
            let bytes = encode_command(&command);
            let old_register_len =
                matches!(&command, MetadataCommand::RegisterNode { .. }).then(|| bytes.len() - 4);
            for len in 0..bytes.len() {
                if old_register_len == Some(len) {
                    continue;
                }
                assert!(
                    decode_command(&bytes[..len]).is_err(),
                    "prefix of {len} bytes must be rejected for {command:?}"
                );
            }
            let mut extended = bytes.to_vec();
            extended.push(0);
            assert!(matches!(
                decode_command(&extended),
                Err(CodecError::Corrupt(_))
            ));
        }
    }

    // Generators mirroring apply.rs's proptest strategy shapes, kept simple:
    // codec correctness must hold for arbitrary field values, not just the
    // fixture set above.
    fn arb_command() -> impl Strategy<Value = MetadataCommand> {
        prop_oneof![
            (any::<i32>(), any::<i64>()).prop_map(|(node_id, node_epoch)| {
                MetadataCommand::CreateStream {
                    node_id,
                    node_epoch,
                }
            }),
            (any::<i32>(), any::<i64>(), any::<u64>(), any::<i64>()).prop_map(
                |(node_id, node_epoch, stream_id, epoch)| MetadataCommand::OpenStream {
                    node_id,
                    node_epoch,
                    stream_id,
                    epoch
                }
            ),
            (
                any::<i32>(),
                any::<i64>(),
                any::<u64>(),
                any::<i64>(),
                any::<u64>()
            )
                .prop_map(
                    |(node_id, node_epoch, stream_id, epoch, new_start_offset)| {
                        MetadataCommand::TrimStream {
                            node_id,
                            node_epoch,
                            stream_id,
                            epoch,
                            new_start_offset,
                        }
                    }
                ),
            (any::<i32>(), any::<i64>(), "[a-z/]{0,32}", 1u32..8).prop_map(
                |(node_id, node_epoch, http_address, slots)| MetadataCommand::RegisterNode {
                    node_id,
                    node_epoch,
                    http_address,
                    slots,
                }
            ),
            any::<u64>().prop_map(|stream_id| MetadataCommand::PlaceStream { stream_id }),
            (any::<u64>(), any::<i32>(), any::<i32>()).prop_map(
                |(stream_id, from_node, to_node)| MetadataCommand::TransferStream {
                    stream_id,
                    from_node,
                    to_node,
                }
            ),
            (any::<u64>(), any::<i64>()).prop_map(|(stream_id, epoch)| {
                MetadataCommand::CompleteTransfer { stream_id, epoch }
            }),
            (any::<i32>(), any::<i64>(), any::<u32>()).prop_map(|(node_id, node_epoch, count)| {
                MetadataCommand::CreateStreams {
                    node_id,
                    node_epoch,
                    count,
                }
            }),
            proptest::collection::vec(any::<u64>(), 0..64)
                .prop_map(|object_ids| MetadataCommand::CleanDestroyedObjects { object_ids }),
            (
                "[a-z/]{0,32}",
                proptest::collection::vec(any::<u8>(), 0..64)
            )
                .prop_map(|(key, value)| MetadataCommand::PutKv {
                    key,
                    value: Bytes::from(value)
                }),
            (
                "[a-z/]{0,32}",
                proptest::collection::vec(any::<u8>(), 0..64)
            )
                .prop_map(|(key, expected)| MetadataCommand::DeleteKvIfMatches {
                    key,
                    expected: Bytes::from(expected)
                }),
        ]
    }

    proptest! {
        #[test]
        fn roundtrip_arbitrary_commands(command in arb_command()) {
            let encoded = encode_command(&command);
            prop_assert_eq!(&decode_command(&encoded).unwrap(), &command);
            prop_assert_eq!(encode_command(&decode_command(&encoded).unwrap()), encoded);
        }

        #[test]
        fn roundtrip_arbitrary_batches(
            commands in proptest::collection::vec(arb_command(), 0..16)
        ) {
            let encoded = encode_batch(&commands);
            prop_assert_eq!(decode_batch(&encoded).unwrap(), commands);
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = decode_command(&bytes);
            let _ = decode_batch(&bytes);
            let _ = decode_result(&bytes);
        }
    }
}
