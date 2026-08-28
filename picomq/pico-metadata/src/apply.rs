//! The state transition function: `apply(state, command) -> result`.
//!
//! Fencing, idempotency and cascade rules all live in this one pure function.
//! It is deterministic and synchronous. No clock (`now_ms` rides in the
//! command), no I/O, no locks. So the same state and command always produce
//! the same next state and result.

use s3stream::{
    CommitStreamSetObjectRequest, CompactOperations, CompactStreamObjectRequest, S3ObjectMetadata,
    S3ObjectType, StreamOffsetRange, StreamState, NOOP_OBJECT_ID,
};

use crate::command::{MetadataCommand, MetadataResult};
use crate::error::MetadataError;
use crate::state::{
    MetadataState, NodeRow, PendingTransfer, StreamObjectRow, StreamRow, StreamSetObjectRow,
};

/// Apply one command. On `Ok` the state advanced, on `Err` it is untouched.
///
/// (single-command path, batching is a
/// sink-level concern, not a state-machine concern).
pub fn apply(
    state: &mut MetadataState,
    command: &MetadataCommand,
) -> Result<MetadataResult, MetadataError> {
    let mut next = state.clone();
    let result = apply_inner(&mut next, command)?;
    *state = next;
    Ok(result)
}

fn apply_inner(
    state: &mut MetadataState,
    command: &MetadataCommand,
) -> Result<MetadataResult, MetadataError> {
    match command {
        MetadataCommand::RegisterNode {
            node_id,
            node_epoch,
            http_address,
            slots,
            protocol_addresses,
        } => register_node(
            state,
            *node_id,
            *node_epoch,
            http_address,
            *slots,
            protocol_addresses,
        ),
        MetadataCommand::PlaceStream { stream_id } => place_stream(state, *stream_id),
        MetadataCommand::CreateStream {
            node_id,
            node_epoch,
        } => create_stream(state, *node_id, *node_epoch),
        MetadataCommand::OpenStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        } => open_stream(state, *node_id, *node_epoch, *stream_id, *epoch),
        MetadataCommand::TrimStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
            new_start_offset,
        } => trim_stream(
            state,
            *node_id,
            *node_epoch,
            *stream_id,
            *epoch,
            *new_start_offset,
        ),
        MetadataCommand::CloseStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        } => close_stream(state, *node_id, *node_epoch, *stream_id, *epoch),
        MetadataCommand::DeleteStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        } => delete_stream(state, *node_id, *node_epoch, *stream_id, *epoch),
        MetadataCommand::PrepareObject {
            node_id,
            node_epoch,
            count,
            ttl_ms,
            now_ms,
        } => prepare_object(state, *node_id, *node_epoch, *count, *ttl_ms, *now_ms),
        MetadataCommand::CommitStreamSetObject {
            node_id,
            node_epoch,
            request,
            now_ms,
        } => commit_stream_set_object(state, *node_id, *node_epoch, request, *now_ms),
        MetadataCommand::CompactStreamObject {
            node_id,
            node_epoch,
            request,
            now_ms,
        } => compact_stream_object(state, *node_id, *node_epoch, request, *now_ms),
        MetadataCommand::ExpirePreparedObjects { now_ms } => {
            expire_prepared_objects(state, *now_ms)
        }
        MetadataCommand::CleanDestroyedObjects { object_ids } => {
            clean_destroyed_objects(state, object_ids)
        }
        MetadataCommand::PutKv { key, value } => put_kv(state, key, value),
        MetadataCommand::PutKvIfAbsent { key, value } => put_kv_if_absent(state, key, value),
        MetadataCommand::DeleteKv { key } => delete_kv(state, key),
        MetadataCommand::DeleteKvIfMatches { key, expected } => {
            delete_kv_if_matches(state, key, expected)
        }
        MetadataCommand::TransferStream {
            stream_id,
            from_node,
            to_node,
        } => transfer_stream(state, *stream_id, *from_node, *to_node),
        MetadataCommand::CompleteTransfer { stream_id, epoch } => {
            complete_transfer(state, *stream_id, *epoch)
        }
        MetadataCommand::CreateStreams {
            node_id,
            node_epoch,
            count,
        } => create_streams(state, *node_id, *node_epoch, *count),
        MetadataCommand::AllocateProducerIds {
            node_id,
            node_epoch,
            count,
        } => allocate_producer_ids(state, *node_id, *node_epoch, *count),
    }
}

fn register_node(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    http_address: &str,
    slots: u32,
    protocol_addresses: &std::collections::BTreeMap<String, String>,
) -> Result<MetadataResult, MetadataError> {
    if let Some(node) = state.nodes.get(&node_id) {
        if node.epoch > node_epoch {
            return Err(MetadataError::NodeEpochMismatch {
                node_id,
                message: format!(
                    "node {node_id} epoch {} fences register with epoch {node_epoch}",
                    node.epoch
                ),
            });
        }
    }
    let http_address = if http_address.is_empty() {
        state
            .nodes
            .get(&node_id)
            .map(|n| n.http_address.clone())
            .unwrap_or_default()
    } else {
        http_address.to_owned()
    };
    let protocol_addresses = if protocol_addresses.is_empty() {
        state
            .nodes
            .get(&node_id)
            .map(|n| n.protocol_addresses.clone())
            .unwrap_or_default()
    } else {
        protocol_addresses.clone()
    };
    state.nodes.insert(
        node_id,
        NodeRow {
            node_id,
            epoch: node_epoch,
            http_address,
            slots,
            protocol_addresses,
        },
    );
    Ok(MetadataResult::Unit)
}

fn node_epoch_check(
    state: &MetadataState,
    node_id: i32,
    node_epoch: i64,
) -> Result<(), MetadataError> {
    match state.nodes.get(&node_id) {
        None => Err(MetadataError::NodeEpochMismatch {
            node_id,
            message: format!("node {node_id} is not registered"),
        }),
        Some(node) if node.epoch != node_epoch => Err(MetadataError::NodeEpochMismatch {
            node_id,
            message: format!(
                "node {node_id} epoch mismatch current={} request={node_epoch}",
                node.epoch
            ),
        }),
        Some(_) => Ok(()),
    }
}

fn create_stream(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    let stream_id = state.next_stream_id;
    state.next_stream_id += 1;
    state.streams.insert(
        stream_id,
        StreamRow {
            stream_id,
            epoch: -1,
            start_offset: 0,
            end_offset: 0,
            state: StreamState::Closed,
            node_id: -1,
        },
    );
    Ok(MetadataResult::Id(stream_id))
}

/// Bounds one log row.
const MAX_CREATE_STREAMS: u32 = 4096;

/// Batched create. Ids are consecutive starting at the returned first id.
fn create_streams(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    count: u32,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    if count == 0 {
        return Err(MetadataError::Unexpected {
            message: "create count must be positive".into(),
        });
    }
    if count > MAX_CREATE_STREAMS {
        return Err(MetadataError::Unexpected {
            message: format!("create count {count} exceeds {MAX_CREATE_STREAMS}"),
        });
    }
    let first = state.next_stream_id;
    for stream_id in first..first + count as u64 {
        state.streams.insert(
            stream_id,
            StreamRow {
                stream_id,
                epoch: -1,
                start_offset: 0,
                end_offset: 0,
                state: StreamState::Closed,
                node_id: -1,
            },
        );
    }
    state.next_stream_id = first + count as u64;
    Ok(MetadataResult::Id(first))
}

/// Pick the registered node with the lowest `(opening + placed) / slots` score
/// (×1000 for integer arithmetic), tie-breaking on lowest `node_id`. A CLOSED
/// stream with `epoch == -1` and `node_id != -1` is already placed. OPENED
/// streams and CLOSED streams with `epoch != -1` (post-close) are idempotent
/// re-place attempts. Returns [`MetadataError::NodeEpochMismatch`] when no
/// registered node has `slots >= 1` (closest existing "no node" variant).
fn place_stream(
    state: &mut MetadataState,
    stream_id: u64,
) -> Result<MetadataResult, MetadataError> {
    let stream = require_stream(state, stream_id)?;

    if stream.node_id != -1 {
        return Ok(MetadataResult::Id(stream.node_id as u64));
    }
    if stream.state != StreamState::Closed || stream.epoch != -1 {
        return Err(MetadataError::Unexpected {
            message: format!(
                "stream {stream_id} cannot be placed in state {:?} epoch {}",
                stream.state, stream.epoch
            ),
        });
    }

    let mut winner: Option<(i32, u64)> = None;
    for (node_id, node) in state.nodes.iter() {
        if node.slots < 1 {
            continue;
        }
        let opening_count = state
            .opening_by_node
            .range((*node_id, 0)..=(*node_id, u64::MAX))
            .count() as u64;
        let placed_count = state
            .placed_by_node
            .range((*node_id, 0)..=(*node_id, u64::MAX))
            .count() as u64;
        let score = (opening_count + placed_count) * 1000 / node.slots as u64;
        match winner {
            None => winner = Some((*node_id, score)),
            Some((best_id, best_score))
                if score < best_score || (score == best_score && *node_id < best_id) =>
            {
                winner = Some((*node_id, score));
            }
            _ => {}
        }
    }
    let Some((winner, _)) = winner else {
        return Err(MetadataError::NodeEpochMismatch {
            node_id: -1,
            message: "no registered nodes with slots >= 1 for stream placement".into(),
        });
    };

    state.streams.insert(
        stream_id,
        StreamRow {
            node_id: winner,
            ..stream
        },
    );
    state.placed_by_node.insert((winner, stream_id), ());
    Ok(MetadataResult::Id(winner as u64))
}

fn require_stream(state: &MetadataState, stream_id: u64) -> Result<StreamRow, MetadataError> {
    state
        .streams
        .get(&stream_id)
        .copied()
        .ok_or(MetadataError::StreamNotExist { stream_id })
}

fn open_stream(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    stream_id: u64,
    epoch: i64,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    let stream = require_stream(state, stream_id)?;

    if stream.epoch > epoch {
        return Err(MetadataError::StreamFenced {
            stream_id,
            epoch,
            message: format!(
                "stream {stream_id} epoch {} fences request epoch {epoch}",
                stream.epoch
            ),
        });
    }
    if stream.epoch == epoch {
        if stream.state == StreamState::Opened && stream.node_id == node_id {
            return Ok(MetadataResult::Stream(stream.to_stream_metadata()));
        }
        return Err(MetadataError::StreamFenced {
            stream_id,
            epoch,
            message: format!("stream {stream_id} epoch {epoch} already used"),
        });
    }
    if stream.state == StreamState::Opened {
        return Err(MetadataError::StreamNotClosed { stream_id });
    }

    let opened = StreamRow {
        epoch,
        state: StreamState::Opened,
        node_id,
        ..stream
    };
    state.streams.insert(stream_id, opened);
    state.placed_by_node.remove(&(stream.node_id, stream_id));
    state.opening_by_node.insert((node_id, stream_id), ());
    Ok(MetadataResult::Stream(opened.to_stream_metadata()))
}

fn require_opened_stream(
    state: &MetadataState,
    stream_id: u64,
    epoch: i64,
) -> Result<StreamRow, MetadataError> {
    let stream = require_stream(state, stream_id)?;
    if stream.state != StreamState::Opened {
        return Err(MetadataError::Unexpected {
            message: format!("stream {stream_id} is not opened"),
        });
    }
    if stream.epoch != epoch {
        return Err(MetadataError::ExpiredEpoch {
            stream_id,
            epoch,
            message: format!(
                "stream {stream_id} epoch {epoch} is not equal to current epoch {}",
                stream.epoch
            ),
        });
    }
    Ok(stream)
}

fn trim_stream(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    stream_id: u64,
    epoch: i64,
    new_start_offset: u64,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    let stream = require_opened_stream(state, stream_id, epoch)?;
    if new_start_offset < stream.start_offset {
        return Err(MetadataError::Unexpected {
            message: format!(
                "stream {stream_id} new start offset {new_start_offset} is less than current start offset {}",
                stream.start_offset
            ),
        });
    }
    if new_start_offset > stream.end_offset {
        return Err(MetadataError::Unexpected {
            message: format!(
                "stream {stream_id} new start offset {new_start_offset} is greater than current end offset {}",
                stream.end_offset
            ),
        });
    }
    state.streams.insert(
        stream_id,
        StreamRow {
            start_offset: new_start_offset,
            ..stream
        },
    );
    Ok(MetadataResult::Unit)
}

/// CLOSED at the same epoch is
/// idempotent success. Otherwise requires OPENED + exact epoch (maintains
/// `opening_by_node`).
fn close_stream(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    stream_id: u64,
    epoch: i64,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    let stream = require_stream(state, stream_id)?;

    if stream.state == StreamState::Closed && stream.epoch == epoch {
        return Ok(MetadataResult::Unit);
    }
    if stream.state != StreamState::Opened {
        return Err(MetadataError::Unexpected {
            message: format!("stream {stream_id} is not opened"),
        });
    }
    if stream.epoch != epoch {
        return Err(MetadataError::ExpiredEpoch {
            stream_id,
            epoch,
            message: format!(
                "stream {stream_id} epoch {epoch} is not equal to current epoch {}",
                stream.epoch
            ),
        });
    }

    state.streams.insert(
        stream_id,
        StreamRow {
            state: StreamState::Closed,
            ..stream
        },
    );
    state.opening_by_node.remove(&(stream.node_id, stream_id));
    Ok(MetadataResult::Unit)
}

/// Record a pending ownership move. The stream must be OPENED on `from_node`
/// and the target must be a registered node with capacity. Re-requesting the
/// same move is idempotent.
fn transfer_stream(
    state: &mut MetadataState,
    stream_id: u64,
    from_node: i32,
    to_node: i32,
) -> Result<MetadataResult, MetadataError> {
    let stream = require_stream(state, stream_id)?;

    if let Some(pending) = state.pending_transfers.get(&stream_id) {
        if pending.from_node == from_node && pending.to_node == to_node {
            return Ok(MetadataResult::Unit);
        }
        return Err(MetadataError::Unexpected {
            message: format!(
                "stream {stream_id} already transferring from {} to {}",
                pending.from_node, pending.to_node
            ),
        });
    }
    if from_node == to_node {
        return Err(MetadataError::Unexpected {
            message: format!("stream {stream_id} transfer target equals source {from_node}"),
        });
    }
    match state.nodes.get(&to_node) {
        Some(node) if node.slots >= 1 => {}
        _ => {
            return Err(MetadataError::NodeEpochMismatch {
                node_id: to_node,
                message: format!("transfer target {to_node} is not a registered node with slots"),
            });
        }
    }
    if stream.state != StreamState::Opened || stream.node_id != from_node {
        return Err(MetadataError::Unexpected {
            message: format!(
                "stream {stream_id} is not opened on node {from_node}, state {:?} node {}",
                stream.state, stream.node_id
            ),
        });
    }

    state
        .pending_transfers
        .insert(stream_id, PendingTransfer { from_node, to_node });
    Ok(MetadataResult::Unit)
}

/// Finish a pending move once the source closed the stream at `epoch`.
/// Re-points the row at the target so routing lands there before the next
/// open. A missing pending entry is a redundant completion.
fn complete_transfer(
    state: &mut MetadataState,
    stream_id: u64,
    epoch: i64,
) -> Result<MetadataResult, MetadataError> {
    let Some(pending) = state.pending_transfers.get(&stream_id).copied() else {
        return Err(MetadataError::Redundant {
            message: format!("no pending transfer for stream {stream_id}"),
        });
    };
    let stream = require_stream(state, stream_id)?;
    if stream.state != StreamState::Closed {
        return Err(MetadataError::StreamNotClosed { stream_id });
    }
    if stream.epoch != epoch {
        return Err(MetadataError::ExpiredEpoch {
            stream_id,
            epoch,
            message: format!(
                "stream {stream_id} epoch {epoch} is not equal to current epoch {}",
                stream.epoch
            ),
        });
    }

    state.streams.insert(
        stream_id,
        StreamRow {
            node_id: pending.to_node,
            ..stream
        },
    );
    state.pending_transfers.remove(&stream_id);
    Ok(MetadataResult::Unit)
}

/// Delete a stream. A missing stream is idempotent success. The stream must
/// be CLOSED at the exact epoch. Every stream object of the stream is dropped
/// from the indexes and marked destroyed (`Delete`), all in the same atomic
/// apply.
fn delete_stream(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    stream_id: u64,
    epoch: i64,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    let Some(stream) = state.streams.get(&stream_id).copied() else {
        return Ok(MetadataResult::Unit); // idempotent
    };
    if stream.state != StreamState::Closed {
        return Err(MetadataError::StreamNotClosed { stream_id });
    }
    if stream.epoch != epoch {
        return Err(MetadataError::ExpiredEpoch {
            stream_id,
            epoch,
            message: format!(
                "stream {stream_id} epoch {epoch} is not equal to current epoch {}",
                stream.epoch
            ),
        });
    }

    state.streams.remove(&stream_id);
    remove_placed_stream(state, stream_id);
    state.pending_transfers.remove(&stream_id);

    let keys: Vec<_> = state
        .stream_objects
        .range((stream_id, 0, 0)..=(stream_id, u64::MAX, u64::MAX))
        .map(|(key, _)| *key)
        .collect();
    for key in keys {
        state.stream_objects.remove(&key);
        state.stream_object_ids.remove(&key.2);
        mark_destroyed(state, key.2, CompactOperations::Delete);
    }
    Ok(MetadataResult::Unit)
}

fn remove_placed_stream(state: &mut MetadataState, stream_id: u64) {
    let keys: Vec<_> = state
        .placed_by_node
        .keys()
        .filter(|(_, sid)| *sid == stream_id)
        .copied()
        .collect();
    for key in keys {
        state.placed_by_node.remove(&key);
    }
}

/// Bounds one log row.
const MAX_ALLOCATE_PRODUCER_IDS: u32 = 4096;

/// Lease `count` consecutive producer ids. Ids are consecutive starting at
/// the returned first id and are never reclaimed.
fn allocate_producer_ids(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    count: u32,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    if count == 0 {
        return Err(MetadataError::Unexpected {
            message: "producer id count must be positive".into(),
        });
    }
    if count > MAX_ALLOCATE_PRODUCER_IDS {
        return Err(MetadataError::Unexpected {
            message: format!("producer id count {count} exceeds {MAX_ALLOCATE_PRODUCER_IDS}"),
        });
    }
    let first = state.next_producer_id;
    state.next_producer_id += count as u64;
    Ok(MetadataResult::Id(first))
}

fn prepare_object(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    count: u32,
    ttl_ms: i64,
    now_ms: i64,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    if count == 0 {
        return Err(MetadataError::Unexpected {
            message: "prepare count must be positive".into(),
        });
    }
    let first = state.next_object_id;
    state.next_object_id += count as u64;
    let deadline = now_ms + ttl_ms.max(0);
    for id in first..first + count as u64 {
        state.prepared.insert(id, deadline);
        state.prepared_by_deadline.insert((deadline, id), ());
    }
    Ok(MetadataResult::Id(first))
}

fn commit_prepared(state: &mut MetadataState, object_id: u64) {
    if let Some(deadline) = state.prepared.remove(&object_id) {
        state.prepared_by_deadline.remove(&(deadline, object_id));
    }
}

fn advance_end_offset(
    state: &mut MetadataState,
    stream_id: u64,
    start_offset: u64,
    end_offset: u64,
    compact: bool,
) -> Result<(), MetadataError> {
    let stream = require_stream(state, stream_id)?;
    if compact {
        if stream.end_offset < end_offset {
            return Err(MetadataError::Unexpected {
                message: format!(
                    "stream {stream_id} end offset {} is lesser than request {end_offset}",
                    stream.end_offset
                ),
            });
        }
        if stream.start_offset > start_offset {
            return Err(MetadataError::Unexpected {
                message: format!(
                    "stream {stream_id} start offset {} is greater than request {start_offset}",
                    stream.start_offset
                ),
            });
        }
        return Ok(());
    }
    if stream.end_offset != start_offset {
        return Err(MetadataError::Unexpected {
            message: format!(
                "stream {stream_id} end offset {} is not equal to start offset of request {start_offset}",
                stream.end_offset
            ),
        });
    }
    state.streams.insert(
        stream_id,
        StreamRow {
            end_offset,
            ..stream
        },
    );
    Ok(())
}

/// LinkedHashMap
/// semantics: re-marking an existing id updates its operation but keeps its
/// original FIFO position.
fn mark_destroyed(state: &mut MetadataState, object_id: u64, op: CompactOperations) {
    if let Some(seq) = state.destroyed_by_id.get(&object_id).copied() {
        state.mark_destroyed.insert(seq, (object_id, op));
        return;
    }
    let seq = state.next_destroyed_seq;
    state.next_destroyed_seq += 1;
    state.mark_destroyed.insert(seq, (object_id, op));
    state.destroyed_by_id.insert(object_id, seq);
}

fn mark_destroy_objects(
    state: &mut MetadataState,
    ids: &[u64],
    ops: &[CompactOperations],
) -> Result<(), MetadataError> {
    if ids.is_empty() {
        return Ok(());
    }
    if ops.is_empty() {
        for &id in ids {
            mark_destroyed(state, id, CompactOperations::Delete);
        }
        return Ok(());
    }
    if ops.len() != ids.len() {
        return Err(MetadataError::Unexpected {
            message: format!(
                "mark destroy ids size {} does not match operations size {}",
                ids.len(),
                ops.len()
            ),
        });
    }
    for (&id, &op) in ids.iter().zip(ops.iter()) {
        mark_destroyed(state, id, op);
    }
    Ok(())
}

/// Whether `object_id` is a committed stream object of `stream_id`.
///
/// O(log n) via
/// `stream_object_ids` instead of scanning the stream's list.
fn stream_object_committed(state: &MetadataState, stream_id: u64, object_id: u64) -> bool {
    matches!(state.stream_object_ids.get(&object_id), Some(key) if key.0 == stream_id)
}

/// Reject a commit that would register `object_id` under two identities.
///
/// A buggy proposer could register one object id under two streams (or as
/// both stream and stream-set object), corrupting cleanup accounting. That is
/// an explicit `Unexpected`. `new_key = Some(..)` allows an identical
/// re-insert (idempotent overwrite of the same row).
fn check_object_id_free(
    state: &MetadataState,
    object_id: u64,
    new_key: Option<crate::state::StreamOffsetKey>,
) -> Result<(), MetadataError> {
    if state.stream_set_objects.contains_key(&object_id) {
        return Err(MetadataError::Unexpected {
            message: format!("object {object_id} is already a committed stream-set object"),
        });
    }
    if let Some(existing) = state.stream_object_ids.get(&object_id) {
        if new_key != Some(*existing) {
            return Err(MetadataError::Unexpected {
                message: format!(
                    "object {object_id} is already committed as stream object {existing:?}"
                ),
            });
        }
    }
    Ok(())
}

fn redundant_commit_check(
    state: &MetadataState,
    request: &CommitStreamSetObjectRequest,
) -> Result<(), MetadataError> {
    if request.object_id != NOOP_OBJECT_ID {
        if state.stream_set_objects.contains_key(&request.object_id) {
            return Err(MetadataError::Redundant {
                message: format!("object {} already committed", request.object_id),
            });
        }
        return Ok(());
    }
    if request.stream_objects.is_empty() {
        return Ok(());
    }
    let all_committed = request
        .stream_objects
        .iter()
        .all(|so| stream_object_committed(state, so.stream_id, so.object_id));
    if all_committed {
        return Err(MetadataError::Redundant {
            message: "all stream objects in commit already committed".into(),
        });
    }
    Ok(())
}

fn commit_stream_set_object(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    request: &CommitStreamSetObjectRequest,
    now_ms: i64,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    redundant_commit_check(state, request)?;

    let compact = !request.compacted_object_ids.is_empty();
    let mut data_time_ms = now_ms;
    if compact {
        for &id in &request.compacted_object_ids {
            let owned =
                state
                    .stream_set_objects
                    .get(&id)
                    .ok_or_else(|| MetadataError::Unexpected {
                        message: format!("compacted stream-set object {id} not found"),
                    })?;
            data_time_ms = data_time_ms.min(owned.object.data_timestamp_ms);
        }
        for &id in &request.compacted_object_ids {
            remove_stream_set_object(state, id);
            mark_destroyed(state, id, CompactOperations::Delete);
        }
    }

    if request.object_id != NOOP_OBJECT_ID {
        check_object_id_free(state, request.object_id, None)?;
        commit_prepared(state, request.object_id);
        for range in &request.stream_ranges {
            advance_end_offset(
                state,
                range.stream_id,
                range.start_offset,
                range.end_offset,
                compact,
            )?;
        }
        let object = S3ObjectMetadata {
            object_id: request.object_id,
            object_type: S3ObjectType::StreamSet,
            offset_ranges: request
                .stream_ranges
                .iter()
                .map(|r| StreamOffsetRange {
                    stream_id: r.stream_id,
                    start_offset: r.start_offset,
                    end_offset: r.end_offset,
                })
                .collect(),
            object_size: request.object_size,
            attributes: s3stream::ObjectAttributes(request.attributes),
            committed_timestamp_ms: now_ms,
            data_timestamp_ms: data_time_ms,
        };
        for range in &request.stream_ranges {
            state.sso_ranges.insert(
                (range.stream_id, range.start_offset, request.object_id),
                range.end_offset,
            );
        }
        state.sso_by_node.insert((node_id, request.object_id), ());
        state
            .stream_set_objects
            .insert(request.object_id, StreamSetObjectRow { node_id, object });
    }

    for stream_object in &request.stream_objects {
        check_object_id_free(
            state,
            stream_object.object_id,
            Some((
                stream_object.stream_id,
                stream_object.start_offset,
                stream_object.object_id,
            )),
        )?;
        commit_prepared(state, stream_object.object_id);
        advance_end_offset(
            state,
            stream_object.stream_id,
            stream_object.start_offset,
            stream_object.end_offset,
            compact,
        )?;
        insert_stream_object(
            state,
            stream_object.stream_id,
            stream_object.object_id,
            stream_object.start_offset,
            stream_object.end_offset,
            stream_object.object_size,
            stream_object.attributes,
            data_time_ms,
            now_ms,
        );
    }
    Ok(MetadataResult::Unit)
}

fn compact_stream_object(
    state: &mut MetadataState,
    node_id: i32,
    node_epoch: i64,
    request: &CompactStreamObjectRequest,
    now_ms: i64,
) -> Result<MetadataResult, MetadataError> {
    node_epoch_check(state, node_id, node_epoch)?;
    let stream_id = request.stream_id;

    if request.object_id != NOOP_OBJECT_ID
        && stream_object_committed(state, stream_id, request.object_id)
    {
        return Err(MetadataError::Redundant {
            message: format!(
                "compact object {} already committed for stream {stream_id}",
                request.object_id
            ),
        });
    }

    let stream = require_stream(state, stream_id)?;
    if stream.epoch != request.stream_epoch as i64 {
        return Err(MetadataError::ExpiredEpoch {
            stream_id,
            epoch: request.stream_epoch as i64,
            message: format!(
                "stream {stream_id} epoch {} is not equal to request {}",
                stream.epoch, request.stream_epoch
            ),
        });
    }
    if stream.end_offset < request.end_offset {
        return Err(MetadataError::Unexpected {
            message: format!(
                "stream {stream_id} end offset {} is lesser than request {}",
                stream.end_offset, request.end_offset
            ),
        });
    }
    if stream.start_offset > request.start_offset {
        return Err(MetadataError::Unexpected {
            message: format!(
                "stream {stream_id} start offset {} is greater than request {}",
                stream.start_offset, request.start_offset
            ),
        });
    }

    if request.object_id != NOOP_OBJECT_ID {
        check_object_id_free(
            state,
            request.object_id,
            Some((stream_id, request.start_offset, request.object_id)),
        )?;
    }
    commit_prepared(state, request.object_id);
    if request.object_id != NOOP_OBJECT_ID {
        insert_stream_object(
            state,
            stream_id,
            request.object_id,
            request.start_offset,
            request.end_offset,
            request.object_size,
            request.attributes,
            now_ms,
            now_ms,
        );
    }
    for &source_id in &request.source_object_ids {
        if let Some(key) = state.stream_object_ids.get(&source_id).copied() {
            if key.0 == stream_id {
                state.stream_objects.remove(&key);
                state.stream_object_ids.remove(&source_id);
            }
        }
    }
    mark_destroy_objects(state, &request.source_object_ids, &request.operations)?;
    Ok(MetadataResult::Unit)
}

#[allow(clippy::too_many_arguments)] // flat commit fields
fn insert_stream_object(
    state: &mut MetadataState,
    stream_id: u64,
    object_id: u64,
    start_offset: u64,
    end_offset: u64,
    object_size: u64,
    attributes: u32,
    data_time_ms: i64,
    now_ms: i64,
) {
    let key = (stream_id, start_offset, object_id);
    state.stream_objects.insert(
        key,
        StreamObjectRow {
            object: S3ObjectMetadata {
                object_id,
                object_type: S3ObjectType::Stream,
                offset_ranges: vec![StreamOffsetRange {
                    stream_id,
                    start_offset,
                    end_offset,
                }],
                object_size,
                attributes: s3stream::ObjectAttributes(attributes),
                committed_timestamp_ms: now_ms,
                data_timestamp_ms: data_time_ms,
            },
        },
    );
    state.stream_object_ids.insert(object_id, key);
}

fn remove_stream_set_object(state: &mut MetadataState, object_id: u64) {
    if let Some(row) = state.stream_set_objects.remove(&object_id) {
        for range in &row.object.offset_ranges {
            state
                .sso_ranges
                .remove(&(range.stream_id, range.start_offset, object_id));
        }
        state.sso_by_node.remove(&(row.node_id, object_id));
    }
}

fn expire_prepared_objects(
    state: &mut MetadataState,
    now_ms: i64,
) -> Result<MetadataResult, MetadataError> {
    let expired: Vec<(i64, u64)> = state
        .prepared_by_deadline
        .range(..=(now_ms, u64::MAX))
        .map(|(key, _)| *key)
        .collect();
    for (deadline, object_id) in &expired {
        state.prepared_by_deadline.remove(&(*deadline, *object_id));
        state.prepared.remove(object_id);
    }
    Ok(MetadataResult::Count(expired.len() as u64))
}

fn clean_destroyed_objects(
    state: &mut MetadataState,
    object_ids: &[u64],
) -> Result<MetadataResult, MetadataError> {
    for id in object_ids {
        if let Some(seq) = state.destroyed_by_id.remove(id) {
            state.mark_destroyed.remove(&seq);
        }
    }
    Ok(MetadataResult::Unit)
}

fn put_kv(
    state: &mut MetadataState,
    key: &str,
    value: &bytes::Bytes,
) -> Result<MetadataResult, MetadataError> {
    if let Some(old) = state.kv.insert(key.to_owned(), value.clone()) {
        state.kv_bytes -= (key.len() + old.len()) as u64;
    }
    state.kv_bytes += (key.len() + value.len()) as u64;
    Ok(MetadataResult::Value(Some(value.clone())))
}

fn put_kv_if_absent(
    state: &mut MetadataState,
    key: &str,
    value: &bytes::Bytes,
) -> Result<MetadataResult, MetadataError> {
    if let Some(existing) = state.kv.get(key) {
        return Ok(MetadataResult::Value(Some(existing.clone())));
    }
    state.kv.insert(key.to_owned(), value.clone());
    state.kv_bytes += (key.len() + value.len()) as u64;
    Ok(MetadataResult::Value(Some(value.clone())))
}

fn delete_kv(state: &mut MetadataState, key: &str) -> Result<MetadataResult, MetadataError> {
    let removed = state.kv.remove(key);
    if let Some(old) = &removed {
        state.kv_bytes -= (key.len() + old.len()) as u64;
    }
    Ok(MetadataResult::Value(removed))
}

fn delete_kv_if_matches(
    state: &mut MetadataState,
    key: &str,
    expected: &bytes::Bytes,
) -> Result<MetadataResult, MetadataError> {
    match state.kv.get(key) {
        Some(current) if current == expected => {
            let removed = state.kv.remove(key);
            if let Some(old) = &removed {
                state.kv_bytes -= (key.len() + old.len()) as u64;
            }
            Ok(MetadataResult::Value(removed))
        }
        _ => Err(MetadataError::Redundant {
            message: format!("kv key {key} missing or value mismatch"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use s3stream::ObjectStreamRange;

    const NODE_1: i32 = 1;
    const NODE_2: i32 = 2;
    const EPOCH_1: i64 = 10;
    const EPOCH_2: i64 = 20;

    fn setup() -> MetadataState {
        let mut state = MetadataState::new();
        register(&mut state, NODE_1, EPOCH_1);
        register(&mut state, NODE_2, EPOCH_2);
        state
    }

    fn register(state: &mut MetadataState, node_id: i32, node_epoch: i64) {
        register_with_slots(state, node_id, node_epoch, 1);
    }

    fn register_with_slots(state: &mut MetadataState, node_id: i32, node_epoch: i64, slots: u32) {
        apply(
            state,
            &MetadataCommand::RegisterNode {
                node_id,
                node_epoch,
                http_address: String::new(),
                slots,
                protocol_addresses: Default::default(),
            },
        )
        .unwrap();
    }

    fn place(state: &mut MetadataState, stream_id: u64) -> i32 {
        match apply(state, &MetadataCommand::PlaceStream { stream_id }).unwrap() {
            MetadataResult::Id(node_id) => node_id as i32,
            other => panic!("unexpected result {other:?}"),
        }
    }

    fn create(state: &mut MetadataState, node_id: i32, node_epoch: i64) -> u64 {
        match apply(
            state,
            &MetadataCommand::CreateStream {
                node_id,
                node_epoch,
            },
        )
        .unwrap()
        {
            MetadataResult::Id(id) => id,
            other => panic!("unexpected result {other:?}"),
        }
    }

    fn open(
        state: &mut MetadataState,
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    ) -> Result<MetadataResult, MetadataError> {
        apply(
            state,
            &MetadataCommand::OpenStream {
                node_id,
                node_epoch,
                stream_id,
                epoch,
            },
        )
    }

    fn close(
        state: &mut MetadataState,
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    ) -> Result<MetadataResult, MetadataError> {
        apply(
            state,
            &MetadataCommand::CloseStream {
                node_id,
                node_epoch,
                stream_id,
                epoch,
            },
        )
    }

    fn prepare(state: &mut MetadataState, count: u32, ttl_ms: i64, now_ms: i64) -> u64 {
        match apply(
            state,
            &MetadataCommand::PrepareObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                count,
                ttl_ms,
                now_ms,
            },
        )
        .unwrap()
        {
            MetadataResult::Id(id) => id,
            other => panic!("unexpected result {other:?}"),
        }
    }

    #[test]
    fn writes_require_registered_node_epoch() {
        let mut state = setup();
        let err = apply(
            &mut state,
            &MetadataCommand::CreateStream {
                node_id: 3,
                node_epoch: 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
        let err = apply(
            &mut state,
            &MetadataCommand::CreateStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
    }

    #[test]
    fn stale_node_epoch_fenced_after_re_registration() {
        let mut state = setup();
        register(&mut state, NODE_1, EPOCH_1 + 5);
        let err = apply(
            &mut state,
            &MetadataCommand::CreateStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
        let err = apply(
            &mut state,
            &MetadataCommand::RegisterNode {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                http_address: String::new(),
                slots: 1,
                protocol_addresses: Default::default(),
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
    }

    #[test]
    fn register_node_keeps_protocol_addresses_unless_replaced() {
        let addrs = |pairs: &[(&str, &str)]| -> std::collections::BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let mut state = MetadataState::new();
        apply(
            &mut state,
            &MetadataCommand::RegisterNode {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                http_address: "http://n1:9090".into(),
                slots: 1,
                protocol_addresses: addrs(&[("kafka", "n1:9092")]),
            },
        )
        .unwrap();
        assert_eq!(
            state
                .nodes
                .get(&NODE_1)
                .unwrap()
                .protocol_addresses
                .get("kafka")
                .map(String::as_str),
            Some("n1:9092")
        );

        // An empty map keeps the stored addresses across a re-register.
        apply(
            &mut state,
            &MetadataCommand::RegisterNode {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 1,
                http_address: String::new(),
                slots: 2,
                protocol_addresses: Default::default(),
            },
        )
        .unwrap();
        let node = state.nodes.get(&NODE_1).unwrap();
        assert_eq!(
            node.protocol_addresses.get("kafka").map(String::as_str),
            Some("n1:9092")
        );
        assert_eq!(node.http_address, "http://n1:9090");

        // A non-empty map replaces the stored addresses wholesale.
        apply(
            &mut state,
            &MetadataCommand::RegisterNode {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 2,
                http_address: String::new(),
                slots: 2,
                protocol_addresses: addrs(&[("kafka", "n1:19092")]),
            },
        )
        .unwrap();
        assert_eq!(
            state
                .nodes
                .get(&NODE_1)
                .unwrap()
                .protocol_addresses
                .get("kafka")
                .map(String::as_str),
            Some("n1:19092")
        );
    }

    #[test]
    fn allocate_producer_ids_advances_and_validates() {
        let mut state = setup();
        let allocate = |state: &mut MetadataState, count| {
            apply(
                state,
                &MetadataCommand::AllocateProducerIds {
                    node_id: NODE_1,
                    node_epoch: EPOCH_1,
                    count,
                },
            )
        };
        assert_eq!(allocate(&mut state, 3).unwrap(), MetadataResult::Id(0));
        assert_eq!(allocate(&mut state, 1).unwrap(), MetadataResult::Id(3));
        assert_eq!(state.next_producer_id, 4);

        assert_eq!(allocate(&mut state, 0).unwrap_err().code(), 99);
        assert_eq!(
            allocate(&mut state, MAX_ALLOCATE_PRODUCER_IDS + 1)
                .unwrap_err()
                .code(),
            99
        );
        let err = apply(
            &mut state,
            &MetadataCommand::AllocateProducerIds {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 1,
                count: 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
        assert_eq!(state.next_producer_id, 4);
    }

    #[test]
    fn open_stream_lifecycle_and_fencing() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);

        let opened = open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let MetadataResult::Stream(meta) = opened else {
            panic!("expected stream")
        };
        assert_eq!(meta.state, StreamState::Opened);

        // Idempotent re-open at the same (epoch, node).
        let retried = open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let MetadataResult::Stream(meta) = retried else {
            panic!("expected stream")
        };
        assert_eq!(meta.state, StreamState::Opened);

        // Same epoch from another node → fenced.
        assert_eq!(
            open(&mut state, NODE_2, EPOCH_2, stream_id, 1)
                .unwrap_err()
                .code(),
            3
        );
        // Older epoch → fenced.
        assert_eq!(
            open(&mut state, NODE_2, EPOCH_2, stream_id, 0)
                .unwrap_err()
                .code(),
            3
        );
        // Newer epoch but still opened → not closed.
        assert_eq!(
            open(&mut state, NODE_2, EPOCH_2, stream_id, 2)
                .unwrap_err()
                .code(),
            2
        );

        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let reopened = open(&mut state, NODE_2, EPOCH_2, stream_id, 2).unwrap();
        let MetadataResult::Stream(meta) = reopened else {
            panic!("expected stream")
        };
        assert_eq!(meta.node_id, NODE_2);
    }

    #[test]
    fn close_stream_is_idempotent() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        assert_eq!(
            state.streams.get(&stream_id).unwrap().state,
            StreamState::Closed
        );
        assert!(state.opening_by_node.get(&(NODE_1, stream_id)).is_none());
    }

    #[test]
    fn delete_stream_idempotent_and_marks_objects_destroyed() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let object_id = prepare(&mut state, 1, 60_000, 0);
        apply(
            &mut state,
            &MetadataCommand::CompactStreamObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request: CompactStreamObjectRequest {
                    object_id,
                    object_size: 10,
                    stream_id,
                    stream_epoch: 1,
                    start_offset: 0,
                    end_offset: 0,
                    source_object_ids: vec![],
                    operations: vec![],
                    attributes: 0,
                },
                now_ms: 1,
            },
        )
        .unwrap();
        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        apply(
            &mut state,
            &MetadataCommand::DeleteStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                stream_id,
                epoch: 1,
            },
        )
        .unwrap();
        // Idempotent second delete.
        apply(
            &mut state,
            &MetadataCommand::DeleteStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                stream_id,
                epoch: 1,
            },
        )
        .unwrap();
        let destroyed: Vec<_> = state.mark_destroyed.values().cloned().collect();
        assert_eq!(destroyed, vec![(object_id, CompactOperations::Delete)]);
        assert!(state.stream_objects.is_empty());
        assert!(state.stream_object_ids.is_empty());
    }

    #[test]
    fn opening_by_node_index_filters_by_node() {
        let mut state = setup();
        let stream1 = create(&mut state, NODE_1, EPOCH_1);
        let stream2 = create(&mut state, NODE_2, EPOCH_2);
        open(&mut state, NODE_1, EPOCH_1, stream1, 1).unwrap();
        open(&mut state, NODE_2, EPOCH_2, stream2, 1).unwrap();

        let node1: Vec<u64> = state
            .opening_by_node
            .range((NODE_1, 0)..=(NODE_1, u64::MAX))
            .map(|(k, _)| k.1)
            .collect();
        assert_eq!(node1, vec![stream1]);
        let node2: Vec<u64> = state
            .opening_by_node
            .range((NODE_2, 0)..=(NODE_2, u64::MAX))
            .map(|(k, _)| k.1)
            .collect();
        assert_eq!(node2, vec![stream2]);
    }

    #[test]
    fn commit_advances_end_offset_and_is_redundant_on_retry() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let object_id = prepare(&mut state, 1, 60_000, 0);
        let request = CommitStreamSetObjectRequest {
            object_id,
            object_size: 64,
            attributes: 0,
            stream_ranges: vec![ObjectStreamRange {
                stream_id,
                epoch: 1,
                start_offset: 0,
                end_offset: 8,
                size: 64,
            }],
            stream_objects: vec![],
            compacted_object_ids: vec![],
        };
        apply(
            &mut state,
            &MetadataCommand::CommitStreamSetObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request: request.clone(),
                now_ms: 1,
            },
        )
        .unwrap();
        assert_eq!(state.streams.get(&stream_id).unwrap().end_offset, 8);
        assert!(state.prepared.is_empty(), "commit consumes the lease");

        let err = apply(
            &mut state,
            &MetadataCommand::CommitStreamSetObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request,
                now_ms: 2,
            },
        )
        .unwrap_err();
        assert!(err.is_redundant());
        assert_eq!(state.streams.get(&stream_id).unwrap().end_offset, 8);
        assert_eq!(
            state.stream_set_objects.len() + state.stream_objects.len(),
            1
        );
        // Index entry exists: (stream, start, object) -> end.
        assert_eq!(state.sso_ranges.get(&(stream_id, 0, object_id)), Some(&8));
        assert!(state.sso_by_node.contains_key(&(NODE_1, object_id)));
    }

    #[test]
    fn compact_retry_is_redundant() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let object_id = prepare(&mut state, 1, 60_000, 0);
        let request = CompactStreamObjectRequest {
            object_id,
            object_size: 10,
            stream_id,
            stream_epoch: 1,
            start_offset: 0,
            end_offset: 0,
            source_object_ids: vec![],
            operations: vec![],
            attributes: 0,
        };
        apply(
            &mut state,
            &MetadataCommand::CompactStreamObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request: request.clone(),
                now_ms: 1,
            },
        )
        .unwrap();
        let err = apply(
            &mut state,
            &MetadataCommand::CompactStreamObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request,
                now_ms: 2,
            },
        )
        .unwrap_err();
        assert!(err.is_redundant());
        assert_eq!(state.stream_objects.len(), 1);
    }

    #[test]
    fn mark_destroy_rejects_mismatched_sizes_atomically() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let object_id = prepare(&mut state, 1, 60_000, 0);
        let before = state.clone();
        let err = apply(
            &mut state,
            &MetadataCommand::CompactStreamObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request: CompactStreamObjectRequest {
                    object_id,
                    object_size: 10,
                    stream_id,
                    stream_epoch: 1,
                    start_offset: 0,
                    end_offset: 0,
                    source_object_ids: vec![1, 2],
                    operations: vec![CompactOperations::Delete],
                    attributes: 0,
                },
                now_ms: 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 99);
        assert_eq!(state, before, "failed apply must not mutate state");
    }

    /// FIFO
    /// order and clean via the destroyed indexes (peek query lands in M3).
    #[test]
    fn clean_destroyed_objects_preserves_fifo() {
        let mut state = setup();
        // Reach mark_destroy via a compact with sources (all Delete default).
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        mark_destroy_objects(
            &mut state,
            &[1, 2, 3],
            &[
                CompactOperations::KeepData,
                CompactOperations::Delete,
                CompactOperations::DeepDelete,
            ],
        )
        .unwrap();
        assert_eq!(state.mark_destroyed.len(), 3);
        apply(
            &mut state,
            &MetadataCommand::CleanDestroyedObjects {
                object_ids: vec![1, 2],
            },
        )
        .unwrap();
        let remaining: Vec<_> = state.mark_destroyed.values().cloned().collect();
        assert_eq!(remaining, vec![(3, CompactOperations::DeepDelete)]);
    }

    #[test]
    fn expire_prepared_objects_removes_only_expired() {
        let mut state = setup();
        prepare(&mut state, 2, 100, 0);
        prepare(&mut state, 1, 10_000, 0);
        let result = apply(
            &mut state,
            &MetadataCommand::ExpirePreparedObjects { now_ms: 200 },
        )
        .unwrap();
        assert_eq!(result, MetadataResult::Count(2));
        assert_eq!(state.prepared.len(), 1);
        assert_eq!(state.prepared_by_deadline.len(), 1);
    }

    #[test]
    fn kv_put_get_delete_and_put_if_absent() {
        let mut state = MetadataState::new();
        let hello = Bytes::from_static(b"hello");
        let world = Bytes::from_static(b"world");

        let result = apply(
            &mut state,
            &MetadataCommand::PutKv {
                key: "k".into(),
                value: hello.clone(),
            },
        )
        .unwrap();
        assert_eq!(result, MetadataResult::Value(Some(hello.clone())));
        assert_eq!(state.kv.get("k"), Some(&hello));

        let result = apply(
            &mut state,
            &MetadataCommand::PutKvIfAbsent {
                key: "k".into(),
                value: world.clone(),
            },
        )
        .unwrap();
        assert_eq!(result, MetadataResult::Value(Some(hello.clone())));
        let result = apply(
            &mut state,
            &MetadataCommand::PutKvIfAbsent {
                key: "k2".into(),
                value: world.clone(),
            },
        )
        .unwrap();
        assert_eq!(result, MetadataResult::Value(Some(world.clone())));

        let result = apply(&mut state, &MetadataCommand::DeleteKv { key: "k".into() }).unwrap();
        assert_eq!(result, MetadataResult::Value(Some(hello)));
        assert!(state.kv.get("k").is_none());
        let result = apply(
            &mut state,
            &MetadataCommand::DeleteKv {
                key: "missing".into(),
            },
        )
        .unwrap();
        assert_eq!(result, MetadataResult::Value(None));
    }

    #[test]
    fn kv_delete_if_matches_and_rejects_mismatch() {
        let mut state = MetadataState::new();
        let hello = Bytes::from_static(b"hello");
        let world = Bytes::from_static(b"world");
        apply(
            &mut state,
            &MetadataCommand::PutKv {
                key: "k".into(),
                value: hello.clone(),
            },
        )
        .unwrap();

        assert!(apply(
            &mut state,
            &MetadataCommand::DeleteKvIfMatches {
                key: "k".into(),
                expected: world.clone(),
            },
        )
        .unwrap_err()
        .is_redundant());
        assert_eq!(state.kv.get("k"), Some(&hello));

        let result = apply(
            &mut state,
            &MetadataCommand::DeleteKvIfMatches {
                key: "k".into(),
                expected: hello.clone(),
            },
        )
        .unwrap();
        assert_eq!(result, MetadataResult::Value(Some(hello)));
        assert!(state.kv.get("k").is_none());

        assert!(apply(
            &mut state,
            &MetadataCommand::DeleteKvIfMatches {
                key: "k".into(),
                expected: world,
            },
        )
        .unwrap_err()
        .is_redundant());
    }

    /// Determinism over a scripted random command sequence.
    #[test]
    fn applying_same_operations_produces_identical_state() {
        fn run(seed: u64) -> MetadataState {
            let mut rng = seed;
            let mut next = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            let mut state = MetadataState::new();
            register(&mut state, NODE_1, EPOCH_1);
            for i in 0..200 {
                let command = match next() % 4 {
                    0 => MetadataCommand::CreateStream {
                        node_id: NODE_1,
                        node_epoch: EPOCH_1,
                    },
                    1 => MetadataCommand::OpenStream {
                        node_id: NODE_1,
                        node_epoch: EPOCH_1,
                        stream_id: next() % 20,
                        epoch: (next() % 5) as i64,
                    },
                    2 => MetadataCommand::CloseStream {
                        node_id: NODE_1,
                        node_epoch: EPOCH_1,
                        stream_id: next() % 20,
                        epoch: (next() % 5) as i64,
                    },
                    _ => MetadataCommand::PrepareObject {
                        node_id: NODE_1,
                        node_epoch: EPOCH_1,
                        count: 1 + (next() % 3) as u32,
                        ttl_ms: 1000,
                        now_ms: i,
                    },
                };
                let _ = apply(&mut state, &command); // errors ignored on purpose
            }
            state
        }
        assert_eq!(run(424242), run(424242));
    }

    // Property tests: index consistency + atomicity over random command runs.

    /// Every secondary index must be exactly derivable from its primary. The
    /// invariant that lets snapshots skip indexes and rebuild on restore.
    fn assert_indexes_consistent(state: &MetadataState) {
        let derived_opening: Vec<(i32, u64)> = state
            .streams
            .iter()
            .filter(|(_, row)| row.state == StreamState::Opened)
            .map(|(id, row)| (row.node_id, *id))
            .collect();
        let mut indexed_opening: Vec<(i32, u64)> = state.opening_by_node.keys().copied().collect();
        indexed_opening.sort_unstable();
        let mut derived_opening = derived_opening;
        derived_opening.sort_unstable();
        assert_eq!(indexed_opening, derived_opening, "opening_by_node");

        let derived_placed: Vec<(i32, u64)> = state
            .streams
            .iter()
            .filter(|(_, row)| {
                row.state == StreamState::Closed && row.epoch == -1 && row.node_id != -1
            })
            .map(|(id, row)| (row.node_id, *id))
            .collect();
        let mut indexed_placed: Vec<(i32, u64)> = state.placed_by_node.keys().copied().collect();
        indexed_placed.sort_unstable();
        let mut derived_placed = derived_placed;
        derived_placed.sort_unstable();
        assert_eq!(indexed_placed, derived_placed, "placed_by_node");

        assert_eq!(
            state.prepared.len(),
            state.prepared_by_deadline.len(),
            "prepared index size"
        );
        for (id, deadline) in state.prepared.iter() {
            assert!(
                state.prepared_by_deadline.contains_key(&(*deadline, *id)),
                "prepared index"
            );
        }

        for (key, _) in state.stream_objects.iter() {
            assert_eq!(
                state.stream_object_ids.get(&key.2),
                Some(key),
                "stream_object_ids"
            );
        }
        assert_eq!(state.stream_objects.len(), state.stream_object_ids.len());

        let mut derived_ranges = 0usize;
        for (object_id, row) in state.stream_set_objects.iter() {
            assert!(
                state.sso_by_node.contains_key(&(row.node_id, *object_id)),
                "sso_by_node"
            );
            for range in &row.object.offset_ranges {
                derived_ranges += 1;
                assert_eq!(
                    state
                        .sso_ranges
                        .get(&(range.stream_id, range.start_offset, *object_id)),
                    Some(&range.end_offset),
                    "sso_ranges"
                );
            }
        }
        assert_eq!(state.sso_ranges.len(), derived_ranges);
        assert_eq!(state.sso_by_node.len(), state.stream_set_objects.len());

        assert_eq!(state.mark_destroyed.len(), state.destroyed_by_id.len());
        for (seq, (id, _)) in state.mark_destroyed.iter() {
            assert_eq!(state.destroyed_by_id.get(id), Some(seq), "destroyed_by_id");
        }

        for (stream_id, _) in state.pending_transfers.iter() {
            assert!(
                state.streams.contains_key(stream_id),
                "pending transfer references a missing stream"
            );
        }
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        fn arb_command() -> impl Strategy<Value = MetadataCommand> {
            let node = prop_oneof![Just((NODE_1, EPOCH_1)), Just((NODE_2, EPOCH_2))];
            let stream_id = 0u64..8;
            let epoch = 0i64..4;
            prop_oneof![
                node.clone()
                    .prop_map(|(n, e)| MetadataCommand::CreateStream {
                        node_id: n,
                        node_epoch: e
                    }),
                stream_id
                    .clone()
                    .prop_map(|s| MetadataCommand::PlaceStream { stream_id: s }),
                (node.clone(), stream_id.clone(), epoch.clone()).prop_map(|((n, ne), s, e)| {
                    MetadataCommand::OpenStream {
                        node_id: n,
                        node_epoch: ne,
                        stream_id: s,
                        epoch: e,
                    }
                }),
                (node.clone(), stream_id.clone(), epoch.clone()).prop_map(|((n, ne), s, e)| {
                    MetadataCommand::CloseStream {
                        node_id: n,
                        node_epoch: ne,
                        stream_id: s,
                        epoch: e,
                    }
                }),
                (node.clone(), stream_id.clone(), epoch.clone()).prop_map(|((n, ne), s, e)| {
                    MetadataCommand::DeleteStream {
                        node_id: n,
                        node_epoch: ne,
                        stream_id: s,
                        epoch: e,
                    }
                }),
                (stream_id.clone(), 1i32..3, 1i32..3).prop_map(|(s, from, to)| {
                    MetadataCommand::TransferStream {
                        stream_id: s,
                        from_node: from,
                        to_node: to,
                    }
                }),
                (stream_id.clone(), epoch.clone()).prop_map(|(s, e)| {
                    MetadataCommand::CompleteTransfer {
                        stream_id: s,
                        epoch: e,
                    }
                }),
                (node.clone(), 1u32..4).prop_map(|((n, ne), c)| MetadataCommand::CreateStreams {
                    node_id: n,
                    node_epoch: ne,
                    count: c,
                }),
                (node.clone(), 1u32..4).prop_map(|((n, ne), c)| {
                    MetadataCommand::AllocateProducerIds {
                        node_id: n,
                        node_epoch: ne,
                        count: c,
                    }
                }),
                (node.clone(), 1u32..3, 0i64..100).prop_map(|((n, ne), c, now)| {
                    MetadataCommand::PrepareObject {
                        node_id: n,
                        node_epoch: ne,
                        count: c,
                        ttl_ms: 50,
                        now_ms: now,
                    }
                }),
                (
                    node.clone(),
                    stream_id.clone(),
                    epoch.clone(),
                    0u64..8,
                    0u64..16
                )
                    .prop_map(|((n, ne), s, e, obj, end)| {
                        MetadataCommand::CommitStreamSetObject {
                            node_id: n,
                            node_epoch: ne,
                            request: CommitStreamSetObjectRequest {
                                object_id: obj,
                                object_size: 64,
                                attributes: 0,
                                stream_ranges: vec![ObjectStreamRange {
                                    stream_id: s,
                                    epoch: e.max(0) as u64,
                                    start_offset: 0,
                                    end_offset: end,
                                    size: 64,
                                }],
                                stream_objects: vec![],
                                compacted_object_ids: vec![],
                            },
                            now_ms: 1,
                        }
                    }),
                (node.clone(), stream_id, epoch, 0u64..8).prop_map(|((n, ne), s, e, obj)| {
                    MetadataCommand::CompactStreamObject {
                        node_id: n,
                        node_epoch: ne,
                        request: CompactStreamObjectRequest {
                            object_id: obj,
                            object_size: 10,
                            stream_id: s,
                            stream_epoch: e.max(0) as u64,
                            start_offset: 0,
                            end_offset: 0,
                            source_object_ids: vec![],
                            operations: vec![],
                            attributes: 0,
                        },
                        now_ms: 1,
                    }
                }),
                (0i64..200).prop_map(|now| MetadataCommand::ExpirePreparedObjects { now_ms: now }),
                proptest::collection::vec(0u64..8, 0..3)
                    .prop_map(|ids| MetadataCommand::CleanDestroyedObjects { object_ids: ids }),
                ("[a-c]{1,2}", proptest::collection::vec(any::<u8>(), 0..4)).prop_map(|(k, v)| {
                    MetadataCommand::PutKv {
                        key: k,
                        value: Bytes::from(v),
                    }
                }),
                "[a-c]{1,2}".prop_map(|k| MetadataCommand::DeleteKv { key: k }),
                ("[a-c]{1,2}", proptest::collection::vec(any::<u8>(), 0..4)).prop_map(|(k, v)| {
                    MetadataCommand::DeleteKvIfMatches {
                        key: k,
                        expected: Bytes::from(v),
                    }
                }),
            ]
        }

        proptest! {
            /// After any command sequence: indexes are derivable from primaries,
            /// and a failed apply leaves the state bit-identical.
            #[test]
            fn indexes_consistent_and_apply_atomic(
                commands in proptest::collection::vec(arb_command(), 0..60)
            ) {
                let mut state = setup();
                for command in &commands {
                    let before = state.clone();
                    match apply(&mut state, command) {
                        Ok(_) => {}
                        Err(_) => prop_assert_eq!(&state, &before, "failed apply mutated state"),
                    }
                    assert_indexes_consistent(&state);
                }
                // Determinism: replay produces the identical state.
                let mut replay = setup();
                for command in &commands {
                    let _ = apply(&mut replay, command);
                }
                prop_assert_eq!(&state, &replay);
                // Snapshot round trip is exact identity on any reachable state.
                let restored = crate::snapshot::decode(&crate::snapshot::encode(&state))
                    .expect("snapshot of reachable state must decode");
                prop_assert_eq!(state, restored);
            }
        }
    }

    #[test]
    fn slot_weighted_placement_distributes_by_capacity() {
        let mut state = MetadataState::new();
        register_with_slots(&mut state, 1, EPOCH_1, 4);
        register_with_slots(&mut state, 2, EPOCH_2, 1);
        let mut owners = Vec::new();
        for _ in 0..5 {
            let stream_id = create(&mut state, NODE_1, EPOCH_1);
            owners.push(place(&mut state, stream_id));
        }
        assert_eq!(owners.iter().filter(|&&n| n == 1).count(), 4);
        assert_eq!(owners.iter().filter(|&&n| n == 2).count(), 1);
    }

    #[test]
    fn placement_tie_breaks_on_lowest_node_id() {
        let mut state = MetadataState::new();
        register_with_slots(&mut state, 2, EPOCH_2, 1);
        register_with_slots(&mut state, 1, EPOCH_1, 1);
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        assert_eq!(place(&mut state, stream_id), 1);
    }

    #[test]
    fn placement_is_idempotent_for_already_placed_stream() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        assert_eq!(place(&mut state, stream_id), NODE_1);
        let after_first = state.clone();
        assert_eq!(place(&mut state, stream_id), NODE_1);
        assert_eq!(state, after_first);
    }

    #[test]
    fn place_open_close_delete_keeps_indexes_balanced() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        place(&mut state, stream_id);
        assert_eq!(state.placed_by_node.len(), 1);
        assert_eq!(state.opening_by_node.len(), 0);

        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        assert_eq!(state.placed_by_node.len(), 0);
        assert_eq!(state.opening_by_node.len(), 1);

        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        assert_eq!(state.placed_by_node.len(), 0);
        assert_eq!(state.opening_by_node.len(), 0);

        apply(
            &mut state,
            &MetadataCommand::DeleteStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                stream_id,
                epoch: 1,
            },
        )
        .unwrap();
        assert_eq!(state.placed_by_node.len(), 0);
        assert_eq!(state.opening_by_node.len(), 0);
    }

    #[test]
    fn place_then_open_by_different_node_moves_indexes() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        place(&mut state, stream_id);
        assert!(state.placed_by_node.contains_key(&(NODE_1, stream_id)));

        open(&mut state, NODE_2, EPOCH_2, stream_id, 1).unwrap();
        assert!(!state.placed_by_node.contains_key(&(NODE_1, stream_id)));
        assert!(state.opening_by_node.contains_key(&(NODE_2, stream_id)));
    }

    #[test]
    fn placing_nonexistent_stream_errors() {
        let mut state = setup();
        let err = apply(&mut state, &MetadataCommand::PlaceStream { stream_id: 999 }).unwrap_err();
        assert_eq!(err.code(), 1);
    }

    #[test]
    fn placement_without_registered_nodes_errors() {
        let mut state = MetadataState::new();
        let stream_id = {
            register(&mut state, NODE_1, EPOCH_1);
            create(&mut state, NODE_1, EPOCH_1)
        };
        state.nodes.remove(&NODE_1);
        let err = apply(&mut state, &MetadataCommand::PlaceStream { stream_id }).unwrap_err();
        assert_eq!(err.code(), 5);
    }

    #[test]
    fn snapshot_roundtrip_preserves_placed_by_node() {
        let mut state = MetadataState::new();
        register_with_slots(&mut state, 1, EPOCH_1, 4);
        register_with_slots(&mut state, 2, EPOCH_2, 1);
        for _ in 0..3 {
            let stream_id = create(&mut state, NODE_1, EPOCH_1);
            place(&mut state, stream_id);
        }
        let before = state.placed_by_node.clone();
        let restored = crate::snapshot::decode(&crate::snapshot::encode(&state)).unwrap();
        assert_eq!(restored.placed_by_node, before);
        assert_eq!(restored, state);
    }

    fn transfer(
        state: &mut MetadataState,
        stream_id: u64,
        from_node: i32,
        to_node: i32,
    ) -> Result<MetadataResult, MetadataError> {
        apply(
            state,
            &MetadataCommand::TransferStream {
                stream_id,
                from_node,
                to_node,
            },
        )
    }

    fn complete(
        state: &mut MetadataState,
        stream_id: u64,
        epoch: i64,
    ) -> Result<MetadataResult, MetadataError> {
        apply(
            state,
            &MetadataCommand::CompleteTransfer { stream_id, epoch },
        )
    }

    #[test]
    fn transfer_records_pending_and_is_idempotent() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();

        transfer(&mut state, stream_id, NODE_1, NODE_2).unwrap();
        let pending = state.pending_transfers.get(&stream_id).unwrap();
        assert_eq!((pending.from_node, pending.to_node), (NODE_1, NODE_2));

        let after_first = state.clone();
        transfer(&mut state, stream_id, NODE_1, NODE_2).unwrap();
        assert_eq!(state, after_first);

        let err = transfer(&mut state, stream_id, NODE_2, NODE_1).unwrap_err();
        assert_eq!(err.code(), 99);
        assert_eq!(state, after_first);
    }

    #[test]
    fn transfer_requires_opened_on_source_and_registered_target() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);

        // Closed stream cannot transfer.
        assert_eq!(
            transfer(&mut state, stream_id, NODE_1, NODE_2)
                .unwrap_err()
                .code(),
            99
        );
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();

        // Wrong source node.
        assert_eq!(
            transfer(&mut state, stream_id, NODE_2, NODE_1)
                .unwrap_err()
                .code(),
            99
        );
        // Unregistered target.
        assert_eq!(
            transfer(&mut state, stream_id, NODE_1, 9)
                .unwrap_err()
                .code(),
            5
        );
        // Self transfer.
        assert_eq!(
            transfer(&mut state, stream_id, NODE_1, NODE_1)
                .unwrap_err()
                .code(),
            99
        );
        // Missing stream.
        assert_eq!(
            transfer(&mut state, 999, NODE_1, NODE_2)
                .unwrap_err()
                .code(),
            1
        );
        assert!(state.pending_transfers.is_empty());
    }

    #[test]
    fn complete_transfer_repoints_stream_and_clears_pending() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        transfer(&mut state, stream_id, NODE_1, NODE_2).unwrap();

        // Not closed yet.
        assert_eq!(complete(&mut state, stream_id, 1).unwrap_err().code(), 2);

        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        // Wrong epoch.
        assert_eq!(complete(&mut state, stream_id, 0).unwrap_err().code(), 4);

        complete(&mut state, stream_id, 1).unwrap();
        let row = state.streams.get(&stream_id).unwrap();
        assert_eq!(row.node_id, NODE_2);
        assert_eq!(row.state, StreamState::Closed);
        assert!(state.pending_transfers.is_empty());
        assert!(state.placed_by_node.is_empty());

        // Redundant completion.
        assert!(complete(&mut state, stream_id, 1)
            .unwrap_err()
            .is_redundant());

        // The target opens with a bumped epoch.
        open(&mut state, NODE_2, EPOCH_2, stream_id, 2).unwrap();
        assert_eq!(state.streams.get(&stream_id).unwrap().node_id, NODE_2);
        assert!(state.opening_by_node.contains_key(&(NODE_2, stream_id)));
    }

    #[test]
    fn delete_stream_clears_pending_transfer() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        transfer(&mut state, stream_id, NODE_1, NODE_2).unwrap();
        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        apply(
            &mut state,
            &MetadataCommand::DeleteStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                stream_id,
                epoch: 1,
            },
        )
        .unwrap();
        assert!(state.pending_transfers.is_empty());
    }

    #[test]
    fn create_streams_assigns_consecutive_ids() {
        let mut state = setup();
        let single = create(&mut state, NODE_1, EPOCH_1);
        let result = apply(
            &mut state,
            &MetadataCommand::CreateStreams {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                count: 3,
            },
        )
        .unwrap();
        assert_eq!(result, MetadataResult::Id(single + 1));
        for id in single + 1..single + 4 {
            let row = state.streams.get(&id).unwrap();
            assert_eq!(row.epoch, -1);
            assert_eq!(row.node_id, -1);
            assert_eq!(row.state, StreamState::Closed);
        }
        assert_eq!(state.next_stream_id, single + 4);
    }

    #[test]
    fn create_streams_validates_count_and_epoch() {
        let mut state = setup();
        let before = state.clone();
        let err = apply(
            &mut state,
            &MetadataCommand::CreateStreams {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                count: 0,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 99);
        let err = apply(
            &mut state,
            &MetadataCommand::CreateStreams {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                count: MAX_CREATE_STREAMS + 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 99);
        let err = apply(
            &mut state,
            &MetadataCommand::CreateStreams {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 1,
                count: 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
        assert_eq!(state, before);
    }
}
