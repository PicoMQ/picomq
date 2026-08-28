//! The metadata state: compact rows in persistent maps, plus the secondary
//! indexes that make every query O(what it touches).

use std::collections::BTreeMap;

use bytes::Bytes;
use im::OrdMap;
use s3stream::{CompactOperations, StreamMetadata, StreamState};

/// One stream's replicated control-plane row. `epoch` is `i64` because a
/// never-opened stream carries epoch `-1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRow {
    pub stream_id: u64,
    pub epoch: i64,
    pub start_offset: u64,
    /// Committed end offset (advanced by object commits).
    pub end_offset: u64,
    pub state: StreamState,
    pub node_id: i32,
}

impl StreamRow {
    /// Convert to the engine's metadata type at the trait boundary.
    ///
    /// The engine's epoch is `u64`. A never-opened stream (epoch `-1`) is not
    /// observable through `StreamManager` (open bumps the epoch first), so
    /// the clamp never loses information in practice.
    pub fn to_stream_metadata(self) -> StreamMetadata {
        StreamMetadata {
            stream_id: self.stream_id,
            epoch: self.epoch.max(0) as u64,
            start_offset: self.start_offset,
            end_offset: self.end_offset,
            state: self.state,
            node_id: self.node_id,
        }
    }
}

/// One registered node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRow {
    pub node_id: i32,
    pub epoch: i64,
    /// Advertised HTTP address. Empty when never provided.
    pub http_address: String,
    /// Placement weight for stream assignment. Default 1.
    pub slots: u32,
    /// Advertised listener addresses of additional wire protocols, keyed by
    /// protocol name (e.g. `"kafka"`). Absent when the node does not serve
    /// that protocol.
    pub protocol_addresses: BTreeMap<String, String>,
}

/// One in-flight ownership transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingTransfer {
    pub from_node: i32,
    pub to_node: i32,
}

/// One committed stream-set object: the committing node id plus the object
/// metadata. Recovery filters on the node id.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamSetObjectRow {
    pub node_id: i32,
    pub object: s3stream::S3ObjectMetadata,
}

/// One committed single-stream object. Stored under a sorted composite key
/// (see [`MetadataState::stream_objects`]) so per-stream range queries need
/// no per-stream list.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamObjectRow {
    pub object: s3stream::S3ObjectMetadata,
}

/// Composite key for offset-sorted per-stream object indexes. Ordering
/// `(stream_id, start_offset, object_id)` makes "objects of stream S
/// overlapping [a, b)" one bounded range scan; `object_id` keeps keys unique.
pub type StreamOffsetKey = (u64, u64, u64);

/// The full replicated metadata state. Mutated only by
/// [`crate::apply::apply`] (single writer); read through O(1) forks published
/// as [`crate::view::MetadataView`]. `PartialEq` is a deep compare for tests.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetadataState {
    pub next_stream_id: u64,
    /// Primary store: one compact row per stream.
    pub streams: OrdMap<u64, StreamRow>,
    pub nodes: OrdMap<i32, NodeRow>,
    /// `get_opening_streams(node)` is a prefix range scan, not a full scan.
    /// Maintained by open/close/delete in `apply`.
    pub opening_by_node: OrdMap<(i32, u64), ()>,
    pub placed_by_node: OrdMap<(i32, u64), ()>,
    /// In-flight ownership transfers keyed by stream id.
    pub pending_transfers: OrdMap<u64, PendingTransfer>,

    /// Next numeric producer identity. Blocks are leased by
    /// `AllocateProducerIds` and never reclaimed, so an id is unique for the
    /// lifetime of the cluster regardless of which protocol handed it out.
    pub next_producer_id: u64,

    pub next_object_id: u64,
    pub prepared: OrdMap<u64, i64>,
    pub prepared_by_deadline: OrdMap<(i64, u64), ()>,
    pub stream_set_objects: OrdMap<u64, StreamSetObjectRow>,
    /// `(stream_id, range_start, object_id) → range_end`, so offset-range
    /// lookups are prefix scans instead of full-table walks.
    pub sso_ranges: OrdMap<StreamOffsetKey, u64>,
    /// `(node_id, object_id)` index: a node's stream-set objects (recovery
    /// input) as a prefix scan.
    pub sso_by_node: OrdMap<(i32, u64), ()>,
    pub stream_objects: OrdMap<StreamOffsetKey, StreamObjectRow>,
    /// `is_object_exist` / redundant-commit checks without scanning lists.
    pub stream_object_ids: OrdMap<u64, StreamOffsetKey>,
    /// Keyed by an
    /// insertion sequence to preserve `peekDestroyedObjects` order.
    pub mark_destroyed: OrdMap<u64, (u64, CompactOperations)>,
    /// `cleanDestroyedObjects` and de-dup on re-mark.
    pub destroyed_by_id: OrdMap<u64, u64>,
    /// Next FIFO sequence for `mark_destroyed`.
    pub next_destroyed_seq: u64,

    /// Sorted map so `list_kv(prefix)` (and future
    /// `start_after`/`limit` pagination) is a bounded range scan.
    pub kv: OrdMap<String, Bytes>,
    /// Sum of `key.len() + value.len()` over `kv`, maintained by `apply`.
    pub kv_bytes: u64,
}

impl MetadataState {
    pub fn new() -> Self {
        Self::default()
    }
}
