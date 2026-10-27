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
    pub stored_bytes: u64,
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
    /// protocol name (e.g. `"kafka"`).
    pub protocol_addresses: BTreeMap<String, String>,
    pub wal: String,
    pub heartbeats: u64,
    pub fenced: bool,
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
    /// Bytes of each of `object.offset_ranges`, in the same order.
    pub range_sizes: Vec<u64>,
    /// The latest time any record in the object was written.
    pub last_write_ms: i64,
}

/// One committed single-stream object. Stored under a sorted composite key
/// (see [`MetadataState::stream_objects`]) so per-stream range queries need
/// no per-stream list.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamObjectRow {
    pub object: s3stream::S3ObjectMetadata,
    /// The latest time any record in the object was written.
    pub last_write_ms: i64,
}

/// One committed object's slice of a stream, as retention sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectSpan {
    pub start_offset: u64,
    pub end_offset: u64,
    pub bytes: u64,
    pub last_write_ms: i64,
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
    pub(crate) next_stream_id: u64,
    /// Primary store: one compact row per stream.
    pub(crate) streams: OrdMap<u64, StreamRow>,
    pub(crate) nodes: OrdMap<i32, NodeRow>,
    /// `get_opening_streams(node)` is a prefix range scan, not a full scan.
    /// The three node indexes are derived from `streams` and maintained only
    /// by [`MetadataState::put_stream`] and [`MetadataState::remove_stream`].
    pub(crate) opening_by_node: OrdMap<(i32, u64), ()>,
    pub(crate) placed_by_node: OrdMap<(i32, u64), ()>,
    /// Streams opened or placed on each node, so placement reads a node's
    /// load without walking its streams.
    pub(crate) streams_by_node: OrdMap<i32, u64>,
    /// In-flight ownership transfers keyed by stream id.
    pub(crate) pending_transfers: OrdMap<u64, PendingTransfer>,
    /// `(stream_id, epoch) → first offset written at that epoch`, recorded on
    /// open and pruned by trim.
    pub(crate) epochs: OrdMap<(u64, i64), u64>,

    /// Next numeric producer identity. Blocks are leased by
    /// `AllocateProducerIds` and never reclaimed, so an id is unique for the
    /// lifetime of the cluster regardless of which protocol handed it out.
    pub(crate) next_producer_id: u64,

    pub(crate) next_object_id: u64,
    pub(crate) prepared: OrdMap<u64, i64>,
    pub(crate) prepared_by_deadline: OrdMap<(i64, u64), ()>,
    pub(crate) stream_set_objects: OrdMap<u64, StreamSetObjectRow>,
    /// `(stream_id, range_start, object_id) → (range_end, bytes)`, so
    /// offset-range lookups are prefix scans instead of full-table walks.
    pub(crate) sso_ranges: OrdMap<StreamOffsetKey, (u64, u64)>,
    /// `(node_id, object_id)` index: a node's stream-set objects (recovery
    /// input) as a prefix scan.
    pub(crate) sso_by_node: OrdMap<(i32, u64), ()>,
    pub(crate) stream_objects: OrdMap<StreamOffsetKey, StreamObjectRow>,
    /// `is_object_exist` / redundant-commit checks without scanning lists.
    pub(crate) stream_object_ids: OrdMap<u64, StreamOffsetKey>,
    /// Keyed by an
    /// insertion sequence to preserve `peekDestroyedObjects` order.
    pub(crate) mark_destroyed: OrdMap<u64, (u64, CompactOperations)>,
    /// `cleanDestroyedObjects` and de-dup on re-mark.
    pub(crate) destroyed_by_id: OrdMap<u64, u64>,
    /// Next FIFO sequence for `mark_destroyed`.
    pub(crate) next_destroyed_seq: u64,

    /// Sorted map so `list_kv(prefix)` (and future
    /// `start_after`/`limit` pagination) is a bounded range scan.
    pub(crate) kv: OrdMap<String, Bytes>,
    /// Sum of `key.len() + value.len()` over `kv`.
    pub(crate) kv_bytes: u64,
    pub(crate) stored_bytes: u64,
}

impl MetadataState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_stream_id(&self) -> u64 {
        self.next_stream_id
    }

    pub fn streams(&self) -> &OrdMap<u64, StreamRow> {
        &self.streams
    }

    pub fn nodes(&self) -> &OrdMap<i32, NodeRow> {
        &self.nodes
    }

    pub fn opening_by_node(&self) -> &OrdMap<(i32, u64), ()> {
        &self.opening_by_node
    }

    pub fn streams_by_node(&self) -> &OrdMap<i32, u64> {
        &self.streams_by_node
    }

    pub fn pending_transfers(&self) -> &OrdMap<u64, PendingTransfer> {
        &self.pending_transfers
    }

    pub fn prepared(&self) -> &OrdMap<u64, i64> {
        &self.prepared
    }

    pub fn stream_set_objects(&self) -> &OrdMap<u64, StreamSetObjectRow> {
        &self.stream_set_objects
    }

    pub fn stream_objects(&self) -> &OrdMap<StreamOffsetKey, StreamObjectRow> {
        &self.stream_objects
    }

    pub fn mark_destroyed(&self) -> &OrdMap<u64, (u64, CompactOperations)> {
        &self.mark_destroyed
    }

    pub fn next_destroyed_seq(&self) -> u64 {
        self.next_destroyed_seq
    }

    pub fn kv(&self) -> &OrdMap<String, Bytes> {
        &self.kv
    }

    pub fn kv_bytes(&self) -> u64 {
        self.kv_bytes
    }

    pub fn stored_bytes(&self) -> u64 {
        self.stored_bytes
    }

    pub(crate) fn put_stream(&mut self, row: StreamRow) {
        let stored_bytes = self
            .streams
            .get(&row.stream_id)
            .map_or(0, |current| current.stored_bytes);
        let row = StreamRow {
            stored_bytes,
            ..row
        };

        let previous = self.streams.insert(row.stream_id, row);
        if previous
            .is_some_and(|previous| (previous.state, previous.node_id) == (row.state, row.node_id))
        {
            return;
        }

        if let Some(previous) = previous {
            self.unindex(&previous);
        }
        self.index(&row);
    }

    pub(crate) fn remove_stream(&mut self, stream_id: u64) {
        if let Some(previous) = self.streams.remove(&stream_id) {
            self.unindex(&previous);
        }
    }

    pub(crate) fn put_prepared(&mut self, object_id: u64, deadline: i64) {
        self.prepared.insert(object_id, deadline);
        self.prepared_by_deadline.insert((deadline, object_id), ());
    }

    pub(crate) fn remove_prepared(&mut self, object_id: u64) {
        if let Some(deadline) = self.prepared.remove(&object_id) {
            self.prepared_by_deadline.remove(&(deadline, object_id));
        }
    }

    pub(crate) fn put_stream_set_object(&mut self, row: StreamSetObjectRow) {
        let object_id = row.object.object_id;
        self.remove_stream_set_object(object_id);

        for (range, size) in row.object.offset_ranges.iter().zip(&row.range_sizes) {
            self.sso_ranges.insert(
                (range.stream_id, range.start_offset, object_id),
                (range.end_offset, *size),
            );
            self.add_stream_bytes(range.stream_id, *size);
        }
        self.stored_bytes += row.object.object_size;

        self.sso_by_node.insert((row.node_id, object_id), ());
        self.stream_set_objects.insert(object_id, row);
    }

    pub(crate) fn remove_stream_set_object(&mut self, object_id: u64) {
        let Some(row) = self.stream_set_objects.remove(&object_id) else {
            return;
        };

        for (range, size) in row.object.offset_ranges.iter().zip(&row.range_sizes) {
            self.sso_ranges
                .remove(&(range.stream_id, range.start_offset, object_id));
            self.remove_stream_bytes(range.stream_id, *size);
        }
        self.stored_bytes -= row.object.object_size;

        self.sso_by_node.remove(&(row.node_id, object_id));
    }

    pub(crate) fn put_stream_object(&mut self, row: StreamObjectRow) {
        let object_id = row.object.object_id;
        self.remove_stream_object(object_id);

        let range = &row.object.offset_ranges[0];
        let key = (range.stream_id, range.start_offset, object_id);
        self.add_stream_bytes(range.stream_id, row.object.object_size);
        self.stored_bytes += row.object.object_size;

        self.stream_object_ids.insert(object_id, key);
        self.stream_objects.insert(key, row);
    }

    pub(crate) fn remove_stream_object(&mut self, object_id: u64) {
        let Some(key) = self.stream_object_ids.remove(&object_id) else {
            return;
        };
        let Some(row) = self.stream_objects.remove(&key) else {
            return;
        };

        self.remove_stream_bytes(key.0, row.object.object_size);
        self.stored_bytes -= row.object.object_size;
    }

    fn add_stream_bytes(&mut self, stream_id: u64, bytes: u64) {
        if let Some(row) = self.streams.get_mut(&stream_id) {
            row.stored_bytes += bytes;
        }
    }

    fn remove_stream_bytes(&mut self, stream_id: u64, bytes: u64) {
        if let Some(row) = self.streams.get_mut(&stream_id) {
            row.stored_bytes -= bytes;
        }
    }

    pub(crate) fn put_destroyed(&mut self, seq: u64, object_id: u64, op: CompactOperations) {
        self.mark_destroyed.insert(seq, (object_id, op));
        self.destroyed_by_id.insert(object_id, seq);
    }

    pub(crate) fn remove_destroyed(&mut self, object_id: u64) {
        if let Some(seq) = self.destroyed_by_id.remove(&object_id) {
            self.mark_destroyed.remove(&seq);
        }
    }

    pub(crate) fn put_kv(&mut self, key: &str, value: Bytes) {
        self.kv_bytes += (key.len() + value.len()) as u64;
        if let Some(old) = self.kv.insert(key.to_owned(), value) {
            self.kv_bytes -= (key.len() + old.len()) as u64;
        }
    }

    pub(crate) fn remove_kv(&mut self, key: &str) -> Option<Bytes> {
        let old = self.kv.remove(key)?;
        self.kv_bytes -= (key.len() + old.len()) as u64;
        Some(old)
    }

    fn index(&mut self, row: &StreamRow) {
        let key = (row.node_id, row.stream_id);
        match row.state {
            StreamState::Opened => self.opening_by_node.insert(key, ()),
            StreamState::Closed if row.node_id != -1 => self.placed_by_node.insert(key, ()),
            StreamState::Closed => return,
        };

        let count = self.streams_by_node.get(&row.node_id).copied().unwrap_or(0);
        self.streams_by_node.insert(row.node_id, count + 1);
    }

    fn unindex(&mut self, row: &StreamRow) {
        let key = (row.node_id, row.stream_id);
        let opened = self.opening_by_node.remove(&key).is_some();
        let placed = self.placed_by_node.remove(&key).is_some();
        if !opened && !placed {
            return;
        }

        match self.streams_by_node.get(&row.node_id).copied() {
            Some(count) if count > 1 => {
                self.streams_by_node.insert(row.node_id, count - 1);
            }
            _ => {
                self.streams_by_node.remove(&row.node_id);
            }
        }
    }
}
