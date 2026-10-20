//! The replicated command set: the *only* way metadata changes.
//!
//! Every command except `RegisterNode` names its [`Proposer`]. Type codes are
//! stable (see the codec).
//!
//! Why a closed command enum (and not methods mutating state): every mutation is
//! a value that goes through the consensus log, so the state machine is
//! deterministic and replayable, duplicate delivery is handled by idempotency
//! rules in `apply` (not by callers), and adding a command is an explicit,
//! versioned protocol change. Commands carry their own timestamps (`now_ms`).
//! Must not read the clock.

use s3stream::{CommitStreamSetObjectRequest, CompactStreamObjectRequest, StreamMetadata};

/// The registered node that proposes a command. Apply rejects a proposer
/// that is unregistered, fenced, or not at its node's current epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Proposer {
    pub node_id: i32,
    pub node_epoch: i64,
}

impl Proposer {
    pub fn new(node_id: i32, node_epoch: i64) -> Self {
        Self {
            node_id,
            node_epoch,
        }
    }
}

/// A command in the replicated metadata log.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataCommand {
    /// Registers a node and assigns its epoch: the requested epoch, or one
    /// past the stored epoch when that is not higher. The new epoch fences
    /// every earlier incarnation. An empty `http_address`,
    /// `protocol_addresses` or `wal` keeps the stored value.
    RegisterNode {
        node_id: i32,
        node_epoch: i64,
        http_address: String,
        slots: u32,
        protocol_addresses: std::collections::BTreeMap<String, String>,
        wal: String,
    },

    /// Sets a registered node's placement weight.
    SetNodeSlots {
        proposer: Proposer,
        node_id: i32,
        slots: u32,
    },

    Heartbeat {
        proposer: Proposer,
    },

    /// Fences `node_id` at `node_epoch` when its heartbeat count is still
    /// `heartbeats`, the count the proposer observed. A node that heartbeated
    /// since, or moved to a new epoch, is not fenced.
    FenceNode {
        proposer: Proposer,
        node_id: i32,
        node_epoch: i64,
        heartbeats: u64,
    },

    /// Puts a stream without a live owner on `node_id`, or on the least
    /// loaded live node when `None`.
    PlaceStream {
        proposer: Proposer,
        stream_id: u64,
        node_id: Option<i32>,
    },

    /// Assigns the next stream id.
    /// The new stream starts CLOSED with epoch -1.
    CreateStream {
        proposer: Proposer,
    },

    /// Bumps the stream epoch (fencing older writers) and makes the proposer
    /// the owner. Re-open with the same (epoch, node) is idempotent.
    OpenStream {
        proposer: Proposer,
        stream_id: u64,
        epoch: i64,
    },

    /// Advances the retention watermark (start offset) of an opened stream.
    /// Only its owner may trim.
    TrimStream {
        proposer: Proposer,
        stream_id: u64,
        epoch: i64,
        new_start_offset: u64,
    },

    /// Releases ownership. Only the owner closes a stream, unless the owner
    /// is fenced (failover recovery). A later `OpenStream` with a newer epoch
    /// may move the stream to another node.
    CloseStream {
        proposer: Proposer,
        stream_id: u64,
        epoch: i64,
    },

    /// Removes a CLOSED stream and marks all of its stream objects destroyed.
    /// Only the owner deletes a stream, unless the owner is fenced.
    DeleteStream {
        proposer: Proposer,
        stream_id: u64,
        epoch: i64,
    },

    /// Leases `count` consecutive
    /// object ids with a TTL. Uncommitted ids expire via [`Self::ExpirePreparedObjects`].
    PrepareObject {
        proposer: Proposer,
        count: u32,
        ttl_ms: i64,
        now_ms: i64,
    },

    /// Atomically commits
    /// one delta-WAL upload (or stream-set compaction): the stream-set object, its
    /// split stream objects, end-offset advances, and compacted-object destruction.
    CommitStreamSetObject {
        proposer: Proposer,
        request: CommitStreamSetObjectRequest,
        now_ms: i64,
    },

    /// Commits one
    /// stream-object compaction: replacement object in, source objects marked
    /// destroyed per their `CompactOperations`.
    CompactStreamObject {
        proposer: Proposer,
        request: CompactStreamObjectRequest,
        now_ms: i64,
    },

    /// Reclaims prepared
    /// object ids whose deadline passed (driven by the leader's timer, `now_ms`
    /// rides in the command so replay is deterministic).
    ExpirePreparedObjects {
        proposer: Proposer,
        now_ms: i64,
    },

    /// Acknowledges that
    /// the object cleaner physically deleted these ids.
    CleanDestroyedObjects {
        proposer: Proposer,
        object_ids: Vec<u64>,
    },

    PutKv {
        proposer: Proposer,
        key: String,
        value: bytes::Bytes,
    },

    /// Returns the existing value
    /// when present (no overwrite).
    PutKvIfAbsent {
        proposer: Proposer,
        key: String,
        value: bytes::Bytes,
    },

    /// Returns the removed value.
    DeleteKv {
        proposer: Proposer,
        key: String,
    },

    /// Deletes only when the stored value equals `expected`. Mismatch or a
    /// missing key is redundant so revoke/expiry cannot wipe a rotated record.
    DeleteKvIfMatches {
        proposer: Proposer,
        key: String,
        expected: bytes::Bytes,
    },

    PutKvIfMatches {
        proposer: Proposer,
        key: String,
        expected: bytes::Bytes,
        value: bytes::Bytes,
    },

    /// Requests a live ownership move. The stream must be OPENED on
    /// `from_node` and `to_node` must be live. Records a pending transfer
    /// that the owning node observes and completes after draining and
    /// closing.
    TransferStream {
        proposer: Proposer,
        stream_id: u64,
        from_node: i32,
        to_node: i32,
    },

    /// Finishes a pending transfer. The stream must be CLOSED at `epoch`.
    /// Re-points the stream at the transfer target and clears the pending
    /// entry.
    CompleteTransfer {
        proposer: Proposer,
        stream_id: u64,
        epoch: i64,
    },

    /// Batched create. Assigns `count` consecutive stream ids and returns
    /// the first.
    CreateStreams {
        proposer: Proposer,
        count: u32,
    },

    /// Leases `count` consecutive numeric producer ids and returns the
    /// first. Ids are never reclaimed, so an id stays unique for the
    /// cluster's lifetime.
    AllocateProducerIds {
        proposer: Proposer,
        count: u32,
    },
}

impl MetadataCommand {
    pub fn type_code(&self) -> u8 {
        match self {
            MetadataCommand::CreateStream { .. } => 1,
            MetadataCommand::OpenStream { .. } => 2,
            MetadataCommand::TrimStream { .. } => 3,
            MetadataCommand::CloseStream { .. } => 4,
            MetadataCommand::DeleteStream { .. } => 5,
            MetadataCommand::PrepareObject { .. } => 6,
            MetadataCommand::CommitStreamSetObject { .. } => 7,
            MetadataCommand::CompactStreamObject { .. } => 8,
            MetadataCommand::ExpirePreparedObjects { .. } => 9,
            MetadataCommand::RegisterNode { .. } => 10,
            MetadataCommand::CleanDestroyedObjects { .. } => 11,
            MetadataCommand::PutKv { .. } => 12,
            MetadataCommand::PutKvIfAbsent { .. } => 13,
            MetadataCommand::DeleteKv { .. } => 14,
            MetadataCommand::TransferStream { .. } => 15,
            MetadataCommand::CompleteTransfer { .. } => 16,
            MetadataCommand::CreateStreams { .. } => 17,
            MetadataCommand::PlaceStream { .. } => 18,
            MetadataCommand::DeleteKvIfMatches { .. } => 19,
            MetadataCommand::AllocateProducerIds { .. } => 20,
            MetadataCommand::PutKvIfMatches { .. } => 21,
            MetadataCommand::Heartbeat { .. } => 22,
            MetadataCommand::FenceNode { .. } => 23,
            MetadataCommand::SetNodeSlots { .. } => 24,
        }
    }

    /// The node proposing this command. `None` only for `RegisterNode`, which
    /// is how a node gets an identity.
    pub fn proposer(&self) -> Option<Proposer> {
        match self {
            MetadataCommand::RegisterNode { .. } => None,
            MetadataCommand::SetNodeSlots { proposer, .. }
            | MetadataCommand::Heartbeat { proposer }
            | MetadataCommand::FenceNode { proposer, .. }
            | MetadataCommand::PlaceStream { proposer, .. }
            | MetadataCommand::CreateStream { proposer }
            | MetadataCommand::OpenStream { proposer, .. }
            | MetadataCommand::TrimStream { proposer, .. }
            | MetadataCommand::CloseStream { proposer, .. }
            | MetadataCommand::DeleteStream { proposer, .. }
            | MetadataCommand::PrepareObject { proposer, .. }
            | MetadataCommand::CommitStreamSetObject { proposer, .. }
            | MetadataCommand::CompactStreamObject { proposer, .. }
            | MetadataCommand::ExpirePreparedObjects { proposer, .. }
            | MetadataCommand::CleanDestroyedObjects { proposer, .. }
            | MetadataCommand::PutKv { proposer, .. }
            | MetadataCommand::PutKvIfAbsent { proposer, .. }
            | MetadataCommand::DeleteKv { proposer, .. }
            | MetadataCommand::DeleteKvIfMatches { proposer, .. }
            | MetadataCommand::PutKvIfMatches { proposer, .. }
            | MetadataCommand::TransferStream { proposer, .. }
            | MetadataCommand::CompleteTransfer { proposer, .. }
            | MetadataCommand::CreateStreams { proposer, .. }
            | MetadataCommand::AllocateProducerIds { proposer, .. } => Some(*proposer),
        }
    }
}

/// The result of applying one command.
///
/// `Long`, `Integer`, `StreamMetadata`, `CommitStreamSetObjectResponse`, or
/// `byte[]`. This enum is the typed version of that union.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataResult {
    Unit,
    /// `CreateStream` → assigned stream id.`PrepareObject` → first leased object
    Id(u64),
    Count(u64),
    Stream(StreamMetadata),
    /// `PutKVIfAbsent` → existing-or-inserted value.`DeleteKV` → removed value
    /// (`None` if the key was absent).
    Value(Option<bytes::Bytes>),
}
