//! Metadata-plane errors.
//!
//! Numeric codes are kept stable so logs, dashboards, and the RPC error
//! mapping stay recognizable. The engine-facing mapping is
//! [`MetadataError::to_stream_error`].
//!
//! Every failure is a value returned from [`crate::apply::apply`]. Variants
//! carry structured fields (ids, epochs) instead of only formatted strings so the
//! engine-facing mapping can produce `s3stream::Error::Fenced { stream_id, epoch }`
//! etc. without re-parsing messages.

use s3stream::Error as StreamError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetadataError {
    #[error("stream {stream_id} not found")]
    StreamNotExist { stream_id: u64 },

    #[error("stream {stream_id} is not closed")]
    StreamNotClosed { stream_id: u64 },

    #[error("stream {stream_id} fenced: {message}")]
    StreamFenced {
        stream_id: u64,
        epoch: i64,
        message: String,
    },

    #[error("stream {stream_id} expired epoch: {message}")]
    ExpiredEpoch {
        stream_id: u64,
        epoch: i64,
        message: String,
    },

    #[error("node {node_id} epoch mismatch: {message}")]
    NodeEpochMismatch { node_id: i32, message: String },

    #[error("redundant operation: {message}")]
    Redundant { message: String },

    #[error("no live node: {message}")]
    NoLiveNode { message: String },

    /// The metadata store could not be reached. The command may or may not
    /// have landed.
    #[error("metadata store unavailable: {message}")]
    Unavailable { message: String },

    #[error("unexpected: {message}")]
    Unexpected { message: String },
}

impl MetadataError {
    pub fn code(&self) -> u8 {
        match self {
            MetadataError::StreamNotExist { .. } => 1,
            MetadataError::StreamNotClosed { .. } => 2,
            MetadataError::StreamFenced { .. } => 3,
            MetadataError::ExpiredEpoch { .. } => 4,
            MetadataError::NodeEpochMismatch { .. } => 5,
            MetadataError::Redundant { .. } => 6,
            MetadataError::NoLiveNode { .. } => 7,
            MetadataError::Unavailable { .. } => 8,
            MetadataError::Unexpected { .. } => 99,
        }
    }

    pub fn is_redundant(&self) -> bool {
        matches!(self, MetadataError::Redundant { .. })
    }

    /// Map to the engine's error type at the `StreamManager`/`ObjectManager`
    /// boundary.
    ///
    /// STREAM_NOT_EXIST → `NotExist`. STREAM_FENCED / EXPIRED_EPOCH →
    /// `EXPIRED_STREAM_EPOCH` (→ `Fenced`). Everything else → `UNEXPECTED`.
    /// STREAM_NOT_CLOSED maps to `Unexpected` because the engine's
    /// `StreamError` models "already closed", not "not yet closed".
    pub fn to_stream_error(&self) -> StreamError {
        match self {
            MetadataError::StreamNotExist { stream_id } => StreamError::NotExist {
                stream_id: *stream_id,
            },
            MetadataError::StreamFenced {
                stream_id, epoch, ..
            }
            | MetadataError::ExpiredEpoch {
                stream_id, epoch, ..
            } => StreamError::Fenced {
                stream_id: *stream_id,
                epoch: (*epoch).max(0) as u64,
            },
            other => StreamError::Unexpected(other.to_string()),
        }
    }
}
