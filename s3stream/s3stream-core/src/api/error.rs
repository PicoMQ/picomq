use thiserror::Error;

/// Errors surfaced by the stream API.
///
#[derive(Debug, Error)]
pub enum StreamError {
    #[error("stream {stream_id} fenced: epoch {epoch} expired")]
    Fenced { stream_id: u64, epoch: u64 },

    #[error("stream {stream_id} closed")]
    Closed { stream_id: u64 },

    #[error("stream {stream_id} does not exist")]
    NotExist { stream_id: u64 },

    #[error(
        "stream {stream_id} offset out of range: requested [{start},{end}), valid [{valid_start},{valid_end})"
    )]
    OffsetOutOfRange {
        stream_id: u64,
        start: u64,
        end: u64,
        valid_start: u64,
        valid_end: u64,
    },

    #[error("fast read fail fast")]
    FastReadFailFast,

    /// Backpressure: engine over capacity (WAL or LogCache full beyond backoff).
    #[error("over capacity: {reason}")]
    OverCapacity { reason: String },

    #[error("object {object_id} does not exist")]
    ObjectNotExist { object_id: u64 },

    #[error("blocks not continuous")]
    BlockNotContinuous,

    #[error("wal: {0}")]
    Wal(#[from] s3stream_wal::WalError),

    #[error("object: {0}")]
    Object(#[from] s3stream_object::ObjectError),

    #[error("unexpected: {0}")]
    Unexpected(String),
}
