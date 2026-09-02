//! Service error surface.
//!
//! The HTTP layers map kinds to status codes. Errors carry structured
//! companions (`next_offset`, `closed`, producer epoch and expected/received
//! seq) so handlers never parse messages.

use crate::types::OffsetToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    NotFound,
    Conflict,
    Closed,
    BadRequest,
    /// A record batch that does not parse as Kafka RecordBatch v2 (bad
    /// magic, truncated, CRC mismatch). Kafka: `CORRUPT_MESSAGE`.
    CorruptBatch,
    /// Well-formed records the stream's bound schema rejects. Kafka:
    /// `INVALID_RECORD`.
    SchemaViolation,
    Fenced,
    SequenceGap,
    MatchFailed,
    Durability,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct ServiceError {
    pub kind: ErrorKind,
    pub next_offset: Option<OffsetToken>,
    pub closed: bool,
    pub producer_epoch: Option<u64>,
    pub expected_seq: Option<u64>,
    pub received_seq: Option<u64>,
    pub message: String,
}

impl ServiceError {
    pub fn kind(kind: ErrorKind) -> Self {
        Self {
            kind,
            next_offset: None,
            closed: false,
            producer_epoch: None,
            expected_seq: None,
            received_seq: None,
            message: format!("{kind:?}"),
        }
    }

    pub fn at(kind: ErrorKind, next_offset: OffsetToken, closed: bool) -> Self {
        Self {
            next_offset: Some(next_offset),
            closed,
            ..Self::kind(kind)
        }
    }

    pub fn with_message(
        kind: ErrorKind,
        next_offset: Option<OffsetToken>,
        closed: bool,
        message: impl Into<String>,
    ) -> Self {
        Self {
            next_offset,
            closed,
            message: message.into(),
            ..Self::kind(kind)
        }
    }

    pub fn fenced(current_epoch: u64) -> Self {
        Self {
            producer_epoch: Some(current_epoch),
            message: "Stale producer epoch".into(),
            ..Self::kind(ErrorKind::Fenced)
        }
    }

    pub fn sequence_gap(expected: u64, received: u64) -> Self {
        Self {
            expected_seq: Some(expected),
            received_seq: Some(received),
            message: "Producer sequence gap".into(),
            ..Self::kind(ErrorKind::SequenceGap)
        }
    }

    pub fn durability(cause: impl std::fmt::Display) -> Self {
        Self {
            message: format!("append not durable: {cause}"),
            ..Self::kind(ErrorKind::Durability)
        }
    }
}

impl From<s3stream::Error> for ServiceError {
    fn from(e: s3stream::Error) -> Self {
        Self::with_message(ErrorKind::BadRequest, None, false, e.to_string())
    }
}
