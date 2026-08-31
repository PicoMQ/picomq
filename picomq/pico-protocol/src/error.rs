#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    NotFound,
    Exists,
    Closed,
    Conflict,
    StaleEpoch,
    Unauthenticated,
    PermissionDenied,
    OffsetGone,
    BadRequest,
    Transport,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    pub status: u16,
    pub kind: ErrorKind,
    pub code: String,
    pub message: Option<String>,
    pub next: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct CodecError {
    pub message: String,
}

impl CodecError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
