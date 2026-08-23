//! Client errors.
//!
//! One error type carries both protocols. The protocol-specific mapping
//! happens where the response is read, so callers match on [`ErrorKind`]
//! instead of on per-protocol errors.

/// What went wrong, at the level a caller can act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// 404.
    NotFound,
    /// 409 on create.
    Exists,
    /// 409 with a closed marker.
    Closed,
    /// 409 from a producer sequence gap or a failed CAS.
    Conflict,
    /// 403: a newer producer epoch fenced this one.
    StaleEpoch,
    /// 401: no credential, or one the server does not accept.
    Unauthenticated,
    /// 403 from scope checks. Distinct from [`StaleEpoch`]
    /// (`ErrorKind::StaleEpoch`): a fencing 403 is not an auth failure.
    PermissionDenied,
    /// 410: the requested position was trimmed away.
    OffsetGone,
    /// 400.
    BadRequest,
    /// The request never got an answer (connect/read failure, timeout).
    Transport,
    /// Any other status, including 5xx.
    Other,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{}", describe(self))]
pub struct ClientError {
    pub kind: ErrorKind,
    pub status: u16,
    /// Server error code (`error` in the Pico error body) or a synthetic one.
    pub code: String,
    pub message: Option<String>,
    /// `next_seq` / `Stream-Next-Offset` when the server reported where the
    /// stream currently ends.
    pub next: Option<String>,
}

impl ClientError {
    pub fn transport(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Transport,
            status: 0,
            code: "transport".to_owned(),
            message: Some(message.into()),
            next: None,
        }
    }

    /// An operation this protocol does not have (DS has no stream listing).
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Other,
            status: 0,
            code: "unsupported".to_owned(),
            message: Some(message.into()),
            next: None,
        }
    }

    pub fn new(status: u16, kind: ErrorKind, code: impl Into<String>) -> Self {
        Self {
            kind,
            status,
            code: code.into(),
            message: None,
            next: None,
        }
    }

    pub fn with_message(mut self, message: Option<String>) -> Self {
        self.message = message.filter(|m| !m.is_empty());
        self
    }

    pub fn with_next(mut self, next: Option<String>) -> Self {
        self.next = next;
        self
    }

    /// A retryable failure: transport errors, 429, and 5xx.
    pub fn retryable(&self) -> bool {
        self.kind == ErrorKind::Transport
            || self.status == 429
            || (500..=599).contains(&self.status)
    }
}

fn describe(error: &ClientError) -> String {
    let mut text = match error.status {
        0 => error.code.clone(),
        status => format!("{} ({})", error.code, status),
    };
    if let Some(message) = &error.message {
        text.push_str(": ");
        text.push_str(message);
    }
    if let Some(next) = &error.next {
        text.push_str(&format!(" [next={next}]"));
    }
    text
}

impl From<reqwest::Error> for ClientError {
    fn from(error: reqwest::Error) -> Self {
        Self::transport(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ClientError>;
