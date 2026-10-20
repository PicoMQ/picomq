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

impl WireError {
    /// An error from a control route, `/_streams` or `/_schemas`, whose body
    /// is `{"error": "<message>"}` on either protocol.
    pub fn control(status: u16, body: &str) -> Self {
        let message = serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|node| node["error"].as_str().map(str::to_owned))
            .or_else(|| Some(body.to_owned()).filter(|body| !body.is_empty()));
        let (kind, code) = match status {
            400 => (ErrorKind::BadRequest, "bad_request"),
            401 => (ErrorKind::Unauthenticated, "unauthenticated"),
            403 => (ErrorKind::PermissionDenied, "permission_denied"),
            404 => (ErrorKind::NotFound, "not_found"),
            409 => (ErrorKind::Conflict, "conflict"),
            _ => (ErrorKind::Other, ""),
        };

        Self {
            status,
            kind,
            code: if code.is_empty() {
                format!("http_{status}")
            } else {
                code.to_owned()
            },
            message,
            next: None,
        }
    }
}
