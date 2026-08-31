pub use picomq_protocol::ErrorKind;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{}", describe(self))]
pub struct ClientError {
    pub kind: ErrorKind,
    pub status: u16,
    pub code: String,
    pub message: Option<String>,
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

impl From<picomq_protocol::WireError> for ClientError {
    fn from(error: picomq_protocol::WireError) -> Self {
        Self {
            kind: error.kind,
            status: error.status,
            code: error.code,
            message: error.message,
            next: error.next,
        }
    }
}

pub type Result<T> = std::result::Result<T, ClientError>;
