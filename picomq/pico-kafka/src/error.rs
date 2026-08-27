use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame too large: {size} > {max}")]
    FrameTooLarge { size: usize, max: usize },
    #[error("invalid frame size: {0}")]
    InvalidFrameSize(i32),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("unsupported api key {api_key} version {api_version}")]
    UnsupportedApi { api_key: i16, api_version: i16 },
}
