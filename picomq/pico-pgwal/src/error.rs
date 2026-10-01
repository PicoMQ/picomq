use s3stream_wal::WalError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sql: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("codec: {0}")]
    Codec(#[from] s3stream_codec::CodecError),
    #[error("invalid wal uri: {0}")]
    Uri(String),
    #[error("unsafe postgres: {0}")]
    Unsafe(String),
    #[error("corrupt wal: {0}")]
    Corrupt(String),
    #[error("fenced: epoch {epoch} of node {node_id} was superseded")]
    Fenced { node_id: u32, epoch: u64 },
}

impl From<Error> for WalError {
    fn from(error: Error) -> Self {
        match error {
            Error::Sql(error) => WalError::Io(std::io::Error::other(error)),
            Error::Codec(error) => WalError::Unmarshal(error),
            Error::Fenced { node_id, epoch } => WalError::Fenced {
                node_id,
                our_epoch: epoch,
            },
            other => WalError::Recovery(other.to_string()),
        }
    }
}
