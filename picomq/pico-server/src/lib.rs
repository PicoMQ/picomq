//! Named-stream service over the s3stream engine (no HTTP, no metadata backend choice).

pub mod auth;
pub mod error;
pub mod framing;
pub mod node;
pub mod ownership;
pub mod registry;
pub mod service;
pub mod transfer;
pub mod types;
pub mod waiter;

pub use auth::{KvTokenStore, TokenService, TOKEN_KEY_PREFIX};
pub use error::{ErrorKind, ServiceError};
pub use node::{NodeConfig, PicoNode};
pub use ownership::{MetadataOwnershipService, OwnershipService};
pub use service::S3StreamService;
pub use transfer::TransferWatcher;
pub use types::{
    AppendCommand, AppendResult, CloseResult, CreateCommand, CreateResult, NodeMeta, OffsetToken,
    Owner, ReadResult, StreamList, StreamMeta, StreamRecord,
};
pub use waiter::StreamWaiterRegistry;
