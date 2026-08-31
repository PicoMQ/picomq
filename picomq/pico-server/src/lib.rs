//! Named-stream service over the s3stream engine (no HTTP, no metadata backend choice).

pub mod auth;
pub mod error;
pub mod framing;
pub mod node;
pub mod ownership;
pub mod producer;
pub mod registry;
pub mod service;
pub mod transfer;
pub mod types;
pub mod waiter;

pub use auth::{KvTokenStore, TokenService, TOKEN_KEY_PREFIX};
pub use error::{ErrorKind, ServiceError};
pub use node::{NodeConfig, PicoNode};
pub use ownership::{MetadataOwnershipService, OwnershipService};
pub use picomq_schema::{
    Batch as SchemaBatch, Record as SchemaRecord, Registry as SchemaRegistry, SchemaFormat,
    SchemaStore,
};
pub use service::{is_reserved_name, S3StreamService};
pub use transfer::TransferWatcher;
pub use types::{
    AppendBatchCommand, AppendBatchResult, AppendCommand, AppendResult, BatchReadResult, BatchSpan,
    CloseResult, CreateCommand, CreateResult, NodeMeta, NumericProducer, OffsetToken, Owner,
    ReadResult, StreamBatch, StreamConfig, StreamList, StreamMeta, StreamRecord, StreamWatermarks,
    SubmittedBatchAppend, UpdateStreamCommand,
};
pub use waiter::StreamWaiterRegistry;
