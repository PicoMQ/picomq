//! Topics over logs on the s3stream engine (no HTTP, no metadata backend choice).

pub mod auth;
mod codec;
pub mod error;
pub mod failover;
pub mod group;
mod leadership;
pub mod log;
pub mod node;
pub mod ownership;
pub mod partition;
pub mod producers;
pub mod record;
pub mod retention;
pub mod service;
pub mod streams;
mod timestamps;
pub mod topic;
pub mod topics;
pub mod transfer;
pub mod types;
pub mod waiter;
pub mod wal;

pub use auth::{KvTokenStore, TOKEN_KEY_PREFIX, TokenService};
pub use error::{ErrorKind, ServiceError};
pub use group::{
    CommittedOffset, GroupCoordinator, GroupDescription, GroupError, GroupState, JoinInput,
    JoinMember, JoinOutcome, JoinProtocol, Joined, ListedGroup, MemberDescription, MemberFence,
    MemberRole, Membership, OffsetCommit, StreamName, SyncInput, SyncOutcome,
};
pub use log::Log;
pub use node::{NodeConfig, PicoNode};
pub use ownership::{MetadataOwnershipService, OwnershipService};
pub use partition::Partition;
pub use picomq_schema::{
    Batch as SchemaBatch, Record as SchemaRecord, Registry as SchemaRegistry, SchemaFormat,
    SchemaStore,
};
pub use producers::ProducerDecision;
pub use record::{LogRecord, StreamRecord};
pub use retention::Retention;
pub use service::S3StreamService;
pub use streams::Streams;
pub use topic::{ClosedBy, Topic, TopicId, TopicName};
pub use topics::{Topics, partition_log};
pub use transfer::TransferWatcher;
pub use types::{
    AppendBatchResult, AppendCommand, AppendResult, BatchReadResult, CloseResult, CreateCommand,
    CreateResult, LogOffsets, LogRead, MAX_NEW_PARTITIONS, NodeMeta, NumericProducer, OffsetToken,
    Owner, ReadResult, StreamBatch, StreamWatermarks, SubmittedBatchAppend, TopicList, TopicMeta,
    UpdateTopicCommand,
};
pub use waiter::StreamWaiterRegistry;
