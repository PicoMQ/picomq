//! The s3stream engine.
//!
//! Specification: `specification/upload-protocol.md` for the pipeline, `specification/wal-protocol.md` for
//! durability semantics.
//!
//! Composition (see `s3stream` facade crate for wiring):
//!
//! ```text

pub mod api;
pub mod cache;
pub mod compact;
pub mod context;
pub mod failover;
pub mod index;
pub mod manager;
pub mod memory;
pub mod metrics;
pub mod storage;
pub mod stream;
pub mod stream_client;
pub mod throttle;
pub mod version;

pub use api::{
    AppendResult, Client, CreateStreamOptions, FetchResult, KVClient, LinkRecordDecoder,
    OpenStreamOptions, RecordBatch, Stream, StreamClient, StreamError,
};
pub use cache::{EventListener, RequestCommitEvent, SnapshotReadCache};
pub use failover::{
    DefaultFailoverFactory, Failover, FailoverFactory, FailoverRequest, FailoverResponse,
    ForceCloseStorageFailureHandler, HaltStorageFailureHandler, LogStorageFailureHandler,
    StorageFailureHandler, StorageFailureHandlerChain, WalRecover,
};
pub use index::LocalStreamRangeIndexCache;
pub use manager::{
    CommitStreamSetObjectHook, HookedObjectManager, HookedStreamManager, ObjectManager,
    StreamCloseHook, StreamManager, StreamMetadata, StreamMetadataListener,
    StreamMetadataListenerHandle,
};
pub use storage::Storage;
pub use throttle::{
    BandwidthLimiter, MeteredBandwidthLimiter, NetworkBandwidthMode, ThrottledObjectStorage,
    build_network_limiters,
};
pub use version::Version;
