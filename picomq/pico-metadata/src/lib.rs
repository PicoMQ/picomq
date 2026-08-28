//! Control-plane state machine: deterministic `apply`, snapshots, and lock-free views.
//!
//! No consensus and no I/O in the core. `apply` is a pure function of `(state, command)`.
//! State uses persistent maps so snapshot/view forks are cheap. Indexes are maintained
//! in `apply` so queries do not scan the world.

pub mod apply;
pub mod codec;
pub mod command;
pub mod error;
pub mod lifecycle;
pub mod manager;
pub mod query;
pub mod sink;
pub mod snapshot;
pub mod state;
pub mod view;

pub use apply::apply;
pub use command::{MetadataCommand, MetadataResult};
pub use error::MetadataError;
pub use lifecycle::{MetadataLifecycle, ObjectCleaner};
pub use manager::{
    MetadataKvClient, MetadataNodeHandle, MetadataObjectManager, MetadataStreamManager,
};
pub use sink::{CommandSink, LocalSink, SinkStats, SnapshotStats};
pub use state::{MetadataState, StreamOffsetKey};
pub use view::{MetadataView, ViewPublisher};
