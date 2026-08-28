//! The write boundary: where commands enter the replicated log.
//!
//! The same manager/KV implementations work over any log:
//!
//! - [`LocalSink`]: apply in-process (single node, tests).
//! - `SqlSink` (crate `pico-sql`): the durable log is an append-only SQL
//!   table. Concurrent proposes coalesce into one log row, which is what
//!   keeps `CommitStreamSetObject` storms cheap.
//!
//! Delivery concerns (leader forwarding, the SQL sink's append
//! conflict-retry) belong to the sink implementation, never to callers.

use std::sync::Arc;

use async_trait::async_trait;

use crate::apply::apply;
use crate::command::{MetadataCommand, MetadataResult};
use crate::error::MetadataError;
use crate::state::MetadataState;
use crate::view::{MetadataView, ViewPublisher};

/// Outcome of a committed proposal: the log index it applied at, plus the
/// command's result. The index feeds `ViewPublisher::wait_applied` for
/// read-your-writes.
#[derive(Debug, Clone, PartialEq)]
pub struct Proposed {
    pub applied_index: u64,
    pub result: MetadataResult,
}

/// Where commands are proposed and (once committed) applied.
///
/// Contract: `propose` returns only after the command is durably committed,
/// applied, AND its resulting view published. A caller that reads
/// `views.load()` right after a successful propose sees its own write.
/// `Err(Redundant)` means "already applied". Whether that is success is the
/// commit commands).
#[async_trait]
pub trait CommandSink: Send + Sync {
    async fn propose(&self, command: MetadataCommand) -> Result<Proposed, MetadataError>;
}

/// In-process sink: commands are applied serially under a mutex and every
/// successful apply publishes a view. No durability. State lives and dies with
/// the process (recovery is the raft/snapshot layer's job).
///
/// Failed applies do NOT consume an index. `applied_index` counts
/// state-changing commands only. (Redundant errors also changed nothing, by the
/// atomic-apply guarantee.) A raft sink numbers by log index instead. Callers
/// only rely on monotonicity.
pub struct LocalSink {
    state: tokio::sync::Mutex<(MetadataState, u64)>,
    views: Arc<ViewPublisher>,
}

impl LocalSink {
    /// Fresh empty state. Returns the sink and the publisher readers use.
    pub fn new() -> (Self, Arc<ViewPublisher>) {
        Self::with_state(MetadataState::new(), 0)
    }

    /// Start from a restored state (e.g. [`crate::snapshot::decode`]) at
    /// `applied_index`.
    pub fn with_state(state: MetadataState, applied_index: u64) -> (Self, Arc<ViewPublisher>) {
        let views = Arc::new(ViewPublisher::with_view(MetadataView {
            applied_index,
            state: state.clone(),
        }));
        let sink = Self {
            state: tokio::sync::Mutex::new((state, applied_index)),
            views: views.clone(),
        };
        (sink, views)
    }
}

#[async_trait]
impl CommandSink for LocalSink {
    async fn propose(&self, command: MetadataCommand) -> Result<Proposed, MetadataError> {
        let mut guard = self.state.lock().await;
        let (state, applied_index) = &mut *guard;
        let result = apply(state, &command)?;
        *applied_index += 1;
        // Publish while still holding the lock so views appear in apply order.
        self.views.publish(MetadataView {
            applied_index: *applied_index,
            state: state.clone(),
        });
        Ok(Proposed {
            applied_index: *applied_index,
            result,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register(node_id: i32, node_epoch: i64) -> MetadataCommand {
        MetadataCommand::RegisterNode {
            node_id,
            node_epoch,
            http_address: String::new(),
            slots: 1,
            protocol_addresses: Default::default(),
        }
    }

    #[tokio::test]
    async fn propose_applies_and_publishes_in_order() {
        let (sink, views) = LocalSink::new();
        let first = sink.propose(register(1, 10)).await.unwrap();
        assert_eq!(first.applied_index, 1);
        let second = sink
            .propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(second.applied_index, 2);
        assert_eq!(second.result, MetadataResult::Id(0));

        // Read-your-writes without waiting: the view is published before
        // propose returns.
        let view = views.load();
        assert_eq!(view.applied_index, 2);
        assert!(view.state.get_stream(0).is_some());
    }

    #[tokio::test]
    async fn failed_propose_consumes_no_index_and_publishes_nothing() {
        let (sink, views) = LocalSink::new();
        sink.propose(register(1, 10)).await.unwrap();
        let err = sink
            .propose(MetadataCommand::CreateStream {
                node_id: 9,
                node_epoch: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), 5);
        assert_eq!(views.load().applied_index, 1);
        let next = sink
            .propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(next.applied_index, 2);
    }

    #[tokio::test]
    async fn restores_from_state() {
        let (sink, _) = LocalSink::new();
        sink.propose(register(1, 10)).await.unwrap();
        sink.propose(MetadataCommand::CreateStream {
            node_id: 1,
            node_epoch: 10,
        })
        .await
        .unwrap();
        let snapshot = {
            let guard = sink.state.lock().await;
            (guard.0.clone(), guard.1)
        };

        let (restored, views) = LocalSink::with_state(snapshot.0, snapshot.1);
        assert_eq!(views.load().applied_index, 2);
        let next = restored
            .propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(
            next.result,
            MetadataResult::Id(1),
            "id counter survived the restore"
        );
    }
}
