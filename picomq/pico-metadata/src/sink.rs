//! The write boundary: where commands enter the replicated log.
//!
//! The same manager/KV implementations work over any log: [`LocalSink`]
//! applies in-process (single node, tests); `SqlSink` (crate `picomq-sql`)
//! appends to a durable SQL table and coalesces concurrent proposes into one
//! row. Delivery concerns (leader forwarding, conflict retry) belong to the
//! sink, never to callers.

use std::sync::atomic::{AtomicU64, Ordering};
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

/// Per-sink lifetime counters, handed out by [`CommandSink::stats`].
#[derive(Debug, Default)]
pub struct SinkStats {
    pub snapshot: SnapshotStats,
}

#[derive(Debug, Default)]
pub struct SnapshotStats {
    pub last_applied_index: AtomicU64,
    pub last_bytes: AtomicU64,
    pub last_duration_ms: AtomicU64,
    /// Unix ms.
    pub last_at_ms: AtomicU64,
    pub taken: AtomicU64,
    pub failed: AtomicU64,
}

impl SnapshotStats {
    pub fn record_success(&self, applied_index: u64, bytes: u64, duration_ms: u64, at_ms: u64) {
        self.last_applied_index
            .store(applied_index, Ordering::Relaxed);
        self.last_bytes.store(bytes, Ordering::Relaxed);
        self.last_duration_ms.store(duration_ms, Ordering::Relaxed);
        self.last_at_ms.store(at_ms, Ordering::Relaxed);
        self.taken.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }
}

/// Where commands are proposed and (once committed) applied.
///
/// `propose` returns only after the command is durably committed, applied,
/// and its resulting view published, so a caller that reads `views.load()`
/// right after a successful propose sees its own write. `Err(Redundant)`
/// means "already applied".
#[async_trait]
pub trait CommandSink: Send + Sync {
    async fn propose(&self, command: MetadataCommand) -> Result<Proposed, MetadataError>;

    /// Default is a shared all-zero unit: correct for sinks with no
    /// background work (`LocalSink`, test doubles).
    fn stats(&self) -> Arc<SinkStats> {
        static ZERO: std::sync::OnceLock<Arc<SinkStats>> = std::sync::OnceLock::new();
        ZERO.get_or_init(|| Arc::new(SinkStats::default())).clone()
    }
}

/// In-process sink: commands apply serially under a mutex, every successful
/// apply publishes a view. No durability. Failed applies do not consume an
/// index; callers only rely on `applied_index` monotonicity.
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
