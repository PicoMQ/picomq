//! Published read views: lock-free, consistent, tagged with the applied index.
//! - **Lock-free**: `ArcSwap::load` on every read path. No contention with the
//!   reads (lookup stream, then its objects) can never observe a torn state.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::state::MetadataState;

/// An immutable, consistent fork of the metadata state.
#[derive(Debug, Clone)]
pub struct MetadataView {
    /// Raft log index (or local sequence) whose apply produced this view.
    pub applied_index: u64,
    /// O(1) fork of the state at that index.
    pub state: MetadataState,
}

/// Single-writer view publication point.
///
/// The apply task calls [`Self::publish`]. Every reader (query layer, ownership
/// router, engine manager impls) holds a clone and calls [`Self::load`].
#[derive(Debug)]
pub struct ViewPublisher {
    current: ArcSwap<MetadataView>,
    /// `applyWaiters` skip-list in `MetadataStateMachine#awaitApplied`.
    notify: tokio::sync::watch::Sender<u64>,
}

impl ViewPublisher {
    /// Start at an empty state, `applied_index == 0`.
    pub fn new() -> Self {
        Self::with_view(MetadataView {
            applied_index: 0,
            state: MetadataState::new(),
        })
    }

    /// Start from a restored view (snapshot install / restart).
    pub fn with_view(view: MetadataView) -> Self {
        let (notify, _) = tokio::sync::watch::channel(view.applied_index);
        Self {
            current: ArcSwap::from_pointee(view),
            notify,
        }
    }

    /// The latest published view (lock-free).
    pub fn load(&self) -> Arc<MetadataView> {
        self.current.load_full()
    }

    /// Publish a new view. Called only by the apply task, with a monotonically
    /// increasing `applied_index`.
    pub fn publish(&self, view: MetadataView) {
        let applied_index = view.applied_index;
        debug_assert!(
            applied_index >= self.notify.borrow().to_owned(),
            "applied_index regressed"
        );
        // Order matters: the view must be visible before waiters wake.
        self.current.store(Arc::new(view));
        self.notify.send_replace(applied_index);
    }

    /// Wait until a view with `applied_index >= index` is published
    ///
    /// The returned view may be newer than `index`. Never older.
    pub async fn wait_applied(&self, index: u64) -> Arc<MetadataView> {
        let mut receiver = self.notify.subscribe();
        // The sender lives in `self`, so `changed` cannot fail while we borrow it.
        receiver
            .wait_for(|applied| *applied >= index)
            .await
            .expect("publisher dropped");
        self.load()
    }

    pub async fn wait_past(&self, index: u64, timeout: std::time::Duration) {
        let _ = tokio::time::timeout(timeout, self.wait_applied(index + 1)).await;
    }
}

impl Default for ViewPublisher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn view_at(applied_index: u64) -> MetadataView {
        MetadataView {
            applied_index,
            state: MetadataState::new(),
        }
    }

    #[test]
    fn publish_and_load() {
        let publisher = ViewPublisher::new();
        assert_eq!(publisher.load().applied_index, 0);
        publisher.publish(view_at(3));
        assert_eq!(publisher.load().applied_index, 3);
    }

    #[tokio::test]
    async fn wait_applied_returns_immediately_when_satisfied() {
        let publisher = ViewPublisher::new();
        publisher.publish(view_at(5));
        assert_eq!(publisher.wait_applied(5).await.applied_index, 5);
        assert_eq!(
            publisher.wait_applied(0).await.applied_index,
            5,
            "never older"
        );
    }

    #[tokio::test]
    async fn wait_applied_wakes_on_publish() {
        let publisher = std::sync::Arc::new(ViewPublisher::new());
        let waiter = {
            let publisher = publisher.clone();
            tokio::spawn(async move { publisher.wait_applied(2).await.applied_index })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        publisher.publish(view_at(1)); // not enough — waiter keeps waiting
        publisher.publish(view_at(2));
        assert!(waiter.await.unwrap() >= 2);
    }

    #[tokio::test]
    async fn restored_publisher_starts_at_snapshot_index() {
        let publisher = ViewPublisher::with_view(view_at(42));
        assert_eq!(publisher.load().applied_index, 42);
        assert_eq!(publisher.wait_applied(42).await.applied_index, 42);
    }
}
