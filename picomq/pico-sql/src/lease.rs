//! `LeaseKeeper`: single-writer election over the `meta_lease` row.
//!
//! A TTL lease row in the SQL store, renewed by CAS. No consensus. The epoch
//! is a fencing token: every takeover bumps it, so a paused old holder's
//! renewal fails instead of splitting leadership.
//!
//! Safety posture: leadership here gates *maintenance work only* (expiry
//! ticks, object GC). Correctness never depends on it. Commands are ordered
//! by the log PK, applies are deterministic, and `CleanDestroyedObjects` is
//! idempotent. So a brief double-leader window during handover degrades to
//! duplicate (harmless) work, never divergence.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use picomq_common::now_ms;

use crate::store::{MetaStore, StoreError, DEFAULT_LEASE_TTL_MS};

/// Lease timing knobs.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// How long an unrenewed lease stays owned (see [`DEFAULT_LEASE_TTL_MS`]).
    pub ttl_ms: i64,
    /// Renew/acquire attempt cadence. Keep well under `ttl_ms` (TTL/3 or
    /// less) so transient store hiccups don't cost leadership.
    pub check_interval: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            ttl_ms: DEFAULT_LEASE_TTL_MS,
            check_interval: Duration::from_millis(DEFAULT_LEASE_TTL_MS as u64 / 4),
        }
    }
}

/// Holds (or contends for) the lease in a background task and publishes
/// leadership on a watch channel (`true` = this holder owns the lease).
///
/// Feed the receiver to `MetadataLifecycle::drive` to gate background loops.
pub struct LeaseKeeper {
    leadership: watch::Receiver<bool>,
    /// Tells the keeper task to release and exit.
    stop: Arc<tokio::sync::Notify>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl LeaseKeeper {
    /// Start contending for the lease as `holder` (must be unique per node,
    /// the store treats equal holders as the same owner).
    pub fn spawn(store: Arc<dyn MetaStore>, holder: String, config: LeaseConfig) -> Self {
        let (tx, leadership) = watch::channel(false);
        let stop = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(keeper_task(store, holder, config, tx, stop.clone()));
        Self {
            leadership,
            stop,
            task: Some(task),
        }
    }

    /// The leadership watch: `true` while this keeper owns the lease.
    pub fn leadership(&self) -> watch::Receiver<bool> {
        self.leadership.clone()
    }

    /// Graceful stop: releases the lease (fenced, a successor's lease is
    /// untouched) so the next holder doesn't wait a full TTL.
    pub async fn shutdown(mut self) {
        self.stop.notify_one();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        // Non-graceful path: the task is aborted. The lease expires by TTL.
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn keeper_task(
    store: Arc<dyn MetaStore>,
    holder: String,
    config: LeaseConfig,
    tx: watch::Sender<bool>,
    stop: Arc<tokio::sync::Notify>,
) {
    // (owned epoch, wall-clock ms of the last successful acquire/renew).
    let mut held: Option<(u64, i64)> = None;
    loop {
        let now = now_ms();
        let attempt: Result<Option<u64>, StoreError> = match held {
            None => store.acquire_lease(&holder, None, now, config.ttl_ms).await,
            Some((epoch, _)) => {
                store
                    .acquire_lease(&holder, Some(epoch), now, config.ttl_ms)
                    .await
            }
        };
        match attempt {
            Ok(Some(epoch)) => {
                held = Some((epoch, now));
                tx.send_if_modified(|leader| !std::mem::replace(leader, true));
            }
            Ok(None) => {
                // Acquire refused (someone owns it) or renewal fenced
                // (someone took over). Either way we are not the leader.
                held = None;
                tx.send_if_modified(|leader| std::mem::replace(leader, false));
            }
            Err(error) => {
                // Store unreachable: keep leadership only while the lease
                // cannot have expired under us. Past TTL, self-demote. A
                // successor may legitimately own it now.
                tracing::warn!(%error, "lease store unreachable");
                if let Some((_, last_ok)) = held {
                    if now - last_ok >= config.ttl_ms {
                        held = None;
                        tx.send_if_modified(|leader| std::mem::replace(leader, false));
                    }
                }
            }
        }
        tokio::select! {
            _ = stop.notified() => break,
            _ = tokio::time::sleep(config.check_interval) => {}
        }
    }
    if let Some((epoch, _)) = held {
        let _ = store.release_lease(&holder, epoch).await;
    }
    let _ = tx.send(false);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SqliteStore;

    fn fast_config() -> LeaseConfig {
        LeaseConfig {
            ttl_ms: 200,
            check_interval: Duration::from_millis(20),
        }
    }

    async fn wait_for(rx: &mut watch::Receiver<bool>, want: bool) {
        tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|v| *v == want))
            .await
            .expect("leadership never changed")
            .expect("keeper dropped");
    }

    /// One keeper on an idle store becomes leader. A second contender does
    /// not, until the first releases on shutdown. Then the second takes over.
    #[tokio::test]
    async fn single_leader_and_handover_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease.db");
        let store_a: Arc<dyn MetaStore> = Arc::new(SqliteStore::open(&path).await.unwrap());
        let store_b: Arc<dyn MetaStore> = Arc::new(SqliteStore::open(&path).await.unwrap());

        let keeper_a = LeaseKeeper::spawn(store_a, "node-a".into(), fast_config());
        let mut rx_a = keeper_a.leadership();
        wait_for(&mut rx_a, true).await;

        let keeper_b = LeaseKeeper::spawn(store_b, "node-b".into(), fast_config());
        let mut rx_b = keeper_b.leadership();
        // B stays follower while A renews.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!*rx_b.borrow(), "two leaders on one lease");
        assert!(*rx_a.borrow());

        // Graceful shutdown releases: B takes over well before a TTL.
        keeper_a.shutdown().await;
        wait_for(&mut rx_b, true).await;
        keeper_b.shutdown().await;
    }

    /// A keeper that dies without releasing (drop = abort) is superseded
    /// after the TTL lapses.
    #[tokio::test]
    async fn takeover_after_ttl_on_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease.db");
        let store_a: Arc<dyn MetaStore> = Arc::new(SqliteStore::open(&path).await.unwrap());
        let store_b: Arc<dyn MetaStore> = Arc::new(SqliteStore::open(&path).await.unwrap());

        let keeper_a = LeaseKeeper::spawn(store_a, "node-a".into(), fast_config());
        let mut rx_a = keeper_a.leadership();
        wait_for(&mut rx_a, true).await;
        drop(keeper_a); // crash: no release

        let keeper_b = LeaseKeeper::spawn(store_b, "node-b".into(), fast_config());
        let mut rx_b = keeper_b.leadership();
        wait_for(&mut rx_b, true).await; // within the 200 ms TTL + polling
        keeper_b.shutdown().await;
    }
}
