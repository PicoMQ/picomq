use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pico_metadata::sink::Proposed;
use pico_metadata::snapshot::SnapshotError;
use pico_metadata::{
    apply, codec, CommandSink, MetadataCommand, MetadataError, MetadataResult, MetadataState,
    MetadataView, SinkStats, ViewPublisher,
};
use tokio::sync::{mpsc, oneshot, Notify};

use crate::store::{MetaStore, StoreError};

/// Per-command apply outcomes for one log row, in batch order.
type BatchResults = Vec<Result<MetadataResult, MetadataError>>;

/// Sink construction/recovery failures.
#[derive(Debug, thiserror::Error)]
pub enum SqlSinkError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("snapshot: {0}")]
    Snapshot(#[from] SnapshotError),
    #[error("corrupt log at idx {idx}: {error}")]
    CorruptLog { idx: u64, error: codec::CodecError },
    #[error("log truncated below idx {idx} but no snapshot covers the gap")]
    TruncatedWithoutSnapshot { idx: u64 },
}

/// Tuning knobs (defaults suit both SQLite-local and Postgres-remote).
#[derive(Debug, Clone)]
pub struct SqlSinkConfig {
    /// Tailer poll interval when no local append nudges it (cross-process
    pub poll_interval: Duration,
    /// Max commands coalesced into one log row (group commit width).
    pub max_batch: usize,
    /// Max rows per tailer fetch.
    pub fetch_limit: u32,
    /// Max commands staged in the propose queue. Backpressure, not safety
    /// (queued commands are unacknowledged either way): when the store stalls,
    /// `propose` awaits a slot instead of growing memory without bound.
    pub queue_depth: usize,
    /// Snapshot + truncate every N applied log rows (0 disables). Bounds the
    /// log table and cold-start replay.
    pub snapshot_every: u64,
    /// Minimum time between snapshot cycles. Rows say a snapshot is due,
    /// time keeps a busy cluster from re-shipping the full state every few
    /// seconds. Zero snapshots on rows alone (deterministic tests).
    pub snapshot_min_interval: Duration,
}

impl Default for SqlSinkConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(10),
            max_batch: 256,
            fetch_limit: 1024,
            queue_depth: 4096,
            snapshot_every: 1024,
            snapshot_min_interval: Duration::from_secs(30),
        }
    }
}

/// Result-delivery registrations, keyed by log index.
///
/// `poisoned` flips when the tailer hits an unrecoverable log error (corrupt
/// row): registrations are dropped (waiters observe a closed channel) and all
/// later proposes fail fast. One mutex covers flag + map so the check-then-
/// register path has no race with poisoning.
#[derive(Default)]
struct Pending {
    poisoned: bool,
    map: HashMap<u64, oneshot::Sender<BatchResults>>,
}

struct Shared {
    store: Arc<dyn MetaStore>,
    views: Arc<ViewPublisher>,
    pending: std::sync::Mutex<Pending>,
    /// Highest log index known to exist (observed, never guessed).
    last_seen: AtomicU64,
    /// Wakes the tailer immediately after a local append (skips one poll).
    nudge: Notify,
    stats: Arc<SinkStats>,
}

type ProposeRequest = (
    MetadataCommand,
    oneshot::Sender<Result<Proposed, MetadataError>>,
);

/// The SQL-backed command sink. Construct with [`SqlSink::open`].
pub struct SqlSink {
    shared: Arc<Shared>,
    queue: mpsc::Sender<ProposeRequest>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl SqlSink {
    /// Open over a store: restore from the snapshot row (if any), replay the
    /// remaining log to the tip, then start the flusher and tailer tasks.
    /// Returns the sink and the publisher readers use.
    pub async fn open(
        store: Arc<dyn MetaStore>,
        config: SqlSinkConfig,
    ) -> Result<(Self, Arc<ViewPublisher>), SqlSinkError> {
        // Cold start: snapshot row, then replay the log tail.
        let (mut state, mut applied) = match store.load_snapshot().await? {
            Some((idx, payload)) => (pico_metadata::snapshot::decode(&payload)?, idx),
            None => (MetadataState::new(), 0),
        };
        let mut snapshot_base = applied;
        'replay: loop {
            let rows = store.fetch_after(applied, config.fetch_limit).await?;
            if rows.is_empty() {
                break;
            }
            for (idx, payload) in rows {
                if idx > applied + 1 {
                    // Another node snapshotted + truncated past us mid-replay:
                    // restart from the (necessarily newer) snapshot.
                    let (snap_idx, snap_payload) = store
                        .load_snapshot()
                        .await?
                        .ok_or(SqlSinkError::TruncatedWithoutSnapshot { idx })?;
                    state = pico_metadata::snapshot::decode(&snap_payload)?;
                    applied = snap_idx;
                    snapshot_base = snap_idx;
                    continue 'replay;
                }
                let commands = codec::decode_batch(&payload)
                    .map_err(|error| SqlSinkError::CorruptLog { idx, error })?;
                for command in &commands {
                    // Replay results are discarded. Errors (e.g. Redundant)
                    // were already surfaced to the original proposer.
                    let _ = apply(&mut state, command);
                }
                applied = idx;
            }
        }

        let views = Arc::new(ViewPublisher::with_view(MetadataView {
            applied_index: applied,
            state: state.clone(),
        }));
        let shared = Arc::new(Shared {
            store,
            views: views.clone(),
            pending: std::sync::Mutex::new(Pending::default()),
            last_seen: AtomicU64::new(applied),
            nudge: Notify::new(),
            stats: Arc::new(SinkStats::default()),
        });

        let (queue, queue_rx) = mpsc::channel(config.queue_depth);
        let tasks = vec![
            tokio::spawn(tailer_task(shared.clone(), state, applied, config.clone())),
            tokio::spawn(snapshot_task(shared.clone(), snapshot_base, config.clone())),
            tokio::spawn(flusher_task(shared.clone(), queue_rx, config)),
        ];
        Ok((
            Self {
                shared,
                queue,
                tasks,
            },
            views,
        ))
    }

    /// The publisher readers (managers, queries) load views from.
    pub fn views(&self) -> Arc<ViewPublisher> {
        self.shared.views.clone()
    }
}

impl Drop for SqlSink {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[async_trait]
impl CommandSink for SqlSink {
    async fn propose(&self, command: MetadataCommand) -> Result<Proposed, MetadataError> {
        let (tx, rx) = oneshot::channel();
        // Bounded: a stalled store backpressures proposers here instead of
        // growing the queue without limit.
        self.queue
            .send((command, tx))
            .await
            .map_err(|_| MetadataError::Unexpected {
                message: "sql sink is shut down".into(),
            })?;
        rx.await.map_err(|_| MetadataError::Unexpected {
            message: "sql sink dropped the proposal (shutdown or log corruption)".into(),
        })?
    }

    fn stats(&self) -> Arc<SinkStats> {
        self.shared.stats.clone()
    }
}

// ---------------------------------------------------------------------------
// Tailer: the single applier. Log rows in, views + results out.
// ---------------------------------------------------------------------------

async fn tailer_task(
    shared: Arc<Shared>,
    mut state: MetadataState,
    mut applied: u64,
    config: SqlSinkConfig,
) {
    'tail: loop {
        let rows = match shared.store.fetch_after(applied, config.fetch_limit).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error, "metadata log fetch failed; retrying");
                tokio::time::sleep(config.poll_interval).await;
                continue;
            }
        };
        if rows.is_empty() {
            // An empty tail is ambiguous: nothing new, or the rows we need
            // were folded into a snapshot and truncated away by another
            // node. The snapshot index (payload-free) disambiguates.
            if let Ok(Some(snap_idx)) = shared.store.snapshot_idx().await {
                if snap_idx > applied {
                    match restore_from_snapshot(&shared, applied).await {
                        Restore::Installed(snap_idx, snap_state) => {
                            state = *snap_state;
                            applied = snap_idx;
                        }
                        Restore::Retry => tokio::time::sleep(config.poll_interval).await,
                        Restore::Poisoned => return,
                    }
                    continue;
                }
            }
            // Nudge (same-process append) or poll (other writers).
            tokio::select! {
                _ = shared.nudge.notified() => {}
                _ = tokio::time::sleep(config.poll_interval) => {}
            }
            continue;
        }
        for (idx, payload) in rows {
            if idx > applied + 1 {
                // Rows we still needed were truncated under a newer snapshot
                // (this node lagged past another node's snapshot cycle):
                // reinstall from the snapshot instead of forking.
                match restore_from_snapshot(&shared, applied).await {
                    Restore::Installed(snap_idx, snap_state) => {
                        state = *snap_state;
                        applied = snap_idx;
                    }
                    Restore::Retry => tokio::time::sleep(config.poll_interval).await,
                    Restore::Poisoned => return,
                }
                continue 'tail; // refetch from the (possibly new) position
            }
            let commands = match codec::decode_batch(&payload) {
                Ok(commands) => commands,
                Err(error) => {
                    // A row that cannot decode is unrecoverable (skipping
                    // would fork this node from every other reader of the
                    // log): poison the sink, fail all waiters, stop.
                    tracing::error!(idx, %error, "corrupt metadata log row; halting sink");
                    let mut pending = shared.pending.lock().expect("pending lock");
                    pending.poisoned = true;
                    pending.map.clear(); // waiters observe closed channels
                    return;
                }
            };
            let results: BatchResults = commands
                .iter()
                .map(|command| apply(&mut state, command))
                .collect();
            applied = idx;
            shared.last_seen.fetch_max(idx, Ordering::SeqCst);
            // Publish BEFORE delivering results: a proposer that returns must
            // see its own write in `views.load()` (CommandSink contract).
            shared.views.publish(MetadataView {
                applied_index: idx,
                state: state.clone(),
            });
            let waiter = shared
                .pending
                .lock()
                .expect("pending lock")
                .map
                .remove(&idx);
            if let Some(waiter) = waiter {
                let _ = waiter.send(results);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshotter: persists published views and drops the covered log prefix.
// ---------------------------------------------------------------------------

/// Runs beside the tailer, never on its path: it observes published views and
/// snapshots when `snapshot_every` rows have accumulated AND
/// `snapshot_min_interval` has elapsed since the last cycle. Any node may
/// snapshot: `store_snapshot` never regresses and truncation only removes
/// rows at or below the index we stored. Errors are retried next cycle
/// (extra log rows are harmless).
async fn snapshot_task(shared: Arc<Shared>, mut last_snapshot: u64, config: SqlSinkConfig) {
    if config.snapshot_every == 0 {
        return;
    }
    loop {
        shared
            .views
            .wait_applied(last_snapshot + config.snapshot_every)
            .await;
        // Fork the newest view, not the one that woke us: rows applied while
        // we slept come along for free.
        let view = shared.views.load();
        let applied = view.applied_index;
        let started = std::time::Instant::now();
        // Encode scales with state size; keep it off the async workers.
        let state = view.state.clone();
        let payload = tokio::task::spawn_blocking(move || pico_metadata::snapshot::encode(&state))
            .await
            .expect("snapshot encode panicked");
        match shared.store.store_snapshot(applied, &payload).await {
            Ok(()) => {
                last_snapshot = applied;
                shared.stats.snapshot.record_success(
                    applied,
                    payload.len() as u64,
                    started.elapsed().as_millis() as u64,
                    unix_ms(),
                );
                if let Err(error) = shared.store.truncate_log(applied).await {
                    tracing::warn!(%error, "log truncation failed; retrying next cycle");
                }
                tokio::time::sleep(config.snapshot_min_interval).await;
            }
            Err(error) => {
                shared.stats.snapshot.record_failure();
                tracing::warn!(%error, "snapshot store failed; retrying next cycle");
                tokio::time::sleep(config.poll_interval).await;
            }
        }
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Outcome of gap recovery (see [`restore_from_snapshot`]).
enum Restore {
    /// Snapshot installed: resume tailing from this index/state. (Boxed:
    /// `MetadataState` is ~240 B inline and this is a cold path.)
    Installed(u64, Box<MetadataState>),
    /// Transient store error: refetch after a poll interval.
    Retry,
    /// No snapshot can cover the gap (store corruption): sink poisoned.
    Poisoned,
}

/// Reinstall state from the stored snapshot after a truncation gap. Publishes
/// the snapshot view and drops any pending registrations it covers (those
/// batches committed and applied globally, but this node can no longer compute
/// their per-command results. The proposer gets an "outcome unknown" error,
/// like a raft client whose leader changed mid-commit).
async fn restore_from_snapshot(shared: &Shared, applied: u64) -> Restore {
    let poison = |message: &str| {
        tracing::error!(applied, message, "halting sink");
        let mut pending = shared.pending.lock().expect("pending lock");
        pending.poisoned = true;
        pending.map.clear();
        Restore::Poisoned
    };
    let snapshot = match shared.store.load_snapshot().await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(%error, "snapshot load failed during gap recovery; retrying");
            return Restore::Retry;
        }
    };
    let Some((snap_idx, payload)) = snapshot else {
        return poison("log truncated but no snapshot exists");
    };
    if snap_idx <= applied {
        return poison("log truncated beyond the stored snapshot");
    }
    let state = match pico_metadata::snapshot::decode(&payload) {
        Ok(state) => state,
        Err(error) => {
            tracing::error!(%error, "snapshot decode failed");
            return poison("corrupt snapshot during gap recovery");
        }
    };
    shared.last_seen.fetch_max(snap_idx, Ordering::SeqCst);
    shared.views.publish(MetadataView {
        applied_index: snap_idx,
        state: state.clone(),
    });
    // Registrations at or below the snapshot can never be resolved locally.
    shared
        .pending
        .lock()
        .expect("pending lock")
        .map
        .retain(|&idx, _| idx > snap_idx);
    Restore::Installed(snap_idx, Box::new(state))
}

// ---------------------------------------------------------------------------
// Flusher: group commit. Drain the queue, one row per batch, retry on
// ---------------------------------------------------------------------------

async fn flusher_task(
    shared: Arc<Shared>,
    mut queue: mpsc::Receiver<ProposeRequest>,
    config: SqlSinkConfig,
) {
    while let Some(first) = queue.recv().await {
        let mut commands = Vec::with_capacity(8);
        let mut waiters = Vec::with_capacity(8);
        commands.push(first.0);
        waiters.push(first.1);
        while commands.len() < config.max_batch {
            match queue.try_recv() {
                Ok((command, waiter)) => {
                    commands.push(command);
                    waiters.push(waiter);
                }
                Err(_) => break,
            }
        }
        let payload = codec::encode_batch(&commands);
        commit_batch(&shared, &payload, commands.len(), waiters).await;
    }
}

/// Append the encoded batch at the next free index (retrying conflicts), wait
/// for the local tailer to apply it, fan the per-command results out.
async fn commit_batch(
    shared: &Shared,
    payload: &[u8],
    command_count: usize,
    waiters: Vec<oneshot::Sender<Result<Proposed, MetadataError>>>,
) {
    loop {
        let next = shared.last_seen.load(Ordering::SeqCst) + 1;
        // Register interest BEFORE appending: the tailer may apply the row
        // before `append` even returns. If we lose the slot we discard the
        // registration (and anything another writer's row delivered to it).
        let rx = {
            let mut pending = shared.pending.lock().expect("pending lock");
            if pending.poisoned {
                fail_all(waiters, "metadata log is poisoned (corrupt row)");
                return;
            }
            let (tx, rx) = oneshot::channel();
            pending.map.insert(next, tx);
            rx
        };
        match shared.store.append(next, payload).await {
            Ok(true) => {
                shared.nudge.notify_waiters();
                match rx.await {
                    Ok(results) => {
                        debug_assert_eq!(results.len(), command_count);
                        for (waiter, result) in waiters.into_iter().zip(results) {
                            let _ = waiter.send(result.map(|result| Proposed {
                                applied_index: next,
                                result,
                            }));
                        }
                    }
                    // Sender dropped: the tailer poisoned and cleared the map.
                    Err(_) => fail_all(waiters, "metadata log is poisoned (corrupt row)"),
                }
                return;
            }
            Ok(false) => {
                // Lost the slot to another writer: forget the registration,
                // learn the real tail, try the next index.
                shared
                    .pending
                    .lock()
                    .expect("pending lock")
                    .map
                    .remove(&next);
                match shared.store.last_idx().await {
                    Ok(last) => {
                        shared.last_seen.fetch_max(last, Ordering::SeqCst);
                    }
                    Err(error) => {
                        fail_all(waiters, &format!("refreshing log tail failed: {error}"));
                        return;
                    }
                }
            }
            Err(error) => {
                shared
                    .pending
                    .lock()
                    .expect("pending lock")
                    .map
                    .remove(&next);
                fail_all(waiters, &format!("append failed: {error}"));
                return;
            }
        }
    }
}

fn fail_all(waiters: Vec<oneshot::Sender<Result<Proposed, MetadataError>>>, message: &str) {
    for waiter in waiters {
        let _ = waiter.send(Err(MetadataError::Unexpected {
            message: message.to_owned(),
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SqliteStore;
    use pico_metadata::LocalSink;

    fn fast_config() -> SqlSinkConfig {
        SqlSinkConfig {
            poll_interval: Duration::from_millis(1),
            snapshot_min_interval: Duration::ZERO,
            ..SqlSinkConfig::default()
        }
    }

    async fn memory_sink() -> (SqlSink, Arc<ViewPublisher>) {
        let store = Arc::new(SqliteStore::memory().await.unwrap());
        SqlSink::open(store, fast_config()).await.unwrap()
    }

    /// Delegating store whose `store_snapshot` parks until released.
    struct GatedSnapshotStore {
        inner: SqliteStore,
        release: tokio::sync::watch::Receiver<bool>,
    }

    #[async_trait]
    impl MetaStore for GatedSnapshotStore {
        async fn append(&self, idx: u64, payload: &[u8]) -> Result<bool, StoreError> {
            self.inner.append(idx, payload).await
        }
        async fn last_idx(&self) -> Result<u64, StoreError> {
            self.inner.last_idx().await
        }
        async fn fetch_after(
            &self,
            after: u64,
            limit: u32,
        ) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
            self.inner.fetch_after(after, limit).await
        }
        async fn load_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            self.inner.load_snapshot().await
        }
        async fn snapshot_idx(&self) -> Result<Option<u64>, StoreError> {
            self.inner.snapshot_idx().await
        }
        async fn store_snapshot(&self, applied_idx: u64, payload: &[u8]) -> Result<(), StoreError> {
            let mut release = self.release.clone();
            release
                .wait_for(|open| *open)
                .await
                .expect("release sender dropped");
            self.inner.store_snapshot(applied_idx, payload).await
        }
        async fn truncate_log(&self, up_to: u64) -> Result<(), StoreError> {
            self.inner.truncate_log(up_to).await
        }
        async fn acquire_lease(
            &self,
            holder: &str,
            prev_epoch: Option<u64>,
            now_ms: i64,
            ttl_ms: i64,
        ) -> Result<Option<u64>, StoreError> {
            self.inner
                .acquire_lease(holder, prev_epoch, now_ms, ttl_ms)
                .await
        }
        async fn release_lease(&self, holder: &str, epoch: u64) -> Result<(), StoreError> {
            self.inner.release_lease(holder, epoch).await
        }
    }

    /// Proposals keep applying while a snapshot store hangs.
    #[tokio::test]
    async fn apply_never_waits_on_snapshot_store() {
        let (release, gate) = tokio::sync::watch::channel(false);
        let store = Arc::new(GatedSnapshotStore {
            inner: SqliteStore::memory().await.unwrap(),
            release: gate,
        });
        let config = SqlSinkConfig {
            snapshot_every: 5,
            ..fast_config()
        };
        let (sink, views) = SqlSink::open(store.clone(), config).await.unwrap();

        sink.propose(register(1, 10)).await.unwrap();
        for _ in 0..30 {
            sink.propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        }
        let applied = views.load().applied_index;
        assert!(applied >= 31, "apply stalled at {applied}");
        assert_eq!(store.inner.snapshot_idx().await.unwrap(), None);

        release.send(true).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshotted = store.inner.snapshot_idx().await.unwrap().is_some();
            let rows = store.inner.fetch_after(0, 1024).await.unwrap();
            if snapshotted && (rows.len() as u64) < applied {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "released snapshot cycle never completed"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn register(node_id: i32, node_epoch: i64) -> MetadataCommand {
        MetadataCommand::RegisterNode {
            node_id,
            node_epoch,
            http_address: String::new(),
            slots: 1,
            protocol_addresses: Default::default(),
        }
    }

    /// A diverse sequence exercising successes, failures (fencing), KV, and
    /// idempotency. The equivalence workload.
    fn workload() -> Vec<MetadataCommand> {
        vec![
            register(1, 10),
            register(2, 20),
            MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            },
            MetadataCommand::CreateStream {
                node_id: 2,
                node_epoch: 20,
            },
            MetadataCommand::OpenStream {
                node_id: 1,
                node_epoch: 10,
                stream_id: 0,
                epoch: 1,
            },
            // Fenced: stale node epoch (fails, must change nothing).
            MetadataCommand::OpenStream {
                node_id: 1,
                node_epoch: 9,
                stream_id: 1,
                epoch: 1,
            },
            MetadataCommand::OpenStream {
                node_id: 2,
                node_epoch: 20,
                stream_id: 1,
                epoch: 1,
            },
            MetadataCommand::PrepareObject {
                node_id: 1,
                node_epoch: 10,
                count: 3,
                ttl_ms: 60_000,
                now_ms: 5,
            },
            MetadataCommand::PutKv {
                key: "a".into(),
                value: bytes::Bytes::from_static(b"1"),
            },
            MetadataCommand::PutKvIfAbsent {
                key: "a".into(),
                value: bytes::Bytes::from_static(b"2"),
            },
            MetadataCommand::DeleteKv { key: "a".into() },
            MetadataCommand::CloseStream {
                node_id: 1,
                node_epoch: 10,
                stream_id: 0,
                epoch: 1,
            },
            MetadataCommand::DeleteStream {
                node_id: 1,
                node_epoch: 10,
                stream_id: 0,
                epoch: 1,
            },
        ]
    }

    /// Same command sequence through LocalSink and SqlSink: identical final
    /// state and identical per-command outcomes (indexes differ by design).
    #[tokio::test]
    async fn equivalent_to_local_sink() {
        let (local, local_views) = LocalSink::new();
        let (sql, sql_views) = memory_sink().await;
        for command in workload() {
            let local_result = local.propose(command.clone()).await.map(|p| p.result);
            let sql_result = sql.propose(command).await.map(|p| p.result);
            assert_eq!(local_result, sql_result);
        }
        assert_eq!(local_views.load().state, sql_views.load().state);
    }

    /// Read-your-writes: the view already reflects a propose when it returns.
    #[tokio::test]
    async fn propose_publishes_before_returning() {
        let (sink, views) = memory_sink().await;
        sink.propose(register(1, 10)).await.unwrap();
        let proposed = sink
            .propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(proposed.result, MetadataResult::Id(0));
        let view = views.load();
        assert!(view.applied_index >= proposed.applied_index);
        assert!(view.state.get_stream(0).is_some());
    }

    /// Restart recovery: reopen the same file, state and id counters survive.
    #[tokio::test]
    async fn restart_replays_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        {
            let store = Arc::new(SqliteStore::open(&path).await.unwrap());
            let (sink, _) = SqlSink::open(store, fast_config()).await.unwrap();
            sink.propose(register(1, 10)).await.unwrap();
            sink.propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
            sink.propose(MetadataCommand::PutKv {
                key: "k".into(),
                value: bytes::Bytes::from_static(b"v"),
            })
            .await
            .unwrap();
        }
        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let (sink, views) = SqlSink::open(store, fast_config()).await.unwrap();
        let view = views.load();
        assert!(view.state.get_stream(0).is_some());
        assert_eq!(
            view.state.get_kv("k"),
            Some(bytes::Bytes::from_static(b"v"))
        );
        // Id counter continued: the next stream gets id 1, not 0.
        let proposed = sink
            .propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(proposed.result, MetadataResult::Id(1));
    }

    /// Two sinks on one database racing proposes: every propose succeeds and
    /// both converge to the same state (conflict-retry correctness).
    #[tokio::test]
    async fn multi_writer_convergence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let store_a = Arc::new(SqliteStore::open(&path).await.unwrap());
        let store_b = Arc::new(SqliteStore::open(&path).await.unwrap());
        let (sink_a, views_a) = SqlSink::open(store_a, fast_config()).await.unwrap();
        let (sink_b, views_b) = SqlSink::open(store_b, fast_config()).await.unwrap();

        sink_a.propose(register(1, 10)).await.unwrap();
        sink_b.propose(register(2, 20)).await.unwrap();

        let sink_a = Arc::new(sink_a);
        let sink_b = Arc::new(sink_b);
        let mut handles = Vec::new();
        for i in 0..20u32 {
            let (sink, node_id, node_epoch) = if i % 2 == 0 {
                (sink_a.clone(), 1, 10)
            } else {
                (sink_b.clone(), 2, 20)
            };
            handles.push(tokio::spawn(async move {
                sink.propose(MetadataCommand::CreateStream {
                    node_id,
                    node_epoch,
                })
                .await
                .unwrap()
                .result
            }));
        }
        let mut ids = Vec::new();
        for handle in handles {
            match handle.await.unwrap() {
                MetadataResult::Id(id) => ids.push(id),
                other => panic!("unexpected result {other:?}"),
            }
        }
        // Every create got a unique id and none were lost.
        ids.sort_unstable();
        assert_eq!(ids, (0..20).collect::<Vec<u64>>());

        // Both replicas converge to the same state.
        let target = views_a
            .load()
            .applied_index
            .max(views_b.load().applied_index);
        let view_a = views_a.wait_applied(target).await;
        let view_b = views_b.wait_applied(target).await;
        assert_eq!(view_a.state, view_b.state);
        assert_eq!(view_a.state.streams.len(), 20);
    }

    /// Group commit actually batches: many concurrent proposes land in fewer
    /// log rows than commands.
    #[tokio::test]
    async fn concurrent_proposes_coalesce() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let (sink, _) = SqlSink::open(store.clone(), fast_config()).await.unwrap();
        let sink = Arc::new(sink);
        sink.propose(register(1, 10)).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..100 {
            let sink = sink.clone();
            handles.push(tokio::spawn(async move {
                sink.propose(MetadataCommand::CreateStream {
                    node_id: 1,
                    node_epoch: 10,
                })
                .await
                .unwrap()
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        let rows = store.fetch_after(0, 10_000).await.unwrap();
        assert!(
            rows.len() < 101,
            "expected batching to produce fewer rows than commands, got {}",
            rows.len()
        );
        // All 100 creates are in the log regardless of batching shape.
        let commands: usize = rows
            .iter()
            .map(|(_, payload)| codec::decode_batch(payload).unwrap().len())
            .sum();
        assert_eq!(commands, 101);
    }

    /// The snapshot cycle bounds the log: after enough rows, a snapshot row
    /// exists and the covered log prefix is gone. And a cold start from
    /// snapshot + tail reproduces the exact state.
    #[tokio::test]
    async fn snapshot_cycle_truncates_log_and_cold_start_restores() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let config = SqlSinkConfig {
            snapshot_every: 5,
            ..fast_config()
        };
        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let final_state = {
            let (sink, views) = SqlSink::open(store.clone(), config.clone()).await.unwrap();
            sink.propose(register(1, 10)).await.unwrap();
            for _ in 0..30 {
                sink.propose(MetadataCommand::CreateStream {
                    node_id: 1,
                    node_epoch: 10,
                })
                .await
                .unwrap();
            }
            // The cycle ran: snapshot exists, log holds < the full history.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let snapshot = store.load_snapshot().await.unwrap();
                let rows = store.fetch_after(0, 10_000).await.unwrap();
                if let Some((snap_idx, _)) = snapshot {
                    if snap_idx > 0 && rows.iter().all(|(idx, _)| *idx > snap_idx) {
                        break;
                    }
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "snapshot cycle never ran"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let rows = store.fetch_after(0, 10_000).await.unwrap();
            assert!(rows.len() < 31, "log not truncated: {} rows", rows.len());
            views.load().state.clone()
        };

        // Cold start = snapshot + tail: identical state, id counters intact.
        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let (sink, views) = SqlSink::open(store, config).await.unwrap();
        assert_eq!(views.load().state, final_state);
        let next = sink
            .propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(next.result, MetadataResult::Id(30));
    }

    /// A reader that lagged past a snapshot+truncation recovers by
    /// reinstalling the snapshot instead of forking or halting.
    #[tokio::test]
    async fn lagging_reader_recovers_through_truncation_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");

        // B first (at idx 0), with a long poll so it lags while A writes.
        let store_b = Arc::new(SqliteStore::open(&path).await.unwrap());
        let config_b = SqlSinkConfig {
            poll_interval: Duration::from_millis(500),
            snapshot_every: 0,
            ..SqlSinkConfig::default()
        };
        let (_sink_b, views_b) = SqlSink::open(store_b, config_b).await.unwrap();

        // A writes past its snapshot threshold: log gets truncated while B
        // still sits at idx 0.
        let store_a = Arc::new(SqliteStore::open(&path).await.unwrap());
        let config_a = SqlSinkConfig {
            snapshot_every: 5,
            ..fast_config()
        };
        let (sink_a, views_a) = SqlSink::open(store_a.clone(), config_a).await.unwrap();
        sink_a.propose(register(1, 10)).await.unwrap();
        for _ in 0..20 {
            sink_a
                .propose(MetadataCommand::CreateStream {
                    node_id: 1,
                    node_epoch: 10,
                })
                .await
                .unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while store_a.load_snapshot().await.unwrap().is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "snapshot never written"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // One more append AFTER truncation so B's next fetch surfaces the gap.
        sink_a
            .propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();

        // B converges to A's state through the snapshot restore path.
        let target = views_a.load().applied_index;
        let view_b = tokio::time::timeout(Duration::from_secs(10), views_b.wait_applied(target))
            .await
            .expect("lagging reader never recovered");
        assert_eq!(view_b.state, views_a.load().state);
        assert_eq!(view_b.state.streams.len(), 21);
    }

    /// Failed commands surface their typed error and change nothing, batched
    /// or not (atomic apply through the sink).
    #[tokio::test]
    async fn failures_are_typed_and_isolated() {
        let (sink, views) = memory_sink().await;
        sink.propose(register(1, 10)).await.unwrap();
        let err = sink
            .propose(MetadataCommand::CreateStream {
                node_id: 9,
                node_epoch: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            5,
            "NodeEpochMismatch must survive the sink boundary"
        );
        let ok = sink
            .propose(MetadataCommand::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(ok.result, MetadataResult::Id(0));
        assert_eq!(views.load().state.streams.len(), 1);
    }
}
