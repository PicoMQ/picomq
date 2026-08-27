//! Leader-gated background maintenance: prepared-object expiry + object GC.
//!
//! Loops start on leader election, stop on step-down, and re-check leadership
//! before every tick. Leadership comes from whatever the host provides (the
//! SQL lease keeper's `watch<bool>` in `pico-sql`, trivially `true` for a
//! single node). The loops are tokio tasks gated on an `AtomicBool`.
//! [`MetadataLifecycle::drive`] adapts a leadership watch channel to
//! `on_leader_start`/`on_leader_stop`. The tick bodies are sink-agnostic:
//! they only need a [`CommandSink`] and a [`ViewPublisher`], so the same
//! lifecycle runs over `LocalSink` and `SqlSink`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use s3stream::{gen_object_key, CompactOperations, ObjectPath, ObjectStorageTrait};

use crate::command::MetadataCommand;
use crate::error::MetadataError;
use crate::sink::CommandSink;
use crate::view::ViewPublisher;

pub const MAX_DELETE_BATCH_COUNT: usize = 2000;

/// SECONDS)` timeouts in `ObjectCleaner#clean`.
const CLEAN_STEP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum CleanError {
    #[error("object storage delete failed: {0}")]
    Storage(String),
    #[error("metadata propose failed: {0}")]
    Metadata(#[from] MetadataError),
    #[error("timed out after {CLEAN_STEP_TIMEOUT:?}")]
    Timeout,
}

pub struct ObjectCleaner {
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
    object_storage: Option<Arc<dyn ObjectStorageTrait>>,
}

impl ObjectCleaner {
    pub fn new(
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
        object_storage: Option<Arc<dyn ObjectStorageTrait>>,
    ) -> Self {
        Self {
            sink,
            views,
            object_storage,
        }
    }

    /// One cleanup pass over at most `limit` (≤ [`MAX_DELETE_BATCH_COUNT`])
    /// destroyed objects. Returns the ids removed from the catalog.
    ///
    /// Peek the FIFO with one consistent read of a loaded view, split
    /// `KEEP_DATA` (catalog-only) from deletable, batch-delete from storage,
    /// then propose `CleanDestroyedObjects` for everything cleaned.
    pub async fn clean(&self, limit: usize) -> Result<Vec<u64>, CleanError> {
        let batch = limit.min(MAX_DELETE_BATCH_COUNT);
        let marked = self.views.load().state.peek_destroyed_objects(batch);
        if marked.is_empty() {
            return Ok(Vec::new());
        }

        let mut deletable = Vec::new();
        let mut catalog_only = Vec::new();
        for (object_id, operation) in marked {
            if operation == CompactOperations::KeepData {
                catalog_only.push(object_id);
            } else {
                deletable.push(object_id);
            }
        }

        let mut cleaned = catalog_only;
        if !deletable.is_empty() {
            if let Some(storage) = &self.object_storage {
                let bucket_id = storage.bucket_id();
                let paths: Vec<ObjectPath> = deletable
                    .iter()
                    .map(|id| ObjectPath {
                        bucket_id,
                        key: gen_object_key(0, *id),
                    })
                    .collect();
                tokio::time::timeout(CLEAN_STEP_TIMEOUT, storage.delete(&paths))
                    .await
                    .map_err(|_| CleanError::Timeout)?
                    .map_err(|e| CleanError::Storage(e.to_string()))?;
                cleaned.append(&mut deletable);
            }
        }

        if cleaned.is_empty() {
            return Ok(Vec::new());
        }
        tokio::time::timeout(
            CLEAN_STEP_TIMEOUT,
            self.sink.propose(MetadataCommand::CleanDestroyedObjects {
                object_ids: cleaned.clone(),
            }),
        )
        .await
        .map_err(|_| CleanError::Timeout)??;
        Ok(cleaned)
    }
}

pub struct MetadataLifecycle {
    sink: Arc<dyn CommandSink>,
    cleaner: Arc<ObjectCleaner>,
    tick: Duration,
    leader: Arc<AtomicBool>,
    loops: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl MetadataLifecycle {
    pub fn new(sink: Arc<dyn CommandSink>, cleaner: Arc<ObjectCleaner>, tick: Duration) -> Self {
        Self {
            sink,
            cleaner,
            tick,
            leader: Arc::new(AtomicBool::new(false)),
            loops: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn on_leader_start(&self) {
        if self.leader.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut loops = self.loops.lock().expect("lifecycle lock");
        loops.push(tokio::spawn(expire_loop(
            self.sink.clone(),
            self.leader.clone(),
            self.tick,
        )));
        loops.push(tokio::spawn(clean_loop(
            self.cleaner.clone(),
            self.leader.clone(),
            self.tick,
        )));
    }

    pub fn on_leader_stop(&self) {
        if !self.leader.swap(false, Ordering::SeqCst) {
            return;
        }
        for task in self.loops.lock().expect("lifecycle lock").drain(..) {
            task.abort();
        }
    }

    pub fn drive(
        self: Arc<Self>,
        mut leadership: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if *leadership.borrow_and_update() {
                    self.on_leader_start();
                } else {
                    self.on_leader_stop();
                }
                if leadership.changed().await.is_err() {
                    self.on_leader_stop();
                    return;
                }
            }
        })
    }
}

impl Drop for MetadataLifecycle {
    fn drop(&mut self) {
        self.leader.store(false, Ordering::SeqCst);
        for task in self.loops.lock().expect("lifecycle lock").drain(..) {
            task.abort();
        }
    }
}

async fn expire_loop(sink: Arc<dyn CommandSink>, leader: Arc<AtomicBool>, tick: Duration) {
    loop {
        tokio::time::sleep(tick).await;
        if !leader.load(Ordering::SeqCst) {
            return;
        }
        if let Err(error) = sink
            .propose(MetadataCommand::ExpirePreparedObjects {
                now_ms: pico_common::now_ms(),
            })
            .await
        {
            tracing::debug!(%error, "expire prepared objects failed");
        }
    }
}

async fn clean_loop(cleaner: Arc<ObjectCleaner>, leader: Arc<AtomicBool>, tick: Duration) {
    loop {
        tokio::time::sleep(tick).await;
        if !leader.load(Ordering::SeqCst) {
            return;
        }
        if let Err(error) = cleaner.clean(MAX_DELETE_BATCH_COUNT).await {
            tracing::warn!(%error, "object cleaner failed, destroyed marks retained");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::MetadataResult;
    use crate::sink::LocalSink;
    use s3stream::MemoryObjectStorage;

    const NODE: i32 = 1;
    const EPOCH: i64 = 1;

    async fn registered_sink() -> (Arc<LocalSink>, Arc<ViewPublisher>) {
        let (sink, views) = LocalSink::new();
        let sink = Arc::new(sink);
        sink.propose(MetadataCommand::RegisterNode {
            node_id: NODE,
            node_epoch: EPOCH,
            http_address: String::new(),
            slots: 1,
            protocol_addresses: Default::default(),
        })
        .await
        .unwrap();
        (sink, views)
    }

    /// Commit `object_id` as a *stream object* of `stream_id`. The kind a
    /// later stream delete marks destroyed (the FIFO the cleaner drains).
    async fn commit_object(sink: &Arc<LocalSink>, stream_id: u64, object_id: u64) {
        sink.propose(MetadataCommand::PrepareObject {
            node_id: NODE,
            node_epoch: EPOCH,
            count: 1,
            ttl_ms: 60_000,
            now_ms: 0,
        })
        .await
        .unwrap();
        sink.propose(MetadataCommand::CompactStreamObject {
            node_id: NODE,
            node_epoch: EPOCH,
            request: s3stream::CompactStreamObjectRequest {
                object_id,
                object_size: 10,
                stream_id,
                stream_epoch: 1,
                start_offset: 0,
                end_offset: 0,
                source_object_ids: Vec::new(),
                operations: Vec::new(),
                attributes: 0,
            },
            now_ms: 0,
        })
        .await
        .unwrap();
    }

    /// Create+open a stream, write an object into it, delete the stream: the
    /// object lands in the destroyed FIFO with a deep-delete op.
    async fn destroyed_object_fixture(sink: &Arc<LocalSink>) -> u64 {
        let stream_id = match sink
            .propose(MetadataCommand::CreateStream {
                node_id: NODE,
                node_epoch: EPOCH,
            })
            .await
            .unwrap()
            .result
        {
            MetadataResult::Id(id) => id,
            other => panic!("unexpected {other:?}"),
        };
        sink.propose(MetadataCommand::OpenStream {
            node_id: NODE,
            node_epoch: EPOCH,
            stream_id,
            epoch: 1,
        })
        .await
        .unwrap();
        commit_object(sink, stream_id, 0).await;
        sink.propose(MetadataCommand::CloseStream {
            node_id: NODE,
            node_epoch: EPOCH,
            stream_id,
            epoch: 1,
        })
        .await
        .unwrap();
        sink.propose(MetadataCommand::DeleteStream {
            node_id: NODE,
            node_epoch: EPOCH,
            stream_id,
            epoch: 1,
        })
        .await
        .unwrap();
        0 // the committed object id
    }

    /// Clean deletes from storage AND drops the catalog marks.
    #[tokio::test]
    async fn clean_deletes_and_unmarks() {
        let (sink, views) = registered_sink().await;
        let object_id = destroyed_object_fixture(&sink).await;
        assert_eq!(views.load().state.peek_destroyed_objects(10).len(), 1);

        let storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
        let cleaner = ObjectCleaner::new(sink.clone(), views.clone(), Some(storage));
        let cleaned = cleaner.clean(MAX_DELETE_BATCH_COUNT).await.unwrap();
        assert_eq!(cleaned, vec![object_id]);
        assert!(views.load().state.peek_destroyed_objects(10).is_empty());

        // Idempotent: nothing left to clean.
        assert!(cleaner
            .clean(MAX_DELETE_BATCH_COUNT)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn clean_without_storage_retains_deletable() {
        let (sink, views) = registered_sink().await;
        destroyed_object_fixture(&sink).await;

        let cleaner = ObjectCleaner::new(sink.clone(), views.clone(), None);
        assert!(cleaner
            .clean(MAX_DELETE_BATCH_COUNT)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(views.load().state.peek_destroyed_objects(10).len(), 1);
    }

    /// The lifecycle loops actually run while leader and stop on step-down:
    /// expiry reclaims a lapsed prepared object end to end.
    #[tokio::test]
    async fn lifecycle_expires_prepared_objects_while_leader() {
        let (sink, views) = registered_sink().await;
        // Prepared at t=0 with a 1 ms TTL: long expired against wall-clock now.
        sink.propose(MetadataCommand::PrepareObject {
            node_id: NODE,
            node_epoch: EPOCH,
            count: 1,
            ttl_ms: 1,
            now_ms: 0,
        })
        .await
        .unwrap();

        let cleaner = Arc::new(ObjectCleaner::new(sink.clone(), views.clone(), None));
        let lifecycle = MetadataLifecycle::new(sink.clone(), cleaner, Duration::from_millis(5));
        lifecycle.on_leader_start();
        lifecycle.on_leader_start(); // idempotent

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            // Committing against the expired id must eventually be rejected
            // (id 0 reclaimed). We detect expiry via state instead: the
            // prepared count drops to zero.
            if views.load().state.prepared_objects_count() == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "expire loop never ran"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        lifecycle.on_leader_stop();
        lifecycle.on_leader_stop(); // idempotent
    }

    /// `drive` follows a leadership watch: loops run only while `true`.
    #[tokio::test]
    async fn drive_follows_leadership_watch() {
        let (sink, views) = registered_sink().await;
        sink.propose(MetadataCommand::PrepareObject {
            node_id: NODE,
            node_epoch: EPOCH,
            count: 1,
            ttl_ms: 1,
            now_ms: 0,
        })
        .await
        .unwrap();

        let cleaner = Arc::new(ObjectCleaner::new(sink.clone(), views.clone(), None));
        let lifecycle = Arc::new(MetadataLifecycle::new(
            sink.clone(),
            cleaner,
            Duration::from_millis(5),
        ));
        let (tx, rx) = tokio::sync::watch::channel(false);
        let driver = lifecycle.clone().drive(rx);

        // Not leader: nothing expires.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(views.load().state.prepared_objects_count(), 1);

        tx.send(true).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while views.load().state.prepared_objects_count() != 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "expire loop never ran"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        drop(tx); // watch closed → driver stops the loops and exits
        driver.await.unwrap();
    }
}
