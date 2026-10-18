use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use picomq_metadata::{MetadataNodeHandle, ViewPublisher};
use s3stream::{
    OpenStreamOptions, Stream, StreamClientTrait as StreamClient, StreamManagerTrait as _,
    StreamState,
};

use crate::error::{ErrorKind, ServiceError};

const TRANSFER_SETTLE_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSFER_SETTLE_POLL: Duration = Duration::from_millis(50);
const COMPACTION_LIVE_OBJECT_THRESHOLD: usize = 64;

pub struct Streams {
    client: Arc<dyn StreamClient>,
    views: Arc<ViewPublisher>,
    node: MetadataNodeHandle,
    open: Mutex<HashMap<u64, Arc<dyn Stream>>>,
    epochs: Mutex<HashMap<u64, u64>>,
    open_lock: tokio::sync::Mutex<()>,
}

impl Streams {
    pub fn new(
        client: Arc<dyn StreamClient>,
        views: Arc<ViewPublisher>,
        node: MetadataNodeHandle,
    ) -> Self {
        Self {
            client,
            views,
            node,
            open: Mutex::new(HashMap::new()),
            epochs: Mutex::new(HashMap::new()),
            open_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn node(&self) -> &MetadataNodeHandle {
        &self.node
    }

    pub async fn register(&self, tag: &str) -> Result<u64, ServiceError> {
        let tags = HashMap::from([("log".to_owned(), tag.to_owned())]);
        Ok(self.node.stream_manager().create_stream(tags).await?)
    }

    pub async fn place(&self, stream_id: u64) -> Result<(), ServiceError> {
        self.node
            .place_stream(stream_id)
            .await
            .map_err(|error| ServiceError::from(error.to_stream_error()))?;

        Ok(())
    }

    pub fn held_ids(&self) -> Vec<u64> {
        self.open.lock().unwrap().keys().copied().collect()
    }

    pub async fn ensure_open(&self, stream_id: u64) -> Result<Arc<dyn Stream>, ServiceError> {
        if let Some(existing) = self.held(stream_id) {
            return Ok(existing);
        }

        let _open = self.open_lock.lock().await;
        if let Some(existing) = self.held(stream_id) {
            return Ok(existing);
        }

        self.await_transfer_settled(stream_id).await?;
        let epoch = self.next_epoch(stream_id);
        let opened = self
            .client
            .open_stream(
                stream_id,
                OpenStreamOptions {
                    epoch,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| self.open_error(stream_id, error))?;

        self.hold(opened.clone());
        Ok(opened)
    }

    pub fn forget(&self, stream_id: u64) {
        self.open.lock().unwrap().remove(&stream_id);
    }

    pub async fn release(&self, stream_id: u64) -> Result<Option<i64>, ServiceError> {
        let held = self.open.lock().unwrap().remove(&stream_id);
        let Some(stream) = held else {
            return Ok(None);
        };

        self.epochs.lock().unwrap().remove(&stream_id);
        let epoch = stream.stream_epoch() as i64;
        stream.close().await?;

        Ok(Some(epoch))
    }

    pub async fn destroy(&self, stream_id: u64) {
        let held = self.open.lock().unwrap().remove(&stream_id);
        self.epochs.lock().unwrap().remove(&stream_id);

        let stream = match held {
            Some(stream) => stream,
            None => {
                let epoch = self.next_epoch(stream_id);
                let options = OpenStreamOptions {
                    epoch,
                    ..Default::default()
                };
                match self.client.open_stream(stream_id, options).await {
                    Ok(stream) => stream,
                    Err(_) => return,
                }
            }
        };

        let _ = stream.destroy().await;
        self.epochs.lock().unwrap().remove(&stream_id);
    }

    pub async fn shutdown(&self) {
        let snapshot: Vec<Arc<dyn Stream>> = self.open.lock().unwrap().values().cloned().collect();
        let mut streams = snapshot.into_iter();
        let mut inflight = tokio::task::JoinSet::new();

        loop {
            while inflight.len() < 1024 {
                let Some(stream) = streams.next() else { break };
                inflight.spawn(async move {
                    let _ = stream.close().await;
                });
            }
            if inflight.join_next().await.is_none() {
                break;
            }
        }

        self.open.lock().unwrap().clear();
    }

    pub fn spawn_compaction_check(self: &Arc<Self>, tick: Duration) -> tokio::task::JoinHandle<()> {
        const PAGE: usize = 4096;
        let streams = self.clone();

        tokio::spawn(async move {
            let mut cursor: Option<picomq_metadata::StreamOffsetKey> = None;
            let mut run: (u64, usize) = (u64::MAX, 0);

            loop {
                tokio::time::sleep(tick).await;

                let page = streams
                    .views
                    .load()
                    .state
                    .stream_object_keys_page(cursor, PAGE);
                let exhausted = page.len() < PAGE;

                for key in page {
                    let (stream_id, _, _) = key;
                    if stream_id != run.0 {
                        run = (stream_id, 0);
                    }
                    run.1 += 1;
                    cursor = Some(key);

                    if run.1 >= COMPACTION_LIVE_OBJECT_THRESHOLD {
                        if let Err(error) = streams
                            .client
                            .compact_stream(stream_id, s3stream::CompactionLevel::MinorV1)
                            .await
                        {
                            tracing::debug!(%error, stream_id, "triggered compaction failed");
                        }
                        cursor = Some((stream_id, u64::MAX, u64::MAX));
                        run = (u64::MAX, 0);
                        break;
                    }
                }

                if exhausted {
                    cursor = None;
                    run = (u64::MAX, 0);
                }
            }
        })
    }

    pub fn held(&self, stream_id: u64) -> Option<Arc<dyn Stream>> {
        self.open.lock().unwrap().get(&stream_id).cloned()
    }

    fn hold(&self, stream: Arc<dyn Stream>) {
        let stream_id = stream.stream_id();
        self.epochs
            .lock()
            .unwrap()
            .insert(stream_id, stream.stream_epoch());
        self.open.lock().unwrap().insert(stream_id, stream);
    }

    fn next_epoch(&self, stream_id: u64) -> u64 {
        let mut epochs = self.epochs.lock().unwrap();
        if let Some(epoch) = epochs.get_mut(&stream_id) {
            *epoch += 1;
            return *epoch;
        }

        let current = self
            .views
            .load()
            .state
            .get_stream(stream_id)
            .map(|m| m.epoch)
            .unwrap_or(0);
        let next = current + 1;
        epochs.insert(stream_id, next);
        next
    }

    fn open_error(&self, stream_id: u64, error: s3stream::Error) -> ServiceError {
        let view = self.views.load();
        match view.state.streams.get(&stream_id) {
            Some(row) if row.state == StreamState::Opened && row.node_id != self.node.node_id() => {
                ServiceError::with_message(
                    ErrorKind::Transferring,
                    None,
                    false,
                    format!("stream {stream_id} is open on node {}", row.node_id),
                )
            }
            _ => error.into(),
        }
    }

    async fn await_transfer_settled(&self, stream_id: u64) -> Result<(), ServiceError> {
        let deadline = tokio::time::Instant::now() + TRANSFER_SETTLE_TIMEOUT;

        loop {
            let view = self.views.load();
            let Some(pending) = view.state.pending_transfers.get(&stream_id).copied() else {
                return Ok(());
            };

            if pending.to_node != self.node.node_id() {
                return Err(ServiceError::with_message(
                    ErrorKind::Transferring,
                    None,
                    false,
                    format!(
                        "stream {stream_id} is transferring to node {}",
                        pending.to_node
                    ),
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ServiceError::with_message(
                    ErrorKind::Transferring,
                    None,
                    false,
                    format!("stream {stream_id} transfer did not settle in time"),
                ));
            }

            let _ = tokio::time::timeout(
                TRANSFER_SETTLE_POLL,
                self.views.wait_applied(view.applied_index + 1),
            )
            .await;
        }
    }
}
