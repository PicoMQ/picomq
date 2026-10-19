use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use picomq_metadata::{MetadataNodeHandle, MetadataState, MetadataView, NodeRow, ViewPublisher};
use s3stream::{
    Client as _, Error as StreamError, FailoverFactory, FailoverRequest, ObjectManagerTrait,
    S3StreamEngine, StreamManagerTrait, WriteAheadLogTrait,
};
use s3stream_wal::{
    AcquirePermissionOptions, BuildOptions, DefaultWalHandle, ObjectWalFactory, OpenMode,
    WalFactory as _, WalHandle as _,
};
use tokio::sync::watch;

use crate::error::ServiceError;
use crate::leadership;
use crate::service::stream_id_key;
use crate::streams::Streams;

const ACQUIRE_TIMEOUT_MS: u64 = 20_000;

#[async_trait]
pub trait WalTakeover: Send + Sync {
    async fn open(
        &self,
        request: &FailoverRequest,
    ) -> Result<Arc<dyn WriteAheadLogTrait>, StreamError>;
}

pub struct ObjectWalTakeover {
    cluster_id: String,
    handle: DefaultWalHandle,
}

impl ObjectWalTakeover {
    pub fn new(cluster_id: &str) -> Self {
        Self {
            cluster_id: cluster_id.to_owned(),
            handle: DefaultWalHandle::new(cluster_id),
        }
    }
}

#[async_trait]
impl WalTakeover for ObjectWalTakeover {
    async fn open(
        &self,
        request: &FailoverRequest,
    ) -> Result<Arc<dyn WriteAheadLogTrait>, StreamError> {
        let options = AcquirePermissionOptions {
            failover_mode: true,
            timeout_ms: ACQUIRE_TIMEOUT_MS,
        };
        self.handle
            .acquire_permission(
                request.node_id,
                request.node_epoch,
                &request.wal_config,
                options,
            )
            .await?;

        let build = BuildOptions {
            node_epoch: request.node_epoch,
            open_mode: OpenMode::Recovery,
        };
        Ok(
            ObjectWalFactory::new(self.cluster_id.clone(), request.node_id)
                .build(&request.wal_config, build)?,
        )
    }
}

pub(crate) struct Factory {
    handle: MetadataNodeHandle,
    wal: Arc<dyn WalTakeover>,
}

impl Factory {
    pub(crate) fn new(handle: MetadataNodeHandle, wal: Arc<dyn WalTakeover>) -> Self {
        Self { handle, wal }
    }
}

#[async_trait]
impl FailoverFactory for Factory {
    fn get_stream_manager(&self, node_id: u32, _epoch: u64) -> Arc<dyn StreamManagerTrait> {
        Arc::new(self.handle.stream_manager_of(node_id as i32))
    }

    fn get_object_manager(&self, _node_id: u32, _epoch: u64) -> Arc<dyn ObjectManagerTrait> {
        Arc::new(self.handle.object_manager())
    }

    async fn get_wal(
        &self,
        request: &FailoverRequest,
    ) -> Result<Arc<dyn WriteAheadLogTrait>, StreamError> {
        self.wal.open(request).await
    }
}

pub(crate) struct Failover {
    pub(crate) handle: MetadataNodeHandle,
    pub(crate) views: Arc<ViewPublisher>,
    pub(crate) engine: Arc<S3StreamEngine>,
    pub(crate) streams: Arc<Streams>,
}

impl Failover {
    pub(crate) async fn run(
        self,
        mut leadership: watch::Receiver<bool>,
        tick: Duration,
        timeout: Duration,
    ) {
        let local = self.handle.node_id();
        let mut liveness = Liveness::new(timeout);

        while let Some(leading) = leadership::tick(&mut leadership, tick).await {
            if !leading {
                liveness.forget();
                continue;
            }

            let view = self.views.load();
            for (node_id, epoch) in liveness.dead(&view, local, Instant::now()) {
                tracing::warn!(node_id, epoch, "node stopped heartbeating, fencing it");
                if let Err(error) = self.handle.fence_node(node_id, epoch).await {
                    tracing::warn!(%error, node_id, "fencing failed");
                }
            }

            let fenced = view
                .state
                .nodes
                .values()
                .filter(|row| row.fenced && row.node_id != local);
            for row in fenced {
                if !*leadership.borrow() {
                    break;
                }
                self.take_over(&view.state, row).await;
            }
        }
    }

    async fn take_over(&self, state: &MetadataState, row: &NodeRow) {
        for stream_id in state.placed_on(row.node_id) {
            let moved = if state.get_kv(&stream_id_key(stream_id)).is_none() {
                self.streams.destroy(stream_id).await
            } else if state.pending_transfers.contains_key(&stream_id) {
                let epoch = state.streams[&stream_id].epoch;
                self.handle
                    .complete_transfer(stream_id, epoch)
                    .await
                    .map_err(|error| ServiceError::from(error.to_stream_error()))
            } else {
                self.handle
                    .place_stream(stream_id, None)
                    .await
                    .map(|_| ())
                    .map_err(|error| ServiceError::from(error.to_stream_error()))
            };

            if let Err(error) = moved {
                tracing::warn!(%error, stream_id, "stream of a fenced node not moved");
            }
        }

        let opened = state
            .opening_by_node
            .range((row.node_id, 0)..=(row.node_id, u64::MAX))
            .next()
            .is_some();
        if opened {
            let request = FailoverRequest {
                node_id: row.node_id as u32,
                node_epoch: row.epoch as u64,
                wal_config: row.wal.clone(),
            };

            if let Err(error) = self.engine.failover(request).await {
                tracing::warn!(%error, node_id = row.node_id, "wal takeover failed");
            }
        }
    }
}

struct Liveness {
    timeout: Duration,
    seen: HashMap<i32, ((i64, u64), Instant)>,
    progress: (u64, Instant),
}

impl Liveness {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            seen: HashMap::new(),
            progress: (0, Instant::now()),
        }
    }

    fn forget(&mut self) {
        self.seen.clear();
    }

    fn dead(&mut self, view: &MetadataView, local: i32, now: Instant) -> Vec<(i32, i64)> {
        if view.applied_index != self.progress.0 {
            self.progress = (view.applied_index, now);
        }
        let stalled = now.duration_since(self.progress.1) >= self.timeout;

        self.seen
            .retain(|node_id, _| view.state.nodes.contains_key(node_id));

        let mut dead = Vec::new();
        for (node_id, row) in view.state.nodes.iter() {
            if *node_id == local || row.fenced {
                continue;
            }

            let beat = (row.epoch, row.heartbeats);
            let (last, since) = self.seen.entry(*node_id).or_insert((beat, now));
            if *last != beat {
                *last = beat;
                *since = now;
                continue;
            }

            if !stalled && now.duration_since(*since) >= self.timeout {
                dead.push((*node_id, row.epoch));
            }
        }

        dead
    }
}

#[cfg(test)]
mod tests {
    use picomq_metadata::{MetadataCommand, apply};

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(30);

    fn register(state: &mut MetadataState, node_id: i32, node_epoch: i64) {
        let command = MetadataCommand::RegisterNode {
            node_id,
            node_epoch,
            http_address: String::new(),
            slots: 1,
            protocol_addresses: Default::default(),
            wal: String::new(),
        };
        apply(state, &command).unwrap();
    }

    fn heartbeat(state: &mut MetadataState, node_id: i32, node_epoch: i64) {
        let command = MetadataCommand::Heartbeat {
            node_id,
            node_epoch,
        };
        apply(state, &command).unwrap();
    }

    fn cluster() -> MetadataState {
        let mut state = MetadataState::new();
        register(&mut state, 1, 10);
        register(&mut state, 2, 20);
        register(&mut state, 3, 30);
        state
    }

    fn view(state: &MetadataState, applied_index: u64) -> MetadataView {
        MetadataView {
            applied_index,
            state: state.clone(),
        }
    }

    #[test]
    fn silent_nodes_are_reported_after_the_timeout() {
        let mut state = cluster();
        let mut liveness = Liveness::new(TIMEOUT);
        let start = Instant::now();
        assert!(liveness.dead(&view(&state, 1), 1, start).is_empty());

        heartbeat(&mut state, 3, 30);
        let later = start + TIMEOUT;

        assert_eq!(liveness.dead(&view(&state, 2), 1, later), vec![(2, 20)]);
    }

    #[test]
    fn this_node_and_fenced_nodes_are_never_reported() {
        let mut state = cluster();
        apply(
            &mut state,
            &MetadataCommand::FenceNode {
                node_id: 3,
                node_epoch: 30,
            },
        )
        .unwrap();
        let mut liveness = Liveness::new(TIMEOUT);
        let start = Instant::now();
        liveness.dead(&view(&state, 1), 1, start);

        let dead = liveness.dead(&view(&state, 2), 1, start + TIMEOUT);

        assert_eq!(dead, vec![(2, 20)]);
    }

    #[test]
    fn a_stalled_view_reports_nobody() {
        let state = cluster();
        let mut liveness = Liveness::new(TIMEOUT);
        let start = Instant::now();
        liveness.dead(&view(&state, 1), 1, start);

        assert!(
            liveness
                .dead(&view(&state, 1), 1, start + TIMEOUT)
                .is_empty()
        );
    }

    #[test]
    fn a_new_epoch_restarts_the_clock() {
        let mut state = cluster();
        let mut liveness = Liveness::new(TIMEOUT);
        let start = Instant::now();
        liveness.dead(&view(&state, 1), 1, start);

        register(&mut state, 2, 21);
        let restarted = start + TIMEOUT / 2;
        assert!(liveness.dead(&view(&state, 2), 1, restarted).is_empty());

        let dead = liveness.dead(&view(&state, 3), 1, start + TIMEOUT);
        assert_eq!(dead, vec![(3, 30)]);
    }
}
