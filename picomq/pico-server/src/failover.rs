//! Node failover, run by the lease holder: fence nodes that stopped
//! heartbeating, then recover their WALs and move their streams to live
//! nodes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use picomq_metadata::{MetadataNodeHandle, MetadataState, NodeRow, Proposer, ViewPublisher};
use s3stream::{
    Client as _, Error as StreamError, FailoverFactory, FailoverRequest, ObjectManagerTrait,
    S3StreamEngine, StreamManagerTrait, WriteAheadLogTrait,
};
use tokio::sync::watch;

use crate::error::ServiceError;
use crate::leadership;
use crate::service::stream_id_key;
use crate::streams::Streams;
use crate::wal::Wal;

pub(crate) struct Factory {
    handle: MetadataNodeHandle,
    wal: Arc<dyn Wal>,
}

impl Factory {
    pub(crate) fn new(handle: MetadataNodeHandle, wal: Arc<dyn Wal>) -> Self {
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
        self.wal.take_over(request).await
    }
}

pub(crate) struct Failover {
    pub(crate) handle: MetadataNodeHandle,
    pub(crate) views: Arc<ViewPublisher>,
    pub(crate) engine: Arc<S3StreamEngine>,
    pub(crate) streams: Arc<Streams>,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) node_timeout: Duration,
}

impl Failover {
    pub(crate) async fn run(self, mut leadership: watch::Receiver<bool>, tick: Duration) {
        let local = self.handle.proposer();
        let mut liveness = Liveness::new(self.heartbeat_interval, self.node_timeout);

        while let Some(leading) = leadership::tick(&mut leadership, tick).await {
            let view = self.views.load();
            if !leading || !view.state.is_live(local) {
                liveness.forget();
                continue;
            }

            for (node_id, epoch, heartbeats) in liveness.dead(&view.state, local) {
                tracing::warn!(node_id, epoch, "node stopped heartbeating, fencing it");
                if let Err(error) = self.handle.fence_node(node_id, epoch, heartbeats).await {
                    tracing::warn!(%error, node_id, "fencing failed");
                }
            }

            let fenced = view
                .state
                .nodes
                .values()
                .filter(|row| row.fenced && row.node_id != local.node_id);
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

/// Silence measured in the leader's own heartbeats as they land in the
/// metadata log: a node is dead once the leader heartbeated `beats` times
/// while the node did not heartbeat at all. A leader that is paused, cut off
/// from the log, or fenced stops this clock with its own heartbeats, so time
/// it did not observe never counts against anyone.
struct Liveness {
    beats: u64,
    seen: HashMap<i32, ((i64, u64), u64)>,
}

impl Liveness {
    fn new(heartbeat_interval: Duration, node_timeout: Duration) -> Self {
        let beats = node_timeout
            .as_millis()
            .div_ceil(heartbeat_interval.as_millis().max(1));
        Self {
            beats: u64::try_from(beats).unwrap_or(u64::MAX).max(1),
            seen: HashMap::new(),
        }
    }

    fn forget(&mut self) {
        self.seen.clear();
    }

    /// Nodes that stayed silent for the timeout, with the epoch and heartbeat
    /// count a fence must still find them at.
    fn dead(&mut self, state: &MetadataState, local: Proposer) -> Vec<(i32, i64, u64)> {
        let Some(clock) = state.nodes.get(&local.node_id).map(|row| row.heartbeats) else {
            return Vec::new();
        };

        self.seen
            .retain(|node_id, _| state.nodes.contains_key(node_id));

        let mut dead = Vec::new();
        for (node_id, row) in state.nodes.iter() {
            if *node_id == local.node_id || row.fenced {
                continue;
            }

            let beat = (row.epoch, row.heartbeats);
            let (last, since) = self.seen.entry(*node_id).or_insert((beat, clock));
            if *last != beat {
                *last = beat;
                *since = clock;
                continue;
            }

            if clock.saturating_sub(*since) >= self.beats {
                dead.push((*node_id, row.epoch, row.heartbeats));
            }
        }

        dead
    }
}

#[cfg(test)]
mod tests {
    use picomq_metadata::{MetadataCommand, apply};

    use super::*;

    const INTERVAL: Duration = Duration::from_secs(5);
    const TIMEOUT: Duration = Duration::from_secs(30);
    const LEADER: Proposer = Proposer {
        node_id: 1,
        node_epoch: 10,
    };

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
            proposer: Proposer::new(node_id, node_epoch),
        };
        apply(state, &command).unwrap();
    }

    fn leader_heartbeats(state: &mut MetadataState, count: u64) {
        for _ in 0..count {
            heartbeat(state, LEADER.node_id, LEADER.node_epoch);
        }
    }

    fn cluster() -> MetadataState {
        let mut state = MetadataState::new();
        register(&mut state, 1, 10);
        register(&mut state, 2, 20);
        register(&mut state, 3, 30);
        state
    }

    #[test]
    fn the_timeout_is_counted_in_heartbeats() {
        assert_eq!(Liveness::new(INTERVAL, TIMEOUT).beats, 6);
        assert_eq!(
            Liveness::new(Duration::from_secs(4), TIMEOUT).beats,
            8,
            "a partial interval rounds up"
        );
    }

    #[test]
    fn silent_nodes_are_reported_after_the_leader_heartbeats_through_the_timeout() {
        let mut state = cluster();
        let mut liveness = Liveness::new(INTERVAL, TIMEOUT);
        assert!(liveness.dead(&state, LEADER).is_empty());

        leader_heartbeats(&mut state, 5);
        heartbeat(&mut state, 3, 30);
        assert!(liveness.dead(&state, LEADER).is_empty());

        leader_heartbeats(&mut state, 1);
        let silent = state.nodes[&2].heartbeats;
        assert_eq!(liveness.dead(&state, LEADER), vec![(2, 20, silent)]);
    }

    #[test]
    fn a_leader_whose_heartbeats_stop_reports_nobody() {
        let state = cluster();
        let mut liveness = Liveness::new(INTERVAL, TIMEOUT);
        liveness.dead(&state, LEADER);

        for _ in 0..100 {
            assert!(liveness.dead(&state, LEADER).is_empty());
        }
    }

    #[test]
    fn this_node_and_fenced_nodes_are_never_reported() {
        let mut state = cluster();
        let heartbeats = state.nodes[&3].heartbeats;
        apply(
            &mut state,
            &MetadataCommand::FenceNode {
                proposer: LEADER,
                node_id: 3,
                node_epoch: 30,
                heartbeats,
            },
        )
        .unwrap();
        let mut liveness = Liveness::new(INTERVAL, TIMEOUT);
        liveness.dead(&state, LEADER);

        leader_heartbeats(&mut state, 6);
        let dead: Vec<i32> = liveness
            .dead(&state, LEADER)
            .into_iter()
            .map(|(node_id, _, _)| node_id)
            .collect();

        assert_eq!(dead, vec![2]);
    }

    #[test]
    fn a_new_epoch_restarts_the_clock() {
        let mut state = cluster();
        let mut liveness = Liveness::new(INTERVAL, TIMEOUT);
        liveness.dead(&state, LEADER);

        leader_heartbeats(&mut state, 3);
        register(&mut state, 2, 21);
        assert!(liveness.dead(&state, LEADER).is_empty());

        leader_heartbeats(&mut state, 3);
        let dead: Vec<i32> = liveness
            .dead(&state, LEADER)
            .into_iter()
            .map(|(node_id, _, _)| node_id)
            .collect();
        assert_eq!(dead, vec![3], "node 2 restarted three heartbeats ago");
    }
}
