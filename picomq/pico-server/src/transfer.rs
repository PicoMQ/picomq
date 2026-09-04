//! Transfer choreography: every node watches the published view and acts on
//! pending transfers that involve it.
//!
//! The source drains and closes the stream, then proposes the completion.
//! The target pre-warms the stream once the completion lands, so the first
//! client request after a transfer does not pay the open cost. The loop also
//! ticks periodically, which retries failed steps and finishes transfers left
//! pending by a crashed source after it restarts.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use picomq_metadata::{MetadataView, ViewPublisher};
use s3stream::StreamState;

use crate::service::S3StreamService;

const RETRY_TICK: Duration = Duration::from_secs(1);

pub struct TransferWatcher;

impl TransferWatcher {
    /// Spawn the watcher task. The node aborts it on close.
    pub fn spawn(
        service: Arc<S3StreamService>,
        views: Arc<ViewPublisher>,
        node_id: i32,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(run(service, views, node_id))
    }
}

async fn run(service: Arc<S3StreamService>, views: Arc<ViewPublisher>, node_id: i32) {
    let mut incoming: HashSet<u64> = HashSet::new();
    loop {
        let view = views.load();
        process(&service, &view, node_id, &mut incoming).await;
        let _ = tokio::time::timeout(RETRY_TICK, views.wait_applied(view.applied_index + 1)).await;
    }
}

async fn process(
    service: &S3StreamService,
    view: &MetadataView,
    node_id: i32,
    incoming: &mut HashSet<u64>,
) {
    for (stream_id, pending) in view.state.pending_transfers.iter() {
        if pending.to_node == node_id {
            incoming.insert(*stream_id);
        }
        if pending.from_node == node_id {
            release_and_complete(service, view, *stream_id).await;
        }
    }

    let completed: Vec<u64> = incoming
        .iter()
        .copied()
        .filter(|id| !view.state.pending_transfers.contains_key(id))
        .collect();
    for stream_id in completed {
        incoming.remove(&stream_id);
        let owned = view
            .state
            .streams
            .get(&stream_id)
            .is_some_and(|row| row.node_id == node_id && row.state == StreamState::Closed);
        if owned && let Err(error) = service.ensure_open(stream_id).await {
            tracing::debug!(%error, stream_id, "transfer pre-warm open failed");
        }
    }
}

/// Source-side step: drain and close the stream, then propose the completion
/// at the epoch it closed at.
async fn release_and_complete(service: &S3StreamService, view: &MetadataView, stream_id: u64) {
    let Some(row) = view.state.streams.get(&stream_id).copied() else {
        return;
    };
    let node = service.node();
    let epoch = match row.state {
        StreamState::Closed => row.epoch,
        StreamState::Opened if row.node_id == node.node_id() => {
            match service.release_for_transfer(stream_id).await {
                Ok(Some(epoch)) => epoch,
                Ok(None) => {
                    // Opened in metadata but not held by this process, so seal
                    // it directly at the current epoch.
                    if let Err(error) = node.close_stream(stream_id, row.epoch).await {
                        tracing::warn!(%error, stream_id, "transfer close failed");
                        return;
                    }
                    row.epoch
                }
                Err(error) => {
                    tracing::warn!(%error, stream_id, "transfer release failed");
                    return;
                }
            }
        }
        _ => return,
    };
    if let Err(error) = node.complete_transfer(stream_id, epoch).await {
        tracing::warn!(%error, stream_id, "transfer completion failed");
    }
}
