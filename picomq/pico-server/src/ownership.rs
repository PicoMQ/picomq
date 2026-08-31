//! Stream ownership: who serves a named stream (the HTTP layer turns a remote
//! owner into a 307 redirect).
//!
//! [`MetadataOwnershipService`] reads the owner from the published metadata
//! view. The decision table:
//!
//! 1. name not registered → local (create lands here).
//! 2. pending transfer → the transfer target.
//! 3. opened, or placed but never opened → the owning node.
//! 4. closed → local (any node can revive it).

use std::sync::Arc;

use async_trait::async_trait;
use picomq_metadata::ViewPublisher;
use s3stream::StreamState;

use crate::error::ServiceError;
use crate::service::S3StreamService;
use crate::types::{NodeMeta, Owner};

#[async_trait]
pub trait OwnershipService: Send + Sync {
    async fn owner_of(&self, name: &str) -> Result<Owner, ServiceError>;

    fn local_node(&self) -> NodeMeta;
}

pub struct MetadataOwnershipService {
    views: Arc<ViewPublisher>,
    node_id: i32,
    http_address: String,
    service: Arc<S3StreamService>,
}

impl MetadataOwnershipService {
    pub fn new(
        views: Arc<ViewPublisher>,
        node_id: i32,
        http_address: String,
        service: Arc<S3StreamService>,
    ) -> Self {
        Self {
            views,
            node_id,
            http_address,
            service,
        }
    }
}

#[async_trait]
impl OwnershipService for MetadataOwnershipService {
    async fn owner_of(&self, name: &str) -> Result<Owner, ServiceError> {
        let Some(stream_id) = self.service.lookup_stream_id(name).await? else {
            return Ok(Owner::local(None));
        };
        let view = self.views.load();

        // A pending transfer routes to the target, which stalls the open
        // until the handoff settles.
        if let Some(pending) = view.state.pending_transfers.get(&stream_id) {
            if pending.to_node == self.node_id {
                return Ok(Owner::local(Some(stream_id)));
            }
            return Ok(match view.state.get_node_address(pending.to_node) {
                Some(address) => Owner::remote(stream_id, pending.to_node, address.to_owned()),
                None => Owner::local(Some(stream_id)),
            });
        }

        let Some(row) = view.state.streams.get(&stream_id).copied() else {
            return Ok(Owner::local(Some(stream_id)));
        };
        // Opened streams route to their owner. Placed streams that never
        // opened route to their placement. Closed streams stay local so any
        // node can revive them after their owner shut down.
        let routable = row.state == StreamState::Opened || (row.epoch == -1 && row.node_id != -1);
        if !routable {
            return Ok(Owner::local(Some(stream_id)));
        }
        let owner_id = row.node_id;
        if owner_id == self.node_id {
            return Ok(Owner::local(Some(stream_id)));
        }
        match view.state.get_node_address(owner_id) {
            Some(address) if !address.is_empty() => {
                Ok(Owner::remote(stream_id, owner_id, address.to_owned()))
            }
            _ => Ok(Owner::local(Some(stream_id))),
        }
    }

    fn local_node(&self) -> NodeMeta {
        NodeMeta {
            node_id: self.node_id,
            advertised_address: self.http_address.clone(),
        }
    }
}
