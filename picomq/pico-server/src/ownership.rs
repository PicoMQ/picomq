//! Log ownership: which node serves a log (the HTTP layer turns a remote
//! owner into a 307 redirect).
//!
//! [`MetadataOwnershipService`] reads the owner from the published metadata
//! view. The decision table:
//!
//! 1. log not registered → local (create lands here).
//! 2. pending transfer to a live node → the transfer target.
//! 3. opened, or placed on a live node → the owning node.
//! 4. owner fenced or never placed → local.

use std::sync::Arc;

use async_trait::async_trait;
use picomq_metadata::ViewPublisher;
use s3stream::StreamState;

use crate::error::ServiceError;
use crate::log::Log;
use crate::service::S3StreamService;
use crate::types::{NodeMeta, Owner};

#[async_trait]
pub trait OwnershipService: Send + Sync {
    async fn owner_of(&self, log: &Log) -> Result<Owner, ServiceError>;

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
    async fn owner_of(&self, log: &Log) -> Result<Owner, ServiceError> {
        let Some(stream_id) = self.service.stream_id(log)? else {
            return Ok(Owner::local(None, -1));
        };
        let view = self.views.load();
        let epoch = view
            .state
            .streams
            .get(&stream_id)
            .map(|row| row.epoch)
            .unwrap_or(-1);

        let pending = view
            .state
            .pending_transfers
            .get(&stream_id)
            .filter(|pending| {
                view.state
                    .nodes
                    .get(&pending.to_node)
                    .is_some_and(|node| !node.fenced)
            });
        if let Some(pending) = pending {
            if pending.to_node == self.node_id {
                return Ok(Owner::local(Some(stream_id), epoch));
            }
            return Ok(match view.state.get_node_address(pending.to_node) {
                Some(address) => {
                    Owner::remote(stream_id, epoch, pending.to_node, address.to_owned())
                }
                None => Owner::local(Some(stream_id), epoch),
            });
        }

        let Some(row) = view.state.streams.get(&stream_id).copied() else {
            return Ok(Owner::local(Some(stream_id), epoch));
        };
        let owner_live = view
            .state
            .nodes
            .get(&row.node_id)
            .is_some_and(|node| !node.fenced);
        let routable = owner_live && (row.state == StreamState::Opened || row.node_id != -1);
        if !routable || row.node_id == self.node_id {
            return Ok(Owner::local(Some(stream_id), epoch));
        }

        match view.state.get_node_address(row.node_id) {
            Some(address) if !address.is_empty() => Ok(Owner::remote(
                stream_id,
                epoch,
                row.node_id,
                address.to_owned(),
            )),
            _ => Ok(Owner::local(Some(stream_id), epoch)),
        }
    }

    fn local_node(&self) -> NodeMeta {
        NodeMeta {
            node_id: self.node_id,
            advertised_address: self.http_address.clone(),
        }
    }
}
