use std::sync::Arc;

use pico_metadata::{MetadataNodeHandle, ViewPublisher};
use pico_server::{MetadataOwnershipService, S3StreamService};
use tokio::sync::Mutex;

use crate::group::GroupCoordinator;

const PRODUCER_ID_BLOCK: u32 = 256;

#[derive(Debug)]
struct ProducerIdLease {
    next: u64,
    remaining: u32,
}

impl ProducerIdLease {
    fn new() -> Self {
        Self {
            next: 0,
            remaining: 0,
        }
    }
}

/// Shared broker state wired from a running Pico node.
#[derive(Clone)]
pub struct BrokerContext {
    pub node_id: i32,
    pub cluster_id: String,
    pub service: Arc<S3StreamService>,
    pub ownership: Arc<MetadataOwnershipService>,
    pub views: Arc<ViewPublisher>,
    pub metadata: MetadataNodeHandle,
    pub groups: Arc<GroupCoordinator>,
    producer_ids: Arc<Mutex<ProducerIdLease>>,
}

impl BrokerContext {
    pub fn new(
        node_id: i32,
        cluster_id: String,
        service: Arc<S3StreamService>,
        ownership: Arc<MetadataOwnershipService>,
        views: Arc<ViewPublisher>,
        metadata: MetadataNodeHandle,
    ) -> Self {
        let groups =
            GroupCoordinator::new(node_id, service.clone(), ownership.clone(), views.clone());
        Self {
            node_id,
            cluster_id,
            service,
            ownership,
            views,
            metadata,
            groups,
            producer_ids: Arc::new(Mutex::new(ProducerIdLease::new())),
        }
    }

    pub fn broker_id(&self) -> i32 {
        self.node_id
    }

    pub async fn allocate_producer_id(&self) -> Result<i64, pico_metadata::MetadataError> {
        let mut lease = self.producer_ids.lock().await;
        if lease.remaining == 0 {
            let first = self
                .metadata
                .allocate_producer_ids(PRODUCER_ID_BLOCK)
                .await?;
            lease.next = first;
            lease.remaining = PRODUCER_ID_BLOCK;
        }
        let id = lease.next as i64;
        lease.next += 1;
        lease.remaining -= 1;
        Ok(id)
    }
}
