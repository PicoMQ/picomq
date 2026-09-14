use std::sync::Arc;

use picomq_metadata::{MetadataNodeHandle, ViewPublisher};
use picomq_server::{GroupCoordinator, MetadataOwnershipService, PicoNode, S3StreamService};
use tokio::sync::Mutex;

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
    pub fn new(node: &PicoNode) -> Self {
        Self {
            node_id: node.config().node_id,
            cluster_id: node.config().cluster_id.clone(),
            service: node.service(),
            ownership: node.ownership(),
            views: node.views(),
            metadata: node.metadata().clone(),
            groups: node.groups(),
            producer_ids: Arc::new(Mutex::new(ProducerIdLease::new())),
        }
    }

    pub fn broker_id(&self) -> i32 {
        self.node_id
    }

    pub async fn allocate_producer_id(&self) -> Result<i64, picomq_metadata::MetadataError> {
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
