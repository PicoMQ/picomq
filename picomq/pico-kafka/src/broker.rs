use std::sync::Arc;

use picomq_auth::Authorizer;
use picomq_metadata::{MetadataNodeHandle, ViewPublisher};
use picomq_server::{
    GroupCoordinator, MetadataOwnershipService, PicoNode, S3StreamService, Topics,
};

/// Shared broker state wired from a running Pico node.
#[derive(Clone)]
pub struct BrokerContext {
    pub node_id: i32,
    pub cluster_id: String,
    pub topics: Arc<Topics>,
    pub ownership: Arc<MetadataOwnershipService>,
    pub views: Arc<ViewPublisher>,
    pub metadata: MetadataNodeHandle,
    pub groups: Arc<GroupCoordinator>,
    pub service: Arc<S3StreamService>,
    pub authorizer: Option<Arc<Authorizer>>,
}

impl BrokerContext {
    pub fn new(node: &PicoNode, authorizer: Option<Arc<Authorizer>>) -> Self {
        Self {
            authorizer,
            node_id: node.config().node_id,
            cluster_id: node.config().cluster_id.clone(),
            topics: node.topics(),
            ownership: node.ownership(),
            views: node.views(),
            metadata: node.metadata().clone(),
            groups: node.groups(),
            service: node.service(),
        }
    }

    pub fn broker_id(&self) -> i32 {
        self.node_id
    }
}
