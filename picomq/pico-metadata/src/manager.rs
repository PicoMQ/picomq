//! Manager implementations over a [`CommandSink`] + published views: the
//! engine's `StreamManager` / `ObjectManager` traits and PicoMQ's KV client.
//! Writes become proposed commands; reads run on the latest published view.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use s3stream::{
    CommitStreamSetObjectRequest, CommitStreamSetObjectResponse, CompactStreamObjectRequest,
    Error as StreamError, ObjectManagerTrait, S3ObjectMetadata, StreamManagerTrait, StreamMetadata,
};

use crate::command::{MetadataCommand, MetadataResult, Proposer};
use crate::error::MetadataError;
use crate::sink::{CommandSink, Proposed};
use crate::view::ViewPublisher;

const RETRY_MIN: Duration = Duration::from_millis(50);
const RETRY_MAX: Duration = Duration::from_secs(2);

/// What a node announces when it registers. `node_epoch` is the requested
/// epoch; the log may assign a higher one.
#[derive(Debug, Clone, Default)]
pub struct Registration {
    pub node_id: i32,
    pub node_epoch: i64,
    pub http_address: String,
    pub slots: u32,
    pub protocol_addresses: std::collections::BTreeMap<String, String>,
    pub wal: String,
}

/// A registered node's identity plus its channels to the metadata plane.
/// Every command it proposes names this node as the proposer. Cheap to
/// clone. The factory for all three manager adapters.
#[derive(Clone)]
pub struct MetadataNodeHandle {
    node_id: i32,
    node_epoch: i64,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
}

impl MetadataNodeHandle {
    /// Returns the handle at the epoch the log assigned.
    pub async fn register(
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
        registration: Registration,
    ) -> Result<Self, MetadataError> {
        let Registration {
            node_id,
            node_epoch,
            http_address,
            slots,
            protocol_addresses,
            wal,
        } = registration;
        let proposed = sink
            .propose(MetadataCommand::RegisterNode {
                node_id,
                node_epoch,
                http_address,
                slots,
                protocol_addresses,
                wal,
            })
            .await?;

        match proposed.result {
            MetadataResult::Id(epoch) => Ok(Self {
                node_id,
                node_epoch: epoch as i64,
                sink,
                views,
            }),
            other => Err(MetadataError::Unexpected {
                message: format!("unexpected register result {other:?}"),
            }),
        }
    }

    pub fn proposer(&self) -> Proposer {
        Proposer::new(self.node_id, self.node_epoch)
    }

    pub fn stream_manager(&self) -> MetadataStreamManager {
        self.stream_manager_of(self.node_id)
    }

    pub fn stream_manager_of(&self, owner: i32) -> MetadataStreamManager {
        MetadataStreamManager {
            node: self.clone(),
            owner,
        }
    }

    pub fn object_manager(&self) -> MetadataObjectManager {
        MetadataObjectManager { node: self.clone() }
    }

    pub fn kv_client(&self) -> MetadataKvClient {
        MetadataKvClient { node: self.clone() }
    }

    pub fn sink_stats(&self) -> Arc<crate::sink::SinkStats> {
        self.sink.stats()
    }

    pub fn node_id(&self) -> i32 {
        self.node_id
    }

    pub fn node_epoch(&self) -> i64 {
        self.node_epoch
    }

    pub async fn place_stream(
        &self,
        stream_id: u64,
        node_id: Option<i32>,
    ) -> Result<i32, MetadataError> {
        match self
            .sink
            .propose(MetadataCommand::PlaceStream {
                proposer: self.proposer(),
                stream_id,
                node_id,
            })
            .await?
            .result
        {
            MetadataResult::Id(node_id) => Ok(node_id as i32),
            other => Err(MetadataError::Unexpected {
                message: format!("unexpected place result {other:?}"),
            }),
        }
    }

    /// Lease a block of `count` numeric producer ids, returning the first.
    pub async fn allocate_producer_ids(&self, count: u32) -> Result<u64, MetadataError> {
        match self
            .sink
            .propose(MetadataCommand::AllocateProducerIds {
                proposer: self.proposer(),
                count,
            })
            .await?
            .result
        {
            MetadataResult::Id(first) => Ok(first),
            other => Err(MetadataError::Unexpected {
                message: format!("unexpected allocate result {other:?}"),
            }),
        }
    }

    pub async fn set_node_slots(&self, node_id: i32, slots: u32) -> Result<(), MetadataError> {
        self.sink
            .propose(MetadataCommand::SetNodeSlots {
                proposer: self.proposer(),
                node_id,
                slots,
            })
            .await?;
        Ok(())
    }

    pub async fn heartbeat(&self) -> Result<(), MetadataError> {
        self.sink
            .propose(MetadataCommand::Heartbeat {
                proposer: self.proposer(),
            })
            .await?;
        Ok(())
    }

    /// A node that moved on is not fenced, and that is success.
    pub async fn fence_node(
        &self,
        node_id: i32,
        node_epoch: i64,
        heartbeats: u64,
    ) -> Result<(), MetadataError> {
        match self
            .sink
            .propose(MetadataCommand::FenceNode {
                proposer: self.proposer(),
                node_id,
                node_epoch,
                heartbeats,
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.is_redundant() => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn expire_prepared_objects(&self, now_ms: i64) -> Result<(), MetadataError> {
        self.sink
            .propose(MetadataCommand::ExpirePreparedObjects {
                proposer: self.proposer(),
                now_ms,
            })
            .await?;
        Ok(())
    }

    pub async fn clean_destroyed_objects(&self, object_ids: Vec<u64>) -> Result<(), MetadataError> {
        self.sink
            .propose(MetadataCommand::CleanDestroyedObjects {
                proposer: self.proposer(),
                object_ids,
            })
            .await?;
        Ok(())
    }

    pub fn views(&self) -> &Arc<ViewPublisher> {
        &self.views
    }

    /// Request a live ownership move of a stream to another node.
    pub async fn propose_transfer(
        &self,
        stream_id: u64,
        from_node: i32,
        to_node: i32,
    ) -> Result<(), MetadataError> {
        self.sink
            .propose(MetadataCommand::TransferStream {
                proposer: self.proposer(),
                stream_id,
                from_node,
                to_node,
            })
            .await?;
        Ok(())
    }

    /// Finish a pending move. A redundant completion is success.
    pub async fn complete_transfer(&self, stream_id: u64, epoch: i64) -> Result<(), MetadataError> {
        match self
            .sink
            .propose(MetadataCommand::CompleteTransfer {
                proposer: self.proposer(),
                stream_id,
                epoch,
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.is_redundant() => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn propose(&self, command: MetadataCommand) -> Result<MetadataResult, StreamError> {
        Ok(self
            .sink
            .propose(command)
            .await
            .map_err(|e| e.to_stream_error())?
            .result)
    }

    /// Proposes an engine command, retrying while the metadata store is
    /// unavailable. Engine commands are idempotent or deduplicated, and the
    /// engine treats any error as fatal for its upload pipeline.
    async fn propose_until_available(
        &self,
        command: MetadataCommand,
    ) -> Result<Proposed, MetadataError> {
        let mut backoff = RETRY_MIN;
        loop {
            match self.sink.propose(command.clone()).await {
                Err(MetadataError::Unavailable { message }) => {
                    tracing::warn!(%message, "metadata store unavailable, retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RETRY_MAX);
                }
                outcome => return outcome,
            }
        }
    }

    async fn propose_engine(
        &self,
        command: MetadataCommand,
    ) -> Result<MetadataResult, StreamError> {
        Ok(self
            .propose_until_available(command)
            .await
            .map_err(|e| e.to_stream_error())?
            .result)
    }
}

fn unexpected_result(result: MetadataResult) -> StreamError {
    StreamError::Unexpected(format!("unexpected metadata result {result:?}"))
}

#[derive(Clone)]
pub struct MetadataStreamManager {
    node: MetadataNodeHandle,
    owner: i32,
}

#[async_trait]
impl StreamManagerTrait for MetadataStreamManager {
    async fn get_opening_streams(&self) -> Result<Vec<StreamMetadata>, StreamError> {
        Ok(self.node.views.load().state.get_opening_streams(self.owner))
    }

    async fn get_streams(&self, stream_ids: &[u64]) -> Result<Vec<StreamMetadata>, StreamError> {
        Ok(self.node.views.load().state.get_streams(stream_ids))
    }

    async fn create_stream(&self, _tags: HashMap<String, String>) -> Result<u64, StreamError> {
        match self
            .node
            .propose_engine(MetadataCommand::CreateStream {
                proposer: self.node.proposer(),
            })
            .await?
        {
            MetadataResult::Id(id) => Ok(id),
            other => Err(unexpected_result(other)),
        }
    }

    async fn open_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        _tags: HashMap<String, String>,
    ) -> Result<StreamMetadata, StreamError> {
        match self
            .node
            .propose_engine(MetadataCommand::OpenStream {
                proposer: self.node.proposer(),
                stream_id,
                epoch: epoch as i64,
            })
            .await?
        {
            MetadataResult::Stream(metadata) => Ok(metadata),
            other => Err(unexpected_result(other)),
        }
    }

    async fn trim_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        new_start_offset: u64,
    ) -> Result<(), StreamError> {
        self.node
            .propose_engine(MetadataCommand::TrimStream {
                proposer: self.node.proposer(),
                stream_id,
                epoch: epoch as i64,
                new_start_offset,
            })
            .await?;
        Ok(())
    }

    async fn close_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        self.node
            .propose_engine(MetadataCommand::CloseStream {
                proposer: self.node.proposer(),
                stream_id,
                epoch: epoch as i64,
            })
            .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        self.node
            .propose_engine(MetadataCommand::DeleteStream {
                proposer: self.node.proposer(),
                stream_id,
                epoch: epoch as i64,
            })
            .await?;
        Ok(())
    }
}

pub struct MetadataObjectManager {
    node: MetadataNodeHandle,
}

#[async_trait]
impl ObjectManagerTrait for MetadataObjectManager {
    async fn prepare_object(&self, count: usize, ttl_ms: u64) -> Result<u64, StreamError> {
        match self
            .node
            .propose_engine(MetadataCommand::PrepareObject {
                proposer: self.node.proposer(),
                count: count as u32,
                ttl_ms: ttl_ms as i64,
                now_ms: picomq_common::now_ms(),
            })
            .await?
        {
            MetadataResult::Id(id) => Ok(id),
            other => Err(unexpected_result(other)),
        }
    }

    /// A redundant commit (duplicate delivery) still yields success.
    async fn commit_stream_set_object(
        &self,
        request: CommitStreamSetObjectRequest,
    ) -> Result<CommitStreamSetObjectResponse, StreamError> {
        let proposed = self
            .node
            .propose_until_available(MetadataCommand::CommitStreamSetObject {
                proposer: self.node.proposer(),
                request,
                now_ms: picomq_common::now_ms(),
            })
            .await;
        match proposed {
            Ok(_) => Ok(CommitStreamSetObjectResponse {}),
            Err(e) if e.is_redundant() => Ok(CommitStreamSetObjectResponse {}),
            Err(e) => Err(e.to_stream_error()),
        }
    }

    /// A redundant compaction (duplicate delivery) still yields success.
    async fn compact_stream_object(
        &self,
        request: CompactStreamObjectRequest,
    ) -> Result<(), StreamError> {
        let proposed = self
            .node
            .propose_until_available(MetadataCommand::CompactStreamObject {
                proposer: self.node.proposer(),
                request,
                now_ms: picomq_common::now_ms(),
            })
            .await;
        match proposed {
            Ok(_) => Ok(()),
            Err(e) if e.is_redundant() => Ok(()),
            Err(e) => Err(e.to_stream_error()),
        }
    }

    async fn get_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        Ok(self
            .node
            .views
            .load()
            .state
            .get_objects(stream_id, start_offset, end_offset, limit))
    }

    async fn get_server_objects(&self) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        Ok(self
            .node
            .views
            .load()
            .state
            .get_server_objects(self.node.node_id))
    }

    async fn get_stream_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        Ok(self.node.views.load().state.get_stream_objects(
            stream_id,
            start_offset,
            end_offset,
            limit,
        ))
    }

    async fn is_object_exist(&self, object_id: u64) -> Result<bool, StreamError> {
        Ok(self.node.views.load().state.is_object_exist(object_id))
    }
}

#[derive(Clone)]
pub struct MetadataKvClient {
    node: MetadataNodeHandle,
}

impl MetadataKvClient {
    pub async fn sync(&self) -> Result<(), StreamError> {
        self.node
            .sink
            .sync()
            .await
            .map_err(|error| error.to_stream_error())
    }

    pub fn get_kv(&self, key: &str) -> Option<Bytes> {
        self.node.views.load().state.get_kv(key)
    }

    pub fn list_kv(&self, prefix: &str) -> Vec<(String, Bytes)> {
        self.node.views.load().state.list_kv(prefix)
    }

    pub async fn put_kv(&self, key: &str, value: Bytes) -> Result<Bytes, StreamError> {
        let command = MetadataCommand::PutKv {
            proposer: self.node.proposer(),
            key: key.to_owned(),
            value,
        };

        match self.node.propose(command).await? {
            MetadataResult::Value(Some(value)) => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    pub async fn put_kv_if_absent(&self, key: &str, value: Bytes) -> Result<Bytes, StreamError> {
        let command = MetadataCommand::PutKvIfAbsent {
            proposer: self.node.proposer(),
            key: key.to_owned(),
            value,
        };

        match self.node.propose(command).await? {
            MetadataResult::Value(Some(value)) => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    pub async fn put_kv_if(
        &self,
        key: &str,
        expected: &Bytes,
        value: Bytes,
    ) -> Result<Option<Bytes>, StreamError> {
        self.conditional(MetadataCommand::PutKvIfMatches {
            proposer: self.node.proposer(),
            key: key.to_owned(),
            expected: expected.clone(),
            value,
        })
        .await
    }

    pub async fn del_kv(&self, key: &str) -> Result<Option<Bytes>, StreamError> {
        let command = MetadataCommand::DeleteKv {
            proposer: self.node.proposer(),
            key: key.to_owned(),
        };

        match self.node.propose(command).await? {
            MetadataResult::Value(value) => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    pub async fn del_kv_if(
        &self,
        key: &str,
        expected: &Bytes,
    ) -> Result<Option<Bytes>, StreamError> {
        self.conditional(MetadataCommand::DeleteKvIfMatches {
            proposer: self.node.proposer(),
            key: key.to_owned(),
            expected: expected.clone(),
        })
        .await
    }

    async fn conditional(&self, command: MetadataCommand) -> Result<Option<Bytes>, StreamError> {
        match self.node.sink.propose(command).await {
            Ok(proposed) => match proposed.result {
                MetadataResult::Value(value) => Ok(value),
                other => Err(unexpected_result(other)),
            },
            Err(error) if error.is_redundant() => Ok(None),
            Err(error) => Err(error.to_stream_error()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::sink::LocalSink;

    /// Unavailable for the next `outage` proposals, then a `LocalSink`.
    struct Flaky {
        sink: LocalSink,
        outage: AtomicUsize,
    }

    #[async_trait]
    impl CommandSink for Flaky {
        async fn propose(&self, command: MetadataCommand) -> Result<Proposed, MetadataError> {
            let outage = self.outage.load(Ordering::SeqCst);
            if outage > 0 {
                self.outage.store(outage - 1, Ordering::SeqCst);
                return Err(MetadataError::Unavailable {
                    message: "store down".into(),
                });
            }
            self.sink.propose(command).await
        }

        async fn sync(&self) -> Result<(), MetadataError> {
            self.sink.sync().await
        }
    }

    #[tokio::test]
    async fn engine_commands_outlast_an_outage_and_kv_writes_do_not() {
        let (sink, views) = LocalSink::new();
        let flaky = Arc::new(Flaky {
            sink,
            outage: AtomicUsize::new(0),
        });
        let registration = Registration {
            node_id: 1,
            node_epoch: 1,
            slots: 1,
            ..Default::default()
        };
        let node = MetadataNodeHandle::register(flaky.clone(), views.clone(), registration)
            .await
            .unwrap();

        flaky.outage.store(3, Ordering::SeqCst);
        let stream_id = node.stream_manager().create_stream(HashMap::new()).await;
        assert!(stream_id.is_ok(), "{stream_id:?}");

        flaky.outage.store(1, Ordering::SeqCst);
        let put = node.kv_client().put_kv("k", Bytes::from_static(b"v")).await;
        assert!(put.is_err());
        assert_eq!(views.load().state.get_kv("k"), None);
    }
}
