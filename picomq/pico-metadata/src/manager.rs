//! Engine-facing manager implementations over a [`CommandSink`] + published
//! views: the adapters that make the metadata plane look like the engine's
//! `StreamManager` / `ObjectManager` / `KVClient` traits. Writes become
//! proposed commands; reads run on the latest published view.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use s3stream::{
    CommitStreamSetObjectRequest, CommitStreamSetObjectResponse, CompactStreamObjectRequest,
    Error as StreamError, KVClient, KeyValue, ObjectManagerTrait, S3ObjectMetadata,
    StreamManagerTrait, StreamMetadata,
};

use crate::command::{MetadataCommand, MetadataResult};
use crate::error::MetadataError;
use crate::sink::CommandSink;
use crate::view::ViewPublisher;

/// This node's identity plus its channels to the metadata plane. Cheap to
/// clone. The factory for all three manager adapters.
#[derive(Clone)]
pub struct MetadataNodeHandle {
    node_id: i32,
    node_epoch: i64,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
}

impl MetadataNodeHandle {
    pub fn new(
        node_id: i32,
        node_epoch: i64,
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
    ) -> Self {
        Self {
            node_id,
            node_epoch,
            sink,
            views,
        }
    }

    /// Register (or re-register at a bumped epoch) this node. Every write
    /// command is fenced on this registration.
    pub async fn register(&self, http_address: &str) -> Result<(), MetadataError> {
        self.sink
            .propose(MetadataCommand::RegisterNode {
                node_id: self.node_id,
                node_epoch: self.node_epoch,
                http_address: http_address.to_owned(),
                slots: 1,
                protocol_addresses: Default::default(),
            })
            .await?;
        Ok(())
    }

    pub fn stream_manager(&self) -> MetadataStreamManager {
        MetadataStreamManager { node: self.clone() }
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

    pub async fn register_with_slots(
        &self,
        http_address: &str,
        slots: u32,
        protocol_addresses: std::collections::BTreeMap<String, String>,
    ) -> Result<(), MetadataError> {
        self.sink
            .propose(MetadataCommand::RegisterNode {
                node_id: self.node_id,
                node_epoch: self.node_epoch,
                http_address: http_address.to_owned(),
                slots,
                protocol_addresses,
            })
            .await?;
        Ok(())
    }

    /// Lease a block of `count` numeric producer ids, returning the first.
    pub async fn allocate_producer_ids(&self, count: u32) -> Result<u64, MetadataError> {
        match self
            .sink
            .propose(MetadataCommand::AllocateProducerIds {
                node_id: self.node_id,
                node_epoch: self.node_epoch,
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

    /// Refresh a registered node's placement weight at its current epoch.
    /// The empty address keeps the node's advertised address unchanged.
    pub async fn update_node_slots(
        &self,
        node_id: i32,
        node_epoch: i64,
        slots: u32,
    ) -> Result<(), MetadataError> {
        self.sink
            .propose(MetadataCommand::RegisterNode {
                node_id,
                node_epoch,
                http_address: String::new(),
                slots,
                protocol_addresses: Default::default(),
            })
            .await?;
        Ok(())
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
            .propose(MetadataCommand::CompleteTransfer { stream_id, epoch })
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.is_redundant() => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Seal a stream at `epoch` directly, without going through the engine.
    pub async fn close_stream(&self, stream_id: u64, epoch: i64) -> Result<(), MetadataError> {
        self.sink
            .propose(MetadataCommand::CloseStream {
                node_id: self.node_id,
                node_epoch: self.node_epoch,
                stream_id,
                epoch,
            })
            .await?;
        Ok(())
    }

    async fn propose(&self, command: MetadataCommand) -> Result<MetadataResult, StreamError> {
        Ok(self
            .sink
            .propose(command)
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
}

#[async_trait]
impl StreamManagerTrait for MetadataStreamManager {
    async fn get_opening_streams(&self) -> Result<Vec<StreamMetadata>, StreamError> {
        Ok(self
            .node
            .views
            .load()
            .state
            .get_opening_streams(self.node.node_id))
    }

    async fn get_streams(&self, stream_ids: &[u64]) -> Result<Vec<StreamMetadata>, StreamError> {
        Ok(self.node.views.load().state.get_streams(stream_ids))
    }

    async fn create_stream(&self, _tags: HashMap<String, String>) -> Result<u64, StreamError> {
        match self
            .node
            .propose(MetadataCommand::CreateStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
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
            .propose(MetadataCommand::OpenStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
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
            .propose(MetadataCommand::TrimStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
                stream_id,
                epoch: epoch as i64,
                new_start_offset,
            })
            .await?;
        Ok(())
    }

    async fn close_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        self.node
            .propose(MetadataCommand::CloseStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
                stream_id,
                epoch: epoch as i64,
            })
            .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        self.node
            .propose(MetadataCommand::DeleteStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
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
            .propose(MetadataCommand::PrepareObject {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
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
            .sink
            .propose(MetadataCommand::CommitStreamSetObject {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
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
            .sink
            .propose(MetadataCommand::CompactStreamObject {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
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

pub struct MetadataKvClient {
    node: MetadataNodeHandle,
}

#[async_trait]
impl KVClient for MetadataKvClient {
    async fn put_kv_if_absent(&self, kv: KeyValue) -> Result<Bytes, StreamError> {
        match self
            .node
            .propose(MetadataCommand::PutKvIfAbsent {
                key: kv.key,
                value: kv.value,
            })
            .await?
        {
            MetadataResult::Value(Some(value)) => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    async fn put_kv(&self, kv: KeyValue) -> Result<Bytes, StreamError> {
        match self
            .node
            .propose(MetadataCommand::PutKv {
                key: kv.key,
                value: kv.value,
            })
            .await?
        {
            MetadataResult::Value(Some(value)) => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    async fn get_kv(&self, key: &str) -> Result<Option<Bytes>, StreamError> {
        Ok(self.node.views.load().state.get_kv(key))
    }

    async fn del_kv(&self, key: &str) -> Result<Option<Bytes>, StreamError> {
        match self
            .node
            .propose(MetadataCommand::DeleteKv {
                key: key.to_owned(),
            })
            .await?
        {
            MetadataResult::Value(value) => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    async fn del_kv_if(&self, key: &str, expected: &Bytes) -> Result<Option<Bytes>, StreamError> {
        let proposed = self
            .node
            .sink
            .propose(MetadataCommand::DeleteKvIfMatches {
                key: key.to_owned(),
                expected: expected.clone(),
            })
            .await;
        match proposed {
            Ok(p) => match p.result {
                MetadataResult::Value(value) => Ok(value),
                other => Err(unexpected_result(other)),
            },
            Err(e) if e.is_redundant() => Ok(None),
            Err(e) => Err(e.to_stream_error()),
        }
    }

    async fn list_kv(&self, prefix: &str) -> Result<Vec<KeyValue>, StreamError> {
        Ok(self
            .node
            .views
            .load()
            .state
            .list_kv(prefix)
            .into_iter()
            .map(|(key, value)| KeyValue { key, value })
            .collect())
    }
}
