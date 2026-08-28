pub mod config;

use std::sync::Arc;

use async_trait::async_trait;
use s3stream_core::api::StreamError;
use s3stream_core::cache::blockcache::StreamReaders;
use s3stream_core::compact::{CompactionConfig, CompactionManager};
use s3stream_core::failover::{DefaultFailoverFactory, FailoverFactory, WalRecover};
use s3stream_core::manager::{
    HookedObjectManager, HookedStreamManager, ObjectManager, StreamManager,
};
use s3stream_core::storage::s3_storage::{LogStorageFailureHandler, S3Storage, S3StorageConfig};
use s3stream_core::stream_client::{S3StreamClient, StreamClientConfig};
use s3stream_core::throttle::build_network_limiters;
use s3stream_core::{Storage, StreamClient, ThrottledObjectStorage};
use s3stream_object::{ObjectStorage, RetryingObjectStorage};
use s3stream_wal::WriteAheadLog;

pub use config::{Config, NetworkBandwidthMode};

// Public API re-exports (hosts should only need `use s3stream::...`).
pub use s3stream_codec::StreamRecordBatch;
pub use s3stream_core::api::{
    AppendResult, Client, CreateStreamOptions, FetchResult, KVClient, KeyValue, OpenStreamOptions,
    PendingAppend, RecordBatch, Stream,
};
pub use s3stream_core::context::{AppendContext, FetchContext};
pub use s3stream_core::manager::{
    CommitStreamSetObjectRequest, CommitStreamSetObjectResponse, CompactStreamObjectRequest,
    StreamMetadata, StreamObject,
};
// Metadata-plane host surface: everything a `StreamManager`/`ObjectManager`/`KVClient`
// implementation (e.g. pico-metadata) needs, so hosts depend only on this
// facade and never on engine internals.
pub use s3stream_core::compact::{CompactOperations, CompactionLevel};
pub use s3stream_core::index::LocalStreamRangeIndexCache;
pub use s3stream_core::manager::{
    ObjectManager as ObjectManagerTrait, StreamManager as StreamManagerTrait, StreamState,
};
pub use s3stream_core::memory::{MemoryKvClient, MemoryMetadataManager};
/// Metric names/labels (`kafka_stream_*`) and recording helpers. Hosts install a
/// [`metrics`](https://docs.rs/metrics) recorder to export them.
pub use s3stream_core::metrics;
pub use s3stream_core::{
    Failover, FailoverRequest, FailoverResponse, StreamClient as StreamClientTrait,
    StreamError as Error,
};
pub use s3stream_object::{
    ObjectAttributes, ObjectStreamRange, S3ObjectType, StreamOffsetRange, NOOP_OBJECT_ID,
};
pub use s3stream_object::{ObjectStorage as ObjectStorageTrait, S3ObjectMetadata};
// ObjectStorage.delete): hosts deleting engine objects by id need the key
// scheme and path/error types.
pub use s3stream_object::{gen_object_key, ObjectError, ObjectPath};
pub use s3stream_object::{IdUri, MemoryObjectStorage, ObjectStoreAdapter};
pub use s3stream_wal::object::{ObjectWalConfig, ObjectWalService};
pub use s3stream_wal::{OpenMode, WriteAheadLog as WriteAheadLogTrait};

/// A wired, started engine: storage pipeline + stream client + compaction.
///
/// Shutdown ordering: streams close (uploading buffered data), then
/// compaction and storage stop.
pub struct S3StreamEngine {
    stream_client: Arc<S3StreamClient>,
    storage: Arc<S3Storage>,
    compaction: Option<Arc<CompactionManager>>,
    range_index_cache: Arc<LocalStreamRangeIndexCache>,
    kv_client: Arc<dyn KVClient>,
    failover: Arc<Failover>,
}

impl S3StreamEngine {
    pub fn stream_client(&self) -> Arc<dyn StreamClient> {
        Arc::clone(&self.stream_client) as Arc<dyn StreamClient>
    }

    /// Host handle for replaying confirmed WAL/objects into the snapshot-read cache.
    ///
    pub fn snapshot_read_cache(&self) -> s3stream_core::SnapshotReadCache {
        self.storage.snapshot_read_cache()
    }

    /// The stream-set object compaction manager (None when disabled).
    pub fn compaction_manager(&self) -> Option<Arc<CompactionManager>> {
        self.compaction.clone()
    }

    /// The node-local stream range index (hosts use `search_object_id` to
    /// resolve cold-read start objects without a metadata-plane round trip).
    pub fn range_index_cache(&self) -> Arc<LocalStreamRangeIndexCache> {
        Arc::clone(&self.range_index_cache)
    }

    /// Graceful shutdown: close all streams (force-uploads buffered data), stop
    /// compaction, stop the storage pipeline and WAL.
    pub async fn shutdown(&self) {
        self.stream_client.shutdown().await;
        if let Some(compaction) = &self.compaction {
            compaction.shutdown();
        }
        self.storage.shutdown().await;
    }
}

#[async_trait]
impl Client for S3StreamEngine {
    async fn start(&self) -> Result<(), StreamError> {
        Ok(())
    }

    async fn shutdown(&self) {
        S3StreamEngine::shutdown(self).await;
    }

    fn stream_client(&self) -> Arc<dyn StreamClient> {
        Arc::clone(&self.stream_client) as Arc<dyn StreamClient>
    }

    fn kv_client(&self) -> Arc<dyn KVClient> {
        Arc::clone(&self.kv_client)
    }

    async fn failover(&self, request: FailoverRequest) -> Result<FailoverResponse, StreamError> {
        self.failover.failover(request).await
    }
}

/// Builder wiring the engine from config + host-provided managers: object
/// storage from bucket URIs, the object WAL from `wal_config`, then storage,
/// block cache, compaction, and the stream client.
pub struct S3StreamBuilder {
    config: Config,
    object_storage: Option<Arc<dyn ObjectStorage>>,
    wal: Option<Arc<dyn WriteAheadLog>>,
    object_manager: Option<Arc<dyn ObjectManager>>,
    stream_manager: Option<Arc<dyn StreamManager>>,
    failure_handler: Option<Arc<dyn s3stream_core::StorageFailureHandler>>,
    kv_client: Option<Arc<dyn KVClient>>,
    failover_factory: Option<Arc<dyn FailoverFactory>>,
}

impl S3StreamBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            object_storage: None,
            wal: None,
            object_manager: None,
            stream_manager: None,
            failure_handler: None,
            kv_client: None,
            failover_factory: None,
        }
    }

    /// Override the data object storage (default: built from `config.data_buckets`).
    pub fn object_storage(mut self, storage: Arc<dyn ObjectStorage>) -> Self {
        self.object_storage = Some(storage);
        self
    }

    /// Override the WAL (default: object WAL built from `config.wal_config`).
    pub fn write_ahead_log(mut self, wal: Arc<dyn WriteAheadLog>) -> Self {
        self.wal = Some(wal);
        self
    }

    pub fn object_manager(mut self, manager: Arc<dyn ObjectManager>) -> Self {
        self.object_manager = Some(manager);
        self
    }

    pub fn stream_manager(mut self, manager: Arc<dyn StreamManager>) -> Self {
        self.stream_manager = Some(manager);
        self
    }

    /// Override the storage-failure handler (default: log only). Hosts that
    /// want self-eviction should pass a chain ending in Halt. Halt must not
    /// be wired in tests.
    pub fn storage_failure_handler(
        mut self,
        handler: Arc<dyn s3stream_core::StorageFailureHandler>,
    ) -> Self {
        self.failure_handler = Some(handler);
        self
    }

    /// The KV client returned by `Client::kv_client`. The default is the
    /// in-memory `MemoryKvClient`. Production hosts must pass a durable one.
    pub fn kv_client(mut self, kv_client: Arc<dyn KVClient>) -> Self {
        self.kv_client = Some(kv_client);
        self
    }

    /// Override the failover factory (default: `DefaultFailoverFactory`
    /// building the dead node's WAL from the request URI). Hosts override
    /// this e.g. to bind managers to the dead node's id/epoch.
    pub fn failover_factory(mut self, factory: Arc<dyn FailoverFactory>) -> Self {
        self.failover_factory = Some(factory);
        self
    }

    /// Wire everything, run startup (WAL recovery), return the engine.
    pub async fn build(self) -> Result<S3StreamEngine, StreamError> {
        let config = self.config;
        config.validate()?;
        let object_manager = self
            .object_manager
            .ok_or_else(|| StreamError::Unexpected("object_manager is required".into()))?;
        let stream_manager = self
            .stream_manager
            .ok_or_else(|| StreamError::Unexpected("stream_manager is required".into()))?;

        // →
        // `GlobalNetworkBandwidthLimiters.setup(mode, baseline, refillPeriod)`.
        // no process singleton. The builder owns the pair. Baseline 0
        // Zero rate disables throttling.
        let limiters = if config.network_baseline_bandwidth > 0 {
            let (inbound, outbound) = build_network_limiters(
                config.network_bandwidth_mode,
                config.network_baseline_bandwidth,
                config.refill_period_ms,
            )?;
            Some((inbound, outbound))
        } else {
            None
        };
        // Inject the same inbound/outbound limiters into every object storage
        // built here. Retry wraps outermost so each retry attempt debits
        // bandwidth.
        let resilient = |storage: Arc<dyn ObjectStorage>| -> Arc<dyn ObjectStorage> {
            let storage: Arc<dyn ObjectStorage> = match &limiters {
                Some((inbound, outbound)) => Arc::new(ThrottledObjectStorage::new(
                    storage,
                    Some(Arc::clone(inbound)),
                    Some(Arc::clone(outbound)),
                )),
                None => storage,
            };
            Arc::new(RetryingObjectStorage::new(storage))
        };

        // Data object storage: explicit override, else the first data bucket URI.
        let object_storage: Arc<dyn ObjectStorage> = resilient(match self.object_storage {
            Some(storage) => storage,
            None => {
                let uri = config.data_buckets.first().ok_or_else(|| {
                    StreamError::Unexpected("config.data_buckets is empty".into())
                })?;
                Arc::new(ObjectStoreAdapter::from_bucket_uri(uri)?)
            }
        });

        // `localIndexCache = LocalStreamRangeIndexCache
        // .create(). LocalIndexCache.init(nodeId, backgroundObjectStorage).
        // Inline. There is no 10ms batch-upload scheduler to start (uploads coalesce
        // on the cache's async mutex).
        let range_index_cache = Arc::new(
            LocalStreamRangeIndexCache::init(config.node_id, Arc::clone(&object_storage)).await,
        );
        // Raw (unhooked) managers for the failover factory: recovered commits
        // belong to the DEAD node and must not feed this node's local range
        // index.
        let raw_object_manager = Arc::clone(&object_manager);
        let raw_stream_manager = Arc::clone(&stream_manager);
        // Index-cache hooks are expressed as decorators so every engine
        // consumer sees the hooked managers.
        let object_manager: Arc<dyn ObjectManager> = Arc::new(HookedObjectManager::new(
            object_manager,
            Arc::clone(&range_index_cache) as _,
        ));
        let stream_manager: Arc<dyn StreamManager> = Arc::new(HookedStreamManager::new(
            stream_manager,
            Arc::clone(&range_index_cache) as _,
        ));

        // WAL: explicit override, else an object WAL on the wal_config bucket.
        // An overridden WAL owns its storage. Only the WAL built here gets the
        // retry and throttle wrappers.
        let wal: Arc<dyn WriteAheadLog> = match self.wal {
            Some(wal) => wal,
            None => {
                let mut wal_config = ObjectWalConfig::from_uri_or_defaults(&config.wal_config)
                    .map_err(StreamError::from)?;
                wal_config.cluster_id = config.cluster_id.clone();
                wal_config.node_id = config.node_id;
                wal_config.epoch = config.node_epoch;
                let wal_storage: Arc<dyn ObjectStorage> = resilient(Arc::new(
                    ObjectStoreAdapter::from_bucket_uri(&config.wal_config)?,
                ));
                Arc::new(ObjectWalService::new(wal_storage, wal_config))
            }
        };

        // Cold read path: StreamReaders (readahead + DataBlockCache).
        let block_cache = StreamReaders::new(
            config.block_cache_size,
            Arc::clone(&object_manager),
            Arc::clone(&object_storage),
            4,
        );

        let storage_config = S3StorageConfig {
            wal_cache_size: config.wal_cache_size,
            wal_upload_threshold: config.wal_upload_threshold,
            wal_upload_interval_ms: config.wal_upload_interval_ms,
            stream_split_size: config.stream_split_size,
            max_stream_num_per_stream_set_object: config.max_stream_num_per_stream_set_object,
            object_block_size: config.object_block_size,
            object_part_size: config.object_part_size,
            snapshot_read_enable: config.snapshot_read_enable,
        };
        let storage = Arc::new(S3Storage::new(
            storage_config.clone(),
            Arc::clone(&wal),
            block_cache,
            Arc::clone(&object_storage),
            Arc::clone(&object_manager),
            Arc::clone(&stream_manager),
            self.failure_handler
                .unwrap_or_else(|| Arc::new(LogStorageFailureHandler)),
            // The facade has no zerozone decoder.
            // Hosts that replay linked WAL records pass one into `SnapshotReadCache::new`.
            None,
        ));
        // WAL start + recovery before serving traffic.
        storage.startup().await?;

        match object_manager.get_server_objects().await {
            Ok(objects) => {
                let live: std::collections::HashSet<u64> =
                    objects.iter().map(|m| m.object_id).collect();
                if let Err(e) = range_index_cache.async_prune(&live).await {
                    tracing::warn!(error = %e, "range index prune after startup failed");
                }
            }
            Err(e) => tracing::warn!(error = %e, "skipping range index prune: {e}"),
        }

        let compaction = if config.compaction_interval_ms > 0 {
            let compaction = CompactionManager::new(
                CompactionConfig {
                    compaction_interval_min: (config.compaction_interval_ms / 60_000).max(1),
                    stream_split_size: config.stream_split_size,
                    object_part_size: config.object_part_size,
                    ..CompactionConfig::defaults()
                },
                Arc::clone(&object_manager),
                Arc::clone(&stream_manager),
                Arc::clone(&object_storage),
            );
            compaction.start();
            Some(compaction)
        } else {
            None
        };

        let stream_client = S3StreamClient::new(
            Arc::clone(&storage) as Arc<dyn Storage>,
            stream_manager,
            object_manager,
            Arc::clone(&object_storage),
            StreamClientConfig::default(),
        );

        // Failover over a factory of
        // failover-mode managers plus a recover callback into `storage.recover`
        // (upload the dead node's uncommitted WAL tail, reset its WAL, close its
        // opening streams).
        let failover_factory: Arc<dyn FailoverFactory> = match self.failover_factory {
            Some(factory) => factory,
            None => Arc::new(DefaultFailoverFactory::new(
                config.cluster_id.clone(),
                raw_stream_manager,
                raw_object_manager,
            )),
        };
        let recover: WalRecover = {
            let storage_config = storage_config.clone();
            let object_storage = Arc::clone(&object_storage);
            Arc::new(move |wal, stream_manager, object_manager| {
                let storage_config = storage_config.clone();
                let object_storage = Arc::clone(&object_storage);
                Box::pin(async move {
                    S3Storage::recover(
                        &storage_config,
                        &*wal,
                        &object_storage,
                        &*stream_manager,
                        &object_manager,
                    )
                    .await
                })
            })
        };
        let failover = Arc::new(Failover::new(failover_factory, recover));

        let kv_client: Arc<dyn KVClient> = match self.kv_client {
            Some(kv) => kv,
            None => MemoryKvClient::new(),
        };

        Ok(S3StreamEngine {
            stream_client,
            storage,
            compaction,
            range_index_cache,
            kv_client,
            failover,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    /// End-to-end through the public facade: build over in-memory storage, create a
    /// stream, append, fetch back, close, reopen at a higher epoch, read again.
    #[tokio::test]
    async fn end_to_end_append_fetch_reopen() {
        let manager = MemoryMetadataManager::new();
        let object_storage: Arc<dyn ObjectStorage> =
            Arc::new(s3stream_object::MemoryObjectStorage::new(0));
        let wal_storage: Arc<dyn ObjectStorage> =
            Arc::new(s3stream_object::MemoryObjectStorage::new(1));
        let mut wal_config = ObjectWalConfig::defaults();
        wal_config.cluster_id = "facade-test".into();
        wal_config.node_id = 1;
        wal_config.epoch = 1;
        let engine = S3StreamBuilder::new(Config::default())
            .object_storage(object_storage)
            .write_ahead_log(Arc::new(ObjectWalService::new(wal_storage, wal_config)))
            .object_manager(manager.clone())
            .stream_manager(manager.clone())
            .build()
            .await
            .unwrap();

        let client = engine.stream_client();
        let options = CreateStreamOptions {
            epoch: 1,
            ..Default::default()
        };
        let stream = client.create_and_open_stream(options).await.unwrap();
        let stream_id = stream.stream_id();

        for i in 0..10u64 {
            let result = stream
                .append(
                    AppendContext::default(),
                    RecordBatch::new(1, 0, Bytes::from(vec![i as u8; 128])),
                )
                .await
                .unwrap();
            assert_eq!(result.base_offset, i);
        }
        assert_eq!(stream.confirm_offset(), 10);

        let fetched = stream
            .fetch(FetchContext::default(), 2, 7, usize::MAX)
            .await
            .unwrap();
        assert_eq!(fetched.records.first().unwrap().base_offset, 2);
        assert_eq!(fetched.records.last().unwrap().last_offset, 7);
        assert_eq!(fetched.records[0].payload, Bytes::from(vec![2u8; 128]));

        assert!(Arc::ptr_eq(&client.get_stream(stream_id).unwrap(), &stream));

        // A second stream so close-time upload commits a real stream SET object
        // case. And stream objects don't produce range-index entries).
        let second = client
            .create_and_open_stream(CreateStreamOptions {
                epoch: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        second
            .append(
                AppendContext::default(),
                RecordBatch::new(1, 0, Bytes::from(vec![9u8; 64])),
            )
            .await
            .unwrap();

        // Close uploads buffered data and releases the stream at the metadata plane.
        stream.close().await.unwrap();

        // Wiring check: the commit hook (`HookedObjectManager` →
        // `LocalStreamRangeIndexCache::update_index_from_request`) fed the local range
        // index, so the committed stream-set object is resolvable without the
        // metadata plane. (The hook is fire-and-forget, poll briefly.)
        let index = engine.range_index_cache();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while index.search_object_id(stream_id, 0).is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "range index never saw the commit"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        second.close().await.unwrap();

        // Reopen at a higher epoch. Committed data must still be readable.
        let reopened = client
            .open_stream(
                stream_id,
                OpenStreamOptions {
                    epoch: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(reopened.next_offset(), 10);
        let fetched = reopened
            .fetch(FetchContext::default(), 0, 10, usize::MAX)
            .await
            .unwrap();
        let total: u64 = fetched
            .records
            .iter()
            .map(|r| r.last_offset - r.base_offset)
            .sum();
        assert_eq!(total, 10);

        engine.shutdown().await;
    }

    /// Drive the engine purely through the `Client` trait: start, KV round trip
    /// (default in-memory KV), and a failover of a dead node whose WAL is empty
    /// `DefaultS3Client#failover(FailoverRequest)`).
    #[tokio::test]
    async fn client_trait_kv_and_failover() {
        let manager = MemoryMetadataManager::new();
        let object_storage: Arc<dyn ObjectStorage> =
            Arc::new(s3stream_object::MemoryObjectStorage::new(0));
        // One shared mem store for this node's WAL and the failover handle, since
        // `mem://` is not a process singleton.
        let wal_storage: Arc<dyn ObjectStorage> =
            Arc::new(s3stream_object::MemoryObjectStorage::new(1));
        let mut wal_config = ObjectWalConfig::defaults();
        wal_config.cluster_id = "facade-test".into();
        wal_config.node_id = 1;
        wal_config.epoch = 1;
        let engine = S3StreamBuilder::new(Config::default())
            .object_storage(object_storage)
            .write_ahead_log(Arc::new(ObjectWalService::new(
                Arc::clone(&wal_storage),
                wal_config,
            )))
            .object_manager(manager.clone())
            .stream_manager(manager.clone())
            .failover_factory(Arc::new(
                s3stream_core::DefaultFailoverFactory::with_storage(
                    "facade-test",
                    Arc::clone(&wal_storage),
                    manager.clone(),
                    manager.clone(),
                ),
            ))
            .build()
            .await
            .unwrap();
        let client: &dyn Client = &engine;
        client.start().await.unwrap();

        // KV round trip through the default MemoryKvClient.
        let kv = client.kv_client();
        kv.put_kv(KeyValue {
            key: "topic".into(),
            value: Bytes::from_static(b"42"),
        })
        .await
        .unwrap();
        assert_eq!(
            kv.get_kv("topic").await.unwrap(),
            Some(Bytes::from_static(b"42"))
        );

        // Dead node 9 never wrote a WAL: acquire fences it, start() reports
        // NotInitialized, failover completes successfully with nothing to recover.
        let resp = client
            .failover(FailoverRequest {
                node_id: 9,
                node_epoch: 3,
                wal_config: "0@mem://failover".into(),
            })
            .await
            .unwrap();
        assert_eq!(resp.node_id, 9);
        assert_eq!(resp.node_epoch, 3);

        Client::shutdown(&engine).await;
    }
}
