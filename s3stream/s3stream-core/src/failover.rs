//! Failover: recover a dead node's WAL and hand its streams back to the cluster.
//!
//! 1. `wal = factory.get_wal(request)` then `wal.start()`.
//! 3. `wal.metadata()`: `nodeId` must match. Reject if `request.epoch < wal.epoch`.
//! 4. Recover = `S3Storage::recover` (upload uncommitted tail, `wal.reset()`, close
//!    opening streams).
//! 5. `wal.shutdown_gracefully()` in `finally`.
//!
//! (`failover=true`) then builds `OpenMode.FAILOVER`. This port has no Kafka wrapper,
//! so the default [`DefaultFailoverFactory::get_wal`] does those two steps. Failover

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use s3stream_object::ObjectStorage;
use s3stream_wal::{
    AcquirePermissionOptions, BuildOptions, DefaultWalHandle, ObjectWalFactory, OpenMode, WalError,
    WalFactory, WalHandle, WriteAheadLog,
};
use tokio::sync::Mutex;

use crate::api::StreamError;
use crate::manager::{ObjectManager, StreamManager};
use crate::stream_client::S3StreamClient;

#[derive(Debug, Clone)]
pub struct FailoverRequest {
    pub node_id: u32,
    pub node_epoch: u64,
    /// WAL config URI of the dead node.
    pub wal_config: String,
}

#[derive(Debug, Clone)]
pub struct FailoverResponse {
    pub node_id: u32,
    pub node_epoch: u64,
}

#[async_trait]
pub trait FailoverFactory: Send + Sync {
    fn get_stream_manager(&self, node_id: u32, epoch: u64) -> Arc<dyn StreamManager>;

    fn get_object_manager(&self, node_id: u32, epoch: u64) -> Arc<dyn ObjectManager>;

    async fn get_wal(
        &self,
        request: &FailoverRequest,
    ) -> Result<Arc<dyn WriteAheadLog>, StreamError>;
}

pub type WalRecover = Arc<
    dyn Fn(
            Arc<dyn WriteAheadLog>,
            Arc<dyn StreamManager>,
            Arc<dyn ObjectManager>,
        ) -> BoxFuture<'static, Result<(), StreamError>>
        + Send
        + Sync,
>;

/// Default factory: acquire reservation with `failover=true`, then build the object WAL
/// in [`OpenMode::Recovery`].
///
/// `ConfirmWal` (acquire then `WalFactory.build(..., FAILOVER)`).
pub struct DefaultFailoverFactory {
    cluster_id: String,
    handle: DefaultWalHandle,
    stream_manager: Arc<dyn StreamManager>,
    object_manager: Arc<dyn ObjectManager>,
    wal_storage: Option<Arc<dyn ObjectStorage>>,
}

impl DefaultFailoverFactory {
    pub fn new(
        cluster_id: impl Into<String>,
        stream_manager: Arc<dyn StreamManager>,
        object_manager: Arc<dyn ObjectManager>,
    ) -> Self {
        let cluster_id = cluster_id.into();
        Self {
            handle: DefaultWalHandle::new(cluster_id.clone()),
            cluster_id,
            stream_manager,
            object_manager,
            wal_storage: None,
        }
    }

    /// Shared object storage for handle + WAL (`mem://` is not a singleton).
    pub fn with_storage(
        cluster_id: impl Into<String>,
        storage: Arc<dyn ObjectStorage>,
        stream_manager: Arc<dyn StreamManager>,
        object_manager: Arc<dyn ObjectManager>,
    ) -> Self {
        let cluster_id = cluster_id.into();
        Self {
            handle: DefaultWalHandle::with_storage(cluster_id.clone(), Arc::clone(&storage)),
            cluster_id,
            stream_manager,
            object_manager,
            wal_storage: Some(storage),
        }
    }
}

#[async_trait]
impl FailoverFactory for DefaultFailoverFactory {
    fn get_stream_manager(&self, _node_id: u32, _epoch: u64) -> Arc<dyn StreamManager> {
        Arc::clone(&self.stream_manager)
    }

    fn get_object_manager(&self, _node_id: u32, _epoch: u64) -> Arc<dyn ObjectManager> {
        Arc::clone(&self.object_manager)
    }

    async fn get_wal(
        &self,
        request: &FailoverRequest,
    ) -> Result<Arc<dyn WriteAheadLog>, StreamError> {
        self.handle
            .acquire_permission(
                request.node_id,
                request.node_epoch,
                &request.wal_config,
                AcquirePermissionOptions {
                    failover_mode: true,
                    timeout_ms: 20_000,
                },
            )
            .await?;
        let wal_factory = match &self.wal_storage {
            Some(storage) => ObjectWalFactory::with_storage(
                self.cluster_id.clone(),
                request.node_id,
                Arc::clone(storage),
            ),
            None => ObjectWalFactory::new(self.cluster_id.clone(), request.node_id),
        };
        Ok(wal_factory.build(
            &request.wal_config,
            BuildOptions {
                node_epoch: request.node_epoch,
                open_mode: OpenMode::Recovery,
            },
        )?)
    }
}

pub struct Failover {
    factory: Arc<dyn FailoverFactory>,
    recover: WalRecover,
    lock: Mutex<()>,
}

impl Failover {
    pub fn new(factory: Arc<dyn FailoverFactory>, recover: WalRecover) -> Self {
        Self {
            factory,
            recover,
            lock: Mutex::new(()),
        }
    }

    /// Execute one failover.
    pub async fn failover(
        &self,
        request: FailoverRequest,
    ) -> Result<FailoverResponse, StreamError> {
        let _guard = self.lock.lock().await;
        tracing::info!(
            node_id = request.node_id,
            epoch = request.node_epoch,
            "failover start"
        );
        let resp = FailoverResponse {
            node_id: request.node_id,
            node_epoch: request.node_epoch,
        };

        let wal = self.factory.get_wal(&request).await?;
        match wal.start().await {
            Ok(()) => {}
            Err(WalError::NotInitialized) => {
                tracing::info!(node_id = request.node_id, "fail over empty wal");
                return Ok(resp);
            }
            Err(e) => return Err(e.into()),
        }

        let result = async {
            let metadata = wal.metadata();
            if request.node_id != metadata.node_id {
                return Err(StreamError::Unexpected(format!(
                    "nodeId mismatch, request node_id={} wal node_id={}",
                    request.node_id, metadata.node_id
                )));
            }
            if request.node_epoch < metadata.epoch {
                return Err(StreamError::Unexpected(format!(
                    "epoch mismatch, request epoch={} wal epoch={}",
                    request.node_epoch, metadata.epoch
                )));
            }
            let stream_manager = self
                .factory
                .get_stream_manager(request.node_id, request.node_epoch);
            let object_manager = self
                .factory
                .get_object_manager(request.node_id, request.node_epoch);
            tracing::info!(node_id = request.node_id, "failover recover");
            (self.recover)(Arc::clone(&wal), stream_manager, object_manager).await
        }
        .await;

        wal.shutdown_gracefully().await;
        result?;
        tracing::info!(node_id = request.node_id, "failover done");
        Ok(resp)
    }
}

/// What the node does when storage hits an unrecoverable failure (WAL fenced,
/// unexpected append/upload error).
///
/// The hook is async so [`ForceCloseStorageFailureHandler`] can await
/// `force_close` before [`HaltStorageFailureHandler`] runs. Handlers run in
/// add-order, and ForceClose completes before Halt.
#[async_trait]
pub trait StorageFailureHandler: Send + Sync {
    async fn handle(&self, error: &StreamError);
}

/// Default handler: log only. Tests and the facade use this unless a host installs a
/// [`StorageFailureHandlerChain`].
pub struct LogStorageFailureHandler;

#[async_trait]
impl StorageFailureHandler for LogStorageFailureHandler {
    async fn handle(&self, error: &StreamError) {
        tracing::error!(%error, "storage failure");
    }
}

pub struct StorageFailureHandlerChain {
    handlers: std::sync::Mutex<Vec<Arc<dyn StorageFailureHandler>>>,
}

impl StorageFailureHandlerChain {
    pub fn new() -> Self {
        Self {
            handlers: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn add_handler(&self, handler: Arc<dyn StorageFailureHandler>) {
        self.handlers
            .lock()
            .expect("handlers poisoned")
            .push(handler);
    }
}

impl Default for StorageFailureHandlerChain {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageFailureHandler for StorageFailureHandlerChain {
    async fn handle(&self, error: &StreamError) {
        let handlers = self.handlers.lock().expect("handlers poisoned").clone();
        let message = error.to_string();
        tokio::spawn(async move {
            let error = StreamError::Unexpected(message);
            for handler in handlers {
                handler.handle(&error).await;
            }
        });
    }
}

pub struct ForceCloseStorageFailureHandler {
    stream_client: Arc<S3StreamClient>,
}

impl ForceCloseStorageFailureHandler {
    pub fn new(stream_client: Arc<S3StreamClient>) -> Self {
        Self { stream_client }
    }
}

#[async_trait]
impl StorageFailureHandler for ForceCloseStorageFailureHandler {
    async fn handle(&self, error: &StreamError) {
        tracing::error!(%error, "Encounter storage fail, try force to close the streams");
        self.stream_client.force_close().await;
        tracing::info!("Complete force to close the streams");
    }
}

pub struct HaltStorageFailureHandler;

#[async_trait]
impl StorageFailureHandler for HaltStorageFailureHandler {
    async fn handle(&self, _error: &StreamError) {
        std::process::abort();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt;
    use s3stream_object::{MemoryObjectStorage, ObjectStorage};
    use s3stream_wal::memory::MemoryWriteAheadLog;
    use s3stream_wal::object::{ObjectReservationService, ObjectWalConfig, ObjectWalService};
    use s3stream_wal::{ReservationService, WalMetadata};

    use super::*;
    use crate::cache::block_cache::DirectBlockCache;
    use crate::context::{AppendContext, FetchContext};
    use crate::memory::MemoryMetadataManager;
    use crate::storage::Storage;
    use crate::storage::s3_storage::{S3Storage, S3StorageConfig};
    use crate::stream_client::{S3StreamClient, StreamClientConfig};
    use crate::{ObjectManager, RecordBatch, StreamClient, StreamManager};

    const WAL_URI: &str = "0@mem://wal";
    const CLUSTER: &str = "failover-test";
    const NODE_A: u32 = 7;

    fn recover_cb(config: S3StorageConfig, object_storage: Arc<dyn ObjectStorage>) -> WalRecover {
        Arc::new(move |wal, stream_manager, object_manager| {
            let config = config.clone();
            let object_storage = Arc::clone(&object_storage);
            Box::pin(async move {
                S3Storage::recover(
                    &config,
                    &*wal,
                    &object_storage,
                    &*stream_manager,
                    &object_manager,
                )
                .await
            })
        })
    }

    fn record_batch(payload: &[u8]) -> RecordBatch {
        RecordBatch::new(1, 0, Bytes::copy_from_slice(payload))
    }

    struct StubFactory {
        wal: Arc<dyn WriteAheadLog>,
        manager: Arc<MemoryMetadataManager>,
    }

    #[async_trait]
    impl FailoverFactory for StubFactory {
        fn get_stream_manager(&self, _node_id: u32, _epoch: u64) -> Arc<dyn StreamManager> {
            Arc::clone(&self.manager) as Arc<dyn StreamManager>
        }

        fn get_object_manager(&self, _node_id: u32, _epoch: u64) -> Arc<dyn ObjectManager> {
            Arc::clone(&self.manager) as Arc<dyn ObjectManager>
        }

        async fn get_wal(
            &self,
            _request: &FailoverRequest,
        ) -> Result<Arc<dyn WriteAheadLog>, StreamError> {
            Ok(Arc::clone(&self.wal))
        }
    }

    struct NotInitializedWal;

    #[async_trait]
    impl WriteAheadLog for NotInitializedWal {
        async fn start(&self) -> Result<(), WalError> {
            Err(WalError::NotInitialized)
        }
        async fn shutdown_gracefully(&self) {}
        fn metadata(&self) -> WalMetadata {
            WalMetadata {
                node_id: 0,
                epoch: 0,
            }
        }
        fn uri(&self) -> &str {
            ""
        }
        fn submit(
            &self,
            _record: s3stream_codec::StreamRecordBatch,
        ) -> Result<s3stream_wal::PendingAppend, WalError> {
            Err(WalError::NotInitialized)
        }
        fn set_append_listener(&self, _listener: s3stream_wal::AppendListener) {}
        async fn get(
            &self,
            _offset: s3stream_wal::RecordOffset,
        ) -> Result<s3stream_codec::StreamRecordBatch, WalError> {
            Err(WalError::NotInitialized)
        }
        async fn get_range(
            &self,
            _start: s3stream_wal::RecordOffset,
            _end: s3stream_wal::RecordOffset,
        ) -> Result<Vec<s3stream_codec::StreamRecordBatch>, WalError> {
            Err(WalError::NotInitialized)
        }
        fn confirm_offset(&self) -> s3stream_wal::RecordOffset {
            s3stream_wal::RecordOffset {
                epoch: 0,
                offset: 0,
                size: 0,
            }
        }
        fn recover(&self) -> s3stream_wal::RecoverStream {
            Box::pin(futures::stream::empty())
        }
        async fn reset(&self) -> Result<(), WalError> {
            Ok(())
        }
        async fn trim(&self, _offset: s3stream_wal::RecordOffset) -> Result<(), WalError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn empty_wal_not_initialized_succeeds() {
        let manager = MemoryMetadataManager::new();
        let recovered = Arc::new(AtomicUsize::new(0));
        let recovered_cb = Arc::clone(&recovered);
        let failover = Failover::new(
            Arc::new(StubFactory {
                wal: Arc::new(NotInitializedWal),
                manager,
            }),
            Arc::new(move |_, _, _| {
                recovered_cb.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }),
        );
        let resp = failover
            .failover(FailoverRequest {
                node_id: 1,
                node_epoch: 1,
                wal_config: WAL_URI.into(),
            })
            .await
            .unwrap();
        assert_eq!(resp.node_id, 1);
        assert_eq!(recovered.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn node_id_mismatch_errors() {
        let manager = MemoryMetadataManager::new();
        let wal = Arc::new(MemoryWriteAheadLog::new(99, 1));
        let failover = Failover::new(
            Arc::new(StubFactory { wal, manager }),
            Arc::new(|_, _, _| Box::pin(async { Ok(()) })),
        );
        let err = failover
            .failover(FailoverRequest {
                node_id: 1,
                node_epoch: 1,
                wal_config: WAL_URI.into(),
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("nodeId mismatch"), "{err}");
    }

    #[tokio::test]
    async fn epoch_mismatch_errors() {
        let manager = MemoryMetadataManager::new();
        let wal = Arc::new(MemoryWriteAheadLog::new(1, 5));
        let failover = Failover::new(
            Arc::new(StubFactory { wal, manager }),
            Arc::new(|_, _, _| Box::pin(async { Ok(()) })),
        );
        let err = failover
            .failover(FailoverRequest {
                node_id: 1,
                node_epoch: 1,
                wal_config: WAL_URI.into(),
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("epoch mismatch"), "{err}");
    }

    struct RecordingHandler {
        name: &'static str,
        log: Arc<StdMutex<Vec<&'static str>>>,
        delay: Duration,
        started: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StorageFailureHandler for RecordingHandler {
        async fn handle(&self, _error: &StreamError) {
            self.started.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.log.lock().unwrap().push(self.name);
        }
    }

    #[tokio::test]
    async fn chain_runs_handlers_in_order_off_caller() {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let started = Arc::new(AtomicUsize::new(0));
        let chain = StorageFailureHandlerChain::new();
        chain.add_handler(Arc::new(RecordingHandler {
            name: "a",
            log: Arc::clone(&log),
            delay: Duration::from_millis(40),
            started: Arc::clone(&started),
        }));
        chain.add_handler(Arc::new(RecordingHandler {
            name: "b",
            log: Arc::clone(&log),
            delay: Duration::from_millis(0),
            started: Arc::clone(&started),
        }));

        let error = StreamError::Unexpected("boom".into());
        chain.handle(&error).await;
        assert!(
            log.lock().unwrap().is_empty(),
            "handle must return before handlers finish"
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if log.lock().unwrap().as_slice() == ["a", "b"] {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("handlers did not complete in order");
        assert_eq!(started.load(Ordering::SeqCst), 2);
    }

    /// Node A appends + is fenced. Node B fails over: all of A's acked records become
    /// readable, A's zombie writes post-fence are excluded.
    #[tokio::test]
    async fn failover_recovers_acked_records_only() {
        let object_storage: Arc<dyn ObjectStorage> = Arc::new(MemoryObjectStorage::new(0));
        let manager = MemoryMetadataManager::new();
        let reservation = Arc::new(ObjectReservationService::new(
            Arc::clone(&object_storage),
            CLUSTER.into(),
            0,
        ));
        reservation.acquire(NODE_A, 1, false).await.unwrap();

        let mut wal_config = ObjectWalConfig::from_uri(WAL_URI).unwrap();
        wal_config.cluster_id = CLUSTER.into();
        wal_config.node_id = NODE_A;
        wal_config.epoch = 1;
        wal_config.batch_interval = Duration::from_millis(5);
        wal_config.reservation_service = Arc::clone(&reservation) as Arc<dyn ReservationService>;
        let wal_a = Arc::new(ObjectWalService::new(
            Arc::clone(&object_storage),
            wal_config,
        ));

        let block_cache = Arc::new(DirectBlockCache::new(
            manager.clone() as Arc<dyn ObjectManager>,
            Arc::clone(&object_storage),
        ));
        let storage_a = Arc::new(S3Storage::new(
            S3StorageConfig::test_defaults(),
            wal_a.clone() as Arc<dyn WriteAheadLog>,
            block_cache,
            Arc::clone(&object_storage),
            manager.clone() as Arc<dyn ObjectManager>,
            manager.clone() as Arc<dyn StreamManager>,
            Arc::new(LogStorageFailureHandler),
            None,
        ));
        storage_a.startup().await.unwrap();

        let stream_id = manager.create_stream(HashMap::new()).await.unwrap();
        manager
            .open_stream(stream_id, 1, HashMap::new())
            .await
            .unwrap();
        let n = 8u64;
        for i in 0..n {
            storage_a
                .append(
                    AppendContext::default(),
                    s3stream_codec::StreamRecordBatch::new(
                        stream_id,
                        1,
                        i,
                        1,
                        Bytes::from(vec![i as u8; 16]),
                    ),
                )
                .await
                .unwrap();
        }

        let factory = DefaultFailoverFactory::with_storage(
            CLUSTER,
            Arc::clone(&object_storage),
            manager.clone() as Arc<dyn StreamManager>,
            manager.clone() as Arc<dyn ObjectManager>,
        );
        let failover = Failover::new(
            Arc::new(factory),
            recover_cb(
                S3StorageConfig::test_defaults(),
                Arc::clone(&object_storage),
            ),
        );
        failover
            .failover(FailoverRequest {
                node_id: NODE_A,
                node_epoch: 1,
                wal_config: WAL_URI.into(),
            })
            .await
            .unwrap();

        assert!(
            !reservation.verify(NODE_A, 1, false).await.unwrap(),
            "zombie verify(failover=false) must fail after failover acquire"
        );

        let mut peek_config = ObjectWalConfig::from_uri(WAL_URI).unwrap();
        peek_config.cluster_id = CLUSTER.into();
        peek_config.node_id = NODE_A;
        peek_config.epoch = 1;
        peek_config.open_mode = OpenMode::Recovery;
        peek_config.reservation_service = Arc::clone(&reservation) as Arc<dyn ReservationService>;
        let peek = ObjectWalService::new(Arc::clone(&object_storage), peek_config);
        peek.start().await.unwrap();
        let recovered: Vec<_> = peek.recover().collect().await;
        let ok_count = recovered.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            ok_count,
            recovered.len(),
            "WAL recover errors after failover"
        );
        assert_eq!(
            recovered.len(),
            0,
            "WAL must be reset after failover recover"
        );
        peek.shutdown_gracefully().await;
        assert!(manager.get_opening_streams().await.unwrap().is_empty());

        let err = wal_a
            .append(s3stream_codec::StreamRecordBatch::new(
                stream_id,
                1,
                n,
                1,
                Bytes::from_static(b"zombie"),
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(err, WalError::Fenced { .. }) || matches!(err, WalError::NotInitialized),
            "zombie append must fail, got {err}"
        );

        manager
            .open_stream(stream_id, 2, HashMap::new())
            .await
            .unwrap();
        let wal_b_config = {
            let mut c = ObjectWalConfig::from_uri(WAL_URI).unwrap();
            c.cluster_id = CLUSTER.into();
            c.node_id = NODE_A;
            c.epoch = 2;
            c.reservation_service = Arc::new(s3stream_wal::NoopReservationService);
            c
        };
        let wal_b = Arc::new(ObjectWalService::new(
            Arc::clone(&object_storage),
            wal_b_config,
        ));
        let block_cache = Arc::new(DirectBlockCache::new(
            manager.clone() as Arc<dyn ObjectManager>,
            Arc::clone(&object_storage),
        ));
        let storage_b = S3Storage::new(
            S3StorageConfig::test_defaults(),
            wal_b as Arc<dyn WriteAheadLog>,
            block_cache,
            Arc::clone(&object_storage),
            manager.clone() as Arc<dyn ObjectManager>,
            manager.clone() as Arc<dyn StreamManager>,
            Arc::new(LogStorageFailureHandler),
            None,
        );
        storage_b.startup().await.unwrap();
        let read = storage_b
            .read(FetchContext::default(), stream_id, 0, n, usize::MAX)
            .await
            .unwrap();
        assert_eq!(read.records.len(), n as usize);
        for (i, record) in read.records.iter().enumerate() {
            assert_eq!(record.base_offset(), i as u64);
            assert_eq!(record.payload().as_ref(), vec![i as u8; 16]);
        }
        storage_a.shutdown().await;
        storage_b.shutdown().await;
    }

    #[tokio::test]
    async fn force_close_still_uploads() {
        let object_storage: Arc<dyn ObjectStorage> = Arc::new(MemoryObjectStorage::new(0));
        let manager = MemoryMetadataManager::new();
        let mut wal_config = ObjectWalConfig::defaults();
        wal_config.cluster_id = CLUSTER.into();
        wal_config.node_id = NODE_A;
        wal_config.epoch = 1;
        wal_config.batch_interval = Duration::from_millis(5);
        let wal = Arc::new(ObjectWalService::new(
            Arc::clone(&object_storage),
            wal_config,
        ));
        let block_cache = Arc::new(DirectBlockCache::new(
            manager.clone() as Arc<dyn ObjectManager>,
            Arc::clone(&object_storage),
        ));
        let storage = Arc::new(S3Storage::new(
            S3StorageConfig::test_defaults(),
            wal as Arc<dyn WriteAheadLog>,
            block_cache,
            Arc::clone(&object_storage),
            manager.clone() as Arc<dyn ObjectManager>,
            manager.clone() as Arc<dyn StreamManager>,
            Arc::new(LogStorageFailureHandler),
            None,
        ));
        storage.startup().await.unwrap();

        let client = S3StreamClient::new(
            Arc::clone(&storage) as Arc<dyn Storage>,
            manager.clone() as Arc<dyn StreamManager>,
            manager.clone() as Arc<dyn ObjectManager>,
            Arc::clone(&object_storage),
            StreamClientConfig {
                compaction_enabled: false,
                ..Default::default()
            },
        );
        let stream = client
            .create_and_open_stream(crate::CreateStreamOptions {
                epoch: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let stream_id = stream.stream_id();
        for i in 0..4u64 {
            stream
                .append(AppendContext::default(), record_batch(&[i as u8; 8]))
                .await
                .unwrap();
        }

        client.force_close().await;
        assert!(client.get_stream(stream_id).is_none());
        let err = client
            .create_and_open_stream(crate::CreateStreamOptions {
                epoch: 2,
                ..Default::default()
            })
            .await
            .err()
            .expect("opens after force_close must fail");
        assert!(err.to_string().contains("already closed"), "{err}");

        manager
            .open_stream(stream_id, 2, HashMap::new())
            .await
            .unwrap();
        let read = storage
            .read(FetchContext::default(), stream_id, 0, 4, usize::MAX)
            .await
            .unwrap();
        assert_eq!(read.records.len(), 4);
        storage.shutdown().await;
    }
}
