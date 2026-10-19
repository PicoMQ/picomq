//! A node's write-ahead log: its own, opened at its registered epoch and
//! fenced against takeovers, and a fenced node's, opened by the leader to
//! recover it.

use std::sync::Arc;

use async_trait::async_trait;
use s3stream::{
    Error as StreamError, FailoverRequest, ObjectReservationService, ObjectStorageTrait,
    ObjectStoreAdapter, ObjectWalConfig, ObjectWalService, OpenMode, ReservationService as _,
    WriteAheadLogTrait,
};

#[async_trait]
pub trait Wal: Send + Sync {
    /// Where the WAL lives, recorded on the node's registration so another
    /// node can take it over.
    fn location(&self) -> String;

    /// Opens this node's WAL. Writes fail once another node takes it over.
    async fn open(
        &self,
        node_id: i32,
        epoch: i64,
    ) -> Result<Arc<dyn WriteAheadLogTrait>, StreamError>;

    /// Opens a fenced node's WAL to recover it, fencing its writer.
    async fn take_over(
        &self,
        request: &FailoverRequest,
    ) -> Result<Arc<dyn WriteAheadLogTrait>, StreamError>;
}

/// A WAL in object storage, fenced by a reservation object. A node writes
/// only while the reservation still names its epoch, and a takeover rewrites
/// it.
pub struct ObjectWal {
    cluster_id: String,
    location: String,
    storage: Arc<dyn ObjectStorageTrait>,
}

impl ObjectWal {
    pub fn new(cluster_id: &str, location: &str, storage: Arc<dyn ObjectStorageTrait>) -> Self {
        Self {
            cluster_id: cluster_id.to_owned(),
            location: location.to_owned(),
            storage,
        }
    }

    fn storage_for(&self, location: &str) -> Result<Arc<dyn ObjectStorageTrait>, StreamError> {
        if location == self.location {
            return Ok(self.storage.clone());
        }

        Ok(Arc::new(ObjectStoreAdapter::from_bucket_uri(location)?))
    }

    fn config(
        &self,
        location: &str,
        node_id: u32,
        epoch: u64,
        open_mode: OpenMode,
    ) -> Result<ObjectWalConfig, StreamError> {
        let mut config = ObjectWalConfig::from_uri_or_defaults(location)?;
        config.cluster_id = self.cluster_id.clone();
        config.node_id = node_id;
        config.epoch = epoch;
        config.open_mode = open_mode;
        Ok(config)
    }

    fn reservation(
        &self,
        storage: &Arc<dyn ObjectStorageTrait>,
        config: &ObjectWalConfig,
    ) -> ObjectReservationService {
        ObjectReservationService::new(storage.clone(), self.cluster_id.clone(), config.bucket_id)
    }
}

#[async_trait]
impl Wal for ObjectWal {
    fn location(&self) -> String {
        self.location.clone()
    }

    async fn open(
        &self,
        node_id: i32,
        epoch: i64,
    ) -> Result<Arc<dyn WriteAheadLogTrait>, StreamError> {
        let (node_id, epoch) = (node_id as u32, epoch as u64);
        let mut config = self.config(&self.location, node_id, epoch, OpenMode::ReadWrite)?;
        let reservation = self.reservation(&self.storage, &config);
        reservation.acquire(node_id, epoch, false).await?;

        config.reservation_service = Arc::new(reservation);
        Ok(Arc::new(ObjectWalService::new(
            self.storage.clone(),
            config,
        )))
    }

    async fn take_over(
        &self,
        request: &FailoverRequest,
    ) -> Result<Arc<dyn WriteAheadLogTrait>, StreamError> {
        let FailoverRequest {
            node_id,
            node_epoch,
            wal_config,
        } = request;
        let storage = self.storage_for(wal_config)?;
        let mut config = self.config(wal_config, *node_id, *node_epoch, OpenMode::Recovery)?;
        let reservation = self.reservation(&storage, &config);

        let visible = reservation.verify(*node_id, *node_epoch, false).await?
            || reservation.verify(*node_id, *node_epoch, true).await?;
        if !visible {
            return Err(StreamError::Unexpected(format!(
                "the WAL of node {node_id} at epoch {node_epoch} is not visible in {wal_config}"
            )));
        }
        reservation.acquire(*node_id, *node_epoch, true).await?;

        config.reservation_service = Arc::new(reservation);
        Ok(Arc::new(ObjectWalService::new(storage, config)))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use s3stream::{MemoryObjectStorage, StreamRecordBatch, WalError};

    use super::*;

    const LOCATION: &str = "0@mem://wal";

    fn wal() -> ObjectWal {
        ObjectWal::new("cluster", LOCATION, Arc::new(MemoryObjectStorage::new(0)))
    }

    fn record(offset: u64) -> StreamRecordBatch {
        StreamRecordBatch::new(9, 1, offset, 1, Bytes::from_static(b"payload"))
    }

    fn takeover(node_id: u32, node_epoch: u64) -> FailoverRequest {
        FailoverRequest {
            node_id,
            node_epoch,
            wal_config: LOCATION.to_owned(),
        }
    }

    #[tokio::test]
    async fn a_taken_over_wal_rejects_its_owner() {
        let wal = wal();
        let own = wal.open(2, 7).await.unwrap();
        own.start().await.unwrap();
        own.append(record(0)).await.unwrap();

        wal.take_over(&takeover(2, 7)).await.unwrap();

        let fenced = own.append(record(1)).await.unwrap_err();
        assert!(matches!(fenced, WalError::Fenced { .. }), "{fenced}");
    }

    #[tokio::test]
    async fn a_takeover_refuses_a_wal_it_cannot_see() {
        let error = wal().take_over(&takeover(2, 7)).await.err().unwrap();

        assert!(error.to_string().contains("is not visible"), "{error}");
    }

    #[tokio::test]
    async fn a_takeover_refuses_a_newer_incarnation() {
        let wal = wal();
        wal.open(2, 7).await.unwrap();
        wal.open(2, 8).await.unwrap();

        let error = wal.take_over(&takeover(2, 7)).await.err().unwrap();
        assert!(error.to_string().contains("is not visible"), "{error}");
    }
}
