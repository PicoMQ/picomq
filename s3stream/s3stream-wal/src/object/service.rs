//! The object WAL service: assembles config + writer + reader + recovery into
//! `WriteAheadLog`.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use s3stream_codec::{StreamRecordBatch, decode_record};
use s3stream_object::{ObjectStorage, ReadOptions, ThrottleStrategy};

use crate::{PendingAppend, RecordOffset, RecoverStream, WalError, WalMetadata, WriteAheadLog};

use super::config::ObjectWalConfig;
use super::header::WAL_HEADER_SIZE_V1;
use super::keys::{
    DATA_FILE_ALIGN_SIZE, TRIM_RECORD_SENTINEL, floor_align_offset, gen_object_path_v1_aligned,
    node_prefix, parse_wal_objects,
};
use super::recover::recover_stream;
use super::writer::ObjectWalWriter;

/// Read side of the object WAL.
///
/// The epoch index (`startOffset -> epoch`)
/// is rebuilt from a listing when a read's end epoch is newer than the cached index.
struct WalReader {
    storage: Arc<dyn ObjectStorage>,
    node_prefix: String,
    index: Mutex<ReaderIndex>,
}

struct ReaderIndex {
    map: BTreeMap<u64, u64>,
    largest_epoch: Option<u64>,
}

impl WalReader {
    fn new(storage: Arc<dyn ObjectStorage>, node_prefix: String) -> Self {
        Self {
            storage,
            node_prefix,
            index: Mutex::new(ReaderIndex {
                map: BTreeMap::new(),
                largest_epoch: None,
            }),
        }
    }

    fn read_options(&self) -> ReadOptions {
        ReadOptions {
            throttle: ThrottleStrategy::Bypass,
            bucket_id: Some(self.storage.bucket_id()),
        }
    }

    async fn get(&self, offset: RecordOffset) -> Result<StreamRecordBatch, WalError> {
        let object_start = floor_align_offset(offset.offset);
        let object_path = gen_object_path_v1_aligned(&self.node_prefix, offset.epoch, object_start);
        let relative_start = offset.offset - object_start + WAL_HEADER_SIZE_V1 as u64;
        let buf = self
            .storage
            .range_read(
                &self.read_options(),
                &object_path,
                relative_start,
                Some(relative_start + offset.size as u64),
            )
            .await?;
        let mut buf = buf;
        Ok(decode_record(&mut buf)?)
    }

    async fn get_range(
        &self,
        start: RecordOffset,
        end: RecordOffset,
    ) -> Result<Vec<StreamRecordBatch>, WalError> {
        if start.offset == end.offset {
            return Ok(Vec::new());
        }
        self.ensure_index(end.epoch).await?;

        // Snapshot the covering (epoch startOffset -> epoch) entries.
        let entries: Vec<(u64, u64)> = {
            let index = self.index.lock().await;
            let Some((&floor_key, _)) = index.map.range(..=start.offset).next_back() else {
                return Err(WalError::Recovery(format!(
                    "Cannot find epoch for [{start:?}, {end:?})"
                )));
            };
            index
                .map
                .range(floor_key..end.offset)
                .map(|(k, v)| (*k, *v))
                .collect()
        };
        if entries.is_empty() {
            return Err(WalError::Recovery(format!(
                "Cannot find epoch for [{start:?}, {end:?})"
            )));
        }

        let mut reads = Vec::new();
        let mut next_get_offset = start.offset;
        for (i, (_, epoch)) in entries.iter().enumerate() {
            let epoch_end_offset = if i == entries.len() - 1 {
                u64::MAX
            } else {
                entries[i + 1].0
            };
            while next_get_offset < epoch_end_offset && next_get_offset < end.offset {
                let object_start = floor_align_offset(next_get_offset);
                let object_path =
                    gen_object_path_v1_aligned(&self.node_prefix, *epoch, object_start);
                let relative_start = next_get_offset - object_start + WAL_HEADER_SIZE_V1 as u64;
                let storage = Arc::clone(&self.storage);
                let options = self.read_options();
                let read_from = next_get_offset;
                reads.push((
                    read_from,
                    tokio::spawn(async move {
                        storage
                            .range_read(&options, &object_path, relative_start, None)
                            .await
                    }),
                ));
                next_get_offset = object_start + DATA_FILE_ALIGN_SIZE;
            }
        }

        let mut batches = Vec::new();
        for (read_from, handle) in reads {
            let buf = handle
                .await
                .map_err(|e| WalError::Recovery(format!("read task failed: {e}")))??;
            let mut buf = buf;
            let mut next_record_offset = read_from;
            while !buf.is_empty() && next_record_offset < end.offset {
                let before = buf.len();
                let batch = decode_record(&mut buf)?;
                let consumed = (before - buf.len()) as u64;
                let is_trigger_trim_record = batch.count() == 0
                    && batch.stream_id() == TRIM_RECORD_SENTINEL
                    && batch.epoch() == TRIM_RECORD_SENTINEL;
                if !is_trigger_trim_record {
                    batches.push(batch);
                }
                next_record_offset += consumed;
            }
        }
        Ok(batches)
    }

    /// (rebuild when the cached index does not
    /// yet cover `epoch`).
    async fn ensure_index(&self, epoch: u64) -> Result<(), WalError> {
        {
            let index = self.index.lock().await;
            if index.largest_epoch.is_some_and(|largest| largest >= epoch) {
                return Ok(());
            }
        }
        let listed = self.storage.list(&self.node_prefix).await?;
        let objects = parse_wal_objects(listed);
        let mut map = BTreeMap::new();
        let mut largest: Option<u64> = None;
        let mut last_epoch: Option<u64> = None;
        for object in objects {
            if last_epoch == Some(object.epoch) {
                continue;
            }
            map.insert(object.start_offset, object.epoch);
            last_epoch = Some(object.epoch);
            largest = Some(object.epoch);
        }
        let mut index = self.index.lock().await;
        index.map = map;
        index.largest_epoch = largest;
        Ok(())
    }
}

/// Object WAL implementation of `WriteAheadLog`.
pub struct ObjectWalService {
    config: ObjectWalConfig,
    storage: Arc<dyn ObjectStorage>,
    writer: ObjectWalWriter,
    reader: WalReader,
}

impl ObjectWalService {
    pub fn new(storage: Arc<dyn ObjectStorage>, config: ObjectWalConfig) -> Self {
        let prefix = node_prefix(
            &config.cluster_id,
            config.node_id,
            Some(config.wal_type.as_str()),
        );
        Self {
            writer: ObjectWalWriter::new(Arc::clone(&storage), config.clone()),
            reader: WalReader::new(Arc::clone(&storage), prefix),
            storage,
            config,
        }
    }
}

#[async_trait]
impl WriteAheadLog for ObjectWalService {
    async fn start(&self) -> Result<(), WalError> {
        tracing::info!("start S3 WAL");
        self.writer.start().await
    }

    async fn shutdown_gracefully(&self) {
        tracing::info!("shutdown S3 WAL");
        self.writer.close().await;
    }

    fn metadata(&self) -> WalMetadata {
        WalMetadata {
            node_id: self.config.node_id,
            epoch: self.config.epoch,
        }
    }

    fn uri(&self) -> &str {
        &self.config.uri
    }

    fn submit(&self, record: StreamRecordBatch) -> Result<PendingAppend, WalError> {
        self.writer.submit(record)
    }

    fn set_append_listener(&self, listener: crate::AppendListener) {
        self.writer.set_append_listener(listener);
    }

    async fn get(&self, offset: RecordOffset) -> Result<StreamRecordBatch, WalError> {
        self.reader.get(offset).await
    }

    async fn get_range(
        &self,
        start: RecordOffset,
        end: RecordOffset,
    ) -> Result<Vec<StreamRecordBatch>, WalError> {
        self.reader.get_range(start, end).await
    }

    fn confirm_offset(&self) -> RecordOffset {
        self.writer.confirm_offset()
    }

    /// Iterate this writer's object list from the persisted trim offset.
    fn recover(&self) -> RecoverStream {
        match self.writer.object_list() {
            Ok(objects) => recover_stream(
                Arc::clone(&self.storage),
                objects,
                self.config.readahead_data_size,
            ),
            Err(e) => Box::pin(futures::stream::once(async move { Err(e) })),
        }
    }

    async fn reset(&self) -> Result<(), WalError> {
        tracing::info!("reset S3 WAL");
        self.writer.reset().await
    }

    async fn trim(&self, offset: RecordOffset) -> Result<(), WalError> {
        tracing::info!(?offset, "trim S3 WAL");
        self.writer.trim(offset).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;
    use s3stream_object::MemoryObjectStorage;
    use std::time::Duration;

    fn config(epoch: u64) -> ObjectWalConfig {
        let mut config = ObjectWalConfig::defaults();
        config.cluster_id = "svc".to_string();
        config.node_id = 7;
        config.epoch = epoch;
        config.batch_interval = Duration::from_millis(5);
        config
    }

    fn record(stream_id: u64, base_offset: u64, payload: &[u8]) -> StreamRecordBatch {
        StreamRecordBatch::new(
            stream_id,
            1,
            base_offset,
            1,
            Bytes::copy_from_slice(payload),
        )
    }

    /// End-to-end: start -> append N -> shutdown -> fresh instance recovers exactly
    /// the N records -> reset -> recover yields nothing.
    #[tokio::test]
    async fn append_recover_reset_cycle() {
        let storage: Arc<MemoryObjectStorage> = Arc::new(MemoryObjectStorage::new(0));

        let wal = ObjectWalService::new(storage.clone(), config(1));
        wal.start().await.unwrap();
        for i in 0..5u64 {
            wal.append(record(9, i, format!("payload-{i}").as_bytes()))
                .await
                .unwrap();
        }
        wal.shutdown_gracefully().await;

        // Fresh instance, higher epoch (as a restart would grant).
        let wal2 = ObjectWalService::new(storage.clone(), config(2));
        wal2.start().await.unwrap();
        let recovered: Vec<_> = wal2.recover().collect().await;
        let records: Vec<StreamRecordBatch> =
            recovered.into_iter().map(|r| r.unwrap().record).collect();
        assert_eq!(records.len(), 5);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.stream_id(), 9);
            assert_eq!(r.base_offset(), i as u64);
            assert_eq!(r.payload().as_ref(), format!("payload-{i}").as_bytes());
        }

        wal2.reset().await.unwrap();
        wal2.shutdown_gracefully().await;

        let wal3 = ObjectWalService::new(storage.clone(), config(3));
        wal3.start().await.unwrap();
        let recovered: Vec<_> = wal3.recover().collect().await;
        assert!(recovered.iter().all(|r| r.is_ok()));
        assert_eq!(recovered.len(), 0);
    }

    /// Read-back paths: single get by RecordOffset and ranged get.
    #[tokio::test]
    async fn get_and_get_range() {
        let storage: Arc<MemoryObjectStorage> = Arc::new(MemoryObjectStorage::new(0));
        let wal = ObjectWalService::new(storage.clone(), config(1));
        wal.start().await.unwrap();

        let r1 = wal.append(record(1, 0, b"alpha")).await.unwrap();
        let r2 = wal.append(record(1, 1, b"beta")).await.unwrap();
        let r3 = wal.append(record(1, 2, b"gamma")).await.unwrap();

        let got = wal.get(r2.record_offset).await.unwrap();
        assert_eq!(got.payload().as_ref(), b"beta");

        let got = wal
            .get_range(r1.record_offset, r3.next_offset)
            .await
            .unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].payload().as_ref(), b"alpha");
        assert_eq!(got[2].payload().as_ref(), b"gamma");

        // Empty range.
        let got = wal
            .get_range(r1.record_offset, r1.record_offset)
            .await
            .unwrap();
        assert!(got.is_empty());

        wal.shutdown_gracefully().await;
    }

    /// Trim + recover: only the untrimmed suffix comes back, fake trim records are
    /// invisible.
    #[tokio::test]
    async fn trim_then_recover_suffix() {
        let storage: Arc<MemoryObjectStorage> = Arc::new(MemoryObjectStorage::new(0));
        let wal = ObjectWalService::new(storage.clone(), config(1));
        wal.start().await.unwrap();

        let r1 = wal.append(record(1, 0, b"committed")).await.unwrap();
        let _r2 = wal.append(record(1, 1, b"survivor-a")).await.unwrap();
        let _r3 = wal.append(record(1, 2, b"survivor-b")).await.unwrap();

        wal.trim(r1.record_offset).await.unwrap();
        wal.shutdown_gracefully().await;

        let wal2 = ObjectWalService::new(storage.clone(), config(2));
        wal2.start().await.unwrap();
        let recovered: Vec<_> = wal2.recover().collect().await;
        let records: Vec<StreamRecordBatch> =
            recovered.into_iter().map(|r| r.unwrap().record).collect();
        // Only records after the trim offset, no trim sentinel records.
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|r| r.stream_id() == 1));
        assert_eq!(records[0].payload().as_ref(), b"survivor-a");
        assert_eq!(records[1].payload().as_ref(), b"survivor-b");
    }

    /// Recovery of a WAL that was never cleanly closed (no shutdown flush needed,
    /// all appends were acked, hence durable).
    #[tokio::test]
    async fn recover_without_clean_shutdown() {
        let storage: Arc<MemoryObjectStorage> = Arc::new(MemoryObjectStorage::new(0));
        let wal = ObjectWalService::new(storage.clone(), config(1));
        wal.start().await.unwrap();
        wal.append(record(4, 0, b"acked")).await.unwrap();
        drop(wal); // simulate crash: no shutdown_gracefully

        let wal2 = ObjectWalService::new(storage.clone(), config(2));
        wal2.start().await.unwrap();
        let recovered: Vec<_> = wal2.recover().collect().await;
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].as_ref().unwrap().record.payload().as_ref(),
            b"acked"
        );
    }
}
