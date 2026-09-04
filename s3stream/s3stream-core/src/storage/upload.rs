//! The delta-WAL upload task: sealed LogCache block -> objects -> atomic commit.
//!
//! Specification: `specification/upload-protocol.md` (upload task state machine).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use s3stream_codec::StreamRecordBatch;
use s3stream_object::{
    NOOP_OBJECT_ID, ObjectAttributes, ObjectStorage, ObjectStreamRange, ObjectWriter, WriteOptions,
};

use crate::api::StreamError;
use crate::manager::{CommitStreamSetObjectRequest, ObjectManager, StreamObject};
use crate::storage::s3_storage::S3StorageConfig;

/// Async token-bucket rate limiter, permits = bytes.
///
/// An acquire reserves capacity and waits out earlier reservations. The cost
/// of the current acquire is charged to *future* acquirers. `burst` lifts
/// the limit for the rest of the task's life.
pub struct AsyncRateLimiter {
    state: std::sync::Mutex<LimiterState>,
}

struct LimiterState {
    /// Bytes per second.`f64::INFINITY` = unlimited.
    rate: f64,
    /// When the next acquire may proceed.
    next_free: Instant,
}

impl AsyncRateLimiter {
    pub fn new(rate: f64) -> Self {
        Self {
            state: std::sync::Mutex::new(LimiterState {
                rate,
                next_free: Instant::now(),
            }),
        }
    }

    /// Wait for `size` bytes of budget. Returns immediately if unlimited.
    pub async fn acquire(&self, size: usize) {
        let wait_until = {
            let mut state = self.state.lock().expect("limiter poisoned");
            if !state.rate.is_finite() || state.rate <= 0.0 {
                return;
            }
            let now = Instant::now();
            let start = state.next_free.max(now);
            let cost = Duration::from_secs_f64(size as f64 / state.rate);
            state.next_free = start + cost;
            start
        };
        tokio::time::sleep_until(tokio::time::Instant::from_std(wait_until)).await;
    }

    pub fn burst(&self) {
        let mut state = self.state.lock().expect("limiter poisoned");
        state.rate = f64::INFINITY;
        state.next_free = Instant::now();
    }
}

/// One upload of a sealed block. State machine: `prepare -> upload -> commit`.
/// Each phase awaitable exactly once, in order.
///
/// - If the block holds exactly one stream, everything goes into a stream object
///   (builder special case).
/// - Otherwise streams with >= `stream_split_size` bytes split into their own stream
///   objects. The rest write into one stream set object.
/// - Object ids are assigned deterministically: the stream set object (if any) takes
///   the first prepared id, then split streams in ascending stream-id order.
pub struct UploadWalTask {
    stream_set_object_map: BTreeMap<u64, Vec<StreamRecordBatch>>,
    stream_object_map: BTreeMap<u64, Vec<StreamRecordBatch>>,
    object_block_size: usize,
    object_part_size: usize,
    limiter: Arc<AsyncRateLimiter>,
    prepared_object_id: Option<u64>,
    commit_request: Option<CommitStreamSetObjectRequest>,
}

impl UploadWalTask {
    /// Plan the upload from a sealed block's records, including the
    /// single-stream special case.
    pub fn plan(
        config: &S3StorageConfig,
        records: BTreeMap<u64, Vec<StreamRecordBatch>>,
        rate: f64,
    ) -> Self {
        let mut stream_set_object_map = BTreeMap::new();
        let mut stream_object_map = BTreeMap::new();
        if records.len() == 1 {
            // When only one stream is present, write only stream data.
            stream_object_map = records;
        } else {
            for (stream_id, stream_records) in records {
                if stream_size(&stream_records) >= config.stream_split_size {
                    stream_object_map.insert(stream_id, stream_records);
                } else {
                    stream_set_object_map.insert(stream_id, stream_records);
                }
            }
        }
        Self {
            stream_set_object_map,
            stream_object_map,
            object_block_size: config.object_block_size,
            object_part_size: config.object_part_size,
            limiter: Arc::new(AsyncRateLimiter::new(rate)),
            prepared_object_id: None,
            commit_request: None,
        }
    }

    pub fn object_count(&self) -> usize {
        self.stream_object_map.len() + usize::from(!self.stream_set_object_map.is_empty())
    }

    /// Bypass the rate limit so the task finishes as fast as possible.
    pub fn burst(&self) {
        self.limiter.burst();
    }

    /// Handle to the task's limiter (S3Storage bursts inflight tasks without taking
    /// the task lock).
    pub fn limiter(&self) -> Arc<AsyncRateLimiter> {
        Arc::clone(&self.limiter)
    }

    pub async fn prepare(&mut self, object_manager: &dyn ObjectManager) -> Result<(), StreamError> {
        let object_id = object_manager
            .prepare_object(self.object_count(), 60 * 60 * 1000)
            .await?;
        self.prepared_object_id = Some(object_id);
        Ok(())
    }

    /// Write all objects (stream set + splits) via ObjectWriter.
    ///
    /// Split stream objects upload
    /// concurrently. The stream set object writes its streams in id order.
    pub async fn upload(
        &mut self,
        object_storage: Arc<dyn ObjectStorage>,
    ) -> Result<(), StreamError> {
        let mut object_id = self.prepared_object_id.expect("prepare before upload");
        let mut request = CommitStreamSetObjectRequest::default();

        let stream_set_object_id = if self.stream_set_object_map.is_empty() {
            NOOP_OBJECT_ID
        } else {
            let id = object_id;
            object_id += 1;
            id
        };

        // Split stream objects, concurrent (ids in ascending stream-id order).
        let mut stream_object_futures = Vec::new();
        for (stream_id, stream_records) in &self.stream_object_map {
            let id = object_id;
            object_id += 1;
            stream_object_futures.push(write_stream_object(
                Arc::clone(&object_storage),
                Arc::clone(&self.limiter),
                id,
                *stream_id,
                stream_records.clone(),
                self.object_block_size,
                self.object_part_size,
            ));
        }
        let stream_set_records = &self.stream_set_object_map;
        let sso_future = async {
            if stream_set_object_id == NOOP_OBJECT_ID {
                return Ok::<Option<(u64, u32, Vec<ObjectStreamRange>)>, StreamError>(None);
            }
            let mut writer = ObjectWriter::open(
                stream_set_object_id,
                &*object_storage,
                self.object_block_size,
                self.object_part_size,
                WriteOptions::default(),
            )
            .await?;
            let mut ranges = Vec::new();
            for (stream_id, records) in stream_set_records {
                let size = stream_size(records);
                self.limiter.acquire(size as usize).await;
                ranges.push(ObjectStreamRange {
                    stream_id: *stream_id,
                    epoch: u64::MAX,
                    start_offset: records[0].base_offset(),
                    end_offset: records.last().unwrap().last_offset(),
                    size,
                });
                writer.write(*stream_id, records).await?;
            }
            writer.close().await?;
            let attributes = ObjectAttributes::new(writer.bucket_id(), false, false).0;
            Ok(Some((writer.size(), attributes, ranges)))
        };

        let (stream_objects, sso) = futures::future::try_join(
            futures::future::try_join_all(stream_object_futures),
            sso_future,
        )
        .await?;

        request.stream_objects = stream_objects;
        request.object_id = stream_set_object_id;
        if let Some((size, attributes, ranges)) = sso {
            request.object_size = size;
            request.attributes = attributes;
            request.stream_ranges = ranges;
        }
        self.commit_request = Some(request);
        Ok(())
    }

    /// Atomically commit everything to the metadata plane.
    pub async fn commit(&mut self, object_manager: &dyn ObjectManager) -> Result<(), StreamError> {
        let request = self.commit_request.clone().expect("upload before commit");
        object_manager.commit_stream_set_object(request).await?;
        Ok(())
    }

    /// The commit request this task will/did send (visible for tests).
    pub fn commit_request(&self) -> &CommitStreamSetObjectRequest {
        self.commit_request
            .as_ref()
            .expect("upload before commit_request")
    }
}

async fn write_stream_object(
    object_storage: Arc<dyn ObjectStorage>,
    limiter: Arc<AsyncRateLimiter>,
    object_id: u64,
    stream_id: u64,
    records: Vec<StreamRecordBatch>,
    block_size: usize,
    part_size: usize,
) -> Result<StreamObject, StreamError> {
    let size = stream_size(&records);
    limiter.acquire(size as usize).await;
    let mut writer = ObjectWriter::open(
        object_id,
        &*object_storage,
        block_size,
        part_size,
        WriteOptions::default(),
    )
    .await?;
    writer.write(stream_id, &records).await?;
    let start_offset = records[0].base_offset();
    let end_offset = records.last().unwrap().last_offset();
    writer.close().await?;
    Ok(StreamObject {
        object_id,
        object_size: writer.size(),
        stream_id,
        start_offset,
        end_offset,
        attributes: ObjectAttributes::new(writer.bucket_id(), false, false).0,
    })
}

fn stream_size(records: &[StreamRecordBatch]) -> u64 {
    records.iter().map(|r| r.size() as u64).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryMetadataManager;
    use bytes::Bytes;
    use s3stream_object::MemoryObjectStorage;

    fn record(stream_id: u64, base_offset: u64, size: usize) -> StreamRecordBatch {
        StreamRecordBatch::new(stream_id, 0, base_offset, 1, Bytes::from(vec![7u8; size]))
    }

    fn config() -> S3StorageConfig {
        S3StorageConfig {
            stream_split_size: 100,
            ..S3StorageConfig::test_defaults()
        }
    }

    /// Split rule: a stream over stream_split_size gets its own object. The rest land
    /// in the stream set object. Ranges in the commit request are exact.
    #[tokio::test]
    async fn split_and_commit_request_shape() {
        let mut records = BTreeMap::new();
        records.insert(1, vec![record(1, 0, 200)]); // splits (>= 100)
        records.insert(2, vec![record(2, 10, 10), record(2, 11, 10)]); // stream set
        records.insert(3, vec![record(3, 5, 10)]); // stream set

        let mut task = UploadWalTask::plan(&config(), records, f64::INFINITY);
        assert_eq!(task.object_count(), 2);

        let storage = Arc::new(MemoryObjectStorage::new(0));
        let manager = MemoryMetadataManager::new();
        task.prepare(&*manager).await.unwrap();
        task.upload(storage.clone() as Arc<dyn ObjectStorage>)
            .await
            .unwrap();

        let request = task.commit_request();
        assert_ne!(request.object_id, NOOP_OBJECT_ID);
        assert_eq!(request.stream_ranges.len(), 2);
        assert_eq!(request.stream_ranges[0].stream_id, 2);
        assert_eq!(request.stream_ranges[0].start_offset, 10);
        assert_eq!(request.stream_ranges[0].end_offset, 12);
        assert_eq!(request.stream_ranges[1].stream_id, 3);
        assert_eq!(request.stream_objects.len(), 1);
        let so = &request.stream_objects[0];
        assert_eq!(so.stream_id, 1);
        assert_eq!((so.start_offset, so.end_offset), (0, 1));
        assert!(so.object_size > 0);
        // Ids: sso gets the first prepared id, the split stream the next.
        assert_eq!(so.object_id, request.object_id + 1);

        task.commit(&*manager).await.unwrap();
    }

    /// Single-stream special case: no stream set object at all.
    #[tokio::test]
    async fn single_stream_writes_stream_object_only() {
        let mut records = BTreeMap::new();
        records.insert(9, vec![record(9, 0, 8)]); // tiny, but still splits
        let mut task = UploadWalTask::plan(&config(), records, f64::INFINITY);
        assert_eq!(task.object_count(), 1);

        let storage = Arc::new(MemoryObjectStorage::new(0));
        let manager = MemoryMetadataManager::new();
        task.prepare(&*manager).await.unwrap();
        task.upload(storage as Arc<dyn ObjectStorage>)
            .await
            .unwrap();
        let request = task.commit_request();
        assert_eq!(request.object_id, NOOP_OBJECT_ID);
        assert_eq!(request.stream_objects.len(), 1);
        assert!(request.stream_ranges.is_empty());
    }

    /// The rate limiter delays acquisition. Burst releases it.
    #[tokio::test(start_paused = true)]
    async fn rate_limiter_paces_and_bursts() {
        let limiter = AsyncRateLimiter::new(1000.0); // 1000 B/s
        let start = tokio::time::Instant::now();
        limiter.acquire(500).await; // first acquire passes at t=0
        limiter.acquire(500).await; // waits ~0.5s (charged by the first)
        assert!(start.elapsed() >= Duration::from_millis(400));

        limiter.burst();
        let before = tokio::time::Instant::now();
        limiter.acquire(1_000_000).await;
        assert!(before.elapsed() < Duration::from_millis(10));
    }
}
