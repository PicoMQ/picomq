//! Object WAL recovery: list, trim-filter, and iterate the durable suffix.
//!
//! Specification: `specification/wal-protocol.md` (fencing and recovery section).
//!
//! Protocol (exact):
//! - The trim offset is read from the newest object's header.
//! - The object list is filtered to the continuous run starting past the trim offset.
//!   Trimmed and discontinuous-tail objects are dropped with a log.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::task::JoinHandle;

use s3stream_codec::{
    RECORD_DATA_MAGIC, StreamRecordBatch, WAL_RECORD_HEADER_SIZE, WalRecordHeader, wal_crc32,
};
use s3stream_object::{ObjectError, ObjectStorage, ReadOptions, ThrottleStrategy};

use crate::{RecordOffset, RecoverResult, RecoverStream, WalError};

use super::header::{MAX_WAL_HEADER_SIZE, TRIM_OFFSET_NONE, WalObjectHeader};
use super::keys::{TRIM_RECORD_SENTINEL, WalObject, parse_wal_objects, skip_overlap_objects};

/// Discover this WAL's objects: LIST under the node prefix, parse keys, sort
/// by `(epoch, start_offset)`, apply `skip_overlap_objects`.
pub async fn discover_wal_objects(
    storage: &dyn ObjectStorage,
    node_prefix: &str,
) -> Result<Vec<WalObject>, WalError> {
    let listed = storage.list(node_prefix).await?;
    let mut objects = parse_wal_objects(listed);
    skip_overlap_objects(&mut objects);
    Ok(objects)
}

async fn get_trim_offset(
    objects: &[WalObject],
    storage: &dyn ObjectStorage,
) -> Result<i64, WalError> {
    let Some(object) = objects.last() else {
        return Ok(TRIM_OFFSET_NONE);
    };
    let options = ReadOptions {
        throttle: ThrottleStrategy::Bypass,
        bucket_id: Some(object.bucket_id),
    };
    let end = (MAX_WAL_HEADER_SIZE as u64).min(object.size);
    let buffer = storage
        .range_read(&options, &object.key, 0, Some(end))
        .await?;
    let header = WalObjectHeader::unmarshal(&buffer)?;
    Ok(header.trim_offset)
}

pub(crate) fn get_continuous_from_trim_offset(
    objects: &[WalObject],
    trim_offset: i64,
) -> Vec<WalObject> {
    if objects.is_empty() {
        return Vec::new();
    }
    let mut start_index = objects.len();
    for (i, object) in objects.iter().enumerate() {
        if object.end_offset as i64 > trim_offset {
            start_index = i;
            break;
        }
    }
    for object in &objects[..start_index.min(objects.len())] {
        tracing::info!(?object, "drop trimmed object");
    }
    if start_index >= objects.len() {
        return Vec::new();
    }

    let mut end_index = start_index + 1;
    for i in (start_index + 1)..objects.len() {
        if objects[i].start_offset != objects[i - 1].end_offset {
            break;
        }
        end_index = i + 1;
    }
    for object in &objects[end_index..] {
        tracing::warn!(?object, "drop discontinuous object");
    }

    objects[start_index..end_index].to_vec()
}

/// The recovery cursor over a WAL's objects.
pub struct RecoverIterator {
    storage: Arc<dyn ObjectStorage>,
    max_readahead_data_size: u64,
    trim_offset: i64,
    objects: Vec<WalObject>,
    next_index: usize,
    readahead: VecDeque<JoinHandle<Result<Bytes, ObjectError>>>,
    readahead_data_size: u64,
    data: Bytes,
    start_offset_to_epoch: BTreeMap<u64, u64>,
}

impl RecoverIterator {
    pub async fn new(
        all_objects: Vec<WalObject>,
        storage: Arc<dyn ObjectStorage>,
        max_readahead_data_size: u64,
    ) -> Result<Self, WalError> {
        let trim_offset = get_trim_offset(&all_objects, &*storage).await?;
        let filtered = get_continuous_from_trim_offset(&all_objects, trim_offset);

        // The epoch map covers ALL objects, not just the filtered run.
        let mut start_offset_to_epoch = BTreeMap::new();
        let mut last_epoch: Option<u64> = None;
        for object in &all_objects {
            if last_epoch != Some(object.epoch) {
                start_offset_to_epoch.insert(object.start_offset, object.epoch);
                last_epoch = Some(object.epoch);
            }
        }

        let mut iterator = Self {
            storage,
            max_readahead_data_size,
            trim_offset,
            objects: filtered,
            next_index: 0,
            readahead: VecDeque::new(),
            readahead_data_size: 0,
            data: Bytes::new(),
            start_offset_to_epoch,
        };
        iterator.try_read_ahead();
        Ok(iterator)
    }

    /// The trim offset recovered from the newest object header.
    pub fn trim_offset(&self) -> i64 {
        self.trim_offset
    }

    /// Yield the next record past the trim offset, skipping fake trim
    /// records. `None` means a clean end of the durable suffix.
    pub async fn next(&mut self) -> Option<Result<RecoverResult, WalError>> {
        loop {
            if !self.has_next0() {
                return None;
            }
            match self.next0().await {
                Err(e) => return Some(Err(e)),
                Ok(result) => {
                    let record = &result.record;
                    if (result.record_offset.offset as i64) <= self.trim_offset
                        || (record.stream_id() == TRIM_RECORD_SENTINEL
                            && record.epoch() == TRIM_RECORD_SENTINEL)
                    {
                        continue;
                    }
                    return Some(Ok(result));
                }
            }
        }
    }

    fn has_next0(&self) -> bool {
        !self.data.is_empty() || !self.readahead.is_empty() || self.next_index < self.objects.len()
    }

    async fn load_next_buffer(&mut self) -> Result<(), WalError> {
        let handle = self
            .readahead
            .pop_front()
            .ok_or_else(|| WalError::Recovery("no prefetched buffer".to_string()))?;
        let buffer = handle
            .await
            .map_err(|e| WalError::Recovery(format!("readahead task failed: {e}")))??;
        self.readahead_data_size = self.readahead_data_size.saturating_sub(buffer.len() as u64);
        let header = WalObjectHeader::unmarshal(&buffer)?;
        self.data = buffer.slice(header.size()..);
        Ok(())
    }

    fn try_read_ahead(&mut self) {
        while self.readahead_data_size < self.max_readahead_data_size
            && self.next_index < self.objects.len()
        {
            let object = self.objects[self.next_index].clone();
            self.next_index += 1;
            let storage = Arc::clone(&self.storage);
            let options = ReadOptions {
                throttle: ThrottleStrategy::Bypass,
                bucket_id: Some(object.bucket_id),
            };
            let size = object.size;
            let handle = tokio::spawn(async move {
                storage
                    .range_read(&options, &object.key, 0, Some(size))
                    .await
            });
            self.readahead.push_back(handle);
            self.readahead_data_size += size;
        }
    }

    async fn next0(&mut self) -> Result<RecoverResult, WalError> {
        if self.data.is_empty() {
            self.load_next_buffer().await?;
        }
        self.try_read_ahead();

        if self.data.len() < WAL_RECORD_HEADER_SIZE {
            return Err(WalError::Recovery("record header truncated".to_string()));
        }
        let header_bytes = self.data.split_to(WAL_RECORD_HEADER_SIZE);
        let header = WalRecordHeader::unmarshal(&header_bytes)?;
        if header.magic != RECORD_DATA_MAGIC {
            return Err(WalError::Recovery(
                "Invalid magic code in record header.".to_string(),
            ));
        }

        let length = header.body_length as usize;
        let body: Bytes = if self.data.len() < length {
            // Body continues in the next object (v0 compatibility).
            let mut assembled = BytesMut::with_capacity(length);
            assembled.extend_from_slice(&self.data.split_to(self.data.len()));
            if self.readahead.is_empty() && self.next_index >= self.objects.len() {
                return Err(WalError::Recovery(
                    "[Bug] There is a record part but no more data to read.".to_string(),
                ));
            }
            self.load_next_buffer().await?;
            let need = length - assembled.len();
            if self.data.len() < need {
                return Err(WalError::Recovery(
                    "[Bug] There is a record part but no more data to read.".to_string(),
                ));
            }
            assembled.extend_from_slice(&self.data.split_to(need));
            assembled.freeze()
        } else {
            self.data.split_to(length)
        };

        if header.body_crc != wal_crc32(&body) {
            return Err(WalError::Recovery(
                "Record body crc check failed.".to_string(),
            ));
        }

        let offset = header.body_offset - WAL_RECORD_HEADER_SIZE as u64;
        let size = (length + WAL_RECORD_HEADER_SIZE) as u32;
        let mut body_buf = body;
        let record = StreamRecordBatch::parse(&mut body_buf)?;

        let epoch = *self
            .start_offset_to_epoch
            .range(..=offset)
            .next_back()
            .map(|(_, epoch)| epoch)
            .ok_or_else(|| {
                WalError::Recovery("[BUG] Cannot find any epoch for offset".to_string())
            })?;

        Ok(RecoverResult {
            record,
            record_offset: RecordOffset {
                epoch,
                offset,
                size,
            },
        })
    }
}

/// Wrap recovery into the `RecoverStream` the `WriteAheadLog` trait exposes. The
pub fn recover_stream(
    storage: Arc<dyn ObjectStorage>,
    objects: Vec<WalObject>,
    max_readahead_data_size: u64,
) -> RecoverStream {
    enum State {
        Init {
            storage: Arc<dyn ObjectStorage>,
            objects: Vec<WalObject>,
            max_readahead: u64,
        },
        Running(RecoverIterator),
        Done,
    }

    let initial = State::Init {
        storage,
        objects,
        max_readahead: max_readahead_data_size,
    };

    Box::pin(futures::stream::unfold(initial, |mut state| async move {
        loop {
            match state {
                State::Init {
                    storage,
                    objects,
                    max_readahead,
                } => match RecoverIterator::new(objects, storage, max_readahead).await {
                    Ok(iterator) => state = State::Running(iterator),
                    Err(e) => return Some((Err(e), State::Done)),
                },
                State::Running(mut iterator) => {
                    return match iterator.next().await {
                        Some(Ok(result)) => Some((Ok(result), State::Running(iterator))),
                        Some(Err(e)) => Some((Err(e), State::Done)),
                        None => None,
                    };
                }
                State::Done => return None,
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3stream_object::{MemoryObjectStorage, WriteOptions};

    /// (fixtures from `conformance/fixtures/wal_objects`).
    #[tokio::test]
    async fn recovers_java_written_wal() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/fixtures/wal_objects/manifest.json");
        let manifest = std::fs::read_to_string(&path).expect("run conformance/generator first");
        let manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        let fixture_dir = path.parent().unwrap();

        let storage: Arc<MemoryObjectStorage> = Arc::new(MemoryObjectStorage::new(0));
        for object in manifest["objects"].as_array().unwrap() {
            let bytes = std::fs::read(
                fixture_dir.join(format!("{}.bin", object["name"].as_str().unwrap())),
            )
            .unwrap();
            storage
                .write(
                    &WriteOptions::default(),
                    object["path"].as_str().unwrap(),
                    Bytes::from(bytes),
                )
                .await
                .unwrap();
        }

        let node_prefix = super::super::keys::node_prefix(
            manifest["cluster_id"].as_str().unwrap(),
            manifest["node_id"].as_u64().unwrap() as u32,
            None,
        );
        let objects = discover_wal_objects(&*storage, &node_prefix).await.unwrap();
        assert_eq!(objects.len(), 2);

        let mut iterator =
            RecoverIterator::new(objects.clone(), storage.clone(), 100 * 1024 * 1024)
                .await
                .unwrap();
        assert_eq!(
            iterator.trim_offset(),
            manifest["trim_at_offset"].as_i64().unwrap()
        );
        assert!(iterator.next().await.is_none());

        // Recover only the first (data) object with no trim: all three records come
        // back in WAL-offset order, decoded from Java-written bytes.
        let mut iterator =
            RecoverIterator::new(objects[..1].to_vec(), storage.clone(), 100 * 1024 * 1024)
                .await
                .unwrap();
        assert_eq!(iterator.trim_offset(), -1);
        let mut recovered = Vec::new();
        while let Some(result) = iterator.next().await {
            recovered.push(result.unwrap());
        }
        assert_eq!(recovered.len(), 3);
        // Sorted by (streamId, baseOffset) inside the object.
        let ids: Vec<(u64, u64)> = recovered
            .iter()
            .map(|r| (r.record.stream_id(), r.record.base_offset()))
            .collect();
        assert_eq!(ids, vec![(1, 5), (1, 10), (2, 0)]);
        let offsets: Vec<u64> = recovered.iter().map(|r| r.record_offset.offset).collect();
        let expected_offsets: Vec<u64> = manifest["append_results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["offset"].as_u64().unwrap())
            .collect();
        let mut expected_sorted = expected_offsets.clone();
        expected_sorted.sort_unstable();
        assert_eq!(offsets, expected_sorted);
        assert!(recovered.iter().all(|r| r.record_offset.epoch == 3));
    }

    fn object(epoch: u64, start: u64, end: u64) -> WalObject {
        WalObject {
            bucket_id: 0,
            key: format!("p/{epoch}/wal/{start}-{end}"),
            epoch,
            start_offset: start,
            end_offset: end,
            size: end - start,
        }
    }

    /// `get_continuous_from_trim_offset` edge cases.
    #[test]
    fn continuous_from_trim_offset() {
        // Empty list.
        assert!(get_continuous_from_trim_offset(&[], 0).is_empty());

        // Everything trimmed.
        let objects = vec![object(1, 0, 100), object(1, 100, 200)];
        assert!(get_continuous_from_trim_offset(&objects, 200).is_empty());

        // Partial trim keeps the object straddling the trim offset.
        let kept = get_continuous_from_trim_offset(&objects, 100);
        assert_eq!(kept, vec![object(1, 100, 200)]);
        let kept = get_continuous_from_trim_offset(&objects, 99);
        assert_eq!(kept.len(), 2);

        // Discontinuous tail dropped.
        let objects = vec![object(1, 0, 100), object(1, 100, 200), object(1, 300, 400)];
        let kept = get_continuous_from_trim_offset(&objects, -1);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept.last().unwrap().end_offset, 200);
    }
}
