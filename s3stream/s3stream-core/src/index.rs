//! Local sparse index: stream offset -> stream-set-object hints, to cut metadata-plane
//! round trips on cold reads.
//!
//! One entry per stream range in a stream set object, persisted per node,
//! updated on every commit, and compacted when it outgrows the size cap.
//! A `std::sync::Mutex` guards the map (mutations are pure memory ops) and
//! `upload()` serializes flushes. Eviction victims come out in plain
//! map-iteration order, which is arbitrary. The per-stream eviction sequence
//! is the tested contract. `NodeRangeIndexCache` (cross-node index cache for
//! snapshot reads) is out of scope until snapshot-read lands.

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use s3stream_object::{NOOP_OBJECT_ID, ObjectStorage, ReadOptions, WriteOptions, gen_index_key};

use crate::api::StreamError;
use crate::manager::CommitStreamSetObjectRequest;

const VERSION: i16 = 0;

/// (env `PICO_STREAM_RANGE_INDEX_COMPACT_NUM`, default 3).
pub const DEFAULT_COMPACT_NUM: usize = 3;

/// (env `PICO_STREAM_RANGE_INDEX_MAX_SIZE`, default 1 MiB).
pub const DEFAULT_MAX_INDEX_SIZE: usize = 1024 * 1024;

/// At most one stream-close-triggered flush per interval.
const UPLOAD_ON_STREAM_CLOSE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(5000);

/// The close path waits at most this long for the flush.
const UPLOAD_ON_STREAM_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// One stream range inside a stream set object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeIndex {
    pub start_offset: u64,
    pub end_offset: u64,
    pub object_id: u64,
}

impl RangeIndex {
    /// 3 longs plus a 16 byte object header. Kept exact so `MAX_INDEX_SIZE`
    /// eviction triggers at the same entry counts.
    pub const OBJECT_SIZE: usize = 3 * 8 + 16;
}

/// Per-stream list of range indexes, sparse after eviction.
#[derive(Debug, Default)]
pub struct SparseRangeIndex {
    compact_num: usize,
    ranges: Vec<RangeIndex>,
    size: usize,
    evict_index: usize,
}

impl SparseRangeIndex {
    pub fn new(compact_num: usize) -> Self {
        Self::with_ranges(compact_num, Vec::new())
    }

    pub fn with_ranges(compact_num: usize, ranges: Vec<RangeIndex>) -> Self {
        let size = ranges.len() * RangeIndex::OBJECT_SIZE;
        Self {
            compact_num,
            ranges,
            size,
            evict_index: 0,
        }
    }

    pub fn append(&mut self, new_range: RangeIndex) -> isize {
        let mut delta: isize = 0;
        if self
            .ranges
            .last()
            .is_some_and(|last| new_range.start_offset <= last.start_offset)
        {
            tracing::error!(
                "unexpected new range index {:?}, last: {:?}, maybe initialized with outdated index file, reset local cache",
                new_range,
                self.ranges.last()
            );
            delta -= self.size as isize;
            self.reset();
        }
        self.ranges.push(new_range);
        self.size += RangeIndex::OBJECT_SIZE;
        delta + RangeIndex::OBJECT_SIZE as isize
    }

    pub fn reset(&mut self) {
        self.ranges.clear();
        self.size = 0;
        self.evict_index = 0;
    }

    pub fn compact(
        &mut self,
        new_range: Option<RangeIndex>,
        compacted_object_ids: &HashSet<u64>,
    ) -> isize {
        if compacted_object_ids.is_empty() {
            return match new_range {
                Some(range) => self.append(range),
                None => 0,
            };
        }
        let mut new_list = Vec::with_capacity(self.ranges.len() + 1);
        let mut inserted = false;
        for range in &self.ranges {
            if compacted_object_ids.contains(&range.object_id) {
                continue;
            }
            if let Some(new_range) = new_range
                && !inserted
                && range.start_offset > new_range.start_offset
            {
                new_list.push(new_range);
                inserted = true;
            }
            new_list.push(*range);
        }
        if let Some(new_range) = new_range
            && !inserted
        {
            new_list.push(new_range);
        }
        let old_size = self.size as isize;
        self.ranges = new_list;
        self.size = self.ranges.len() * RangeIndex::OBJECT_SIZE;
        self.evict_index = 0;
        self.size as isize - old_size
    }

    pub fn evict_once(&mut self) -> usize {
        let len = self.ranges.len();
        let index_to_evict = if len == 0 {
            return 0;
        } else if len == 1 {
            0
        } else if len <= 1 + self.compact_num {
            1
        } else {
            if self.evict_index.is_multiple_of(len) || self.evict_index >= len - self.compact_num {
                self.evict_index = 1;
            }
            let i = self.evict_index;
            self.evict_index += 1;
            i
        };
        self.ranges.remove(index_to_evict);
        self.size -= RangeIndex::OBJECT_SIZE;
        RangeIndex::OBJECT_SIZE
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn ranges(&self) -> &[RangeIndex] {
        &self.ranges
    }
}

pub fn binary_search_object_id(start_offset: u64, ranges: &[RangeIndex]) -> Option<u64> {
    if ranges.is_empty() {
        return None;
    }
    let index = match ranges.binary_search_by(|r| r.start_offset.cmp(&start_offset)) {
        Ok(i) => i,
        Err(insertion) => {
            if insertion == 0 {
                return None;
            }
            insertion - 1
        }
    };
    Some(ranges[index.min(ranges.len() - 1)].object_id)
}

struct CacheState {
    streams: BTreeMap<u64, SparseRangeIndex>,
    total_size: usize,
}

/// The node-local persisted stream range index.
pub struct LocalStreamRangeIndexCache {
    node_id: u32,
    storage: std::sync::Arc<dyn ObjectStorage>,
    compact_num: usize,
    max_index_size: usize,
    state: Mutex<CacheState>,
    /// `upload()` calls. Here concurrent uploads just queue on this mutex).
    upload_lock: tokio::sync::Mutex<()>,
    last_upload_time: Mutex<Option<std::time::Instant>>,
    pruned: std::sync::atomic::AtomicBool,
}

impl LocalStreamRangeIndexCache {
    /// Create and seed from the persisted index object, if present.
    pub async fn init(node_id: u32, storage: std::sync::Arc<dyn ObjectStorage>) -> Self {
        let cache = Self {
            node_id,
            storage,
            compact_num: env_usize("PICO_STREAM_RANGE_INDEX_COMPACT_NUM", DEFAULT_COMPACT_NUM),
            max_index_size: env_usize("PICO_STREAM_RANGE_INDEX_MAX_SIZE", DEFAULT_MAX_INDEX_SIZE),
            state: Mutex::new(CacheState {
                streams: BTreeMap::new(),
                total_size: 0,
            }),
            upload_lock: tokio::sync::Mutex::new(()),
            last_upload_time: Mutex::new(None),
            pruned: std::sync::atomic::AtomicBool::new(false),
        };
        let key = gen_index_key(0, node_id as u64);
        match cache.storage.read(&ReadOptions::default(), &key).await {
            Ok(data) => match Self::from_buffer(&data) {
                Ok(streams) => {
                    let mut state = cache.state.lock().expect("index poisoned");
                    for (stream_id, ranges) in streams {
                        state.total_size += ranges.len() * RangeIndex::OBJECT_SIZE;
                        state.streams.insert(
                            stream_id,
                            SparseRangeIndex::with_ranges(cache.compact_num, ranges),
                        );
                    }
                    tracing::info!(
                        "loaded sparse index from object storage for {} streams at node {node_id}",
                        state.streams.len()
                    );
                }
                Err(e) => tracing::error!("failed to parse persisted sparse index: {e}"),
            },
            Err(_) => tracing::info!("sparse index not found for node {node_id}"),
        }
        cache
    }

    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    pub fn total_size(&self) -> usize {
        self.state.lock().expect("index poisoned").total_size
    }

    pub fn clear(&self) {
        let mut state = self.state.lock().expect("index poisoned");
        state.streams.clear();
        state.total_size = 0;
    }

    pub fn append(&self, range_index_map: &BTreeMap<u64, Option<RangeIndex>>) {
        let mut state = self.state.lock().expect("index poisoned");
        for (&stream_id, range) in range_index_map {
            if let Some(range) = range {
                let sparse = state
                    .streams
                    .entry(stream_id)
                    .or_insert_with(|| SparseRangeIndex::new(self.compact_num));
                let delta = sparse.append(*range);
                state.total_size = (state.total_size as isize + delta) as usize;
            }
        }
        self.evict_if_necessary(&mut state);
    }

    fn evict_if_necessary(&self, state: &mut CacheState) {
        if state.total_size <= self.max_index_size {
            return;
        }
        let mut has_sufficient_index = true;
        while state.total_size > self.max_index_size {
            let stream_ids: Vec<u64> = state.streams.keys().copied().collect();
            let mut evicted = false;
            for stream_id in stream_ids {
                let sparse = state
                    .streams
                    .get_mut(&stream_id)
                    .expect("key just collected");
                if sparse.len() <= 1 + self.compact_num && has_sufficient_index {
                    continue;
                }
                state.total_size -= sparse.evict_once();
                evicted = true;
                if sparse.is_empty() {
                    state.streams.remove(&stream_id);
                }
                if state.total_size <= self.max_index_size {
                    break;
                }
            }
            if !evicted {
                has_sufficient_index = false;
            }
        }
    }

    pub fn compact(
        &self,
        range_index_map: &BTreeMap<u64, Option<RangeIndex>>,
        compacted_object_ids: &HashSet<u64>,
    ) {
        let mut state = self.state.lock().expect("index poisoned");
        let stream_ids: Vec<u64> = state.streams.keys().copied().collect();
        for stream_id in stream_ids {
            let sparse = state
                .streams
                .get_mut(&stream_id)
                .expect("key just collected");
            let new_range = range_index_map.get(&stream_id).copied().flatten();
            let delta = sparse.compact(new_range, compacted_object_ids);
            state.total_size = (state.total_size as isize + delta) as usize;
            if state
                .streams
                .get(&stream_id)
                .expect("still present")
                .is_empty()
            {
                state.streams.remove(&stream_id);
            }
        }
    }

    pub async fn update_index_from_request(
        &self,
        request: &CommitStreamSetObjectRequest,
    ) -> Result<(), StreamError> {
        let range_index_map = Self::range_index_map_from_request(request);
        if request.compacted_object_ids.is_empty() {
            self.append(&range_index_map);
            return Ok(());
        }
        let compacted: HashSet<u64> = request.compacted_object_ids.iter().copied().collect();
        self.compact(&range_index_map, &compacted);
        self.upload().await
    }

    fn range_index_map_from_request(
        request: &CommitStreamSetObjectRequest,
    ) -> BTreeMap<u64, Option<RangeIndex>> {
        let mut map = BTreeMap::new();
        for range in &request.stream_ranges {
            let new_range = (request.object_id != NOOP_OBJECT_ID).then_some(RangeIndex {
                start_offset: range.start_offset,
                end_offset: range.end_offset,
                object_id: request.object_id,
            });
            map.insert(range.stream_id, new_range);
        }
        if !request.compacted_object_ids.is_empty() {
            for stream_object in &request.stream_objects {
                map.entry(stream_object.stream_id).or_insert(None);
            }
        }
        map
    }

    pub fn search_object_id(&self, stream_id: u64, start_offset: u64) -> Option<u64> {
        let state = self.state.lock().expect("index poisoned");
        let sparse = state.streams.get(&stream_id)?;
        binary_search_object_id(start_offset, sparse.ranges())
    }

    pub async fn prune(&self, live_object_ids: &HashSet<u64>) -> Result<(), StreamError> {
        let pruned = {
            let mut state = self.state.lock().expect("index poisoned");
            let stream_ids: Vec<u64> = state.streams.keys().copied().collect();
            let mut pruned = false;
            for stream_id in stream_ids {
                let sparse = state
                    .streams
                    .get_mut(&stream_id)
                    .expect("key just collected");
                let invalid: HashSet<u64> = sparse
                    .ranges()
                    .iter()
                    .map(|r| r.object_id)
                    .filter(|id| !live_object_ids.contains(id))
                    .collect();
                if invalid.is_empty() {
                    continue;
                }
                let delta = sparse.compact(None, &invalid);
                state.total_size = (state.total_size as isize + delta) as usize;
                if state
                    .streams
                    .get(&stream_id)
                    .expect("still present")
                    .is_empty()
                {
                    state.streams.remove(&stream_id);
                }
                pruned = true;
            }
            pruned
        };
        if pruned {
            self.upload().await?;
        }
        Ok(())
    }

    /// Persist the index object.
    pub async fn upload(&self) -> Result<(), StreamError> {
        let _guard = self.upload_lock.lock().await;
        let buf = {
            let state = self.state.lock().expect("index poisoned");
            if state.streams.is_empty() {
                return Ok(());
            }
            Self::to_buffer(&state.streams)
        };
        let key = gen_index_key(0, self.node_id as u64);
        self.storage
            .write(&WriteOptions::default(), &key, buf)
            .await
            .map_err(StreamError::from)?;
        Ok(())
    }

    /// Rate-limited flush for the stream-close path: at most one flush per 5s
    /// window. Rate-limited calls return immediately.
    pub async fn upload_on_stream_close(&self) -> Result<(), StreamError> {
        {
            let mut last = self.last_upload_time.lock().expect("index poisoned");
            let now = std::time::Instant::now();
            match *last {
                Some(t) if now.duration_since(t) <= UPLOAD_ON_STREAM_CLOSE_INTERVAL => {
                    return Ok(());
                }
                _ => *last = Some(now),
            }
        }
        tracing::info!("upload local index cache on stream close");
        match tokio::time::timeout(UPLOAD_ON_STREAM_CLOSE_TIMEOUT, self.upload()).await {
            Ok(result) => result,
            Err(_) => Err(StreamError::Unexpected(
                "upload local index on stream close timed out".into(),
            )),
        }
    }

    /// One-shot prune after startup: drop entries whose objects are gone.
    /// Repeat calls are no-ops. The first call runs `prune` with the live
    /// stream-set-object ids.
    pub async fn async_prune(&self, live_object_ids: &HashSet<u64>) -> Result<(), StreamError> {
        if self
            .pruned
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            return self.prune(live_object_ids).await;
        }
        Ok(())
    }

    pub fn to_buffer(streams: &BTreeMap<u64, SparseRangeIndex>) -> Bytes {
        let stream_count = streams.values().filter(|s| !s.is_empty()).count();
        let payload: usize = streams
            .values()
            .filter(|s| !s.is_empty())
            .map(|s| 8 + 4 + s.len() * 24)
            .sum();
        let mut buf = BytesMut::with_capacity(2 + 4 + payload);
        buf.put_i16(VERSION);
        buf.put_i32(stream_count as i32);
        for (&stream_id, sparse) in streams {
            if sparse.is_empty() {
                continue;
            }
            buf.put_u64(stream_id);
            buf.put_i32(sparse.len() as i32);
            for range in sparse.ranges() {
                buf.put_u64(range.start_offset);
                buf.put_u64(range.end_offset);
                buf.put_u64(range.object_id);
            }
        }
        buf.freeze()
    }

    pub fn from_buffer(data: &Bytes) -> Result<BTreeMap<u64, Vec<RangeIndex>>, StreamError> {
        let mut buf = data.clone();
        if buf.remaining() < 6 {
            return Err(StreamError::Unexpected(
                "sparse index buffer truncated".into(),
            ));
        }
        let version = buf.get_i16();
        if version != VERSION {
            return Err(StreamError::Unexpected(format!(
                "unrecognized sparse index version: {version}"
            )));
        }
        let stream_count = buf.get_i32();
        let mut streams = BTreeMap::new();
        for _ in 0..stream_count {
            if buf.remaining() < 12 {
                return Err(StreamError::Unexpected(
                    "sparse index buffer truncated".into(),
                ));
            }
            let stream_id = buf.get_u64();
            let range_count = buf.get_i32() as usize;
            if buf.remaining() < range_count * 24 {
                return Err(StreamError::Unexpected(
                    "sparse index buffer truncated".into(),
                ));
            }
            let entry: &mut Vec<RangeIndex> = streams.entry(stream_id).or_default();
            for _ in 0..range_count {
                entry.push(RangeIndex {
                    start_offset: buf.get_u64(),
                    end_offset: buf.get_u64(),
                    object_id: buf.get_u64(),
                });
            }
        }
        Ok(streams)
    }
}

/// Every successful stream-set commit feeds the index.
#[async_trait::async_trait]
impl crate::manager::CommitStreamSetObjectHook for LocalStreamRangeIndexCache {
    async fn on_commit_success(
        &self,
        request: &CommitStreamSetObjectRequest,
    ) -> Result<(), StreamError> {
        self.update_index_from_request(request).await
    }
}

#[async_trait::async_trait]
impl crate::manager::StreamCloseHook for LocalStreamRangeIndexCache {
    async fn before_stream_close(&self, _stream_id: u64) -> Result<(), StreamError> {
        self.upload_on_stream_close().await
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use s3stream_object::MemoryObjectStorage;

    use crate::manager::StreamObject;
    use s3stream_object::ObjectStreamRange;

    fn range(start: u64, end: u64, object_id: u64) -> RangeIndex {
        RangeIndex {
            start_offset: start,
            end_offset: end,
            object_id,
        }
    }

    /// The documented eviction order for [0..6) with compact_num=2.
    #[test]
    fn evict_order_matches_java() {
        let mut sparse = SparseRangeIndex::with_ranges(
            2,
            (0..6).map(|i| range(i * 10, i * 10 + 10, i)).collect(),
        );
        let ids = |s: &SparseRangeIndex| s.ranges().iter().map(|r| r.object_id).collect::<Vec<_>>();
        sparse.evict_once();
        assert_eq!(ids(&sparse), vec![0, 2, 3, 4, 5]);
        sparse.evict_once();
        assert_eq!(ids(&sparse), vec![0, 2, 4, 5]);
        sparse.evict_once();
        assert_eq!(ids(&sparse), vec![0, 4, 5]);
        sparse.evict_once();
        assert_eq!(ids(&sparse), vec![0, 5]);
        sparse.evict_once();
        assert_eq!(ids(&sparse), vec![0]);
        sparse.evict_once();
        assert!(sparse.is_empty());
        assert_eq!(sparse.evict_once(), 0);
    }

    #[test]
    fn compact_replaces_consumed_objects() {
        let mut sparse = SparseRangeIndex::with_ranges(
            3,
            vec![range(0, 10, 1), range(10, 20, 2), range(20, 30, 3)],
        );
        // Objects 1+2 compacted into object 9 covering [0,20).
        let delta = sparse.compact(Some(range(0, 20, 9)), &HashSet::from([1u64, 2u64]));
        assert_eq!(delta, -(RangeIndex::OBJECT_SIZE as isize));
        assert_eq!(
            sparse
                .ranges()
                .iter()
                .map(|r| r.object_id)
                .collect::<Vec<_>>(),
            vec![9, 3]
        );
        // Out-of-order append resets (stale index file protection).
        let delta = sparse.append(range(5, 6, 99));
        assert_eq!(sparse.len(), 1);
        assert!(delta < 0);
    }

    #[test]
    fn binary_search_semantics() {
        let ranges = vec![range(10, 20, 1), range(50, 60, 2), range(100, 200, 3)];
        assert_eq!(binary_search_object_id(5, &ranges), None);
        assert_eq!(binary_search_object_id(10, &ranges), Some(1));
        assert_eq!(binary_search_object_id(30, &ranges), Some(1));
        assert_eq!(binary_search_object_id(50, &ranges), Some(2));
        assert_eq!(binary_search_object_id(99, &ranges), Some(2));
        assert_eq!(binary_search_object_id(150, &ranges), Some(3));
        assert_eq!(binary_search_object_id(10_000, &ranges), Some(3));
        assert_eq!(binary_search_object_id(0, &[]), None);
    }

    /// Persisted-format conformance: parse the Java-written buffer, re-encode
    #[test]
    fn java_persisted_format_round_trip() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/fixtures/range_index");
        let golden = Bytes::from(
            std::fs::read(dir.join("index_v0.bin")).expect("run conformance/generator first"),
        );
        let manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join("manifest.json")).unwrap())
                .unwrap();

        let parsed = LocalStreamRangeIndexCache::from_buffer(&golden).unwrap();
        for stream in manifest["streams"].as_array().unwrap() {
            let stream_id = stream["stream_id"].as_u64().unwrap();
            let expected: Vec<RangeIndex> = stream["ranges"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    let r = r.as_array().unwrap();
                    range(
                        r[0].as_u64().unwrap(),
                        r[1].as_u64().unwrap(),
                        r[2].as_u64().unwrap(),
                    )
                })
                .collect();
            assert_eq!(parsed[&stream_id], expected, "stream {stream_id}");
        }

        // Re-encode: byte-identical to Java (generator writes streams sorted).
        let streams: BTreeMap<u64, SparseRangeIndex> = parsed
            .into_iter()
            .map(|(id, ranges)| (id, SparseRangeIndex::with_ranges(3, ranges)))
            .collect();
        assert_eq!(LocalStreamRangeIndexCache::to_buffer(&streams), golden);

        // Index object keys match ObjectUtils.genIndexKey.
        for case in manifest["index_keys"].as_array().unwrap() {
            let node_id = case["node_id"].as_u64().unwrap();
            let expected = case["key"].as_str().unwrap();
            assert_eq!(gen_index_key(0, node_id), expected, "node {node_id}");
        }
    }

    /// Full cache lifecycle: commit updates, search, persist, reload.
    #[tokio::test]
    async fn cache_persists_and_reloads() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let cache =
            LocalStreamRangeIndexCache::init(7, storage.clone() as Arc<dyn ObjectStorage>).await;

        // Delta commit: stream set object 100 with two stream ranges.
        let request = CommitStreamSetObjectRequest {
            object_id: 100,
            object_size: 1000,
            stream_ranges: vec![
                ObjectStreamRange {
                    stream_id: 1,
                    epoch: 1,
                    start_offset: 0,
                    end_offset: 50,
                    size: 500,
                },
                ObjectStreamRange {
                    stream_id: 2,
                    epoch: 1,
                    start_offset: 10,
                    end_offset: 30,
                    size: 500,
                },
            ],
            ..Default::default()
        };
        cache.update_index_from_request(&request).await.unwrap();
        assert_eq!(cache.search_object_id(1, 20), Some(100));
        assert_eq!(cache.search_object_id(2, 10), Some(100));
        assert_eq!(cache.total_size(), 2 * RangeIndex::OBJECT_SIZE);

        // Compaction commit: object 100 consumed. Stream 1 re-homed to object 200,
        // stream 2 split into stream objects (entry dropped).
        let request = CommitStreamSetObjectRequest {
            object_id: 200,
            object_size: 900,
            stream_ranges: vec![ObjectStreamRange {
                stream_id: 1,
                epoch: 1,
                start_offset: 0,
                end_offset: 50,
                size: 500,
            }],
            stream_objects: vec![StreamObject {
                object_id: 201,
                object_size: 400,
                stream_id: 2,
                start_offset: 10,
                end_offset: 30,
                attributes: 0,
            }],
            compacted_object_ids: vec![100],
            ..Default::default()
        };
        cache.update_index_from_request(&request).await.unwrap();
        assert_eq!(cache.search_object_id(1, 20), Some(200));
        assert_eq!(cache.search_object_id(2, 10), None);

        // The compaction path persisted. A fresh cache reloads the same state.
        let reloaded =
            LocalStreamRangeIndexCache::init(7, storage.clone() as Arc<dyn ObjectStorage>).await;
        assert_eq!(reloaded.search_object_id(1, 20), Some(200));
        assert_eq!(reloaded.total_size(), RangeIndex::OBJECT_SIZE);

        // Prune drops entries for dead objects and persists.
        reloaded.prune(&HashSet::new()).await.unwrap();
        assert_eq!(reloaded.search_object_id(1, 20), None);
        assert_eq!(reloaded.total_size(), 0);
    }

    /// Counts writes so throttle/one-shot behavior is observable.
    struct CountingStorage {
        inner: MemoryObjectStorage,
        writes: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ObjectStorage for CountingStorage {
        async fn readiness_check(&self) -> Result<(), s3stream_object::ObjectError> {
            self.inner.readiness_check().await
        }
        async fn range_read(
            &self,
            options: &ReadOptions,
            key: &str,
            start: u64,
            end: Option<u64>,
        ) -> Result<bytes::Bytes, s3stream_object::ObjectError> {
            self.inner.range_read(options, key, start, end).await
        }
        async fn write(
            &self,
            options: &WriteOptions,
            key: &str,
            data: bytes::Bytes,
        ) -> Result<s3stream_object::WriteResult, s3stream_object::ObjectError> {
            self.writes
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            self.inner.write(options, key, data).await
        }
        async fn writer(
            &self,
            options: &WriteOptions,
            key: &str,
        ) -> Result<Box<dyn s3stream_object::MultipartWriter>, s3stream_object::ObjectError>
        {
            self.inner.writer(options, key).await
        }
        async fn list(
            &self,
            prefix: &str,
        ) -> Result<Vec<s3stream_object::ObjectInfo>, s3stream_object::ObjectError> {
            self.inner.list(prefix).await
        }
        async fn delete(
            &self,
            paths: &[s3stream_object::ObjectPath],
        ) -> Result<(), s3stream_object::ObjectError> {
            self.inner.delete(paths).await
        }
        fn bucket_id(&self) -> i16 {
            self.inner.bucket_id()
        }
    }

    #[tokio::test]
    async fn upload_on_stream_close_is_rate_limited() {
        let storage = Arc::new(CountingStorage {
            inner: MemoryObjectStorage::new(0),
            writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let cache =
            LocalStreamRangeIndexCache::init(3, storage.clone() as Arc<dyn ObjectStorage>).await;
        cache.append(&BTreeMap::from([(1u64, Some(range(0, 10, 42)))]));

        cache.upload_on_stream_close().await.unwrap();
        let after_first = storage.writes.load(std::sync::atomic::Ordering::Acquire);
        assert_eq!(after_first, 1);

        // Within the 5s window: no extra flush.
        cache.upload_on_stream_close().await.unwrap();
        cache.upload_on_stream_close().await.unwrap();
        assert_eq!(
            storage.writes.load(std::sync::atomic::Ordering::Acquire),
            after_first
        );
    }

    /// Only the first call prunes (and uploads). Repeats are no-ops.
    #[tokio::test]
    async fn async_prune_runs_once() {
        let storage = Arc::new(CountingStorage {
            inner: MemoryObjectStorage::new(0),
            writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let cache =
            LocalStreamRangeIndexCache::init(4, storage.clone() as Arc<dyn ObjectStorage>).await;
        cache.append(&BTreeMap::from([
            (1u64, Some(range(0, 10, 100))),
            (2u64, Some(range(0, 10, 200))),
        ]));

        // Object 200 is dead: pruned + persisted.
        cache.async_prune(&HashSet::from([100u64])).await.unwrap();
        assert_eq!(cache.search_object_id(1, 0), Some(100));
        assert_eq!(cache.search_object_id(2, 0), None);
        let writes = storage.writes.load(std::sync::atomic::Ordering::Acquire);
        assert_eq!(writes, 1);

        // Second call is a no-op even though object 100 would now be pruned.
        cache.async_prune(&HashSet::new()).await.unwrap();
        assert_eq!(cache.search_object_id(1, 0), Some(100));
        assert_eq!(
            storage.writes.load(std::sync::atomic::Ordering::Acquire),
            writes
        );
    }

    /// Size-bounded eviction keeps `1 + compact_num` per stream while possible.
    #[test]
    fn eviction_respects_max_size() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let cache = LocalStreamRangeIndexCache {
            node_id: 1,
            storage: storage as Arc<dyn ObjectStorage>,
            compact_num: 3,
            max_index_size: 10 * RangeIndex::OBJECT_SIZE,
            state: Mutex::new(CacheState {
                streams: BTreeMap::new(),
                total_size: 0,
            }),
            upload_lock: tokio::sync::Mutex::new(()),
            last_upload_time: Mutex::new(None),
            pruned: std::sync::atomic::AtomicBool::new(false),
        };
        // Stream 1: 12 appends -> must evict down to the cap.
        for i in 0..12u64 {
            let map = BTreeMap::from([(1u64, Some(range(i * 10, i * 10 + 10, i)))]);
            cache.append(&map);
        }
        assert!(cache.total_size() <= 10 * RangeIndex::OBJECT_SIZE);
        // First entry always survives (query anchor for old offsets).
        assert_eq!(cache.search_object_id(1, 5), Some(0));
    }
}
