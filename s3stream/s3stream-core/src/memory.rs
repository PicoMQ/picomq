//! In-memory metadata plane for tests and single-process emulation.
//!
//! Implements `ObjectManager` + `StreamManager` with plain maps: object ids
//! from a counter, prepare/commit tracked in memory, stream epochs enforced
//! exactly like the real control plane (open with stale epoch -> fenced).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::api::StreamError;
use crate::manager::{
    CommitStreamSetObjectRequest, CommitStreamSetObjectResponse, CompactStreamObjectRequest,
    ObjectManager, StreamManager, StreamMetadata, StreamMetadataListener,
    StreamMetadataListenerHandle, StreamState,
};
use s3stream_object::{
    NOOP_OBJECT_ID, ObjectAttributes, S3ObjectMetadata, S3ObjectType, StreamOffsetRange,
};

#[derive(Default)]
struct MetadataState {
    /// Committed stream set objects (object id -> metadata with per-stream ranges).
    stream_set_objects: BTreeMap<u64, S3ObjectMetadata>,
    /// Committed stream objects, per stream, ordered by start offset.
    stream_objects: HashMap<u64, BTreeMap<u64, S3ObjectMetadata>>,
    streams: BTreeMap<u64, StreamMetadata>,
    next_stream_id: u64,
    /// Per-stream metadata listeners.
    ///
    /// `MemoryMetadataManager` throws. We register so SNAPSHOT_READ tests can
    /// observe `onNewStreamMetadata`). Protocol: `Handle#close` removes the entry.
    listeners: HashMap<u64, Vec<RegisteredListener>>,
    next_listener_id: u64,
}

/// A registered metadata listener: (registration id, listener). The id backs
/// handle-close removal.
type RegisteredListener = (u64, Arc<dyn StreamMetadataListener>);

pub struct MemoryMetadataManager {
    object_id_alloc: AtomicU64,
    state: Arc<Mutex<MetadataState>>,
}

impl Default for MemoryMetadataManager {
    fn default() -> Self {
        Self {
            object_id_alloc: AtomicU64::new(1),
            state: Arc::new(Mutex::new(MetadataState::default())),
        }
    }
}

impl MemoryMetadataManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Set `StreamMetadata.nodeId` for tests that assert `RequestCommitEvent`.
    /// Create/open assigns no server id, so tests assign the owner node the
    /// snapshot-read cache would see from the metadata plane.
    pub fn set_node_id(&self, stream_id: u64, node_id: i32) {
        let mut state = self.state.lock().expect("metadata poisoned");
        if let Some(stream) = state.streams.get_mut(&stream_id) {
            stream.node_id = node_id;
        }
    }
}

struct MemoryListenerHandle {
    state: std::sync::Weak<Mutex<MetadataState>>,
    stream_id: u64,
    id: u64,
}

impl StreamMetadataListenerHandle for MemoryListenerHandle {
    fn close(&self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut state = state.lock().expect("metadata poisoned");
        if let Some(list) = state.listeners.get_mut(&self.stream_id) {
            list.retain(|(id, _)| *id != self.id);
        }
    }
}

/// In-memory `KVClient` for tests and single-process emulation.
///
/// ConcurrentHashMap. Split into its own type here so hosts can compose freely.
#[derive(Default)]
pub struct MemoryKvClient {
    map: Mutex<HashMap<String, bytes::Bytes>>,
}

impl MemoryKvClient {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl crate::api::KVClient for MemoryKvClient {
    async fn put_kv_if_absent(
        &self,
        kv: crate::api::KeyValue,
    ) -> Result<bytes::Bytes, StreamError> {
        let mut map = self.map.lock().expect("kv poisoned");
        Ok(map.entry(kv.key).or_insert(kv.value).clone())
    }

    async fn put_kv(&self, kv: crate::api::KeyValue) -> Result<bytes::Bytes, StreamError> {
        let mut map = self.map.lock().expect("kv poisoned");
        map.insert(kv.key, kv.value.clone());
        Ok(kv.value)
    }

    async fn get_kv(&self, key: &str) -> Result<Option<bytes::Bytes>, StreamError> {
        Ok(self.map.lock().expect("kv poisoned").get(key).cloned())
    }

    async fn del_kv(&self, key: &str) -> Result<Option<bytes::Bytes>, StreamError> {
        Ok(self.map.lock().expect("kv poisoned").remove(key))
    }

    async fn del_kv_if(
        &self,
        key: &str,
        expected: &bytes::Bytes,
    ) -> Result<Option<bytes::Bytes>, StreamError> {
        let mut map = self.map.lock().expect("kv poisoned");
        match map.get(key) {
            Some(current) if current == expected => Ok(map.remove(key)),
            _ => Ok(None),
        }
    }

    async fn list_kv(&self, prefix: &str) -> Result<Vec<crate::api::KeyValue>, StreamError> {
        let map = self.map.lock().expect("kv poisoned");
        let mut entries: Vec<crate::api::KeyValue> = map
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| crate::api::KeyValue {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(entries)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[async_trait]
impl ObjectManager for MemoryMetadataManager {
    async fn prepare_object(&self, count: usize, _ttl_ms: u64) -> Result<u64, StreamError> {
        Ok(self
            .object_id_alloc
            .fetch_add(count as u64, Ordering::SeqCst))
    }

    /// Atomic commit: registers the stream set object and its split stream objects
    /// together, and advances each affected stream's committed end offset.
    async fn commit_stream_set_object(
        &self,
        request: CommitStreamSetObjectRequest,
    ) -> Result<CommitStreamSetObjectResponse, StreamError> {
        let mut state = self.state.lock().expect("metadata poisoned");
        if request.object_id != NOOP_OBJECT_ID {
            let metadata = S3ObjectMetadata {
                object_id: request.object_id,
                object_type: S3ObjectType::StreamSet,
                offset_ranges: request
                    .stream_ranges
                    .iter()
                    .map(|r| StreamOffsetRange {
                        stream_id: r.stream_id,
                        start_offset: r.start_offset,
                        end_offset: r.end_offset,
                    })
                    .collect(),
                object_size: request.object_size,
                attributes: ObjectAttributes(request.attributes),
                committed_timestamp_ms: now_ms(),
                data_timestamp_ms: now_ms(),
            };
            state.stream_set_objects.insert(request.object_id, metadata);
        }
        for so in &request.stream_objects {
            let metadata = S3ObjectMetadata {
                object_id: so.object_id,
                object_type: S3ObjectType::Stream,
                offset_ranges: vec![StreamOffsetRange {
                    stream_id: so.stream_id,
                    start_offset: so.start_offset,
                    end_offset: so.end_offset,
                }],
                object_size: so.object_size,
                attributes: ObjectAttributes(so.attributes),
                committed_timestamp_ms: now_ms(),
                data_timestamp_ms: now_ms(),
            };
            state
                .stream_objects
                .entry(so.stream_id)
                .or_default()
                .insert(so.start_offset, metadata);
        }
        for id in &request.compacted_object_ids {
            state.stream_set_objects.remove(id);
        }
        // Advance committed end offsets (the control plane's stream view).
        let mut advances: Vec<(u64, u64)> = request
            .stream_ranges
            .iter()
            .map(|r| (r.stream_id, r.end_offset))
            .collect();
        advances.extend(
            request
                .stream_objects
                .iter()
                .map(|s| (s.stream_id, s.end_offset)),
        );
        // Notify metadata listeners after endOffset advances (SNAPSHOT_READ streams
        // → confirm/start follow the owner).
        // controllers push `onNewStreamMetadata` on commit. Snapshot out of the lock
        // so a listener `get_streams` cannot deadlock. Protocol (listener sees the
        // new endOffset) is unchanged.
        let mut pending: Vec<(Vec<Arc<dyn StreamMetadataListener>>, StreamMetadata)> = Vec::new();
        for (stream_id, end_offset) in advances {
            let listeners = state
                .listeners
                .get(&stream_id)
                .map(|list| list.iter().map(|(_, l)| Arc::clone(l)).collect())
                .unwrap_or_default();
            if let Some(stream) = state.streams.get_mut(&stream_id) {
                stream.end_offset = stream.end_offset.max(end_offset);
                pending.push((listeners, stream.clone()));
            }
        }
        drop(state);
        for (listeners, metadata) in pending {
            for listener in listeners {
                listener.on_new_stream_metadata(metadata.clone());
            }
        }
        Ok(CommitStreamSetObjectResponse {})
    }

    async fn compact_stream_object(
        &self,
        request: CompactStreamObjectRequest,
    ) -> Result<(), StreamError> {
        let mut state = self.state.lock().expect("metadata poisoned");
        let per_stream = state.stream_objects.entry(request.stream_id).or_default();
        per_stream.retain(|_, m| !request.source_object_ids.contains(&m.object_id));
        // NOOP_OBJECT_ID means pure cleanup: sources removed, nothing added.
        if request.object_id != NOOP_OBJECT_ID {
            let metadata = S3ObjectMetadata {
                object_id: request.object_id,
                object_type: S3ObjectType::Stream,
                offset_ranges: vec![StreamOffsetRange {
                    stream_id: request.stream_id,
                    start_offset: request.start_offset,
                    end_offset: request.end_offset,
                }],
                object_size: request.object_size,
                attributes: ObjectAttributes(request.attributes),
                committed_timestamp_ms: now_ms(),
                data_timestamp_ms: now_ms(),
            };
            per_stream.insert(request.start_offset, metadata);
        }
        Ok(())
    }

    /// Logical slices covering `[start_offset, end_offset)`, continuous, in order.
    async fn get_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        let state = self.state.lock().expect("metadata poisoned");
        let mut slices: Vec<(u64, S3ObjectMetadata)> = Vec::new();
        for object in state.stream_set_objects.values() {
            for range in &object.offset_ranges {
                if range.stream_id == stream_id
                    && range.start_offset < end_offset
                    && range.end_offset > start_offset
                {
                    slices.push((range.start_offset, object.clone()));
                }
            }
        }
        if let Some(per_stream) = state.stream_objects.get(&stream_id) {
            for object in per_stream.values() {
                let range = &object.offset_ranges[0];
                if range.start_offset < end_offset && range.end_offset > start_offset {
                    slices.push((range.start_offset, object.clone()));
                }
            }
        }
        slices.sort_by_key(|(start, m)| (*start, m.object_id));
        Ok(slices
            .into_iter()
            .map(|(_, m)| m)
            .take(limit.max(1))
            .collect())
    }

    async fn get_server_objects(&self) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        let state = self.state.lock().expect("metadata poisoned");
        Ok(state.stream_set_objects.values().cloned().collect())
    }

    async fn get_stream_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        let state = self.state.lock().expect("metadata poisoned");
        Ok(state
            .stream_objects
            .get(&stream_id)
            .map(|per_stream| {
                per_stream
                    .values()
                    .filter(|m| {
                        let r = &m.offset_ranges[0];
                        r.start_offset < end_offset && r.end_offset > start_offset
                    })
                    .take(limit.max(1))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn is_object_exist(&self, object_id: u64) -> Result<bool, StreamError> {
        let state = self.state.lock().expect("metadata poisoned");
        Ok(state.stream_set_objects.contains_key(&object_id)
            || state
                .stream_objects
                .values()
                .any(|per_stream| per_stream.values().any(|m| m.object_id == object_id)))
    }
}

#[async_trait]
impl StreamManager for MemoryMetadataManager {
    async fn get_opening_streams(&self) -> Result<Vec<StreamMetadata>, StreamError> {
        let state = self.state.lock().expect("metadata poisoned");
        Ok(state
            .streams
            .values()
            .filter(|s| s.state == StreamState::Opened)
            .cloned()
            .collect())
    }

    async fn get_streams(&self, stream_ids: &[u64]) -> Result<Vec<StreamMetadata>, StreamError> {
        let state = self.state.lock().expect("metadata poisoned");
        Ok(stream_ids
            .iter()
            .filter_map(|id| state.streams.get(id).cloned())
            .collect())
    }

    async fn create_stream(&self, _tags: HashMap<String, String>) -> Result<u64, StreamError> {
        let mut state = self.state.lock().expect("metadata poisoned");
        let stream_id = state.next_stream_id;
        state.next_stream_id += 1;
        state.streams.insert(
            stream_id,
            StreamMetadata {
                stream_id,
                epoch: 0,
                start_offset: 0,
                end_offset: 0,
                state: StreamState::Closed,
                node_id: -1,
            },
        );
        Ok(stream_id)
    }

    async fn open_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        _tags: HashMap<String, String>,
    ) -> Result<StreamMetadata, StreamError> {
        let mut state = self.state.lock().expect("metadata poisoned");
        let stream = state
            .streams
            .get_mut(&stream_id)
            .ok_or(StreamError::NotExist { stream_id })?;
        if stream.epoch > epoch || (stream.epoch == epoch && stream.state == StreamState::Opened) {
            return Err(StreamError::Fenced { stream_id, epoch });
        }
        stream.epoch = epoch;
        stream.state = StreamState::Opened;
        Ok(stream.clone())
    }

    async fn trim_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        new_start_offset: u64,
    ) -> Result<(), StreamError> {
        let mut state = self.state.lock().expect("metadata poisoned");
        let stream = state
            .streams
            .get_mut(&stream_id)
            .ok_or(StreamError::NotExist { stream_id })?;
        if stream.epoch != epoch {
            return Err(StreamError::Fenced { stream_id, epoch });
        }
        stream.start_offset = stream.start_offset.max(new_start_offset);
        let metadata = stream.clone();
        let listeners = state
            .listeners
            .get(&stream_id)
            .map(|list| list.iter().map(|(_, l)| Arc::clone(l)).collect::<Vec<_>>())
            .unwrap_or_default();
        drop(state);
        for listener in listeners {
            listener.on_new_stream_metadata(metadata.clone());
        }
        Ok(())
    }

    async fn close_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        let mut state = self.state.lock().expect("metadata poisoned");
        let stream = state
            .streams
            .get_mut(&stream_id)
            .ok_or(StreamError::NotExist { stream_id })?;
        if stream.epoch != epoch {
            return Err(StreamError::Fenced { stream_id, epoch });
        }
        stream.state = StreamState::Closed;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        let mut state = self.state.lock().expect("metadata poisoned");
        match state.streams.get(&stream_id) {
            Some(stream) if stream.epoch == epoch => {
                state.streams.remove(&stream_id);
                state.stream_objects.remove(&stream_id);
                Ok(())
            }
            Some(_) => Err(StreamError::Fenced { stream_id, epoch }),
            None => Err(StreamError::NotExist { stream_id }),
        }
    }

    fn add_metadata_listener(
        &self,
        stream_id: u64,
        listener: Arc<dyn StreamMetadataListener>,
    ) -> Arc<dyn StreamMetadataListenerHandle> {
        let mut state = self.state.lock().expect("metadata poisoned");
        let id = state.next_listener_id;
        state.next_listener_id += 1;
        state
            .listeners
            .entry(stream_id)
            .or_default()
            .push((id, listener));
        Arc::new(MemoryListenerHandle {
            state: Arc::downgrade(&self.state),
            stream_id,
            id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Epoch fencing parity with the real control plane: opening with a stale epoch
    /// fails. A newer epoch's open succeeds.
    #[tokio::test]
    async fn open_stream_enforces_epoch_fencing() {
        let manager = MemoryMetadataManager::new();
        let stream_id = manager.create_stream(HashMap::new()).await.unwrap();
        manager
            .open_stream(stream_id, 1, HashMap::new())
            .await
            .unwrap();
        // Same epoch while opened: fenced. Lower epoch: fenced.
        assert!(
            manager
                .open_stream(stream_id, 1, HashMap::new())
                .await
                .is_err()
        );
        assert!(
            manager
                .open_stream(stream_id, 0, HashMap::new())
                .await
                .is_err()
        );
        // Newer epoch wins.
        manager
            .open_stream(stream_id, 2, HashMap::new())
            .await
            .unwrap();
        // Close then reopen at same epoch is allowed (failback path).
        manager.close_stream(stream_id, 2).await.unwrap();
        manager
            .open_stream(stream_id, 2, HashMap::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_objects_ordered_and_filtered() {
        let manager = MemoryMetadataManager::new();
        let stream_id = manager.create_stream(HashMap::new()).await.unwrap();
        manager
            .open_stream(stream_id, 1, HashMap::new())
            .await
            .unwrap();

        let mut request = CommitStreamSetObjectRequest {
            object_id: 100,
            object_size: 10,
            ..Default::default()
        };
        request
            .stream_ranges
            .push(s3stream_object::ObjectStreamRange {
                stream_id,
                epoch: u64::MAX,
                start_offset: 10,
                end_offset: 20,
                size: 10,
            });
        request.stream_objects.push(crate::manager::StreamObject {
            object_id: 101,
            object_size: 5,
            stream_id,
            start_offset: 0,
            end_offset: 10,
            attributes: 0,
        });
        manager.commit_stream_set_object(request).await.unwrap();

        let objects = manager.get_objects(stream_id, 0, 20, 10).await.unwrap();
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].object_id, 101); // starts at 0
        assert_eq!(objects[1].object_id, 100); // starts at 10
        // Committed end offset advanced.
        let stream = &manager.get_streams(&[stream_id]).await.unwrap()[0];
        assert_eq!(stream.end_offset, 20);
    }

    #[tokio::test]
    async fn list_kv_prefix_ordered() {
        use crate::api::{KVClient, KeyValue};
        let kv = MemoryKvClient::new();
        for key in ["s2s:b/2", "s2s:a/1", "other:x", "s2s:a/0"] {
            kv.put_kv(KeyValue {
                key: key.into(),
                value: bytes::Bytes::from_static(b"v"),
            })
            .await
            .unwrap();
        }

        let listed = kv.list_kv("s2s:").await.unwrap();
        let keys: Vec<&str> = listed.iter().map(|kv| kv.key.as_str()).collect();
        assert_eq!(keys, vec!["s2s:a/0", "s2s:a/1", "s2s:b/2"]);

        assert!(kv.list_kv("missing:").await.unwrap().is_empty());
    }
}
