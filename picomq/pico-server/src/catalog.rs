//! Lease-holder projection of registry KV mutations onto `/_sys/catalog`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use pico_metadata::{codec, MetadataCommand, ViewPublisher};
use serde_json::{json, Value};

use crate::registry::RegistryEntry;
use crate::service::{is_reserved_name, S3StreamService};
use crate::types::{AppendCommand, CreateCommand, OffsetToken, Producer};

pub const CATALOG_STREAM: &str = "/_sys/catalog";
/// Fixed external id so protocol frontends can address the stream by UUID.
pub const CATALOG_EXTERNAL_ID: [u8; 16] = *b"picomq.catalog.0";
const PRODUCER_ID: &str = "catalog";
const FETCH_LIMIT: u32 = 64;
const CHECKPOINT_EVERY: u32 = 1024;
const BASELINE_CHUNK: usize = 512;

#[async_trait]
pub trait CatalogSource: Send + Sync {
    async fn fetch_after(&self, after: u64, limit: u32) -> Result<Vec<(u64, Vec<u8>)>, String>;
    fn set_flushable_idx(&self, idx: u64);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEvent {
    pub op: &'static str,
    pub name: String,
    pub stream_id: u64,
    pub content_type: String,
    pub closed: bool,
}

#[derive(Debug, Clone)]
struct ShadowEntry {
    stream_id: u64,
    closed: bool,
    content_type: String,
    /// Exact registry bytes, unknown for entries recovered from the fold.
    value: Option<Bytes>,
}

impl ShadowEntry {
    fn matches(&self, entry: &RegistryEntry) -> bool {
        self.stream_id == entry.stream_id
            && self.closed == entry.closed
            && self.content_type == entry.content_type
    }
}

/// The registry at the projector's cursor, replayed with apply semantics.
#[derive(Default)]
pub struct Shadow {
    entries: HashMap<String, ShadowEntry>,
}

pub fn replay(command: &MetadataCommand, shadow: &mut Shadow) -> Option<CatalogEvent> {
    match command {
        MetadataCommand::PutKvIfAbsent { key, value } => {
            if !is_registry_key(key) || shadow.entries.contains_key(key) {
                return None;
            }
            let entry = RegistryEntry::decode(value).ok()?;
            Some(shadow.put("create", key, &entry, value))
        }
        MetadataCommand::PutKv { key, value } => {
            if !is_registry_key(key) {
                return None;
            }
            let entry = RegistryEntry::decode(value).ok()?;
            let op = match shadow.entries.get_mut(key) {
                Some(prev) if prev.matches(&entry) => {
                    prev.value = Some(value.clone());
                    return None;
                }
                Some(_) => "update",
                None => "create",
            };
            Some(shadow.put(op, key, &entry, value))
        }
        MetadataCommand::DeleteKv { key } => shadow.delete(key),
        MetadataCommand::DeleteKvIfMatches { key, expected } => {
            let matched = match shadow.entries.get(key) {
                None => false,
                Some(prev) => match &prev.value {
                    Some(value) => value == expected,
                    None => RegistryEntry::decode(expected).is_ok_and(|entry| prev.matches(&entry)),
                },
            };
            if !matched {
                return None;
            }
            shadow.delete(key)
        }
        _ => None,
    }
}

impl Shadow {
    fn put(
        &mut self,
        op: &'static str,
        key: &str,
        entry: &RegistryEntry,
        value: &Bytes,
    ) -> CatalogEvent {
        self.entries.insert(
            key.to_owned(),
            ShadowEntry {
                stream_id: entry.stream_id,
                closed: entry.closed,
                content_type: entry.content_type.clone(),
                value: Some(value.clone()),
            },
        );
        CatalogEvent {
            op,
            name: key.to_owned(),
            stream_id: entry.stream_id,
            content_type: entry.content_type.clone(),
            closed: entry.closed,
        }
    }

    fn delete(&mut self, key: &str) -> Option<CatalogEvent> {
        if !is_registry_key(key) {
            return None;
        }
        let prev = self.entries.remove(key)?;
        Some(CatalogEvent {
            op: "delete",
            name: key.to_owned(),
            stream_id: prev.stream_id,
            content_type: prev.content_type,
            closed: prev.closed,
        })
    }

    fn apply_folded(&mut self, event: &Value) -> Option<()> {
        let op = event.get("op")?.as_str()?;
        let name = event.get("name")?.as_str()?;
        match op {
            "create" | "update" => {
                self.entries.insert(
                    name.to_owned(),
                    ShadowEntry {
                        stream_id: event.get("stream_id")?.as_u64()?,
                        closed: event.get("closed")?.as_bool()?,
                        content_type: event.get("content_type")?.as_str()?.to_owned(),
                        value: None,
                    },
                );
            }
            "delete" => {
                self.entries.remove(name);
            }
            _ => {}
        }
        Some(())
    }
}

fn is_registry_key(key: &str) -> bool {
    key.starts_with('/') && !is_reserved_name(key)
}

pub fn spawn_catalog_loop(
    service: Arc<S3StreamService>,
    views: Arc<ViewPublisher>,
    source: Arc<dyn CatalogSource>,
    mut leadership: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut running: Option<tokio::task::JoinHandle<()>> = None;
        loop {
            if *leadership.borrow_and_update() {
                if running.is_none() {
                    let service = service.clone();
                    let views = views.clone();
                    let source = source.clone();
                    running = Some(tokio::spawn(async move {
                        loop {
                            if let Err(error) =
                                run_as_leader(&service, &views, source.as_ref()).await
                            {
                                tracing::warn!(%error, "catalog projector failed");
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                        }
                    }));
                }
            } else if let Some(task) = running.take() {
                task.abort();
            }
            if leadership.changed().await.is_err() {
                if let Some(task) = running.take() {
                    task.abort();
                }
                return;
            }
        }
    })
}

async fn run_as_leader(
    service: &S3StreamService,
    views: &ViewPublisher,
    source: &dyn CatalogSource,
) -> Result<(), String> {
    ensure_catalog(service, views).await?;
    let mut state = fold(service).await?;
    source.set_flushable_idx(state.cursor);
    let mut idle = 0u32;

    loop {
        views.wait_applied(state.cursor.saturating_add(1)).await;
        let rows = source
            .fetch_after(state.cursor, FETCH_LIMIT)
            .await
            .map_err(|e| e.to_string())?;
        if rows.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        for (idx, payload) in rows {
            if idx != state.cursor + 1 {
                // Gap without any checkpoint: the log predates the projector.
                if state.checkpointed {
                    tracing::error!(
                        expected = state.cursor + 1,
                        got = idx,
                        "catalog projector hit a log gap"
                    );
                    return Err("log gap".into());
                }
                state = bootstrap(service, views).await?;
                source.set_flushable_idx(state.cursor);
                break;
            }
            let commands = codec::decode_batch(&payload).map_err(|e| e.to_string())?;
            let events: Vec<CatalogEvent> = commands
                .iter()
                .filter_map(|c| replay(c, &mut state.shadow))
                .collect();
            if !events.is_empty() {
                append_events(service, idx, &events).await?;
                source.set_flushable_idx(idx);
                idle = 0;
            } else {
                idle += 1;
                if idle >= CHECKPOINT_EVERY {
                    append_checkpoint(service, idx).await?;
                    state.checkpointed = true;
                    source.set_flushable_idx(idx);
                    idle = 0;
                }
            }
            state.cursor = idx;
        }
    }
}

struct Projection {
    shadow: Shadow,
    cursor: u64,
    checkpointed: bool,
}

/// Rebuilds the shadow at the cursor from the catalog's own records.
async fn fold(service: &S3StreamService) -> Result<Projection, String> {
    let mut state = Projection {
        shadow: Shadow::default(),
        cursor: 0,
        checkpointed: false,
    };
    let Some(meta) = service
        .head(CATALOG_STREAM)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(state);
    };
    let end = meta.next_offset.record_offset();
    let mut pos = 0u64;
    while pos < end {
        let read = service
            .read(
                CATALOG_STREAM,
                OffsetToken::of_record_offset(pos),
                4 * 1024 * 1024,
                4096,
            )
            .await
            .map_err(|e| e.to_string())?;
        if read.records.is_empty() {
            return Err("catalog fold stalled".into());
        }
        for record in &read.records {
            let event: Value =
                serde_json::from_slice(&record.payload).map_err(|e| e.to_string())?;
            let idx = event
                .get("applied_idx")
                .and_then(Value::as_u64)
                .ok_or("catalog record without applied_idx")?;
            state.cursor = state.cursor.max(idx);
            if event.get("op").and_then(Value::as_str) == Some("checkpoint") {
                state.checkpointed = true;
            } else {
                state.shadow.apply_folded(&event);
            }
        }
        pos = read.next_offset.record_offset();
    }
    Ok(state)
}

/// Baseline for a log that predates the projector, completed by a checkpoint.
async fn bootstrap(service: &S3StreamService, views: &ViewPublisher) -> Result<Projection, String> {
    let view = views.load();
    let cursor = view.applied_index;
    let mut shadow = Shadow::default();
    let mut events = Vec::new();
    for (key, value) in view.state.kv.iter() {
        if !is_registry_key(key) {
            continue;
        }
        let Ok(entry) = RegistryEntry::decode(value) else {
            continue;
        };
        events.push(shadow.put("create", key, &entry, value));
    }
    for chunk in events.chunks(BASELINE_CHUNK) {
        append_events(service, cursor, chunk).await?;
    }
    append_checkpoint(service, cursor).await?;
    Ok(Projection {
        shadow,
        cursor,
        checkpointed: true,
    })
}

async fn ensure_catalog(service: &S3StreamService, views: &ViewPublisher) -> Result<(), String> {
    let command = CreateCommand {
        name: CATALOG_STREAM.into(),
        content_type: "application/json".into(),
        ttl_seconds: None,
        expires_at_ms: None,
        closed: false,
        initial_payload: Bytes::new(),
        external_id: Some(CATALOG_EXTERNAL_ID),
        internal: true,
    };
    service.create(command).await.map_err(|e| e.to_string())?;
    let node_id = service.node_id();
    loop {
        let view = views.load();
        let Some(entry) = view.state.get_kv(CATALOG_STREAM) else {
            views
                .wait_applied(view.applied_index.saturating_add(1))
                .await;
            continue;
        };
        let entry = RegistryEntry::decode(&entry).map_err(|e| e.to_string())?;
        let Some(row) = view.state.get_stream(entry.stream_id) else {
            views
                .wait_applied(view.applied_index.saturating_add(1))
                .await;
            continue;
        };
        if row.node_id == node_id {
            return Ok(());
        }
        service
            .request_catalog_transfer(entry.stream_id, row.node_id, node_id)
            .await?;
        views
            .wait_applied(view.applied_index.saturating_add(1))
            .await;
    }
}

async fn append_events(
    service: &S3StreamService,
    applied_idx: u64,
    events: &[CatalogEvent],
) -> Result<(), String> {
    let payloads = events
        .iter()
        .map(|e| encode_event(e, applied_idx))
        .collect();
    append_payloads(service, payloads).await
}

async fn append_checkpoint(service: &S3StreamService, applied_idx: u64) -> Result<(), String> {
    let payload = Bytes::from(json!({"op": "checkpoint", "applied_idx": applied_idx}).to_string());
    append_payloads(service, vec![payload]).await
}

async fn append_payloads(service: &S3StreamService, payloads: Vec<Bytes>) -> Result<(), String> {
    let seq = next_producer_seq(service).await?;
    service
        .append(AppendCommand {
            name: CATALOG_STREAM.into(),
            payloads,
            content_type: Some("application/json".into()),
            producer: Some(Producer::new(PRODUCER_ID, 0, seq).map_err(|e| e.to_string())?),
            atomic: true,
            internal: true,
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn next_producer_seq(service: &S3StreamService) -> Result<u64, String> {
    let Some(entry) = service
        .get_entry(CATALOG_STREAM, false)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(0);
    };
    Ok(entry
        .producers
        .get(PRODUCER_ID)
        .map(|s| s.last_seq + 1)
        .unwrap_or(0))
}

fn encode_event(event: &CatalogEvent, applied_idx: u64) -> Bytes {
    Bytes::from(
        json!({
            "op": event.op,
            "name": event.name,
            "stream_id": event.stream_id,
            "content_type": event.content_type,
            "closed": event.closed,
            "applied_idx": applied_idx,
        })
        .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{ProducerState, RegistryEntry};
    use std::collections::BTreeMap;

    fn entry(stream_id: u64, closed: bool) -> RegistryEntry {
        RegistryEntry {
            stream_id,
            content_type: "text/plain".into(),
            ttl_seconds: None,
            expires_at_ms: None,
            closed,
            deadline_ms: 0,
            last_seq: None,
            producers: BTreeMap::new(),
            closed_by: None,
            external_id: [0; 16],
            numeric_producers: BTreeMap::new(),
            producer_state_offset: 0,
        }
    }

    #[test]
    fn filters_non_registry_keys() {
        let mut shadow = Shadow::default();
        let put = MetadataCommand::PutKv {
            key: "auth/token/x".into(),
            value: entry(1, false).encode(),
        };
        assert!(replay(&put, &mut shadow).is_none());
        let idx = MetadataCommand::PutKv {
            key: "idx/sid/1".into(),
            value: Bytes::from_static(b"/orders"),
        };
        assert!(replay(&idx, &mut shadow).is_none());
        let sys = MetadataCommand::PutKv {
            key: CATALOG_STREAM.into(),
            value: entry(9, false).encode(),
        };
        assert!(replay(&sys, &mut shadow).is_none());
        let create = MetadataCommand::CreateStream {
            node_id: 1,
            node_epoch: 1,
        };
        assert!(replay(&create, &mut shadow).is_none());
    }

    #[test]
    fn create_update_delete_and_producer_noise() {
        let mut shadow = Shadow::default();
        let name = "/orders".to_owned();
        let created = replay(
            &MetadataCommand::PutKvIfAbsent {
                key: name.clone(),
                value: entry(3, false).encode(),
            },
            &mut shadow,
        )
        .unwrap();
        assert_eq!(created.op, "create");
        assert_eq!(created.stream_id, 3);
        assert_eq!(created.content_type, "text/plain");
        assert!(!created.closed);

        let mut noisy = entry(3, false);
        noisy.producers.insert(
            "p".into(),
            ProducerState {
                epoch: 1,
                last_seq: 4,
                last_touched_ms: 1,
            },
        );
        assert!(replay(
            &MetadataCommand::PutKv {
                key: name.clone(),
                value: noisy.encode(),
            },
            &mut shadow,
        )
        .is_none());

        let closed = replay(
            &MetadataCommand::PutKv {
                key: name.clone(),
                value: entry(3, true).encode(),
            },
            &mut shadow,
        )
        .unwrap();
        assert_eq!(closed.op, "update");
        assert!(closed.closed);

        let deleted = replay(&MetadataCommand::DeleteKv { key: name }, &mut shadow).unwrap();
        assert_eq!(deleted.op, "delete");
        assert_eq!(deleted.stream_id, 3);
    }

    #[test]
    fn conditional_outcomes() {
        let mut shadow = Shadow::default();
        let name = "/orders".to_owned();
        let winner = entry(3, false).encode();
        assert!(replay(
            &MetadataCommand::PutKvIfAbsent {
                key: name.clone(),
                value: winner.clone(),
            },
            &mut shadow,
        )
        .is_some());
        assert!(replay(
            &MetadataCommand::PutKvIfAbsent {
                key: name.clone(),
                value: entry(7, false).encode(),
            },
            &mut shadow,
        )
        .is_none());
        assert!(replay(
            &MetadataCommand::DeleteKvIfMatches {
                key: name.clone(),
                expected: entry(7, false).encode(),
            },
            &mut shadow,
        )
        .is_none());
        assert!(replay(
            &MetadataCommand::DeleteKv {
                key: "/missing".into(),
            },
            &mut shadow,
        )
        .is_none());
        let deleted = replay(
            &MetadataCommand::DeleteKvIfMatches {
                key: name,
                expected: winner,
            },
            &mut shadow,
        )
        .unwrap();
        assert_eq!(deleted.stream_id, 3);
    }

    #[test]
    fn fold_rebuilds_shadow() {
        let mut shadow = Shadow::default();
        let folded = [
            json!({"op": "create", "name": "/a", "stream_id": 1, "content_type": "text/plain", "closed": false, "applied_idx": 5}),
            json!({"op": "delete", "name": "/a", "stream_id": 1, "content_type": "text/plain", "closed": false, "applied_idx": 6}),
            json!({"op": "create", "name": "/a", "stream_id": 2, "content_type": "text/plain", "closed": false, "applied_idx": 7}),
            json!({"op": "checkpoint", "applied_idx": 9}),
        ];
        for event in &folded {
            shadow.apply_folded(event);
        }
        let deleted = replay(&MetadataCommand::DeleteKv { key: "/a".into() }, &mut shadow).unwrap();
        assert_eq!(deleted.stream_id, 2);
        let mut shadow = Shadow::default();
        shadow.apply_folded(
            &json!({"op": "create", "name": "/b", "stream_id": 4, "content_type": "text/plain", "closed": false, "applied_idx": 1}),
        );
        assert!(replay(
            &MetadataCommand::DeleteKvIfMatches {
                key: "/b".into(),
                expected: entry(9, false).encode(),
            },
            &mut shadow,
        )
        .is_none());
        assert!(replay(
            &MetadataCommand::DeleteKvIfMatches {
                key: "/b".into(),
                expected: entry(4, false).encode(),
            },
            &mut shadow,
        )
        .is_some());
    }
}
