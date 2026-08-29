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
const PRODUCER_ID: &str = "catalog";
const FETCH_LIMIT: u32 = 64;

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
}

type Fingerprint = (u64, bool, String);

pub fn catalog_event(
    command: &MetadataCommand,
    seen: &mut HashMap<String, Fingerprint>,
) -> Option<CatalogEvent> {
    match command {
        MetadataCommand::PutKvIfAbsent { key, value } => {
            if !is_registry_key(key) {
                return None;
            }
            let entry = RegistryEntry::decode(value).ok()?;
            let fp = (entry.stream_id, entry.closed, entry.content_type.clone());
            if seen.get(key) == Some(&fp) {
                return None;
            }
            seen.insert(key.clone(), fp);
            Some(CatalogEvent {
                op: "create",
                name: key.clone(),
                stream_id: entry.stream_id,
            })
        }
        MetadataCommand::PutKv { key, value } => {
            if !is_registry_key(key) {
                return None;
            }
            let entry = RegistryEntry::decode(value).ok()?;
            let fp = (entry.stream_id, entry.closed, entry.content_type.clone());
            if seen.get(key) == Some(&fp) {
                return None;
            }
            seen.insert(key.clone(), fp);
            Some(CatalogEvent {
                op: "update",
                name: key.clone(),
                stream_id: entry.stream_id,
            })
        }
        MetadataCommand::DeleteKv { key } | MetadataCommand::DeleteKvIfMatches { key, .. } => {
            if !is_registry_key(key) {
                return None;
            }
            let stream_id = seen.remove(key).map(|fp| fp.0).unwrap_or(0);
            Some(CatalogEvent {
                op: "delete",
                name: key.clone(),
                stream_id,
            })
        }
        _ => None,
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
    let mut seen = HashMap::new();
    let mut cursor = last_applied_idx(service).await?;
    source.set_flushable_idx(cursor);

    loop {
        views.wait_applied(cursor.saturating_add(1)).await;
        let rows = source
            .fetch_after(cursor, FETCH_LIMIT)
            .await
            .map_err(|e| e.to_string())?;
        if rows.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        for (idx, payload) in rows {
            if idx != cursor + 1 {
                tracing::error!(
                    expected = cursor + 1,
                    got = idx,
                    "catalog projector hit a log gap"
                );
                return Err("log gap".into());
            }
            let commands = codec::decode_batch(&payload).map_err(|e| e.to_string())?;
            let events: Vec<CatalogEvent> = commands
                .iter()
                .filter_map(|c| catalog_event(c, &mut seen))
                .collect();
            if !events.is_empty() {
                append_events(service, idx, &events).await?;
            }
            cursor = idx;
            source.set_flushable_idx(idx);
        }
    }
}

async fn ensure_catalog(service: &S3StreamService, views: &ViewPublisher) -> Result<(), String> {
    let command = CreateCommand {
        name: CATALOG_STREAM.into(),
        content_type: "application/json".into(),
        ttl_seconds: None,
        expires_at_ms: None,
        closed: false,
        initial_payload: Bytes::new(),
        external_id: None,
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

async fn last_applied_idx(service: &S3StreamService) -> Result<u64, String> {
    let Some(meta) = service
        .head(CATALOG_STREAM)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(0);
    };
    let end = meta.next_offset.record_offset();
    if end == 0 {
        return Ok(0);
    }
    let from = OffsetToken::of_record_offset(end - 1);
    let read = service
        .read(CATALOG_STREAM, from, 64 * 1024, 1)
        .await
        .map_err(|e| e.to_string())?;
    let Some(record) = read.records.last() else {
        return Ok(0);
    };
    Ok(applied_idx_of(&record.payload).unwrap_or(0))
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
    let seq = next_producer_seq(service).await?;
    service
        .append(AppendCommand {
            name: CATALOG_STREAM.into(),
            payloads,
            content_type: Some("application/json".into()),
            producer: Some(Producer::new(PRODUCER_ID, 0, seq).map_err(|e| e.to_string())?),
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
            "applied_idx": applied_idx,
        })
        .to_string(),
    )
}

fn applied_idx_of(payload: &[u8]) -> Option<u64> {
    let value: Value = serde_json::from_slice(payload).ok()?;
    value.get("applied_idx")?.as_u64()
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
        let mut seen = HashMap::new();
        let put = MetadataCommand::PutKv {
            key: "auth/token/x".into(),
            value: entry(1, false).encode(),
        };
        assert!(catalog_event(&put, &mut seen).is_none());
        let idx = MetadataCommand::PutKv {
            key: "idx/sid/1".into(),
            value: Bytes::from_static(b"/orders"),
        };
        assert!(catalog_event(&idx, &mut seen).is_none());
        let sys = MetadataCommand::PutKv {
            key: CATALOG_STREAM.into(),
            value: entry(9, false).encode(),
        };
        assert!(catalog_event(&sys, &mut seen).is_none());
        let create = MetadataCommand::CreateStream {
            node_id: 1,
            node_epoch: 1,
        };
        assert!(catalog_event(&create, &mut seen).is_none());
    }

    #[test]
    fn create_update_delete_and_producer_noise() {
        let mut seen = HashMap::new();
        let name = "/orders".to_owned();
        let created = catalog_event(
            &MetadataCommand::PutKvIfAbsent {
                key: name.clone(),
                value: entry(3, false).encode(),
            },
            &mut seen,
        )
        .unwrap();
        assert_eq!(created.op, "create");
        assert_eq!(created.stream_id, 3);

        let mut noisy = entry(3, false);
        noisy.producers.insert(
            "p".into(),
            ProducerState {
                epoch: 1,
                last_seq: 4,
                last_touched_ms: 1,
            },
        );
        assert!(catalog_event(
            &MetadataCommand::PutKv {
                key: name.clone(),
                value: noisy.encode(),
            },
            &mut seen,
        )
        .is_none());

        let closed = catalog_event(
            &MetadataCommand::PutKv {
                key: name.clone(),
                value: entry(3, true).encode(),
            },
            &mut seen,
        )
        .unwrap();
        assert_eq!(closed.op, "update");

        let deleted = catalog_event(&MetadataCommand::DeleteKv { key: name }, &mut seen).unwrap();
        assert_eq!(deleted.op, "delete");
        assert_eq!(deleted.stream_id, 3);
    }
}
