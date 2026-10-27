use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use axum::Json;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use picomq_auth::{AuthError, Operation};
use picomq_metadata::{MetadataState, StreamRow};
use picomq_server::{Partition, Topic, TopicId, partition_log};
use s3stream::StreamState;
use serde_json::{Value, json};

use super::{AdminState, allowed};
use crate::common::{auth_error, authenticate, error_response, read_json};

const MAX_PARTITIONS: usize = 1_000;
const MAX_BODY: usize = MAX_PARTITIONS * 512;

#[derive(Debug, Clone, PartialEq, Eq)]
struct OffsetsRequest {
    topic: String,
    index: u32,
    timestamp: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Offsets {
    pub(crate) start: u64,
    pub(crate) next: u64,
    pub(crate) at_timestamp: Option<u64>,
}

pub(crate) async fn owned_offsets(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
    request: Request,
) -> Response {
    if id != state.node.config().node_id {
        return state.forward_to(id, request).await;
    }

    let principal = match authenticate(state.authorizer.as_deref(), request.headers()).await {
        Ok(principal) => principal,
        Err(response) => return *response,
    };
    let body = match read_json(request, MAX_BODY).await {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let Some(requests) = requests(&body).filter(|requests| requests.len() <= MAX_PARTITIONS) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("expected up to {MAX_PARTITIONS} partitions as {{\"topic\", \"index\"}}"),
        );
    };

    let mut topics: BTreeMap<String, Option<Topic>> = BTreeMap::new();
    let mut owned = Vec::with_capacity(requests.len());
    for request in requests {
        if let Entry::Vacant(slot) = topics.entry(request.topic.clone()) {
            let topic = state.node.topics().get(&request.topic).await;
            slot.insert(topic.ok().flatten());
        }
        let Some(topic) = topics.get(&request.topic).and_then(Option::as_ref) else {
            continue;
        };
        if !allowed(principal.as_ref(), Operation::Head, topic) {
            return auth_error(&AuthError::Denied);
        }
        let Some(offsets) = local(&state, topic, request.index, request.timestamp).await else {
            continue;
        };

        owned.push(json!({
            "topicId": topic.id.hex(),
            "index": request.index,
            "startOffset": offsets.start,
            "nextOffset": offsets.next,
            "atOffset": offsets.at_timestamp,
        }));
    }

    (StatusCode::OK, Json(json!({ "partitions": owned }))).into_response()
}

pub(crate) fn row_of(
    state: &AdminState,
    view: &MetadataState,
    topic: &Topic,
    index: u32,
) -> Option<StreamRow> {
    let log = partition_log(topic, index).ok()?;
    let stream_id = state.node.service().stream_id(&log).ok()??;
    view.streams().get(&stream_id).copied()
}

pub(crate) async fn current(
    state: &AdminState,
    headers: &HeaderMap,
    view: &MetadataState,
    partitions: &[(&Topic, u32, Option<StreamRow>)],
    timestamp: Option<i64>,
) -> BTreeMap<Partition, (Option<Offsets>, bool)> {
    let local_node = state.node.config().node_id;

    let mut remote: BTreeMap<i32, Vec<OffsetsRequest>> = BTreeMap::new();
    for (topic, index, row) in partitions {
        let owner = row
            .and_then(|row| open_elsewhere(&row, local_node))
            .filter(|owner| !fenced(view, *owner));
        if let Some(owner) = owner {
            remote.entry(owner).or_default().push(OffsetsRequest {
                topic: topic.name.spelling().to_owned(),
                index: *index,
                timestamp,
            });
        }
    }
    let owned = from_owners(state, headers, remote).await;

    let mut known = BTreeMap::new();
    for (topic, index, row) in partitions {
        let partition = Partition::new(topic.id, *index);
        let offsets = match owned.get(&partition) {
            Some(offsets) => (Some(*offsets), true),
            None => {
                let elsewhere = row.and_then(|row| open_elsewhere(&row, local_node));
                let offsets = local(state, topic, *index, timestamp).await;
                (offsets, elsewhere.is_none())
            }
        };
        known.insert(partition, offsets);
    }
    known
}

pub(crate) fn fenced(view: &MetadataState, node: i32) -> bool {
    view.nodes().get(&node).is_some_and(|row| row.fenced)
}

async fn from_owners(
    state: &AdminState,
    headers: &HeaderMap,
    remote: BTreeMap<i32, Vec<OffsetsRequest>>,
) -> BTreeMap<Partition, Offsets> {
    let requests = remote.into_iter().map(|(owner, requests)| async move {
        let body = json!({
            "partitions": requests
                .iter()
                .map(|request| json!({
                    "topic": request.topic,
                    "index": request.index,
                    "timestamp": request.timestamp,
                }))
                .collect::<Vec<_>>(),
        });
        let path = format!("/admin/nodes/{owner}/offsets");
        let owned = state.post_json(owner, &path, headers, &body).await;
        owned.map_or_else(Vec::new, |owned| reported(&owned))
    });

    futures::future::join_all(requests)
        .await
        .into_iter()
        .flatten()
        .collect()
}

async fn local(
    state: &AdminState,
    topic: &Topic,
    index: u32,
    timestamp: Option<i64>,
) -> Option<Offsets> {
    let topics = state.node.topics();
    let offsets = topics.offsets(topic, index).ok()?;
    let next = offsets.next_offset.record_offset();

    let at_timestamp = match timestamp {
        Some(timestamp) => Some(
            topics
                .record_at_or_after(topic, index, timestamp)
                .await
                .ok()?
                .map_or(next, |record| record.offset.record_offset()),
        ),
        None => None,
    };
    Some(Offsets {
        start: offsets.start_offset.record_offset(),
        next,
        at_timestamp,
    })
}

fn open_elsewhere(row: &StreamRow, local: i32) -> Option<i32> {
    (row.state == StreamState::Opened && row.node_id != local).then_some(row.node_id)
}

fn requests(body: &Value) -> Option<Vec<OffsetsRequest>> {
    body["partitions"]
        .as_array()?
        .iter()
        .map(|entry| {
            Some(OffsetsRequest {
                topic: entry["topic"].as_str()?.to_owned(),
                index: u32::try_from(entry["index"].as_u64()?).ok()?,
                timestamp: entry["timestamp"].as_i64(),
            })
        })
        .collect()
}

fn reported(body: &Value) -> Vec<(Partition, Offsets)> {
    body["partitions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let topic = TopicId::from_hex(entry["topicId"].as_str()?)?;
            let index = u32::try_from(entry["index"].as_u64()?).ok()?;
            let offsets = Offsets {
                start: entry["startOffset"].as_u64()?,
                next: entry["nextOffset"].as_u64()?,
                at_timestamp: entry["atOffset"].as_u64(),
            };
            Some((Partition::new(topic, index), offsets))
        })
        .collect()
}
