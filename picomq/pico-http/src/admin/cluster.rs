use std::sync::atomic::Ordering;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use picomq_auth::Operation;
use picomq_metadata::PendingTransfer;
use picomq_server::{Log, Page, ServiceError, Topic, partition_log};
use serde_json::{Value, json};

use super::{AdminState, missing_topic};
use crate::common::{error_response, gate, json_body, service_error};
use crate::paging::Paging;
use crate::partition;

pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/admin/cluster", get(cluster))
        .route("/admin/transfers", get(transfers).post(transfer))
}

async fn cluster(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::ClusterRead,
        None,
    )
    .await
    {
        return *response;
    }
    let config = state.node.config();
    let view = state.node.views().load();
    let row = state.node.registration();
    let snapshot = &state.node.metadata().sink_stats().snapshot;
    let body = json!({
        "clusterId": config.cluster_id,
        "nodeId": config.node_id,
        "nodeEpoch": state.node.metadata().node_epoch(),
        "advertisedAddress": config.http_address,
        "registered": row.is_some(),
        "fenced": row.is_some_and(|row| row.fenced),
        "appliedIndex": view.applied_index,
        "streamCount": view.state.streams().len(),
        "objectCount": view.state.stream_set_objects().len() + view.state.stream_objects().len(),
        "streamSetObjectCount": view.state.stream_set_objects().len(),
        "streamObjectCount": view.state.stream_objects().len(),
        "preparedObjectCount": view.state.prepared().len(),
        // A head that does not advance while depth grows means a stuck cleaner.
        "gc": {
            "backlog": view.state.mark_destroyed().len(),
            "oldestSeq": view.state.mark_destroyed().keys().next().copied(),
            "nextSeq": view.state.next_destroyed_seq(),
        },
        "kvEntryCount": view.state.kv().len(),
        "kvBytes": view.state.kv_bytes(),
        "pendingTransferCount": view.state.pending_transfers().len(),
        "leaseHolder": state.lease_holder(),
        "snapshot": {
            "lastAppliedIndex": snapshot.last_applied_index.load(Ordering::Relaxed),
            "lastBytes": snapshot.last_bytes.load(Ordering::Relaxed),
            "lastDurationMs": snapshot.last_duration_ms.load(Ordering::Relaxed),
            "lastAtMs": snapshot.last_at_ms.load(Ordering::Relaxed),
            "taken": snapshot.taken.load(Ordering::Relaxed),
            "failed": snapshot.failed.load(Ordering::Relaxed),
        },
    });
    (StatusCode::OK, Json(body)).into_response()
}

/// Requests a live ownership move. Returns 202: the move completes
/// asynchronously via the transfer watcher on the owning node.
async fn transfer(State(state): State<AdminState>, headers: HeaderMap, body: Bytes) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::TransferStream,
        None,
    )
    .await
    {
        return *response;
    }
    let body = match json_body(&body) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let Some(name) = body.get("topic").and_then(Value::as_str) else {
        return error_response(StatusCode::BAD_REQUEST, "missing \"topic\"");
    };
    let Some(to_node) = body
        .get("toNode")
        .and_then(Value::as_i64)
        .and_then(|node| i32::try_from(node).ok())
    else {
        return error_response(StatusCode::BAD_REQUEST, "expected \"toNode\": node id");
    };
    let requested = match body.get("partition") {
        None => None,
        Some(value) => match value.as_u64().and_then(|index| u32::try_from(index).ok()) {
            Some(index) => Some(index),
            None => return error_response(StatusCode::BAD_REQUEST, "invalid \"partition\""),
        },
    };
    let topic = match state.node.topics().get(name).await {
        Ok(Some(topic)) => topic,
        Ok(None) => return missing_topic(name),
        Err(e) => return service_error(e),
    };
    let log = match partition_of(&topic, requested) {
        Ok(log) => log,
        Err(e) => return service_error(e),
    };
    let pending = match state.node.transfer(&log, to_node).await {
        Ok(pending) => pending,
        Err(e) => return service_error(e),
    };
    match log_json(&state, Some(&log)).await {
        Ok(log) => (StatusCode::ACCEPTED, Json(transfer_json(log, pending))).into_response(),
        Err(e) => service_error(e),
    }
}

async fn transfers(State(state): State<AdminState>, uri: Uri, headers: HeaderMap) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::ClusterRead,
        None,
    )
    .await
    {
        return *response;
    }
    let paging = match Paging::from_query(&uri, &[]) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let after = match paging.after_as::<u64>() {
        Ok(after) => after,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };

    let page = state.node.transfers(after, paging.limit);
    let mut items = Vec::with_capacity(page.items.len());
    for (stream_id, (log, pending)) in page.items {
        match log_json(&state, log.as_ref()).await {
            Ok(log) => items.push((stream_id, transfer_json(log, pending))),
            Err(e) => return service_error(e),
        }
    }
    paging.reply(
        Page {
            items,
            next: page.next,
        },
        |_, item| item.clone(),
    )
}

fn transfer_json(log: Value, pending: PendingTransfer) -> Value {
    json!({ "log": log, "fromNode": pending.from_node, "toNode": pending.to_node })
}

async fn log_json(state: &AdminState, log: Option<&Log>) -> Result<Value, ServiceError> {
    match log {
        Some(Log::Partition(partition)) => {
            let topic = state.node.topics().by_id(&partition.topic).await?;
            Ok(topic.map_or(
                Value::Null,
                |topic| json!({ "topic": topic.name.spelling(), "partition": partition.index }),
            ))
        }
        Some(Log::Group(group)) => Ok(json!({ "group": group })),
        None => Ok(Value::Null),
    }
}

fn partition_of(topic: &Topic, requested: Option<u32>) -> Result<Log, ServiceError> {
    let index = partition::choose(topic, requested)?;
    partition_log(topic, index)
}
