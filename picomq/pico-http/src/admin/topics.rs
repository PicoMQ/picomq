use std::collections::BTreeMap;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use picomq_auth::{AuthError, Operation};
use picomq_metadata::{MetadataState, StreamRow};
use picomq_protocol::config::ConfigUpdate;
use picomq_server::{CreateCommand, Page, Partition, Retention, Topic};
use s3stream::StreamState;
use serde_json::{Value, json};

use super::offsets::{self, Offsets};
use super::{AdminState, allowed, authorized_topic, missing_partition, missing_topic};
use crate::common::streams::update_command;
use crate::common::{
    auth_error, authenticate, error_response, json_body, read_json, service_error,
};
use crate::paging::Paging;

const TRIM_TIMEOUT: Duration = Duration::from_secs(10);
const TRIM_BODY: usize = 4096;

pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/admin/topics", get(list).post(create))
        .route(
            "/admin/topics/{name}",
            get(detail).patch(update).delete(delete_topic),
        )
        .route("/admin/topics/{name}/partitions", get(partitions))
        .route("/admin/topics/{name}/partitions/{index}", get(partition))
        .route("/admin/topics/{name}/partitions/{index}/trim", post(trim))
}

async fn list(State(state): State<AdminState>, uri: Uri, headers: HeaderMap) -> Response {
    let principal = match authenticate(state.authorizer.as_deref(), &headers).await {
        Ok(principal) => principal,
        Err(response) => return *response,
    };
    let paging = match Paging::from_query(&uri, &["prefix"]) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };

    let page = state.node.topics().list(
        paging.filter("prefix").unwrap_or("/"),
        paging.after.as_deref(),
        paging.limit,
        |topic| allowed(principal.as_ref(), Operation::List, topic),
    );

    paging.reply(page, |_, topic| topic_json(topic))
}

async fn detail(
    State(state): State<AdminState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    let topic = match authorized_topic(&state, &headers, Operation::Head, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };

    let view = state.node.views().load();
    let mut stored_bytes = 0;
    let mut owners: BTreeMap<i32, u32> = BTreeMap::new();
    for index in 0..topic.partitions {
        let Some(row) = offsets::row_of(&state, &view.state, &topic, index) else {
            continue;
        };
        stored_bytes += row.stored_bytes;
        *owners.entry(row.node_id).or_default() += 1;
    }

    let mut body = topic_json(&topic);
    body["storedBytes"] = json!(stored_bytes);
    body["owners"] = owners
        .into_iter()
        .map(|(node, partitions)| json!({ "nodeId": owner_id(node), "partitions": partitions }))
        .collect();
    (StatusCode::OK, Json(body)).into_response()
}

async fn partitions(
    State(state): State<AdminState>,
    Path(name): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let topic = match authorized_topic(&state, &headers, Operation::Head, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };
    let paging = match Paging::from_query(&uri, &["node"]) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let Ok(node) = paging.filter("node").map(str::parse::<i32>).transpose() else {
        return error_response(StatusCode::BAD_REQUEST, "node must be a node id");
    };
    let Ok(after) = paging.after.as_deref().map(str::parse::<u32>).transpose() else {
        return error_response(StatusCode::BAD_REQUEST, "the cursor is not valid");
    };

    let view = state.node.views().load();
    let start = after.map_or(0, |after| after.saturating_add(1));
    let entries = (start..topic.partitions)
        .map(|index| (index, offsets::row_of(&state, &view.state, &topic, index)));
    let page = Page::scan(entries, paging.limit, |_, row| {
        node.is_none_or(|node| row.is_some_and(|row| row.node_id == node))
    });

    let rows: Vec<_> = page
        .items
        .iter()
        .map(|(index, row)| (&topic, *index, *row))
        .collect();
    let known = offsets::current(&state, &headers, &view.state, &rows, None).await;

    paging.reply(page, |index, row| {
        let (offsets, current) = known
            .get(&Partition::new(topic.id, *index))
            .copied()
            .unwrap_or((None, false));
        partition_json(*index, row.as_ref(), &view.state, offsets, current)
    })
}

async fn partition(
    State(state): State<AdminState>,
    Path((name, index)): Path<(String, u32)>,
    headers: HeaderMap,
) -> Response {
    let topic = match authorized_topic(&state, &headers, Operation::Head, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };
    if index >= topic.partitions {
        return missing_partition(index);
    }

    let view = state.node.views().load();
    let row = offsets::row_of(&state, &view.state, &topic, index);
    let known =
        offsets::current(&state, &headers, &view.state, &[(&topic, index, row)], None).await;

    let (offsets, current) = known
        .get(&Partition::new(topic.id, index))
        .copied()
        .unwrap_or((None, false));
    let body = partition_json(index, row.as_ref(), &view.state, offsets, current);
    (StatusCode::OK, Json(body)).into_response()
}

async fn create(State(state): State<AdminState>, headers: HeaderMap, body: Bytes) -> Response {
    let principal = match authenticate(state.authorizer.as_deref(), &headers).await {
        Ok(principal) => principal,
        Err(response) => return *response,
    };
    let body = match json_body(&body) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let command = match create_command(&body) {
        Ok(command) => command,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    if !principal.as_ref().is_none_or(|principal| {
        principal
            .scope
            .allows(Operation::Create, Some(&command.name))
    }) {
        return auth_error(&AuthError::Denied);
    }

    match state.node.topics().create(command).await {
        Ok(result) => {
            let status = if result.created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            (status, Json(topic_json(&result.meta.topic))).into_response()
        }
        Err(error) => service_error(error),
    }
}

async fn update(
    State(state): State<AdminState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let topic = match authorized_topic(&state, &headers, Operation::Create, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };
    let update = match ConfigUpdate::decode(topic.name.spelling().to_owned(), &body) {
        Ok(update) => update,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, error.message),
    };

    let command = update_command(topic.name.spelling().to_owned(), update);
    match state.node.topics().update(command).await {
        Ok(topic) => (StatusCode::OK, Json(topic_json(&topic))).into_response(),
        Err(error) => service_error(error),
    }
}

async fn delete_topic(
    State(state): State<AdminState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    let topic = match authorized_topic(&state, &headers, Operation::Delete, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };

    match state.node.topics().delete(topic.name.spelling()).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => missing_topic(&name),
        Err(error) => service_error(error),
    }
}

async fn trim(
    State(state): State<AdminState>,
    Path((name, index)): Path<(String, u32)>,
    request: Request,
) -> Response {
    let topic = match authorized_topic(&state, request.headers(), Operation::Trim, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };
    match state.remote_owner(&topic, index).await {
        Ok(Some(owner)) => return state.forward_to(owner, request).await,
        Ok(None) => {}
        Err(response) => return *response,
    }

    let body = match read_json(request, TRIM_BODY).await {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let Some(before) = body["before"].as_u64() else {
        return error_response(StatusCode::BAD_REQUEST, "expected {\"before\": offset}");
    };

    match state
        .node
        .topics()
        .delete_records(&topic, index, before, TRIM_TIMEOUT)
        .await
    {
        Ok(start) => (StatusCode::OK, Json(json!({ "startOffset": start }))).into_response(),
        Err(error) => service_error(error),
    }
}

fn owner_id(node: i32) -> Option<i32> {
    (node != -1).then_some(node)
}

fn topic_json(topic: &Topic) -> Value {
    json!({
        "name": topic.name.spelling(),
        "id": topic.id.hex(),
        "partitions": topic.partitions,
        "contentType": topic.content_type,
        "kafkaName": topic.name.kafka(),
        "retentionMs": topic.retention.ms,
        "retentionBytes": topic.retention.bytes,
        "ttlSeconds": topic.ttl_seconds,
        "expiresAtMs": topic.expires_at_ms,
        "closed": topic.closed,
        "schema": topic.schema_name,
        "schemaValidate": topic.schema_validate,
    })
}

fn partition_json(
    index: u32,
    row: Option<&StreamRow>,
    view: &MetadataState,
    offsets: Option<Offsets>,
    current: bool,
) -> Value {
    let pending = row
        .and_then(|row| view.pending_transfers().get(&row.stream_id))
        .map(|pending| json!({ "fromNode": pending.from_node, "toNode": pending.to_node }));

    json!({
        "index": index,
        "streamId": row.map(|row| row.stream_id),
        "ownerNodeId": row.and_then(|row| owner_id(row.node_id)),
        "epoch": row.map(|row| row.epoch),
        "state": row.map(|row| match row.state {
            StreamState::Opened => "opened",
            StreamState::Closed => "closed",
        }),
        "pendingTransfer": pending,
        "startOffset": offsets.map(|offsets| offsets.start),
        "nextOffset": offsets.map(|offsets| offsets.next),
        "current": current,
        "storedBytes": row.map(|row| row.stored_bytes),
    })
}

fn create_command(body: &Value) -> Result<CreateCommand, String> {
    let name = body["name"]
        .as_str()
        .filter(|name| !name.is_empty())
        .ok_or("name is required")?;
    let content_type = body["contentType"]
        .as_str()
        .unwrap_or("application/octet-stream");
    let partitions = match &body["partitions"] {
        Value::Null => 1,
        value => value
            .as_u64()
            .and_then(|partitions| u32::try_from(partitions).ok())
            .ok_or("partitions must be a positive number")?,
    };

    let mut command = CreateCommand::new(name, content_type);
    command.partitions = partitions;
    command.ttl_seconds = optional_u64(body, "ttlSeconds")?;
    command.retention = Retention {
        ms: optional_u64(body, "retentionMs")?,
        bytes: optional_u64(body, "retentionBytes")?,
    };
    command.schema_name = match &body["schema"] {
        Value::Null => None,
        value => Some(
            value
                .as_str()
                .filter(|schema| !schema.is_empty())
                .ok_or("schema must be a schema name")?
                .to_owned(),
        ),
    };
    command.schema_validate = match &body["schemaValidate"] {
        Value::Null => false,
        value => value.as_bool().ok_or("schemaValidate must be a boolean")?,
    };
    Ok(command)
}

fn optional_u64(body: &Value, field: &str) -> Result<Option<u64>, String> {
    match &body[field] {
        Value::Null => Ok(None),
        value => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("{field} must be a number")),
    }
}
