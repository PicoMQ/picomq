use std::collections::BTreeMap;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use picomq_auth::{AuthError, AuthPrincipal, Operation};
use picomq_metadata::{MetadataState, StreamRow};
use picomq_protocol::config::ConfigUpdate;
use picomq_server::{CreateCommand, Page, Retention, Topic, partition_log};
use s3stream::StreamState;
use serde_json::{Value, json};

use crate::admin::AdminState;
use crate::common::streams::update_command;
use crate::common::{auth_error, authenticate, error_response, json_body, service_error};
use crate::offsets::{self, Offsets};
use crate::paging::Paging;

const TRIM_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/admin/topics", get(list).post(create))
        .route(
            "/admin/topics/{name}",
            get(detail).patch(update).delete(remove),
        )
        .route("/admin/topics/{name}/partitions", get(partitions))
        .route("/admin/topics/{name}/partitions/{index}", get(partition))
        .route("/admin/topics/{name}/partitions/{index}/trim", post(trim))
}

async fn list(State(state): State<AdminState>, uri: Uri, headers: HeaderMap) -> Response {
    let principal = match caller(&state, &headers).await {
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
    let topic = match visible(&state, &headers, Operation::Head, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };

    let view = state.node.views().load();
    let mut stored_bytes = 0;
    let mut owners: BTreeMap<i32, u32> = BTreeMap::new();
    for index in 0..topic.partitions {
        let Some(row) = row_of(&state, &view.state, &topic, index) else {
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
    let topic = match visible(&state, &headers, Operation::Head, &name).await {
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
    let entries =
        (start..topic.partitions).map(|index| (index, row_of(&state, &view.state, &topic, index)));
    let page = Page::scan(entries, paging.limit, |_, row| {
        node.is_none_or(|node| row.is_some_and(|row| row.node_id == node))
    });

    let known = offsets_of(&state, &headers, &view.state, &topic, &page.items).await;

    paging.reply(page, |index, row| {
        let (offsets, current) = known.get(index).copied().unwrap_or((None, false));
        partition_json(*index, row.as_ref(), &view.state, offsets, current)
    })
}

async fn partition(
    State(state): State<AdminState>,
    Path((name, index)): Path<(String, u32)>,
    headers: HeaderMap,
) -> Response {
    let topic = match visible(&state, &headers, Operation::Head, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };
    if index >= topic.partitions {
        return error_response(
            StatusCode::NOT_FOUND,
            format!("partition {index} not found"),
        );
    }

    let view = state.node.views().load();
    let row = row_of(&state, &view.state, &topic, index);
    let known = offsets_of(&state, &headers, &view.state, &topic, &[(index, row)]).await;

    let (offsets, current) = known.get(&index).copied().unwrap_or((None, false));
    let body = partition_json(index, row.as_ref(), &view.state, offsets, current);
    (StatusCode::OK, Json(body)).into_response()
}

async fn create(State(state): State<AdminState>, headers: HeaderMap, body: Bytes) -> Response {
    let principal = match caller(&state, &headers).await {
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
    let topic = match visible(&state, &headers, Operation::Create, &name).await {
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

async fn remove(
    State(state): State<AdminState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    let topic = match visible(&state, &headers, Operation::Delete, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };

    match state.node.topics().delete(topic.name.spelling()).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => missing(&name),
        Err(error) => service_error(error),
    }
}

async fn trim(
    State(state): State<AdminState>,
    Path((name, index)): Path<(String, u32)>,
    request: Request,
) -> Response {
    let topic = match visible(&state, request.headers(), Operation::Trim, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };
    let view = state.node.views().load();
    let Some(row) = row_of(&state, &view.state, &topic, index) else {
        return error_response(
            StatusCode::NOT_FOUND,
            format!("partition {index} not found"),
        );
    };

    let local = state.node.config().node_id;
    if row.node_id != -1 && row.node_id != local {
        let Some(forward) = &state.forward else {
            return error_response(
                StatusCode::BAD_GATEWAY,
                "this node does not forward requests",
            );
        };
        return forward
            .to(&state.node, row.node_id, request, TRIM_TIMEOUT)
            .await;
    }

    let body = match axum::body::to_bytes(request.into_body(), 4096).await {
        Ok(body) => body,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "expected {\"before\": offset}"),
    };
    let Some(before) = json_body(&body)
        .ok()
        .and_then(|body| body["before"].as_u64())
    else {
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

async fn caller(
    state: &AdminState,
    headers: &HeaderMap,
) -> Result<Option<AuthPrincipal>, Box<Response>> {
    let principal = authenticate(state.authorizer.as_deref(), headers).await?;
    if principal
        .as_ref()
        .is_some_and(|principal| principal.scope.auto_prefix)
    {
        return Err(Box::new(auth_error(&AuthError::Denied)));
    }
    Ok(principal)
}

async fn visible(
    state: &AdminState,
    headers: &HeaderMap,
    op: Operation,
    name: &str,
) -> Result<Topic, Box<Response>> {
    let principal = caller(state, headers).await?;
    let topic = match state.node.topics().get(name).await {
        Ok(Some(topic)) => topic,
        Ok(None) => return Err(Box::new(missing(name))),
        Err(error) => return Err(Box::new(service_error(error))),
    };
    if !allowed(principal.as_ref(), op, &topic) {
        return Err(Box::new(auth_error(&AuthError::Denied)));
    }
    Ok(topic)
}

fn allowed(principal: Option<&AuthPrincipal>, op: Operation, topic: &Topic) -> bool {
    principal.is_none_or(|principal| principal.scope.allows(op, Some(topic.name.spelling())))
}

fn row_of(
    state: &AdminState,
    view: &MetadataState,
    topic: &Topic,
    index: u32,
) -> Option<StreamRow> {
    let log = partition_log(topic, index).ok()?;
    let stream_id = state.node.service().stream_id(&log).ok()??;
    view.streams().get(&stream_id).copied()
}

async fn offsets_of(
    state: &AdminState,
    headers: &HeaderMap,
    view: &MetadataState,
    topic: &Topic,
    partitions: &[(u32, Option<StreamRow>)],
) -> BTreeMap<u32, (Option<Offsets>, bool)> {
    let local = state.node.config().node_id;
    let spelling = topic.name.spelling().to_owned();

    let mut remote: BTreeMap<i32, Vec<(String, u32)>> = BTreeMap::new();
    for (index, row) in partitions {
        let owner = row
            .and_then(|row| open_elsewhere(&row, local))
            .filter(|owner| answers(view, *owner));
        if let Some(owner) = owner {
            remote
                .entry(owner)
                .or_default()
                .push((spelling.clone(), *index));
        }
    }
    let answered = offsets::from_owners(state, headers, remote).await;

    partitions
        .iter()
        .map(|(index, row)| {
            let known = match answered.get(&(spelling.clone(), *index)) {
                Some(offsets) => (Some(*offsets), true),
                None => {
                    let elsewhere = row.and_then(|row| open_elsewhere(&row, local));
                    (local_offsets(state, topic, *index), elsewhere.is_none())
                }
            };
            (*index, known)
        })
        .collect()
}

fn open_elsewhere(row: &StreamRow, local: i32) -> Option<i32> {
    (row.state == StreamState::Opened && row.node_id != local).then_some(row.node_id)
}

fn answers(view: &MetadataState, node: i32) -> bool {
    view.nodes().get(&node).is_some_and(|row| !row.fenced)
}

fn local_offsets(state: &AdminState, topic: &Topic, index: u32) -> Option<Offsets> {
    let offsets = state.node.topics().current_offsets(topic, index).ok()?;
    Some(Offsets {
        start: offsets.start_offset.record_offset(),
        next: offsets.next_offset.record_offset(),
    })
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

fn missing(name: &str) -> Response {
    error_response(StatusCode::NOT_FOUND, format!("topic {name} not found"))
}
