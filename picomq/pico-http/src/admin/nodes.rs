use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use picomq_auth::Operation;
use picomq_metadata::{MetadataState, NodeRow};
use serde_json::{Value, json};

use super::{AdminState, offsets};
use crate::common::{error_response, gate, json_body, service_error};
use crate::forward::served_by;
use crate::paging::Paging;

pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/admin/nodes", get(nodes))
        .route("/admin/nodes/{id}", post(update_node))
        .route("/admin/nodes/{id}/metrics", get(node_metrics))
        .route("/admin/nodes/{id}/offsets", post(offsets::owned_offsets))
}

async fn nodes(State(state): State<AdminState>, uri: Uri, headers: HeaderMap) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::NodeRead,
        None,
    )
    .await
    {
        return *response;
    }
    let paging = match Paging::from_query(&uri, &["fenced"]) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let fenced = match paging.filter("fenced") {
        None => None,
        Some("true") => Some(true),
        Some("false") => Some(false),
        Some(_) => return error_response(StatusCode::BAD_REQUEST, "fenced must be true or false"),
    };
    let after = match paging.after_as::<i32>() {
        Ok(after) => after,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };

    let page = state.node.nodes(after, paging.limit, |_, node| {
        fenced.is_none_or(|fenced| node.fenced == fenced)
    });
    let local_id = state.node.config().node_id;
    let view = state.node.views().load();
    paging.reply(page, |_, node| node_json(&view.state, node, local_id))
}

async fn metrics(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::NodeRead,
        None,
    )
    .await
    {
        return *response;
    }

    rendered(&state)
}

async fn node_metrics(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
    request: Request,
) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        request.headers(),
        Operation::NodeRead,
        None,
    )
    .await
    {
        return *response;
    }

    if id == state.node.config().node_id {
        return rendered(&state);
    }
    state.forward_to(id, request).await
}

fn rendered(state: &AdminState) -> Response {
    let Some(metrics) = &state.metrics else {
        return error_response(StatusCode::NOT_FOUND, "this node does not collect metrics");
    };

    let response = (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics.render(state.node.logs_owned()),
    )
        .into_response();
    served_by(&state.node, response)
}

/// Updates a registered node's placement weight by refreshing its
/// registration at its current epoch.
async fn update_node(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::UpdateNodeSlots,
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
    let Some(slots) = body
        .get("slots")
        .and_then(Value::as_u64)
        .and_then(|slots| u32::try_from(slots).ok())
    else {
        return error_response(StatusCode::BAD_REQUEST, "expected {\"slots\": count}");
    };

    match state.node.set_slots(id, slots).await {
        Ok(node) => {
            let view = state.node.views().load();
            let local_id = state.node.config().node_id;
            (
                StatusCode::OK,
                Json(node_json(&view.state, &node, local_id)),
            )
                .into_response()
        }
        Err(error) => service_error(error),
    }
}

fn node_json(state: &MetadataState, node: &NodeRow, local_id: i32) -> Value {
    json!({
        "nodeId": node.node_id,
        "nodeEpoch": node.epoch,
        "advertisedAddress": if node.http_address.is_empty() {
            Value::Null
        } else {
            Value::String(node.http_address.clone())
        },
        "slots": node.slots,
        "local": node.node_id == local_id,
        "openingCount": state.opening_count(node.node_id),
        "placedCount": state.placed_count(node.node_id),
        "fenced": node.fenced,
    })
}
