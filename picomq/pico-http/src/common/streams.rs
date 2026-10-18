use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use picomq_auth::Operation;
use picomq_server::{Topic, UpdateTopicCommand};
use serde_json::{Value, json};

use super::{CommonState, gate, service_error};
use crate::route::route;

pub const STREAM_CONFIG_PATH_PREFIX: &str = "/_streams";

pub fn router() -> Router<CommonState> {
    Router::new().route("/_streams/{*name}", get(get_config).patch(patch_config))
}

async fn get_config(
    State(state): State<CommonState>,
    Path(name): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let name = format!("/{name}");
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::StreamInspect,
        Some(&name),
    )
    .await
    {
        return *response;
    }
    if let Some(response) = route(
        state.ownership.as_ref(),
        &state.topics,
        state.mode,
        &method,
        &uri,
        &name,
    )
    .await
    {
        return response;
    }
    match state.topics.get(&name).await {
        Ok(Some(topic)) => Json(config_json(&topic)).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => service_error(error),
    }
}

async fn patch_config(
    State(state): State<CommonState>,
    Path(name): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let name = format!("/{name}");
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::Create,
        Some(&name),
    )
    .await
    {
        return *response;
    }
    if let Some(response) = route(
        state.ownership.as_ref(),
        &state.topics,
        state.mode,
        &method,
        &uri,
        &name,
    )
    .await
    {
        return response;
    }
    let Some(obj) = body.as_object() else {
        return bad_request("expected a JSON object");
    };
    let schema_name = match obj.get("schema") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(s)) if !s.is_empty() => Some(Some(s.clone())),
        Some(_) => return bad_request("schema must be a non-empty string or null"),
    };
    let schema_validate = match obj.get("schemaValidate") {
        None => None,
        Some(Value::Bool(v)) => Some(*v),
        Some(_) => return bad_request("schemaValidate must be a boolean"),
    };
    if obj.contains_key("kafkaTopic") {
        return bad_request("kafkaTopic is derived from the stream name and cannot be set");
    }
    match state
        .topics
        .update(UpdateTopicCommand {
            name,
            schema_name,
            schema_validate,
        })
        .await
    {
        Ok(topic) => Json(config_json(&topic)).into_response(),
        Err(error) => service_error(error),
    }
}

fn config_json(topic: &Topic) -> Value {
    json!({
        "schema": topic.schema_name,
        "schemaValidate": topic.schema_validate,
        "kafkaTopic": topic.name.kafka(),
    })
}

fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
}
