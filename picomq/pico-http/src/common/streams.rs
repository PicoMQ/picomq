use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use picomq_auth::Operation;
use picomq_protocol::config::{ConfigUpdate, TopicConfig};
use picomq_protocol::pico::CT_JSON;
use picomq_server::{Topic, UpdateTopicCommand};
use serde_json::json;

use super::{CommonState, service_error};
use crate::route::route;

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
    let name = match state
        .admit_stream(&headers, Operation::Head, &format!("/{name}"))
        .await
    {
        Ok(name) => name,
        Err(response) => return *response,
    };
    if let Some(response) = route(
        state.ownership.as_ref(),
        &state.topics,
        state.mode,
        &method,
        &uri,
        &name,
        Some(0),
    )
    .await
    {
        return response;
    }
    match state.topics.get(&name).await {
        Ok(Some(topic)) => config(&topic),
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
    body: Bytes,
) -> Response {
    let name = match state
        .admit_stream(&headers, Operation::Create, &format!("/{name}"))
        .await
    {
        Ok(name) => name,
        Err(response) => return *response,
    };
    if let Some(response) = route(
        state.ownership.as_ref(),
        &state.topics,
        state.mode,
        &method,
        &uri,
        &name,
        Some(0),
    )
    .await
    {
        return response;
    }
    let update = match ConfigUpdate::decode(name.to_string(), &body) {
        Ok(update) => update,
        Err(error) => return bad_request(&error.message),
    };
    match state
        .topics
        .update(UpdateTopicCommand {
            name,
            schema_name: update.schema,
            schema_validate: update.schema_validate,
            retention_ms: update.retention_ms,
            retention_bytes: update.retention_bytes,
            partitions: update.partitions,
            ..Default::default()
        })
        .await
    {
        Ok(topic) => config(&topic),
        Err(error) => service_error(error),
    }
}

fn config(topic: &Topic) -> Response {
    let config = TopicConfig {
        schema: topic.schema_name.clone(),
        schema_validate: topic.schema_validate,
        kafka_topic: topic.name.kafka().map(str::to_owned),
        partitions: topic.partitions,
        retention_ms: topic.retention.ms,
        retention_bytes: topic.retention.bytes,
    };
    ([(header::CONTENT_TYPE, CT_JSON)], config.encode()).into_response()
}

fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
}
