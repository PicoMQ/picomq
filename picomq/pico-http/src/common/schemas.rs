use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use picomq_auth::Operation;
use picomq_server::SchemaFormat;
use serde_json::json;

use super::{CommonState, gate};

pub const SCHEMA_PATH_PREFIX: &str = "/_schemas";

pub fn router() -> Router<CommonState> {
    Router::new().route(
        "/_schemas/{*name}",
        get(get_schema).put(put_schema).delete(delete_schema),
    )
}

async fn put_schema(
    State(state): State<CommonState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
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
    let Some(registry) = state.topics.schema_registry() else {
        return schema_registry_disabled();
    };
    let format = match schema_format_from_headers(&headers, &name) {
        Ok(format) => format,
        Err(response) => return *response,
    };
    match registry.put(&name, format, body).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => schema_failure(error),
    }
}

async fn get_schema(
    State(state): State<CommonState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
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
    let Some(registry) = state.topics.schema_registry() else {
        return schema_registry_disabled();
    };
    match registry.get(&name).await {
        Ok(Some((format, bytes))) => {
            let mut response = bytes.into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static(format.content_type()),
            );
            response
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => schema_failure(error),
    }
}

async fn delete_schema(
    State(state): State<CommonState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::Delete,
        Some(&name),
    )
    .await
    {
        return *response;
    }
    let Some(registry) = state.topics.schema_registry() else {
        return schema_registry_disabled();
    };
    match registry.delete(&name).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => schema_failure(error),
    }
}

fn schema_failure(error: impl std::fmt::Display) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": error.to_string()})),
    )
        .into_response()
}

fn schema_registry_disabled() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "schema registry is not configured"})),
    )
        .into_response()
}

fn schema_format_from_headers(
    headers: &HeaderMap,
    name: &str,
) -> Result<SchemaFormat, Box<Response>> {
    if let Some(ct) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        && let Some(format) = SchemaFormat::from_content_type(ct)
    {
        return Ok(format);
    }
    if let Some(ext) = name.rsplit('.').next().filter(|_| name.contains('.'))
        && let Some(format) = SchemaFormat::from_extension(ext)
    {
        return Ok(format);
    }
    Err(Box::new(
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "unsupported schema Content-Type; use application/schema+json, application/avro, or application/x-protobuf"
            })),
        )
            .into_response(),
    ))
}
