use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use picomq_auth::Operation;
use picomq_server::SchemaFormat;
use serde_json::json;

use super::{gate, service_error, CommonState};

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
    if state.service.schema_registry().is_none() {
        return schema_registry_disabled();
    }
    let format = match schema_format_from_headers(&headers, &name) {
        Ok(format) => format,
        Err(response) => return *response,
    };
    match state.service.put_schema(&name, format, body).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => service_error(error),
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
    if state.service.schema_registry().is_none() {
        return schema_registry_disabled();
    }
    match state.service.get_schema(&name).await {
        Ok(Some((format, bytes))) => {
            let mut response = bytes.into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static(format.content_type()),
            );
            response
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => service_error(error),
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
    if state.service.schema_registry().is_none() {
        return schema_registry_disabled();
    }
    match state.service.delete_schema(&name).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => service_error(error),
    }
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
    {
        if let Some(format) = SchemaFormat::from_content_type(ct) {
            return Ok(format);
        }
    }
    if let Some(ext) = name.rsplit('.').next().filter(|_| name.contains('.')) {
        if let Some(format) = SchemaFormat::from_extension(ext) {
            return Ok(format);
        }
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
