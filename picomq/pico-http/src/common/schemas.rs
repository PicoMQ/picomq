use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use pico_auth::{Audience, AuthError, AuthPrincipal, Operation};
use pico_server::{ErrorKind, SchemaFormat, ServiceError};
use serde_json::json;

use super::CommonState;

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
    if let Err(response) = gate(&state, &headers, Operation::Create, Some(&name)).await {
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
    if let Err(response) = gate(&state, &headers, Operation::StreamInspect, Some(&name)).await {
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
    if let Err(response) = gate(&state, &headers, Operation::Delete, Some(&name)).await {
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

async fn gate(
    state: &CommonState,
    headers: &HeaderMap,
    op: Operation,
    resource: Option<&str>,
) -> Result<Option<AuthPrincipal>, Box<Response>> {
    let Some(authorizer) = &state.authorizer else {
        return Ok(None);
    };
    let credential = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| Box::new(auth_error(&AuthError::Unauthenticated)))?;
    let principal = authorizer
        .authenticate(credential, Audience::Admin, pico_common::now_ms())
        .await
        .map_err(|err| Box::new(auth_error(&err)))?;
    authorizer
        .authorize(&principal, op, resource)
        .map_err(|err| Box::new(auth_error(&err)))?;
    Ok(Some(principal))
}

fn auth_error(err: &AuthError) -> Response {
    if let AuthError::Store(detail) = err {
        tracing::warn!(%detail, "common schema auth store failure");
    }
    let (status, _, message) = crate::auth::status_code(err);
    let mut response = (
        StatusCode::from_u16(status).expect("known status"),
        Json(json!({ "error": message })),
    )
        .into_response();
    if status == 401 {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    }
    response
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

fn service_error(error: ServiceError) -> Response {
    let status = match error.kind {
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        _ => StatusCode::BAD_REQUEST,
    };
    (status, Json(json!({"error": error.message}))).into_response()
}
