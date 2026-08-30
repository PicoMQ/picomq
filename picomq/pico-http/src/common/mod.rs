mod schemas;
mod streams;

use std::sync::Arc;

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use pico_auth::{Audience, AuthError, AuthPrincipal, Authorizer, Operation};
use pico_server::ownership::OwnershipService;
use pico_server::{ErrorKind, S3StreamService, ServiceError};
use serde_json::json;

use crate::RoutingMode;

pub use schemas::SCHEMA_PATH_PREFIX;
pub use streams::STREAM_CONFIG_PATH_PREFIX;

#[derive(Clone)]
pub struct CommonState {
    pub service: Arc<S3StreamService>,
    pub ownership: Arc<dyn OwnershipService>,
    pub mode: RoutingMode,
    pub authorizer: Option<Arc<Authorizer>>,
}

pub fn router(
    service: Arc<S3StreamService>,
    ownership: Arc<dyn OwnershipService>,
    mode: RoutingMode,
    authorizer: Option<Arc<Authorizer>>,
    max_request_size: usize,
) -> Router {
    let state = CommonState {
        service,
        ownership,
        mode,
        authorizer,
    };
    schemas::router()
        .merge(streams::router())
        .layer(axum::extract::DefaultBodyLimit::max(max_request_size))
        .with_state(state)
}

/// Bearer-token gate for admin-audience routes. `Ok(None)`: auth off.
/// `Ok(Some)`: authenticated and allowed.
pub(crate) async fn gate(
    authorizer: Option<&Authorizer>,
    headers: &HeaderMap,
    op: Operation,
    resource: Option<&str>,
) -> Result<Option<AuthPrincipal>, Box<Response>> {
    let principal = authenticate(authorizer, headers).await?;
    if let Some(principal) = &principal {
        authorizer
            .expect("principal implies authorizer")
            .authorize(principal, op, resource)
            .map_err(|err| Box::new(auth_error(&err)))?;
    }
    Ok(principal)
}

pub(crate) async fn authenticate(
    authorizer: Option<&Authorizer>,
    headers: &HeaderMap,
) -> Result<Option<AuthPrincipal>, Box<Response>> {
    let Some(authorizer) = authorizer else {
        return Ok(None);
    };
    let credential = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| Box::new(auth_error(&AuthError::Unauthenticated)))?;
    authorizer
        .authenticate(credential, Audience::Admin, pico_common::now_ms())
        .await
        .map(Some)
        .map_err(|err| Box::new(auth_error(&err)))
}

pub(crate) fn auth_error(err: &AuthError) -> Response {
    if let AuthError::Store(detail) = err {
        tracing::warn!(%detail, "auth store failure");
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

pub(crate) fn service_error(error: ServiceError) -> Response {
    let status = match error.kind {
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        _ => StatusCode::BAD_REQUEST,
    };
    (status, Json(json!({"error": error.message}))).into_response()
}
