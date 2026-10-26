mod schemas;
pub(crate) mod streams;

use std::sync::Arc;

use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use picomq_auth::{Audience, AuthError, AuthPrincipal, Authorizer, Operation, Resource};
use picomq_server::ownership::OwnershipService;
use picomq_server::{ErrorKind, ServiceError, Topics};
use serde_json::{Value, json};

use crate::RoutingMode;

/// The stream config and schema routes beside a protocol listener. They
/// authenticate like that listener's stream routes.
#[derive(Clone)]
pub struct CommonState {
    pub topics: Arc<Topics>,
    pub ownership: Arc<dyn OwnershipService>,
    pub mode: RoutingMode,
    pub authorizer: Option<Arc<Authorizer>>,
    pub audience: Audience,
}

pub fn router(
    topics: Arc<Topics>,
    ownership: Arc<dyn OwnershipService>,
    mode: RoutingMode,
    authorizer: Option<Arc<Authorizer>>,
    audience: Audience,
    max_request_size: usize,
) -> Router {
    let state = CommonState {
        topics,
        ownership,
        mode,
        authorizer,
        audience,
    };
    schemas::router()
        .merge(streams::router())
        .layer(axum::extract::DefaultBodyLimit::max(max_request_size))
        .with_state(state)
}

impl CommonState {
    /// Authorizes `op` on a stream, resolving the caller's name prefix.
    /// Returns the stored name. Auth off passes the name through.
    pub(crate) async fn admit_stream(
        &self,
        headers: &HeaderMap,
        op: Operation,
        name: &str,
    ) -> Result<String, Box<Response>> {
        let Some(authorizer) = self.authorizer.as_deref() else {
            return Ok(name.to_owned());
        };
        let caller = crate::auth::authenticate(authorizer, self.audience, headers).await?;
        let name = caller.resolve(Resource::Stream, name)?;
        caller.authorize(op, Some(&name))?;
        Ok(name)
    }

    /// Authorizes `op` on a schema, matched like a stream name.
    pub(crate) async fn admit_schema(
        &self,
        headers: &HeaderMap,
        op: Operation,
        name: &str,
    ) -> Result<(), Box<Response>> {
        let Some(authorizer) = self.authorizer.as_deref() else {
            return Ok(());
        };
        let caller = crate::auth::authenticate(authorizer, self.audience, headers).await?;
        caller.authorize(op, Some(name))
    }
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
    if let Some(principal) = &principal
        && !principal.scope.allows(op, resource)
    {
        return Err(Box::new(auth_error(&AuthError::Denied)));
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
    let principal = authorizer
        .authenticate(credential, Audience::Admin, picomq_common::now_ms())
        .await
        .map_err(|err| Box::new(auth_error(&err)))?;
    if principal.scope.auto_prefix {
        return Err(Box::new(auth_error(&AuthError::Denied)));
    }
    Ok(Some(principal))
}

pub(crate) async fn read_json(request: Request, limit: usize) -> Result<Value, Box<Response>> {
    let body = axum::body::to_bytes(request.into_body(), limit)
        .await
        .map_err(|_| {
            Box::new(error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "the body is too large",
            ))
        })?;
    json_body(&body)
}

pub(crate) fn auth_error(err: &AuthError) -> Response {
    if let AuthError::Store(detail) = err {
        tracing::warn!(%detail, "auth store failure");
    }
    crate::observe::auth_failure(Audience::Admin, err);
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

pub(crate) fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

pub(crate) fn json_body(body: &[u8]) -> Result<Value, Box<Response>> {
    serde_json::from_slice(body).map_err(|_| {
        Box::new(error_response(
            StatusCode::BAD_REQUEST,
            "expected a JSON body",
        ))
    })
}

pub(crate) fn service_error(error: ServiceError) -> Response {
    let status = match error.kind {
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Conflict | ErrorKind::Closed => StatusCode::CONFLICT,
        ErrorKind::Durability | ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        ErrorKind::Timeout => StatusCode::GATEWAY_TIMEOUT,
        ErrorKind::Transferring | ErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::BadRequest
        | ErrorKind::InvalidName
        | ErrorKind::CorruptBatch
        | ErrorKind::SchemaViolation
        | ErrorKind::Fenced
        | ErrorKind::SequenceGap
        | ErrorKind::MatchFailed => StatusCode::BAD_REQUEST,
    };
    (status, Json(json!({"error": error.message}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_moving_stream_is_unavailable_not_a_conflict() {
        let moving = service_error(ServiceError::kind(ErrorKind::Transferring));
        assert_eq!(moving.status(), StatusCode::SERVICE_UNAVAILABLE);
        let conflict = service_error(ServiceError::kind(ErrorKind::Conflict));
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
    }
}
