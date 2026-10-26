use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use picomq_auth::{
    AccessToken, Audience, AuthError, Operation, TokenRecord, TokenStore as _, check_issue,
    scope_from_json, scope_to_json,
};
use picomq_server::Page;
use serde_json::{Value, json};

use super::AdminState;
use crate::common::{auth_error, authenticate, error_response, gate, json_body};
use crate::paging::Paging;

pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/admin/tokens", get(list_tokens).post(issue_token))
        .route("/admin/tokens/{*id}", delete(revoke_token))
}

async fn list_tokens(State(state): State<AdminState>, uri: Uri, headers: HeaderMap) -> Response {
    let principal = match authenticate(state.authorizer.as_deref(), &headers).await {
        Ok(principal) => principal,
        Err(response) => return *response,
    };
    if let Some(principal) = &principal
        && !principal.scope.allows_operation(Operation::ListTokens)
    {
        return auth_error(&AuthError::Denied);
    }
    let paging = match Paging::from_query(&uri, &["prefix"]) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };

    let records = match state
        .node
        .tokens()
        .store()
        .list_page(
            paging.filter("prefix").unwrap_or(""),
            paging.after.as_deref(),
            paging.fetch_count(),
        )
        .await
    {
        Ok(records) => records,
        Err(err) => return auth_error(&err),
    };
    let page = Page::scan(
        records
            .into_iter()
            .map(|record| (record.id.clone(), record)),
        paging.limit,
        |id, _| {
            principal
                .as_ref()
                .is_none_or(|principal| principal.scope.tokens.matches(id))
        },
    );

    paging.reply(page, |_, record| token_json(record))
}

/// Issues a token narrowed from the caller's scope. With auth off the issuer
/// is the root scope. The secret is in the response once and never stored.
async fn issue_token(State(state): State<AdminState>, headers: HeaderMap, body: Bytes) -> Response {
    let principal = match authenticate(state.authorizer.as_deref(), &headers).await {
        Ok(principal) => principal,
        Err(response) => return *response,
    };
    let body = match json_body(&body) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let Some(id) = body.get("id").and_then(Value::as_str) else {
        return error_response(StatusCode::BAD_REQUEST, "missing \"id\"");
    };
    let Some(scope_value) = body.get("scope") else {
        return error_response(StatusCode::BAD_REQUEST, "missing \"scope\"");
    };
    let requested = match scope_from_json(scope_value) {
        Ok(requested) => requested,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid scope"),
    };
    // An uncredentialed admin plane is never intended.
    if id == picomq_auth::ANONYMOUS_TOKEN_ID && requested.allows_audience(Audience::Admin) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "the anonymous grant cannot carry the admin audience",
        );
    }
    let issuer = principal
        .as_ref()
        .map(|p| p.scope.clone())
        .unwrap_or_else(picomq_auth::Scope::root);
    if let Err(err) = check_issue(&issuer, id, &requested) {
        return match err {
            AuthError::Malformed => {
                error_response(StatusCode::BAD_REQUEST, "dead or invalid scope")
            }
            AuthError::NarrowingRejected => {
                error_response(StatusCode::FORBIDDEN, "scope exceeds issuer")
            }
            other => auth_error(&other),
        };
    }
    let Ok((token, verifier)) = AccessToken::issue(id) else {
        return error_response(StatusCode::BAD_REQUEST, "invalid token id");
    };
    let record = TokenRecord {
        id: id.to_owned(),
        verifier,
        scope: requested,
        created_at_ms: picomq_common::now_ms(),
        issued_by: principal.map(|p| p.id.clone()).unwrap_or_default(),
    };
    let stored = match state
        .node
        .tokens()
        .store()
        .put_if_absent(record.clone())
        .await
    {
        Ok(stored) => stored,
        Err(err) => return auth_error(&err),
    };
    if !stored {
        return error_response(StatusCode::CONFLICT, format!("token {id} already exists"));
    }
    let body = json!({
        "id": id,
        "token": token.render(),
        "scope": scope_to_json(&record.scope),
        "createdAtMs": record.created_at_ms,
    });
    (StatusCode::CREATED, Json(body)).into_response()
}

/// Conditional on the stored verifier, so a concurrent reissue of the same id
/// is never revoked by a stale request.
async fn revoke_token(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::RevokeToken,
        Some(&id),
    )
    .await
    {
        return *response;
    }
    let store = state.node.tokens().store();
    let record = match store.get(&id).await {
        Ok(Some(record)) => record,
        Ok(None) => return error_response(StatusCode::NOT_FOUND, format!("no token {id}")),
        Err(err) => return auth_error(&err),
    };
    match store.delete_if(&id, &record.verifier).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => error_response(StatusCode::CONFLICT, "token changed, retry"),
        Err(err) => auth_error(&err),
    }
}

fn token_json(record: &TokenRecord) -> Value {
    json!({
        "id": record.id,
        "scope": scope_to_json(&record.scope),
        "createdAtMs": record.created_at_ms,
        "issuedBy": record.issued_by,
    })
}
