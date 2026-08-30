//! The admin surface over a node: liveness, readiness, cluster and node
//! introspection, admin writes (stream transfer, slot updates), and the
//! embedded dashboard. [`serve`](crate::serve) binds this router on the
//! admin address.
//!
//! The shape is lease/SQL-native. Raft-shaped endpoints (`/admin/peers`,
//! `/admin/transfer-leader`, snapshot archives, a `leaderKnown` readiness
//! term) are intentionally never added: the metadata plane is a SQL-backed
//! log, and maintenance leadership is a lease, reported as `leaseHolder`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use pico_auth::{
    check_issue, scope_from_json, scope_to_json, AccessToken, Audience, AuthError, AuthPrincipal,
    Authorizer, Operation, TokenRecord, TokenStore as _,
};
use pico_metadata::MetadataState;
use pico_server::registry::RegistryEntry;
use pico_server::{ErrorKind, OwnershipService, PicoNode, ServiceError};
use s3stream::StreamState;
use serde_json::{json, Value};
use tokio::sync::watch;

/// What `/ready` reports on.
///
/// `#drainBeforeShutdown` clears. It lives here rather than on [`PicoNode`]
/// because it is a serving-lifecycle fact (this process still wants traffic),
/// not node state: an embedded node with no HTTP has no use for it.
#[derive(Clone)]
pub struct AdminState {
    node: Arc<PicoNode>,
    serving: Arc<AtomicBool>,
    /// Maintenance-lease holdership, when the host runs a lease keeper.
    leadership: Option<watch::Receiver<bool>>,
    /// Bearer enforcement on `/admin` routes. `None` leaves them open.
    authorizer: Option<Arc<Authorizer>>,
}

impl AdminState {
    pub fn new(node: Arc<PicoNode>) -> Self {
        Self {
            node,
            serving: Arc::new(AtomicBool::new(true)),
            leadership: None,
            authorizer: None,
        }
    }

    pub fn with_leadership(mut self, leadership: Option<watch::Receiver<bool>>) -> Self {
        self.leadership = leadership;
        self
    }

    pub fn with_authorizer(mut self, authorizer: Option<Arc<Authorizer>>) -> Self {
        self.authorizer = authorizer;
        self
    }

    pub fn stop_serving(&self) {
        self.serving.store(false, Ordering::Relaxed);
    }

    fn lease_holder(&self) -> Option<bool> {
        self.leadership.as_ref().map(|rx| *rx.borrow())
    }
}

#[derive(rust_embed::RustEmbed)]
#[folder = "_dashboard/"]
struct Dashboard;

const DASHBOARD_HINT: &str = "<!doctype html><html><body style=\"font-family: sans-serif\">\
<h3>PicoMQ admin</h3>\
<p>This binary was built without the dashboard. Build it with\
<code> cd dashboard && npm install && npm run build</code> and recompile,\
or use the Docker image. The <code>/admin</code> API is available.</p>\
</body></html>";

/// Probes and dashboard assets are open. `/admin` routes are gated when an
/// authorizer is set: the asset shell carries no data, it prompts for a token.
pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/admin/cluster", get(cluster))
        .route("/admin/nodes", get(nodes))
        .route("/admin/nodes/{id}", post(update_node))
        .route("/admin/streams/{*name}", get(stream))
        .route("/admin/transfer", post(transfer))
        .route("/admin/tokens", get(list_tokens).post(issue_token))
        .route("/admin/tokens/{*id}", delete(revoke_token))
        .route("/", get(|| async { asset("index.html") }))
        .fallback(get(|uri: Uri| async move {
            asset(uri.path().trim_start_matches('/'))
        }))
        .layer(axum::middleware::from_fn(cors))
        .with_state(state)
}

/// OPTIONS preflight plus CORS headers, so a dashboard served from another
/// origin can send `Authorization`.
async fn cors(request: Request, next: Next) -> Response {
    if request.method() == Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        cors_headers(response.headers_mut());
        return response;
    }
    let mut response = next.run(request).await;
    cors_headers(response.headers_mut());
    response
}

fn cors_headers(headers: &mut HeaderMap) {
    headers.insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    headers.insert(
        "Access-Control-Allow-Methods",
        HeaderValue::from_static("GET, PUT, POST, DELETE, OPTIONS"),
    );
    headers.insert(
        "Access-Control-Allow-Headers",
        HeaderValue::from_static("authorization, content-type"),
    );
}

/// `Ok(None)`: auth off. `Ok(Some)`: authenticated and allowed.
async fn gate(
    state: &AdminState,
    headers: &HeaderMap,
    op: Operation,
    resource: Option<&str>,
) -> Result<Option<AuthPrincipal>, Box<Response>> {
    let principal = authenticate(state, headers).await?;
    if let Some(principal) = &principal {
        state
            .authorizer
            .as_ref()
            .expect("principal implies authorizer")
            .authorize(principal, op, resource)
            .map_err(|err| Box::new(auth_error(&err)))?;
    }
    Ok(principal)
}

async fn authenticate(
    state: &AdminState,
    headers: &HeaderMap,
) -> Result<Option<AuthPrincipal>, Box<Response>> {
    let Some(authorizer) = &state.authorizer else {
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

fn auth_error(err: &AuthError) -> Response {
    if let AuthError::Store(detail) = err {
        tracing::warn!(%detail, "admin auth store failure");
    }
    let (status, _, message) = crate::auth::status_code(err);
    let mut response = error_response(StatusCode::from_u16(status).expect("known status"), message);
    if status == 401 {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    }
    response
}

fn asset(path: &str) -> Response {
    let Some(file) = Dashboard::get(path) else {
        if path == "index.html" {
            return (
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                DASHBOARD_HINT,
            )
                .into_response();
        }
        return error_response(StatusCode::NOT_FOUND, format!("no such path /{path}"));
    };
    let mime = match path.rsplit_once('.').map(|(_, ext)| ext) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript",
        Some("css") => "text/css",
        Some("svg") => "image/svg+xml",
        Some("map" | "json") => "application/json",
        _ => "application/octet-stream",
    };
    let cache = if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    (
        [(header::CONTENT_TYPE, mime), (header::CACHE_CONTROL, cache)],
        file.data.into_owned(),
    )
        .into_response()
}

async fn health() -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// 200 once this node can serve, 503 otherwise,
/// with a diagnostic body of the same shape.
///
/// Registration is the readiness signal: `PicoNode::start` returns only after
/// its `RegisterNode` command applied and the engine recovered its WAL, so a
/// node present in the applied metadata state is a node that can serve.
/// shows an operator whether the metadata tailer is making progress.
async fn ready(State(state): State<AdminState>) -> Response {
    let node_id = state.node.config().node_id;
    let view = state.node.views().load();
    let registered = view.state.get_node_address(node_id).is_some();
    let serving = state.serving.load(Ordering::Relaxed);
    let ready = serving && registered;
    let body = json!({
        "ready": ready,
        "serving": serving,
        "registered": registered,
        "appliedIndex": view.applied_index,
        "nodeId": node_id,
    });
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body)).into_response()
}

async fn cluster(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(response) = gate(&state, &headers, Operation::ClusterRead, None).await {
        return *response;
    }
    let config = state.node.config();
    let view = state.node.views().load();
    let snapshot = &state.node.metadata().sink_stats().snapshot;
    let body = json!({
        "clusterId": config.cluster_id,
        "nodeId": config.node_id,
        "nodeEpoch": config.node_epoch,
        "advertisedAddress": config.http_address,
        "registered": view.state.get_node_address(config.node_id).is_some(),
        "appliedIndex": view.applied_index,
        "streamCount": view.state.streams.len(),
        "objectCount": view.state.stream_set_objects.len() + view.state.stream_objects.len(),
        "streamSetObjectCount": view.state.stream_set_objects.len(),
        "streamObjectCount": view.state.stream_objects.len(),
        "preparedObjectCount": view.state.prepared.len(),
        // A head that does not advance while depth grows means a stuck cleaner.
        "gc": {
            "backlog": view.state.mark_destroyed.len(),
            "oldestSeq": view.state.mark_destroyed.keys().next().copied(),
            "nextSeq": view.state.next_destroyed_seq,
        },
        "kvEntryCount": view.state.kv.len(),
        "kvBytes": view.state.kv_bytes,
        "pendingTransfers": pending_transfers_json(&view.state),
        "leaseHolder": state.lease_holder(),
        "snapshot": {
            "lastAppliedIndex": snapshot.last_applied_index.load(Ordering::Relaxed),
            "lastBytes": snapshot.last_bytes.load(Ordering::Relaxed),
            "lastDurationMs": snapshot.last_duration_ms.load(Ordering::Relaxed),
            "lastAtMs": snapshot.last_at_ms.load(Ordering::Relaxed),
            "taken": snapshot.taken.load(Ordering::Relaxed),
            "failed": snapshot.failed.load(Ordering::Relaxed),
        },
    });
    (StatusCode::OK, Json(body)).into_response()
}

async fn nodes(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(response) = gate(&state, &headers, Operation::NodeRead, None).await {
        return *response;
    }
    let local_id = state.node.config().node_id;
    let view = state.node.views().load();
    let nodes: Vec<Value> = view
        .state
        .nodes
        .values()
        .map(|node| node_json(&view.state, node, local_id))
        .collect();
    (StatusCode::OK, Json(json!({ "nodes": nodes }))).into_response()
}

/// Stream detail by name, read entirely from the published view so an admin
/// GET never opens (and thereby claims) the stream.
async fn stream(
    State(state): State<AdminState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = gate(&state, &headers, Operation::StreamInspect, None).await {
        return *response;
    }
    let name = format!("/{name}");
    let view = state.node.views().load();
    let Some(value) = view.state.get_kv(&name) else {
        return not_found(&name);
    };
    let Ok(entry) = RegistryEntry::decode(&value) else {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, "corrupt registry entry");
    };
    let row = view.state.streams.get(&entry.stream_id).copied();
    let owner = match state.node.ownership().owner_of(&name).await {
        Ok(owner) => owner,
        Err(e) => return service_error(e),
    };
    let config = state.node.config();
    let (owner_node_id, owner_address) = if owner.local {
        (config.node_id, config.http_address.clone())
    } else {
        (
            owner.owner_node_id.unwrap_or(-1),
            owner.owner_advertised_address.clone().unwrap_or_default(),
        )
    };
    let pending = view
        .state
        .pending_transfers
        .get(&entry.stream_id)
        .map(|p| json!({ "fromNode": p.from_node, "toNode": p.to_node }));

    let body = json!({
        "name": name,
        "streamId": entry.stream_id,
        "ownerNodeId": owner_node_id,
        "ownerAdvertisedAddress": owner_address,
        "ownerLocal": owner.local,
        "contentType": crate::pico::user_ct_of(&entry.content_type),
        "ttlSeconds": entry.ttl_seconds,
        "expiresAtMs": entry.expires_at_ms,
        "closed": entry.closed,
        "state": row.map(|r| match r.state {
            StreamState::Opened => "opened",
            StreamState::Closed => "closed",
        }),
        "epoch": row.map(|r| r.epoch),
        "nodeId": row.map(|r| r.node_id),
        "startOffset": row.map(|r| r.start_offset),
        "endOffset": row.map(|r| r.end_offset),
        "pendingTransfer": pending,
    });
    (StatusCode::OK, Json(body)).into_response()
}

/// Requests a live ownership move. Returns 202: the move completes
/// asynchronously via the transfer watcher on the owning node.
async fn transfer(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Err(response) = gate(&state, &headers, Operation::TransferStream, None).await {
        return *response;
    }
    let Some(name) = body.get("stream").and_then(Value::as_str) else {
        return error_response(StatusCode::BAD_REQUEST, "missing \"stream\"");
    };
    let Some(to_node) = body.get("toNode").and_then(Value::as_i64) else {
        return error_response(StatusCode::BAD_REQUEST, "missing \"toNode\"");
    };
    match state.node.transfer_stream(name, to_node as i32).await {
        Ok(stream_id) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "stream": name,
                "streamId": stream_id,
                "toNode": to_node,
                "pending": true,
            })),
        )
            .into_response(),
        Err(e) => service_error(e),
    }
}

/// Updates a registered node's placement weight by refreshing its
/// registration at its current epoch.
async fn update_node(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Err(response) = gate(&state, &headers, Operation::UpdateNodeSlots, None).await {
        return *response;
    }
    let Some(slots) = body.get("slots").and_then(Value::as_u64) else {
        return error_response(StatusCode::BAD_REQUEST, "missing \"slots\"");
    };
    let view = state.node.views().load();
    let Some(node) = view.state.nodes.get(&id).cloned() else {
        return error_response(
            StatusCode::NOT_FOUND,
            format!("node {id} is not registered"),
        );
    };
    if let Err(e) = state
        .node
        .metadata()
        .update_node_slots(id, node.epoch, slots as u32)
        .await
    {
        return error_response(StatusCode::CONFLICT, e.to_string());
    }
    let view = state.node.views().load();
    let local_id = state.node.config().node_id;
    match view.state.nodes.get(&id) {
        Some(node) => {
            (StatusCode::OK, Json(node_json(&view.state, node, local_id))).into_response()
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            format!("node {id} is not registered"),
        ),
    }
}

/// Listing is filtered to ids the caller's token matcher allows. `count` is
/// the number of visible records, informational only, never an enforced cap.
async fn list_tokens(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    let principal = match authenticate(&state, &headers).await {
        Ok(principal) => principal,
        Err(response) => return *response,
    };
    if let Some(principal) = &principal {
        if !principal.scope.allows_operation(Operation::ListTokens) {
            return auth_error(&AuthError::Denied);
        }
    }
    let records = match state.node.tokens().store().list_prefix("").await {
        Ok(records) => records,
        Err(err) => return auth_error(&err),
    };
    let tokens: Vec<Value> = records
        .iter()
        .filter(|record| match &principal {
            Some(principal) => principal.scope.allows_token_id(&record.id),
            None => true,
        })
        .map(token_json)
        .collect();
    let body = json!({ "count": tokens.len(), "tokens": tokens });
    (StatusCode::OK, Json(body)).into_response()
}

/// Issues a token narrowed from the caller's scope. With auth off the issuer
/// is the root scope. The secret is in the response once and never stored.
async fn issue_token(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let principal = match authenticate(&state, &headers).await {
        Ok(principal) => principal,
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
    if id == pico_auth::ANONYMOUS_TOKEN_ID && requested.allows_audience(Audience::Admin) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "the anonymous grant cannot carry the admin audience",
        );
    }
    let issuer = principal
        .as_ref()
        .map(|p| p.scope.clone())
        .unwrap_or_else(pico_auth::Scope::root);
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
        created_at_ms: pico_common::now_ms(),
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
    if let Err(response) = gate(&state, &headers, Operation::RevokeToken, Some(&id)).await {
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

fn node_json(state: &MetadataState, node: &pico_metadata::state::NodeRow, local_id: i32) -> Value {
    let opening = state
        .opening_by_node
        .range((node.node_id, 0)..=(node.node_id, u64::MAX))
        .count();
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
        "openingCount": opening,
        "placedCount": state.placed_count(node.node_id),
    })
}

fn pending_transfers_json(state: &MetadataState) -> Vec<Value> {
    state
        .pending_transfers
        .iter()
        .map(|(stream_id, p)| {
            json!({
                "streamId": stream_id,
                "fromNode": p.from_node,
                "toNode": p.to_node,
            })
        })
        .collect()
}

fn not_found(name: &str) -> Response {
    error_response(StatusCode::NOT_FOUND, format!("stream {name} not found"))
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

fn service_error(e: ServiceError) -> Response {
    let status = match e.kind {
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        _ => StatusCode::BAD_REQUEST,
    };
    error_response(status, e.message)
}
