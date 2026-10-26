//! The admin surface over a node: liveness, readiness, cluster and node
//! introspection, admin writes (stream transfer, slot updates), and the
//! embedded dashboard. [`serve`](crate::serve) binds this router on the
//! admin address.
//!
//! The shape is lease/SQL-native. Raft-shaped endpoints (`/admin/peers`,
//! `/admin/transfer-leader`, snapshot archives, a `leaderKnown` readiness
//! term) are intentionally never added: the metadata plane is a SQL-backed
//! log, and maintenance leadership is a lease, reported as `leaseHolder`.

mod cluster;
mod groups;
mod nodes;
mod offsets;
mod overview;
mod records;
mod tokens;
mod topics;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use picomq_auth::{AuthError, AuthPrincipal, Authorizer, Operation};
use picomq_metrics::Metrics;
use picomq_server::{OwnershipService as _, PicoNode, Topic, partition_log};
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::common::{auth_error, authenticate, error_response, service_error};
use crate::forward::Forward;

pub const PROTOCOL_NAME: &str = "admin";

/// What `/ready` reports on.
///
/// `#drainBeforeShutdown` clears. It lives here rather than on [`PicoNode`]
/// because it is a serving-lifecycle fact (this process still wants traffic),
/// not node state: an embedded node with no HTTP has no use for it.
#[derive(Clone)]
pub struct AdminState {
    pub(crate) node: Arc<PicoNode>,
    serving: Arc<AtomicBool>,
    /// Maintenance-lease holdership, when the host runs a lease keeper.
    leadership: Option<watch::Receiver<bool>>,
    /// Bearer enforcement on `/admin` routes. `None` leaves them open.
    pub(crate) authorizer: Option<Arc<Authorizer>>,
    pub(crate) forward: Option<Forward>,
    metrics: Option<Metrics>,
}

impl AdminState {
    pub fn new(node: Arc<PicoNode>) -> Self {
        Self {
            node,
            serving: Arc::new(AtomicBool::new(true)),
            leadership: None,
            authorizer: None,
            forward: None,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Option<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_forward(mut self, forward: Option<Forward>) -> Self {
        self.forward = forward;
        self
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

    async fn forward_to(&self, target: i32, request: Request) -> Response {
        let Some(forward) = &self.forward else {
            return error_response(
                StatusCode::BAD_GATEWAY,
                "this node does not forward requests",
            );
        };
        forward.to(&self.node, target, request).await
    }

    async fn post_json(
        &self,
        target: i32,
        path: &str,
        headers: &HeaderMap,
        body: &Value,
    ) -> Option<Value> {
        self.forward
            .as_ref()?
            .post_json(&self.node, target, path, headers, body)
            .await
    }

    async fn remote_owner(&self, topic: &Topic, index: u32) -> Result<Option<i32>, Box<Response>> {
        if index >= topic.partitions {
            return Err(Box::new(missing_partition(index)));
        }

        let log = partition_log(topic, index).map_err(|error| Box::new(service_error(error)))?;
        let owner = self
            .node
            .ownership()
            .owner_of(&log)
            .await
            .map_err(|error| Box::new(service_error(error)))?;
        Ok(owner.owner_node_id.filter(|_| !owner.local))
    }
}

async fn authorized_topic(
    state: &AdminState,
    headers: &HeaderMap,
    op: Operation,
    name: &str,
) -> Result<Topic, Box<Response>> {
    let principal = authenticate(state.authorizer.as_deref(), headers).await?;
    let topic = match state.node.topics().get(name).await {
        Ok(Some(topic)) => topic,
        Ok(None) => return Err(Box::new(missing_topic(name))),
        Err(error) => return Err(Box::new(service_error(error))),
    };
    if !allowed(principal.as_ref(), op, &topic) {
        return Err(Box::new(auth_error(&AuthError::Denied)));
    }
    Ok(topic)
}

fn allowed(principal: Option<&AuthPrincipal>, op: Operation, topic: &Topic) -> bool {
    principal.is_none_or(|principal| principal.scope.allows(op, Some(topic.name.spelling())))
}

fn missing_topic(name: &str) -> Response {
    error_response(StatusCode::NOT_FOUND, format!("topic {name} not found"))
}

fn missing_partition(index: u32) -> Response {
    error_response(
        StatusCode::NOT_FOUND,
        format!("partition {index} not found"),
    )
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
        .merge(nodes::routes())
        .merge(cluster::routes())
        .merge(overview::routes())
        .merge(tokens::routes())
        .merge(topics::routes())
        .merge(records::routes())
        .merge(groups::routes())
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
/// its `RegisterNode` command applied and the engine recovered its WAL. A
/// fenced registration, or one replaced at a later epoch, is not ready.
async fn ready(State(state): State<AdminState>) -> Response {
    let config = state.node.config();
    let view = state.node.views().load();
    let row = state.node.registration();
    let registered = row.is_some();
    let fenced = row.is_some_and(|row| row.fenced);
    let serving = state.serving.load(Ordering::Relaxed);
    let ready = serving && registered && !fenced;
    let body = json!({
        "ready": ready,
        "serving": serving,
        "registered": registered,
        "fenced": fenced,
        "appliedIndex": view.applied_index,
        "nodeId": config.node_id,
    });
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body)).into_response()
}
