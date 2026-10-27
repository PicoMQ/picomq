use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use picomq_auth::Operation;
use serde_json::json;

use super::AdminState;
use crate::common::{gate, service_error};

pub(crate) fn routes() -> Router<AdminState> {
    Router::new().route("/admin/overview", get(overview))
}

async fn overview(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(response) = gate(
        state.authorizer.as_deref(),
        &headers,
        Operation::ClusterRead,
        None,
    )
    .await
    {
        return *response;
    }

    let rankings = match state.node.overview().rankings().await {
        Ok(rankings) => rankings,
        Err(error) => return service_error(error),
    };
    let by_size: Vec<_> = rankings
        .by_size
        .iter()
        .map(|(topic, stored_bytes)| {
            json!({ "topic": topic.name.spelling(), "storedBytes": stored_bytes })
        })
        .collect();
    let by_write_rate: Vec<_> = rankings
        .by_write_rate
        .iter()
        .map(|(topic, rate)| json!({ "topic": topic.name.spelling(), "recordsPerSecond": rate }))
        .collect();

    let body = json!({
        "computedAtMs": rankings.computed_at_ms,
        "topicsBySize": by_size,
        "topicsByWriteRate": by_write_rate,
        "writeRateSinceMs": rankings.write_rate_since_ms,
    });
    (StatusCode::OK, Json(body)).into_response()
}
