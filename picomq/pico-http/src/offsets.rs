use std::collections::BTreeMap;
use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use picomq_auth::{AuthError, AuthPrincipal, Operation};
use picomq_server::Topic;
use serde_json::{Value, json};

use crate::admin::AdminState;
use crate::common::{auth_error, authenticate, error_response, json_body};

const MAX_PARTITIONS: usize = 1_000;
const MAX_BODY: usize = MAX_PARTITIONS * 512;
const OWNER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Offsets {
    pub(crate) start: u64,
    pub(crate) next: u64,
}

pub(crate) async fn answer(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
    request: Request,
) -> Response {
    if id != state.node.config().node_id {
        let Some(forward) = &state.forward else {
            return error_response(
                StatusCode::BAD_GATEWAY,
                "this node does not forward requests",
            );
        };
        return forward.to(&state.node, id, request, OWNER_TIMEOUT).await;
    }

    let principal = match authenticate(state.authorizer.as_deref(), request.headers()).await {
        Ok(principal) => principal,
        Err(response) => return *response,
    };
    if principal
        .as_ref()
        .is_some_and(|principal| principal.scope.auto_prefix)
    {
        return auth_error(&AuthError::Denied);
    }
    let body = match axum::body::to_bytes(request.into_body(), MAX_BODY).await {
        Ok(body) => body,
        Err(_) => return error_response(StatusCode::PAYLOAD_TOO_LARGE, "too many partitions"),
    };
    let body = match json_body(&body) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let Some(wanted) = partitions(&body).filter(|wanted| wanted.len() <= MAX_PARTITIONS) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("expected up to {MAX_PARTITIONS} partitions as {{\"topic\", \"index\"}}"),
        );
    };

    let mut topics: BTreeMap<String, Option<Topic>> = BTreeMap::new();
    let mut answered = Vec::with_capacity(wanted.len());
    for (name, index) in wanted {
        if !allowed(principal.as_ref(), &name) {
            return auth_error(&AuthError::Denied);
        }
        if !topics.contains_key(&name) {
            let topic = state.node.topics().get(&name).await.ok().flatten();
            topics.insert(name.clone(), topic);
        }
        let Some(topic) = topics.get(&name).and_then(Option::as_ref) else {
            continue;
        };
        let Ok(offsets) = state.node.topics().current_offsets(topic, index) else {
            continue;
        };

        answered.push(json!({
            "topic": name,
            "index": index,
            "startOffset": offsets.start_offset.record_offset(),
            "nextOffset": offsets.next_offset.record_offset(),
        }));
    }

    (StatusCode::OK, Json(json!({ "partitions": answered }))).into_response()
}

pub(crate) async fn from_owners(
    state: &AdminState,
    headers: &HeaderMap,
    wanted: BTreeMap<i32, Vec<(String, u32)>>,
) -> BTreeMap<(String, u32), Offsets> {
    let Some(forward) = &state.forward else {
        return BTreeMap::new();
    };

    let asks = wanted.into_iter().map(|(owner, partitions)| async move {
        let body = json!({
            "partitions": partitions
                .iter()
                .map(|(topic, index)| json!({ "topic": topic, "index": index }))
                .collect::<Vec<_>>(),
        });
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(format!("/admin/nodes/{owner}/offsets"))
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(credential) = headers.get(header::AUTHORIZATION) {
            request = request.header(header::AUTHORIZATION, credential);
        }
        let Ok(request) = request.body(Body::from(body.to_string())) else {
            return Vec::new();
        };

        let response = forward.to(&state.node, owner, request, OWNER_TIMEOUT).await;
        if response.status() != StatusCode::OK {
            return Vec::new();
        }
        let Ok(body) = axum::body::to_bytes(response.into_body(), MAX_BODY).await else {
            return Vec::new();
        };
        answers(&body)
    });

    futures::future::join_all(asks)
        .await
        .into_iter()
        .flatten()
        .collect()
}

fn partitions(body: &Value) -> Option<Vec<(String, u32)>> {
    body["partitions"]
        .as_array()?
        .iter()
        .map(|entry| {
            let topic = entry["topic"].as_str()?.to_owned();
            let index = u32::try_from(entry["index"].as_u64()?).ok()?;
            Some((topic, index))
        })
        .collect()
}

fn answers(body: &[u8]) -> Vec<((String, u32), Offsets)> {
    let Ok(body) = serde_json::from_slice::<Value>(body) else {
        return Vec::new();
    };

    body["partitions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let topic = entry["topic"].as_str()?.to_owned();
            let index = u32::try_from(entry["index"].as_u64()?).ok()?;
            let offsets = Offsets {
                start: entry["startOffset"].as_u64()?,
                next: entry["nextOffset"].as_u64()?,
            };
            Some(((topic, index), offsets))
        })
        .collect()
}

fn allowed(principal: Option<&AuthPrincipal>, name: &str) -> bool {
    principal.is_none_or(|principal| principal.scope.allows(Operation::Head, Some(name)))
}
