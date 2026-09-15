use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::{get, post, put};
use picomq_auth::{Audience, Authorizer, Operation};
use picomq_protocol::pico::{
    CT_JSON, E_BAD_REQUEST, E_CAPACITY_EXCEEDED, E_COORDINATOR_UNAVAILABLE, E_DURABILITY, E_FENCED,
    E_ILLEGAL_GENERATION, E_INCONSISTENT_PROTOCOL, E_NOT_FOUND, E_REBALANCE_IN_PROGRESS,
    E_UNKNOWN_MEMBER,
};
use picomq_server::ownership::OwnershipService;
use picomq_server::{
    CommittedOffset, GroupCoordinator, GroupDescription, GroupError, JoinInput, Joined,
    MemberFence, MemberRole, Membership, OffsetCommit,
};
use serde_json::{Map, Value, json};

use crate::auth::{Caller, authenticate};
use crate::http::{base_response, query_param, query_params, set_header};
use crate::pico::error;
use crate::route::{RoutingMode, owner_redirect};

#[derive(Clone)]
pub struct GroupState {
    pub groups: Arc<GroupCoordinator>,
    pub ownership: Arc<dyn OwnershipService>,
    pub mode: RoutingMode,
    pub authorizer: Option<Arc<Authorizer>>,
}

pub fn router(state: GroupState, max_request_size: usize) -> Router {
    Router::new()
        .route("/_groups", get(list))
        .route("/_groups/{group}", get(describe))
        .route("/_groups/{group}/members", post(join))
        .route(
            "/_groups/{group}/members/{member}",
            get(assignment).delete(leave),
        )
        .route(
            "/_groups/{group}/members/{member}/heartbeat",
            post(heartbeat),
        )
        .route("/_groups/{group}/offsets", put(commit).get(fetch))
        .layer(axum::extract::DefaultBodyLimit::max(max_request_size))
        .with_state(state)
}

impl GroupState {
    async fn admit(&self, headers: &HeaderMap) -> Result<Option<Caller>, Box<Response>> {
        match &self.authorizer {
            Some(authorizer) => authenticate(authorizer, Audience::Pico, headers)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn redirect(&self, uri: &Uri, group: &str) -> Option<Response> {
        owner_redirect(
            self.ownership.as_ref(),
            self.mode,
            uri,
            &GroupCoordinator::stream_name(group),
        )
        .await
    }

    fn resolve(
        &self,
        caller: &Option<Caller>,
        names: &[String],
    ) -> Result<Vec<String>, Box<Response>> {
        let (Some(authorizer), Some(caller)) = (&self.authorizer, caller) else {
            return Ok(names.to_vec());
        };
        names
            .iter()
            .map(|name| caller.resolve(authorizer, name))
            .collect()
    }

    fn strip<'a>(&self, caller: &Option<Caller>, stored: &'a str) -> &'a str {
        match (&self.authorizer, caller) {
            (Some(authorizer), Some(caller)) => {
                authorizer.strip_stream_name(&caller.principal, stored)
            }
            _ => stored,
        }
    }

    fn allow(
        &self,
        caller: &Option<Caller>,
        op: Operation,
        streams: &[String],
    ) -> Result<(), Box<Response>> {
        let (Some(authorizer), Some(caller)) = (&self.authorizer, caller) else {
            return Ok(());
        };
        if streams.is_empty() {
            return caller.authorize(authorizer, op, None);
        }
        streams
            .iter()
            .try_for_each(|stream| caller.authorize(authorizer, op, Some(stream)))
    }

    fn allowed(&self, caller: &Option<Caller>, stream: &str) -> bool {
        match (&self.authorizer, caller) {
            (Some(authorizer), Some(caller)) => caller
                .authorize(authorizer, Operation::Read, Some(stream))
                .is_ok(),
            _ => true,
        }
    }

    async fn member_streams(
        &self,
        caller: &Option<Caller>,
        group: &str,
        member: &str,
    ) -> Result<(), Box<Response>> {
        if self.authorizer.is_none() {
            return Ok(());
        }
        let subscription = self
            .groups
            .subscription(group, member)
            .await
            .map_err(|error| Box::new(group_error(error)))?;
        self.allow(caller, Operation::Read, &subscription)
    }
}

async fn list(State(state): State<GroupState>, headers: HeaderMap) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    if let Err(response) = state.allow(&caller, Operation::StreamInspect, &[]) {
        return *response;
    }
    let groups: Vec<Value> = state
        .groups
        .list()
        .await
        .into_iter()
        .map(|group| json!({ "group": group.group_id, "state": group.state.as_str() }))
        .collect();
    ok(json!({ "groups": groups }))
}

async fn describe(
    State(state): State<GroupState>,
    Path(group): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    if let Err(response) = state.allow(&caller, Operation::StreamInspect, &[]) {
        return *response;
    }
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    match state.groups.describe(&group).await {
        Ok(described) => ok(description_json(&state, &caller, described)),
        Err(error) => group_error(error),
    }
}

async fn join(
    State(state): State<GroupState>,
    Path(group): Path<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    let body = match object(&body) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let subscription = match strings(&body, "subscription") {
        Ok(Some(subscription)) => subscription,
        Ok(None) => return bad_request("subscription is required"),
        Err(response) => return *response,
    };
    let subscription = match state.resolve(&caller, &subscription) {
        Ok(subscription) => subscription,
        Err(response) => return *response,
    };
    if let Err(response) = state.allow(&caller, Operation::Read, &subscription) {
        return *response;
    }
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    let input = match (
        string(&body, "memberId"),
        string(&body, "instanceId"),
        string(&body, "clientId"),
        integer(&body, "sessionTimeoutMs"),
        integer(&body, "rebalanceTimeoutMs"),
    ) {
        (Ok(member_id), Ok(instance_id), Ok(client_id), Ok(session), Ok(rebalance)) => JoinInput {
            group_id: group.clone(),
            member_id: member_id.unwrap_or_default(),
            instance_id,
            client_id: client_id.unwrap_or_else(|| "pico".to_owned()),
            membership: Membership::Subscribed(subscription),
            session_timeout_ms: session.unwrap_or(30_000),
            rebalance_timeout_ms: rebalance.unwrap_or(0),
            require_known_member_id: false,
        },
        (Err(response), ..)
        | (_, Err(response), ..)
        | (_, _, Err(response), ..)
        | (_, _, _, Err(response), _)
        | (_, _, _, _, Err(response)) => return *response,
    };
    let outcome = state.groups.join(input).await;
    match outcome.result {
        Ok(Joined::Subscribed {
            assignment,
            members,
        }) => ok(json!({
            "memberId": outcome.member_id,
            "generation": outcome.generation,
            "assignment": stripped(&state, &caller, &assignment),
            "members": members,
        })),
        Ok(Joined::Client { .. }) => group_error(GroupError::InconsistentProtocol),
        Err(error) => group_error(error),
    }
}

async fn assignment(
    State(state): State<GroupState>,
    Path((group, member)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    let Some(generation) = query_param(&uri, "generation").and_then(|g| g.parse::<i32>().ok())
    else {
        return bad_request("generation query parameter is required");
    };
    let instance_id = query_param(&uri, "instanceId");
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    if let Err(response) = state.member_streams(&caller, &group, &member).await {
        return *response;
    }
    let fence = MemberFence {
        generation,
        member_id: &member,
        instance_id: instance_id.as_deref(),
    };
    match state.groups.assignment(&group, fence).await {
        Ok(assignment) => ok(json!({
            "generation": generation,
            "assignment": stripped(&state, &caller, &assignment),
        })),
        Err(error) => group_error(error),
    }
}

async fn heartbeat(
    State(state): State<GroupState>,
    Path((group, member)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    let body = match object(&body) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let (generation, instance_id) =
        match (integer(&body, "generation"), string(&body, "instanceId")) {
            (Ok(Some(generation)), Ok(instance_id)) => (generation, instance_id),
            (Ok(None), _) => return bad_request("generation is required"),
            (Err(response), _) | (_, Err(response)) => return *response,
        };
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    if let Err(response) = state.member_streams(&caller, &group, &member).await {
        return *response;
    }
    let fence = MemberFence {
        generation,
        member_id: &member,
        instance_id: instance_id.as_deref(),
    };
    match state.groups.heartbeat(&group, fence).await {
        Ok(()) => no_content(),
        Err(error) => group_error(error),
    }
}

async fn leave(
    State(state): State<GroupState>,
    Path((group, member)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    if let Err(response) = state.member_streams(&caller, &group, &member).await {
        return *response;
    }
    let instance_id = query_param(&uri, "instanceId");
    match state
        .groups
        .leave(&group, &[(member, instance_id)])
        .await
        .pop()
        .expect("one result per member")
    {
        Ok(()) => no_content(),
        Err(error) => group_error(error),
    }
}

async fn commit(
    State(state): State<GroupState>,
    Path(group): Path<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    let body = match object(&body) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let Some(Value::Object(offsets)) = body.get("offsets") else {
        return bad_request("offsets must be an object keyed by stream");
    };
    let mut commits = Vec::with_capacity(offsets.len());
    for (stream, value) in offsets {
        let Some(entry) = value.as_object() else {
            return bad_request("each offset must be an object");
        };
        let (position, metadata) = match (entry.get("position"), string(entry, "metadata")) {
            (Some(Value::Number(position)), Ok(metadata)) => match position.as_u64() {
                Some(position) => (position, metadata),
                None => return bad_request("position must be a non-negative integer"),
            },
            (_, Err(response)) => return *response,
            _ => return bad_request("position must be a non-negative integer"),
        };
        commits.push(OffsetCommit {
            stream: stream.clone(),
            value: CommittedOffset { position, metadata },
        });
    }
    let streams: Vec<String> = commits.iter().map(|c| c.stream.clone()).collect();
    let streams = match state.resolve(&caller, &streams) {
        Ok(streams) => streams,
        Err(response) => return *response,
    };
    if let Err(response) = state.allow(&caller, Operation::Read, &streams) {
        return *response;
    }
    for (commit, stream) in commits.iter_mut().zip(streams) {
        commit.stream = stream;
    }
    let (member_id, generation, instance_id) = match (
        string(&body, "memberId"),
        integer(&body, "generation"),
        string(&body, "instanceId"),
    ) {
        (Ok(member_id), Ok(generation), Ok(instance_id)) => (member_id, generation, instance_id),
        (Err(response), ..) | (_, Err(response), _) | (_, _, Err(response)) => return *response,
    };
    let fence = match (&member_id, generation) {
        (Some(member_id), Some(generation)) => Some(MemberFence {
            generation,
            member_id,
            instance_id: instance_id.as_deref(),
        }),
        (None, None) => None,
        _ => return bad_request("memberId and generation go together"),
    };
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    match state.groups.commit_offsets(&group, fence, &commits).await {
        Ok(()) => no_content(),
        Err(error) => group_error(error),
    }
}

async fn fetch(
    State(state): State<GroupState>,
    Path(group): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    let requested = query_params(&uri, "stream");
    let requested = match state.resolve(&caller, &requested) {
        Ok(requested) => requested,
        Err(response) => return *response,
    };
    if !requested.is_empty()
        && let Err(response) = state.allow(&caller, Operation::Read, &requested)
    {
        return *response;
    }
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    let offsets = match state
        .groups
        .fetch_offsets(
            &group,
            (!requested.is_empty()).then_some(requested.as_slice()),
        )
        .await
    {
        Ok(offsets) => offsets,
        Err(error) => return group_error(error),
    };
    let offsets: Map<String, Value> = offsets
        .into_iter()
        .filter(|(stream, _)| state.allowed(&caller, stream))
        .map(|(stream, value)| {
            (
                state.strip(&caller, &stream).to_owned(),
                json!({ "position": value.position, "metadata": value.metadata }),
            )
        })
        .collect();
    ok(json!({ "offsets": offsets }))
}

fn description_json(
    state: &GroupState,
    caller: &Option<Caller>,
    described: GroupDescription,
) -> Value {
    let members: Vec<Value> = described
        .members
        .into_iter()
        .map(|member| {
            let mut json = json!({
                "memberId": member.member_id,
                "instanceId": member.instance_id,
                "clientId": member.client_id,
            });
            if let MemberRole::Subscribed {
                subscription,
                assignment,
            } = member.role
            {
                json["subscription"] = stripped(state, caller, &subscription).into();
                json["assignment"] = stripped(state, caller, &assignment).into();
            }
            json
        })
        .collect();
    let mut json = json!({
        "group": described.group_id,
        "state": described.state.as_str(),
        "generation": described.generation,
        "members": members,
    });
    if !described.protocol_type.is_empty() {
        json["protocolType"] = described.protocol_type.into();
    }
    json
}

fn stripped(state: &GroupState, caller: &Option<Caller>, streams: &[String]) -> Vec<String> {
    streams
        .iter()
        .map(|stream| state.strip(caller, stream).to_owned())
        .collect()
}

fn object(body: &Bytes) -> Result<Map<String, Value>, Box<Response>> {
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(object)) => Ok(object),
        _ => Err(rejected("expected a JSON object")),
    }
}

fn string(body: &Map<String, Value>, key: &str) -> Result<Option<String>, Box<Response>> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(rejected(&format!("{key} must be a string"))),
    }
}

fn integer(body: &Map<String, Value>, key: &str) -> Result<Option<i32>, Box<Response>> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| rejected(&format!("{key} must be a 32-bit integer"))),
        Some(_) => Err(rejected(&format!("{key} must be an integer"))),
    }
}

fn strings(body: &Map<String, Value>, key: &str) -> Result<Option<Vec<String>>, Box<Response>> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| rejected(&format!("{key} must be an array of strings")))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Some(_) => Err(rejected(&format!("{key} must be an array of strings"))),
    }
}

fn ok(body: Value) -> Response {
    let mut response = base_response(200);
    set_header(&mut response, header::CONTENT_TYPE.as_str(), CT_JSON);
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    *response.body_mut() = axum::body::Body::from(serde_json::to_vec(&body).expect("json"));
    response
}

fn no_content() -> Response {
    let mut response = base_response(StatusCode::NO_CONTENT.as_u16());
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    response
}

fn bad_request(message: &str) -> Response {
    error(400, E_BAD_REQUEST, message, None)
}

fn rejected(message: &str) -> Box<Response> {
    Box::new(bad_request(message))
}

fn group_error(err: GroupError) -> Response {
    let (status, code) = match err {
        GroupError::CoordinatorNotAvailable | GroupError::NotCoordinator => {
            (503, E_COORDINATOR_UNAVAILABLE)
        }
        GroupError::IllegalGeneration => (409, E_ILLEGAL_GENERATION),
        GroupError::InconsistentProtocol => (409, E_INCONSISTENT_PROTOCOL),
        GroupError::UnknownMember => (404, E_UNKNOWN_MEMBER),
        GroupError::RebalanceInProgress => (409, E_REBALANCE_IN_PROGRESS),
        GroupError::InvalidRequest | GroupError::MemberIdRequired => (400, E_BAD_REQUEST),
        GroupError::Storage => (500, E_DURABILITY),
        GroupError::GroupNotFound => (404, E_NOT_FOUND),
        GroupError::CapacityExceeded => (429, E_CAPACITY_EXCEEDED),
        GroupError::FencedInstance => (403, E_FENCED),
    };
    error(status, code, &err.to_string(), None)
}
