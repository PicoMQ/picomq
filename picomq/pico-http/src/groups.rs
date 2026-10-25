use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::{get, post, put};
use picomq_auth::{Audience, AuthError, Authorizer, Operation, Resource, Scope};
use picomq_protocol::groups::{
    self as wire, AssignmentResponse, CommitRequest, GroupListing, HeartbeatRequest, JoinRequest,
    JoinResponse, OffsetsResponse, Q_GENERATION, Q_INSTANCE_ID, Q_STREAM,
};
use picomq_protocol::pico::{
    CT_JSON, E_BAD_REQUEST, E_CAPACITY_EXCEEDED, E_CONFLICT, E_COORDINATOR_UNAVAILABLE,
    E_DURABILITY, E_FENCED, E_ILLEGAL_GENERATION, E_INCONSISTENT_PROTOCOL, E_NOT_FOUND,
    E_PERMISSION_DENIED, E_REBALANCE_IN_PROGRESS, E_UNKNOWN_MEMBER,
};
use picomq_server::ownership::OwnershipService;
use picomq_server::{
    CommittedOffset, GroupCoordinator, GroupDescription, GroupError, JoinInput, Joined,
    MemberFence, MemberRole, Membership, OffsetCommit, Partition, Topic, Topics,
};

use crate::auth::{Caller, authenticate};
use crate::http::{base_response, query_param, query_params, set_header};
use crate::pico::{error, service_error_response};
use crate::route::{RoutingMode, owner_redirect};

#[derive(Clone)]
pub struct GroupState {
    pub groups: Arc<GroupCoordinator>,
    pub topics: Arc<Topics>,
    pub ownership: Arc<dyn OwnershipService>,
    pub mode: RoutingMode,
    pub authorizer: Option<Arc<Authorizer>>,
}

pub fn router(state: GroupState, max_request_size: usize) -> Router {
    Router::new()
        .route("/_groups", get(list))
        .route("/_groups/{group}", get(describe).delete(remove))
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
            &GroupCoordinator::log(group),
        )
        .await
    }

    async fn topic(&self, name: &str) -> Result<Option<Topic>, Box<Response>> {
        match self.topics.get(name).await {
            Ok(Some(topic)) if topic.partitions > 1 => Err(Box::new(error(
                400,
                E_BAD_REQUEST,
                &format!(
                    "{name} has {} partitions, groups over HTTP need one",
                    topic.partitions
                ),
                None,
            ))),
            Ok(topic) => Ok(topic),
            Err(e) => Err(Box::new(service_error_response(e))),
        }
    }

    async fn partitions(&self, names: &[String]) -> Result<Vec<Partition>, Box<Response>> {
        let mut partitions = Vec::with_capacity(names.len());

        for name in names {
            let Some(topic) = self.topic(name).await? else {
                return Err(Box::new(error(
                    404,
                    E_NOT_FOUND,
                    &format!("no such stream {name}"),
                    None,
                )));
            };
            partitions.push(Partition::new(topic.id, 0));
        }

        Ok(partitions)
    }
}

async fn list(State(state): State<GroupState>, headers: HeaderMap) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    let groups = state
        .groups
        .list(scope(&caller))
        .iter()
        .map(|group| strip_group(&caller, group).to_owned())
        .collect();
    ok(GroupListing { groups }.encode())
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
    let group = match resolve_group(&caller, &group) {
        Ok(group) => group,
        Err(response) => return *response,
    };
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    match state.groups.describe(scope(&caller), &group).await {
        Ok(described) => ok(description(&caller, described).encode()),
        Err(error) => refused(&caller, error),
    }
}

async fn remove(
    State(state): State<GroupState>,
    Path(group): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let caller = match state.admit(&headers).await {
        Ok(caller) => caller,
        Err(response) => return *response,
    };
    let group = match resolve_group(&caller, &group) {
        Ok(group) => group,
        Err(response) => return *response,
    };
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    match state.groups.delete(scope(&caller), &group).await {
        Ok(()) => no_content(),
        Err(error) => refused(&caller, error),
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
    let request = match JoinRequest::decode(group, &body) {
        Ok(request) => request,
        Err(error) => return bad_request(&error.message),
    };
    let group = match resolve_group(&caller, &request.group) {
        Ok(group) => group,
        Err(response) => return *response,
    };
    let subscription = match resolve_streams(&caller, &request.subscription) {
        Ok(subscription) => subscription,
        Err(response) => return *response,
    };
    if let Err(response) = allow_streams(&caller, Operation::Read, &subscription) {
        return *response;
    }
    let mut subscribed = Vec::with_capacity(subscription.len());
    for name in subscription {
        match state.topic(&name).await {
            Ok(Some(topic)) => subscribed.push(topic.name.spelling().to_owned()),
            Ok(None) => subscribed.push(name),
            Err(response) => return *response,
        }
    }
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    let outcome = state
        .groups
        .join(
            scope(&caller),
            JoinInput {
                group_id: group,
                member_id: request.member_id.unwrap_or_default(),
                instance_id: request.instance_id,
                client_id: request.client_id.unwrap_or_else(|| "pico".to_owned()),
                membership: Membership::Subscribed(subscribed),
                session_timeout_ms: request
                    .session_timeout_ms
                    .map_or(wire::DEFAULT_SESSION_TIMEOUT_MS as i32, saturating_i32),
                rebalance_timeout_ms: request.rebalance_timeout_ms.map_or(0, saturating_i32),
                require_known_member_id: false,
            },
        )
        .await;
    match outcome.result {
        Ok(Joined::Subscribed {
            assignment,
            members,
        }) => ok(JoinResponse {
            member_id: outcome.member_id,
            generation: outcome.generation,
            assignment: stripped(&caller, &assignment),
            members,
        }
        .encode()),
        Ok(Joined::Client { .. }) => group_error(GroupError::InconsistentProtocol),
        Err(error) => refused(&caller, error),
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
    let generation = match param(&uri, Q_GENERATION) {
        Ok(raw) => raw.and_then(|g| g.parse::<i32>().ok()),
        Err(response) => return *response,
    };
    let Some(generation) = generation else {
        return bad_request("generation query parameter is required");
    };
    let instance_id = match param(&uri, Q_INSTANCE_ID) {
        Ok(instance_id) => instance_id,
        Err(response) => return *response,
    };
    let group = match resolve_group(&caller, &group) {
        Ok(group) => group,
        Err(response) => return *response,
    };
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    let fence = MemberFence {
        generation,
        member_id: &member,
        instance_id: instance_id.as_deref(),
    };
    match state.groups.assignment(scope(&caller), &group, fence).await {
        Ok(assignment) => ok(AssignmentResponse {
            generation,
            assignment: stripped(&caller, &assignment),
        }
        .encode()),
        Err(error) => refused(&caller, error),
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
    let request = match HeartbeatRequest::decode(group, member, &body) {
        Ok(request) => request,
        Err(error) => return bad_request(&error.message),
    };
    let group = match resolve_group(&caller, &request.group) {
        Ok(group) => group,
        Err(response) => return *response,
    };
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    let fence = MemberFence {
        generation: request.fence.generation,
        member_id: &request.fence.member_id,
        instance_id: request.fence.instance_id.as_deref(),
    };
    match state.groups.heartbeat(scope(&caller), &group, fence).await {
        Ok(()) => no_content(),
        Err(error) => refused(&caller, error),
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
    let group = match resolve_group(&caller, &group) {
        Ok(group) => group,
        Err(response) => return *response,
    };
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    let instance_id = match param(&uri, Q_INSTANCE_ID) {
        Ok(instance_id) => instance_id,
        Err(response) => return *response,
    };
    match state
        .groups
        .leave(scope(&caller), &group, &[(member, instance_id)])
        .await
        .and_then(|mut results| results.pop().expect("one result per member"))
    {
        Ok(()) => no_content(),
        Err(error) => refused(&caller, error),
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
    let request = match CommitRequest::decode(group, &body) {
        Ok(request) => request,
        Err(error) => return bad_request(&error.message),
    };
    let group = match resolve_group(&caller, &request.group) {
        Ok(group) => group,
        Err(response) => return *response,
    };
    let streams: Vec<String> = request.offsets.keys().cloned().collect();
    let streams = match resolve_streams(&caller, &streams) {
        Ok(streams) => streams,
        Err(response) => return *response,
    };
    if let Err(response) = allow_streams(&caller, Operation::Read, &streams) {
        return *response;
    }
    let partitions = match state.partitions(&streams).await {
        Ok(partitions) => partitions,
        Err(response) => return *response,
    };
    let commits: Vec<OffsetCommit> = request
        .offsets
        .into_values()
        .zip(partitions)
        .map(|(offset, partition)| OffsetCommit {
            partition,
            value: CommittedOffset {
                position: offset.position,
                leader_epoch: None,
                metadata: offset.metadata,
            },
        })
        .collect();
    let fence = request.fence.as_ref().map(|fence| MemberFence {
        generation: fence.generation,
        member_id: &fence.member_id,
        instance_id: fence.instance_id.as_deref(),
    });
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    match state
        .groups
        .commit_offsets(scope(&caller), &group, fence, &commits)
        .await
    {
        Ok(()) => no_content(),
        Err(error) => refused(&caller, error),
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
    let group = match resolve_group(&caller, &group) {
        Ok(group) => group,
        Err(response) => return *response,
    };
    let requested = match params(&uri, Q_STREAM) {
        Ok(requested) => requested,
        Err(response) => return *response,
    };
    let requested = match resolve_streams(&caller, &requested) {
        Ok(requested) => requested,
        Err(response) => return *response,
    };
    if let Err(response) = allow_streams(&caller, Operation::Read, &requested) {
        return *response;
    }
    let partitions = match state.partitions(&requested).await {
        Ok(partitions) => partitions,
        Err(response) => return *response,
    };
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }

    let offsets = match state
        .groups
        .fetch_offsets(
            scope(&caller),
            &group,
            (!requested.is_empty()).then_some(partitions.as_slice()),
        )
        .await
    {
        Ok(offsets) => offsets,
        Err(error) => return refused(&caller, error),
    };

    let mut named = BTreeMap::new();
    for (partition, value) in offsets {
        let Ok(Some(topic)) = state.topics.by_id(&partition.topic).await else {
            continue;
        };
        if topic.partitions > 1 {
            continue;
        }

        let stream = topic.name.spelling();
        if !allowed(&caller, stream) {
            continue;
        }
        named.insert(
            strip_stream(&caller, stream).to_owned(),
            wire::CommittedOffset {
                position: value.position,
                metadata: value.metadata,
            },
        );
    }
    ok(OffsetsResponse { offsets: named }.encode())
}

fn description(caller: &Option<Caller>, described: GroupDescription) -> wire::GroupDescription {
    let members = described
        .members
        .into_iter()
        .map(|member| {
            let (subscription, assignment) = match member.role {
                MemberRole::Subscribed {
                    subscription,
                    assignment,
                } => (
                    Some(stripped(caller, &subscription)),
                    Some(stripped(caller, &assignment)),
                ),
                MemberRole::Client { .. } => (None, None),
            };
            wire::MemberDescription {
                member_id: member.member_id,
                instance_id: member.instance_id,
                client_id: member.client_id,
                subscription,
                assignment,
            }
        })
        .collect();
    wire::GroupDescription {
        group: strip_group(caller, &described.group_id).to_owned(),
        state: described.state.as_str().to_owned(),
        generation: described.generation,
        protocol_type: (!described.protocol_type.is_empty()).then_some(described.protocol_type),
        members,
    }
}

fn stripped(caller: &Option<Caller>, streams: &[String]) -> Vec<String> {
    streams
        .iter()
        .map(|stream| strip_stream(caller, stream).to_owned())
        .collect()
}

fn scope(caller: &Option<Caller>) -> Option<&Scope> {
    caller.as_ref().map(|caller| &caller.principal.scope)
}

fn resolve_streams(
    caller: &Option<Caller>,
    names: &[String],
) -> Result<Vec<String>, Box<Response>> {
    match caller {
        Some(caller) => names
            .iter()
            .map(|name| caller.resolve(Resource::Stream, name))
            .collect(),
        None => Ok(names.to_vec()),
    }
}

fn resolve_group(caller: &Option<Caller>, client: &str) -> Result<String, Box<Response>> {
    match caller {
        Some(caller) => caller.resolve(Resource::Group, client),
        None => Ok(client.to_owned()),
    }
}

fn strip_stream<'a>(caller: &Option<Caller>, stored: &'a str) -> &'a str {
    match caller {
        Some(caller) => caller.principal.scope.strip(Resource::Stream, stored),
        None => stored,
    }
}

fn strip_group<'a>(caller: &Option<Caller>, stored: &'a str) -> &'a str {
    match caller {
        Some(caller) => caller.principal.scope.strip(Resource::Group, stored),
        None => stored,
    }
}

fn allow_streams(
    caller: &Option<Caller>,
    op: Operation,
    streams: &[String],
) -> Result<(), Box<Response>> {
    match caller {
        Some(caller) => streams
            .iter()
            .try_for_each(|stream| caller.authorize(op, Some(stream))),
        None => Ok(()),
    }
}

fn allowed(caller: &Option<Caller>, stream: &str) -> bool {
    caller
        .as_ref()
        .is_none_or(|caller| caller.principal.scope.allows(Operation::Read, Some(stream)))
}

fn refused(caller: &Option<Caller>, error: GroupError) -> Response {
    match (error, caller) {
        (GroupError::Unauthorized, Some(caller)) => *caller.demote(AuthError::Denied),
        _ => group_error(error),
    }
}

fn saturating_i32(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn ok(body: Bytes) -> Response {
    let mut response = base_response(200);
    set_header(&mut response, header::CONTENT_TYPE.as_str(), CT_JSON);
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    *response.body_mut() = axum::body::Body::from(body);
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

fn param(uri: &Uri, name: &str) -> Result<Option<String>, Box<Response>> {
    query_param(uri, name).map_err(|error| Box::new(service_error_response(error)))
}

fn params(uri: &Uri, name: &str) -> Result<Vec<String>, Box<Response>> {
    query_params(uri, name).map_err(|error| Box::new(service_error_response(error)))
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
        GroupError::NonEmpty => (409, E_CONFLICT),
        GroupError::CapacityExceeded => (429, E_CAPACITY_EXCEEDED),
        GroupError::FencedInstance => (403, E_FENCED),
        GroupError::Superseded => (409, E_CONFLICT),
        GroupError::Unauthorized => (403, E_PERMISSION_DENIED),
    };
    error(status, code, &err.to_string(), None)
}
