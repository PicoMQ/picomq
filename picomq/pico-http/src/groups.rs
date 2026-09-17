use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::{get, post, put};
use picomq_auth::{Audience, Authorizer, Operation};
use picomq_protocol::groups::{
    self as wire, AssignmentResponse, CommitRequest, GroupListing, GroupSummary, HeartbeatRequest,
    JoinRequest, JoinResponse, OffsetsResponse, Q_GENERATION, Q_INSTANCE_ID, Q_STREAM,
};
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
    let groups = state
        .groups
        .list()
        .await
        .into_iter()
        .map(|group| GroupSummary {
            group: group.group_id,
            state: group.state.as_str().to_owned(),
        })
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
    if let Err(response) = state.allow(&caller, Operation::StreamInspect, &[]) {
        return *response;
    }
    if let Some(response) = state.redirect(&uri, &group).await {
        return response;
    }
    match state.groups.describe(&group).await {
        Ok(described) => ok(description(&state, &caller, described).encode()),
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
    let request = match JoinRequest::decode(group, &body) {
        Ok(request) => request,
        Err(error) => return bad_request(&error.message),
    };
    let subscription = match state.resolve(&caller, &request.subscription) {
        Ok(subscription) => subscription,
        Err(response) => return *response,
    };
    if let Err(response) = state.allow(&caller, Operation::Read, &subscription) {
        return *response;
    }
    if let Some(response) = state.redirect(&uri, &request.group).await {
        return response;
    }
    let outcome = state
        .groups
        .join(JoinInput {
            group_id: request.group,
            member_id: request.member_id.unwrap_or_default(),
            instance_id: request.instance_id,
            client_id: request.client_id.unwrap_or_else(|| "pico".to_owned()),
            membership: Membership::Subscribed(subscription),
            session_timeout_ms: request
                .session_timeout_ms
                .map_or(wire::DEFAULT_SESSION_TIMEOUT_MS as i32, saturating_i32),
            rebalance_timeout_ms: request.rebalance_timeout_ms.map_or(0, saturating_i32),
            require_known_member_id: false,
        })
        .await;
    match outcome.result {
        Ok(Joined::Subscribed {
            assignment,
            members,
        }) => ok(JoinResponse {
            member_id: outcome.member_id,
            generation: outcome.generation,
            assignment: stripped(&state, &caller, &assignment),
            members,
        }
        .encode()),
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
    let Some(generation) = query_param(&uri, Q_GENERATION).and_then(|g| g.parse::<i32>().ok())
    else {
        return bad_request("generation query parameter is required");
    };
    let instance_id = query_param(&uri, Q_INSTANCE_ID);
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
        Ok(assignment) => ok(AssignmentResponse {
            generation,
            assignment: stripped(&state, &caller, &assignment),
        }
        .encode()),
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
    let request = match HeartbeatRequest::decode(group, member, &body) {
        Ok(request) => request,
        Err(error) => return bad_request(&error.message),
    };
    let group = &request.group;
    if let Some(response) = state.redirect(&uri, group).await {
        return response;
    }
    if let Err(response) = state
        .member_streams(&caller, group, &request.fence.member_id)
        .await
    {
        return *response;
    }
    let fence = MemberFence {
        generation: request.fence.generation,
        member_id: &request.fence.member_id,
        instance_id: request.fence.instance_id.as_deref(),
    };
    match state.groups.heartbeat(group, fence).await {
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
    let instance_id = query_param(&uri, Q_INSTANCE_ID);
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
    let request = match CommitRequest::decode(group, &body) {
        Ok(request) => request,
        Err(error) => return bad_request(&error.message),
    };
    let streams: Vec<String> = request.offsets.keys().cloned().collect();
    let streams = match state.resolve(&caller, &streams) {
        Ok(streams) => streams,
        Err(response) => return *response,
    };
    if let Err(response) = state.allow(&caller, Operation::Read, &streams) {
        return *response;
    }
    let commits: Vec<OffsetCommit> = request
        .offsets
        .into_values()
        .zip(streams)
        .map(|(offset, stream)| OffsetCommit {
            stream,
            value: CommittedOffset {
                position: offset.position,
                metadata: offset.metadata,
            },
        })
        .collect();
    let fence = request.fence.as_ref().map(|fence| MemberFence {
        generation: fence.generation,
        member_id: &fence.member_id,
        instance_id: fence.instance_id.as_deref(),
    });
    if let Some(response) = state.redirect(&uri, &request.group).await {
        return response;
    }
    match state
        .groups
        .commit_offsets(&request.group, fence, &commits)
        .await
    {
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
    let requested = query_params(&uri, Q_STREAM);
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
    let offsets = offsets
        .into_iter()
        .filter(|(stream, _)| state.allowed(&caller, stream))
        .map(|(stream, value)| {
            (
                state.strip(&caller, &stream).to_owned(),
                wire::CommittedOffset {
                    position: value.position,
                    metadata: value.metadata,
                },
            )
        })
        .collect();
    ok(OffsetsResponse { offsets }.encode())
}

fn description(
    state: &GroupState,
    caller: &Option<Caller>,
    described: GroupDescription,
) -> wire::GroupDescription {
    let members = described
        .members
        .into_iter()
        .map(|member| {
            let (subscription, assignment) = match member.role {
                MemberRole::Subscribed {
                    subscription,
                    assignment,
                } => (
                    Some(stripped(state, caller, &subscription)),
                    Some(stripped(state, caller, &assignment)),
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
        group: described.group_id,
        state: described.state.as_str().to_owned(),
        generation: described.generation,
        protocol_type: (!described.protocol_type.is_empty()).then_some(described.protocol_type),
        members,
    }
}

fn stripped(state: &GroupState, caller: &Option<Caller>, streams: &[String]) -> Vec<String> {
    streams
        .iter()
        .map(|stream| state.strip(caller, stream).to_owned())
        .collect()
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
