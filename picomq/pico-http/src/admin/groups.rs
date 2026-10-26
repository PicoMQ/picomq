use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use picomq_auth::{AuthError, AuthPrincipal, Operation, Scope};
use picomq_common::name::canonical;
use picomq_metadata::MetadataState;
use picomq_server::{
    CommittedOffset, GroupCoordinator, GroupError, ListedGroup, MemberDescription, MemberRole,
    OffsetCommit, Page, Partition, Topic, TopicId,
};
use s3stream::StreamState;
use serde_json::{Value, json};

use super::offsets;
use super::{AdminState, allowed, missing_topic};
use crate::common::{auth_error, authenticate, error_response, read_json, service_error};
use crate::paging::Paging;

const MAX_LISTED: usize = 1_000;
const MAX_BODY: usize = 64 * 1024;

pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/admin/groups", get(list))
        .route("/admin/groups/{group}", get(detail).delete(delete_group))
        .route(
            "/admin/groups/{group}/offsets",
            get(offsets).delete(delete_offsets),
        )
        .route("/admin/groups/{group}/offsets/reset", post(reset))
        .route("/admin/groups/{group}/members", get(members))
        .route("/admin/nodes/{id}/groups", post(listed_groups))
}

async fn list(State(state): State<AdminState>, uri: Uri, headers: HeaderMap) -> Response {
    let principal = match authenticate(state.authorizer.as_deref(), &headers).await {
        Ok(principal) => principal,
        Err(response) => return *response,
    };
    let paging = match Paging::from_query(&uri, &["prefix"]) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let groups = state.node.groups();
    let page = groups.list(
        scope(principal.as_ref()),
        paging.filter("prefix").unwrap_or(""),
        paging.after.as_deref(),
        paging.limit,
    );

    let view = state.node.views().load();
    let local = state.node.config().node_id;
    let mut coordinators = BTreeMap::new();
    let mut by_node: BTreeMap<i32, Vec<String>> = BTreeMap::new();
    for (group_id, _) in &page.items {
        let coordinator = coordinator_of(&state, &view.state, group_id);
        if let Some(node) = coordinator {
            by_node.entry(node).or_default().push(group_id.clone());
        }
        coordinators.insert(group_id.clone(), coordinator);
    }

    let mut listed: BTreeMap<String, Value> = BTreeMap::new();
    if let Some(ids) = by_node.remove(&local) {
        for group in groups.listed_groups(scope(principal.as_ref()), &ids).await {
            listed.insert(group.group_id.clone(), listed_json(&group));
        }
    }
    listed.extend(remote_listed_groups(&state, &headers, &view.state, by_node).await);

    paging.reply(page, |group_id, _| {
        let coordinator = coordinators.get(group_id).copied().flatten();
        let mut row = match (listed.get(group_id), coordinator) {
            (Some(group), _) => group.clone(),
            (None, None) => listed_json(&ListedGroup::unloaded(group_id)),
            (None, Some(_)) => json!({ "groupId": group_id, "state": null }),
        };
        row["coordinatorNodeId"] = json!(coordinator);
        row["current"] = json!(listed.contains_key(group_id) || coordinator.is_none());
        row
    })
}

async fn listed_groups(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
    request: Request,
) -> Response {
    if id != state.node.config().node_id {
        return state.forward_to(id, request).await;
    }

    let principal = match authenticate(state.authorizer.as_deref(), request.headers()).await {
        Ok(principal) => principal,
        Err(response) => return *response,
    };
    let body = match read_json(request, MAX_BODY).await {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let Some(ids) = body["groups"]
        .as_array()
        .and_then(|ids| {
            ids.iter()
                .map(|id| id.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
        })
        .filter(|ids| ids.len() <= MAX_LISTED)
    else {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("expected up to {MAX_LISTED} group ids as {{\"groups\": [..]}}"),
        );
    };

    let listed = state
        .node
        .groups()
        .listed_groups(scope(principal.as_ref()), &ids)
        .await;
    let groups: Vec<Value> = listed.iter().map(listed_json).collect();
    (StatusCode::OK, Json(json!({ "groups": groups }))).into_response()
}

async fn detail(
    State(state): State<AdminState>,
    Path(group): Path<String>,
    request: Request,
) -> Response {
    let (_, principal) = match at_coordinator(&state, &group, request, true).await {
        Ok(local) => local,
        Err(response) => return *response,
    };

    match state
        .node
        .groups()
        .listed_group(scope(principal.as_ref()), &group)
        .await
    {
        Ok((listed, committed)) => {
            let mut body = listed_json(&listed);
            body["coordinatorNodeId"] = json!(state.node.config().node_id);
            body["committedPartitions"] = json!(committed);
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(error) => group_error(error),
    }
}

async fn members(
    State(state): State<AdminState>,
    Path(group): Path<String>,
    request: Request,
) -> Response {
    let (request, principal) = match at_coordinator(&state, &group, request, true).await {
        Ok(local) => local,
        Err(response) => return *response,
    };
    let paging = match Paging::from_query(request.uri(), &[]) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };

    match state
        .node
        .groups()
        .members(
            scope(principal.as_ref()),
            &group,
            paging.after.as_deref(),
            paging.limit,
        )
        .await
    {
        Ok(page) => paging.reply(page, |_, member| member_json(member)),
        Err(error) => group_error(error),
    }
}

async fn offsets(
    State(state): State<AdminState>,
    Path(group): Path<String>,
    request: Request,
) -> Response {
    let (request, principal) = match at_coordinator(&state, &group, request, true).await {
        Ok(local) => local,
        Err(response) => return *response,
    };
    let headers = request.headers().clone();
    let paging = match Paging::from_query(request.uri(), &["topic"]) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let filter = match paging.filter("topic") {
        Some(name) => match state.node.topics().get(name).await {
            Ok(Some(topic)) => Some(topic.id),
            Ok(None) => return missing_topic(name),
            Err(error) => return service_error(error),
        },
        None => None,
    };
    let Ok(after) = paging.after.as_deref().map(decode_cursor).transpose() else {
        return error_response(StatusCode::BAD_REQUEST, "the cursor is not valid");
    };

    let window = match state
        .node
        .groups()
        .fetch_offsets_after(
            scope(principal.as_ref()),
            &group,
            filter,
            after,
            paging.fetch_count(),
        )
        .await
    {
        Ok(window) => window,
        Err(error) => return group_error(error),
    };
    let mut topics: BTreeMap<TopicId, Option<Topic>> = BTreeMap::new();
    for (partition, _) in &window {
        if let Entry::Vacant(slot) = topics.entry(partition.topic) {
            let topic = state.node.topics().by_id(&partition.topic).await;
            slot.insert(topic.ok().flatten());
        }
    }
    let page = Page::scan(window, paging.limit, |partition, _| {
        topics
            .get(&partition.topic)
            .and_then(Option::as_ref)
            .is_some_and(|topic| allowed(principal.as_ref(), Operation::Head, topic))
    });

    let view = state.node.views().load();
    let rows: Vec<_> = page
        .items
        .iter()
        .filter_map(|(partition, _)| {
            let topic = topics.get(&partition.topic)?.as_ref()?;
            let row = offsets::row_of(&state, &view.state, topic, partition.index);
            Some((topic, partition.index, row))
        })
        .collect();
    let known = offsets::current(&state, &headers, &view.state, &rows, None).await;

    let page = Page {
        items: page
            .items
            .into_iter()
            .map(|(partition, offset)| (encode_cursor(&partition), (partition, offset)))
            .collect(),
        next: page.next.as_ref().map(encode_cursor),
    };
    paging.reply(page, |_, (partition, offset)| {
        let topic = topics.get(&partition.topic).and_then(Option::as_ref);
        let (latest, current) = known.get(partition).copied().unwrap_or((None, false));
        json!({
            "topic": topic.map(|topic| topic.name.spelling()),
            "partition": partition.index,
            "committedOffset": offset.position,
            "metadata": offset.metadata,
            "nextOffset": latest.map(|latest| latest.next),
            "lag": latest.map(|latest| latest.next.saturating_sub(offset.position)),
            "current": current,
        })
    })
}

async fn reset(
    State(state): State<AdminState>,
    Path(group): Path<String>,
    request: Request,
) -> Response {
    let (request, principal) = match at_coordinator(&state, &group, request, false).await {
        Ok(local) => local,
        Err(response) => return *response,
    };
    let headers = request.headers().clone();
    let body = match read_json(request, MAX_BODY).await {
        Ok(body) => body,
        Err(response) => return *response,
    };
    let Some(name) = body["topic"].as_str() else {
        return error_response(StatusCode::BAD_REQUEST, "topic is required");
    };
    let topic = match state.node.topics().get(name).await {
        Ok(Some(topic)) => topic,
        Ok(None) => return missing_topic(name),
        Err(error) => return service_error(error),
    };
    if !allowed(principal.as_ref(), Operation::Read, &topic) {
        return auth_error(&AuthError::Denied);
    }
    let Ok(target) = Target::parse(&body["to"]) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "to must be \"earliest\", \"latest\", {\"offset\": n} or {\"timestamp\": ms}",
        );
    };
    let indexes: Vec<u32> = match &body["partitions"] {
        Value::Null => (0..topic.partitions).collect(),
        value => match value.as_array().and_then(|indexes| {
            indexes
                .iter()
                .map(|index| index.as_u64().and_then(|index| u32::try_from(index).ok()))
                .collect::<Option<Vec<_>>>()
        }) {
            Some(indexes) if indexes.iter().all(|index| *index < topic.partitions) => indexes,
            _ => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "partitions must be indexes of the topic",
                );
            }
        },
    };

    let positions = match positions(&state, &headers, &topic, &indexes, target).await {
        Ok(positions) => positions,
        Err(response) => return *response,
    };
    let commits: Vec<OffsetCommit> = positions
        .iter()
        .map(|(index, position)| OffsetCommit {
            partition: Partition::new(topic.id, *index),
            value: CommittedOffset {
                position: *position,
                leader_epoch: None,
                metadata: None,
            },
        })
        .collect();

    match state
        .node
        .groups()
        .commit_offsets(scope(principal.as_ref()), &group, None, &commits)
        .await
    {
        Ok(()) => {
            let partitions: Vec<Value> = positions
                .iter()
                .map(|(index, offset)| json!({ "partition": index, "offset": offset }))
                .collect();
            (StatusCode::OK, Json(json!({ "partitions": partitions }))).into_response()
        }
        Err(GroupError::UnknownMember) => error_response(
            StatusCode::CONFLICT,
            "the group has active members, stop them before resetting offsets",
        ),
        Err(error) => group_error(error),
    }
}

async fn delete_offsets(
    State(state): State<AdminState>,
    Path(group): Path<String>,
    request: Request,
) -> Response {
    let (request, principal) = match at_coordinator(&state, &group, request, true).await {
        Ok(local) => local,
        Err(response) => return *response,
    };
    let paging = match Paging::from_query(request.uri(), &["topic"]) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let Some(name) = paging.filter("topic") else {
        return error_response(StatusCode::BAD_REQUEST, "topic is required");
    };
    let topic = match state.node.topics().get(name).await {
        Ok(Some(topic)) => topic,
        Ok(None) => return missing_topic(name),
        Err(error) => return service_error(error),
    };
    let groups = state.node.groups();
    let scope = scope(principal.as_ref());

    match groups.subscribed_topics(scope, &group).await {
        Ok(subscribed) if subscribed.contains(&canonical(topic.name.spelling())) => {
            return error_response(
                StatusCode::CONFLICT,
                format!("the group is subscribed to {}", topic.name.spelling()),
            );
        }
        Ok(_) => {}
        Err(error) => return group_error(error),
    }
    let partitions: Vec<Partition> = match groups
        .fetch_offsets_after(
            scope,
            &group,
            Some(topic.id),
            None,
            topic.partitions as usize,
        )
        .await
    {
        Ok(committed) => committed
            .into_iter()
            .map(|(partition, _)| partition)
            .collect(),
        Err(error) => return group_error(error),
    };

    match groups.delete_offsets(scope, &group, &partitions).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => group_error(error),
    }
}

async fn delete_group(
    State(state): State<AdminState>,
    Path(group): Path<String>,
    request: Request,
) -> Response {
    let (_, principal) = match at_coordinator(&state, &group, request, true).await {
        Ok(local) => local,
        Err(response) => return *response,
    };

    match state
        .node
        .groups()
        .delete(scope(principal.as_ref()), &group)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => group_error(error),
    }
}

#[derive(Debug, Clone, Copy)]
enum Target {
    Earliest,
    Latest,
    Offset(u64),
    Timestamp(i64),
}

impl Target {
    fn parse(to: &Value) -> Result<Self, ()> {
        match to {
            Value::String(to) if to == "earliest" => Ok(Self::Earliest),
            Value::String(to) if to == "latest" => Ok(Self::Latest),
            Value::Object(to) => match (to.get("offset"), to.get("timestamp")) {
                (Some(offset), None) => offset.as_u64().map(Self::Offset).ok_or(()),
                (None, Some(timestamp)) => timestamp.as_i64().map(Self::Timestamp).ok_or(()),
                _ => Err(()),
            },
            _ => Err(()),
        }
    }
}

async fn positions(
    state: &AdminState,
    headers: &HeaderMap,
    topic: &Topic,
    indexes: &[u32],
    target: Target,
) -> Result<Vec<(u32, u64)>, Box<Response>> {
    if let Target::Offset(offset) = target {
        return Ok(indexes.iter().map(|index| (*index, offset)).collect());
    }

    let view = state.node.views().load();
    let rows: Vec<_> = indexes
        .iter()
        .map(|index| {
            (
                topic,
                *index,
                offsets::row_of(state, &view.state, topic, *index),
            )
        })
        .collect();
    let timestamp = match target {
        Target::Timestamp(timestamp) => Some(timestamp),
        _ => None,
    };
    let known = offsets::current(state, headers, &view.state, &rows, timestamp).await;

    let mut positions = Vec::with_capacity(indexes.len());
    for index in indexes {
        let partition = Partition::new(topic.id, *index);
        let Some((Some(offsets), true)) = known.get(&partition).copied() else {
            return Err(Box::new(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("the owner of partition {index} did not answer, retry"),
            )));
        };
        let position = match target {
            Target::Earliest => offsets.start,
            Target::Latest => offsets.next,
            Target::Timestamp(_) => offsets.at_timestamp.unwrap_or(offsets.next),
            Target::Offset(offset) => offset,
        };
        positions.push((*index, position));
    }
    Ok(positions)
}

async fn at_coordinator(
    state: &AdminState,
    group: &str,
    request: Request,
    existing: bool,
) -> Result<(Request, Option<AuthPrincipal>), Box<Response>> {
    let principal = authenticate(state.authorizer.as_deref(), request.headers()).await?;
    if existing
        && !matches!(
            state
                .node
                .service()
                .stream_id(&GroupCoordinator::log(group)),
            Ok(Some(_))
        )
    {
        return Err(Box::new(group_error(GroupError::GroupNotFound)));
    }

    let coordinator = state
        .node
        .groups()
        .find_coordinator(scope(principal.as_ref()), group)
        .await
        .map_err(|error| Box::new(group_error(error)))?;
    if coordinator == state.node.config().node_id {
        return Ok((request, principal));
    }

    let view = state.node.views().load();
    if offsets::fenced(&view.state, coordinator) {
        return Err(Box::new(group_error(GroupError::CoordinatorNotAvailable)));
    }
    Err(Box::new(state.forward_to(coordinator, request).await))
}

async fn remote_listed_groups(
    state: &AdminState,
    headers: &HeaderMap,
    view: &MetadataState,
    by_node: BTreeMap<i32, Vec<String>>,
) -> BTreeMap<String, Value> {
    let requests = by_node
        .into_iter()
        .filter(|(node, _)| !offsets::fenced(view, *node))
        .map(|(node, ids)| async move {
            let path = format!("/admin/nodes/{node}/groups");
            let body = json!({ "groups": ids });
            let listed = state.post_json(node, &path, headers, &body).await;
            listed.map_or_else(Vec::new, |listed| {
                listed["groups"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|group| {
                        let id = group["groupId"].as_str()?.to_owned();
                        Some((id, group.clone()))
                    })
                    .collect()
            })
        });

    futures::future::join_all(requests)
        .await
        .into_iter()
        .flatten()
        .collect()
}

fn coordinator_of(state: &AdminState, view: &MetadataState, group: &str) -> Option<i32> {
    let stream_id = state
        .node
        .service()
        .stream_id(&GroupCoordinator::log(group))
        .ok()??;
    let row = view.streams().get(&stream_id)?;
    (row.state == StreamState::Opened).then_some(row.node_id)
}

fn listed_json(listed: &ListedGroup) -> Value {
    json!({
        "groupId": listed.group_id,
        "state": listed.state.as_str(),
        "protocolType": listed.protocol_type,
        "protocolName": listed.protocol_name,
        "generation": listed.generation,
        "members": listed.members,
    })
}

fn member_json(member: &MemberDescription) -> Value {
    let assignment = match &member.role {
        MemberRole::Subscribed { assignment, .. } => json!({ "streams": assignment }),
        MemberRole::Client {
            partitions: Some(partitions),
            ..
        } => json!({
            "topics": partitions
                .iter()
                .map(|(topic, partitions)| json!({ "topic": topic, "partitions": partitions }))
                .collect::<Vec<_>>(),
        }),
        MemberRole::Client { assignment, .. } => json!({ "bytes": assignment.len() }),
    };

    json!({
        "memberId": member.member_id,
        "instanceId": member.instance_id,
        "clientId": member.client_id,
        "clientHost": member.client_host,
        "assignment": assignment,
    })
}

fn encode_cursor(partition: &Partition) -> String {
    format!("{}:{}", partition.topic.hex(), partition.index)
}

fn decode_cursor(cursor: &str) -> Result<Partition, ()> {
    let (topic, index) = cursor.split_once(':').ok_or(())?;
    let topic = TopicId::from_hex(topic).ok_or(())?;
    let index = index.parse().map_err(|_| ())?;
    Ok(Partition::new(topic, index))
}

fn scope(principal: Option<&AuthPrincipal>) -> Option<&Scope> {
    principal.map(|principal| &principal.scope)
}

fn group_error(error: GroupError) -> Response {
    let status = match error {
        GroupError::GroupNotFound => StatusCode::NOT_FOUND,
        GroupError::Unauthorized => return auth_error(&AuthError::Denied),
        GroupError::InvalidRequest => StatusCode::BAD_REQUEST,
        GroupError::CoordinatorNotAvailable
        | GroupError::NotCoordinator
        | GroupError::CapacityExceeded => StatusCode::SERVICE_UNAVAILABLE,
        GroupError::Storage => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::CONFLICT,
    };
    error_response(status, error.to_string())
}
