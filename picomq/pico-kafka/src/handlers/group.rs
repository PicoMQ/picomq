use std::collections::BTreeMap;

use bytes::Bytes;
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::join_group_response::{JoinGroupResponse, JoinGroupResponseMember};
use kafka_protocol::messages::leave_group_response::{LeaveGroupResponse, MemberResponse};
use kafka_protocol::messages::list_groups_response::{
    ListGroupsResponse, ListedGroup as WireListedGroup,
};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    ApiKey, DescribeGroupsRequest, FindCoordinatorRequest, HeartbeatRequest, HeartbeatResponse,
    JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest, OffsetCommitRequest,
    OffsetFetchRequest, SyncGroupRequest,
};
use kafka_protocol::protocol::{Decodable, StrBytes};

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    COORDINATOR_NOT_AVAILABLE, FENCED_INSTANCE_ID, GROUP_ID_NOT_FOUND, GROUP_MAX_SIZE_REACHED,
    ILLEGAL_GENERATION, INCONSISTENT_GROUP_PROTOCOL, INVALID_REQUEST, KAFKA_STORAGE_ERROR,
    MEMBER_ID_REQUIRED, NO_ERROR, NOT_COORDINATOR, REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID,
    UNKNOWN_TOPIC_OR_PARTITION, broker_address, broker_id, encode_response, parse_host_port,
    resolve_topic, topic_name,
};
use crate::handlers::{HandlerError, HandlerOutcome};
use picomq_server::{
    CommittedOffset, GroupError, JoinInput, JoinProtocol, Joined, MemberFence, MemberRole,
    Membership, OffsetCommit, SyncInput,
};

fn error_code(error: GroupError) -> i16 {
    match error {
        GroupError::CoordinatorNotAvailable => COORDINATOR_NOT_AVAILABLE,
        GroupError::NotCoordinator => NOT_COORDINATOR,
        GroupError::IllegalGeneration => ILLEGAL_GENERATION,
        GroupError::InconsistentProtocol => INCONSISTENT_GROUP_PROTOCOL,
        GroupError::UnknownMember => UNKNOWN_MEMBER_ID,
        GroupError::RebalanceInProgress => REBALANCE_IN_PROGRESS,
        GroupError::InvalidRequest => INVALID_REQUEST,
        GroupError::Storage => KAFKA_STORAGE_ERROR,
        GroupError::GroupNotFound => GROUP_ID_NOT_FOUND,
        GroupError::MemberIdRequired => MEMBER_ID_REQUIRED,
        GroupError::CapacityExceeded => GROUP_MAX_SIZE_REACHED,
        GroupError::FencedInstance => FENCED_INSTANCE_ID,
    }
}

pub async fn handle(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    match req.api_key {
        key if key == ApiKey::FindCoordinator as i16 => find_coordinator(ctx, req, body).await,
        key if key == ApiKey::JoinGroup as i16 => join_group(ctx, req, body).await,
        key if key == ApiKey::SyncGroup as i16 => sync_group(ctx, req, body).await,
        key if key == ApiKey::Heartbeat as i16 => heartbeat(ctx, req, body).await,
        key if key == ApiKey::LeaveGroup as i16 => leave_group(ctx, req, body).await,
        key if key == ApiKey::DescribeGroups as i16 => describe_groups(ctx, req, body).await,
        key if key == ApiKey::ListGroups as i16 => list_groups(ctx, req, body).await,
        key if key == ApiKey::OffsetCommit as i16 => offset_commit(ctx, req, body).await,
        key if key == ApiKey::OffsetFetch as i16 => offset_fetch(ctx, req, body).await,
        other => Err(HandlerError::Unimplemented(other)),
    }
}

async fn find_coordinator(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = FindCoordinatorRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let result = if request.key_type == 0 {
        ctx.groups.find_coordinator(request.key.as_str()).await
    } else {
        Err(GroupError::InvalidRequest)
    };
    let response = match result {
        Ok(node_id) => {
            let view = ctx.views.load();
            match broker_address(&view.state, node_id) {
                Some(address) => {
                    let (host, port) = parse_host_port(address);
                    FindCoordinatorResponse::default()
                        .with_error_code(NO_ERROR)
                        .with_node_id(broker_id(node_id))
                        .with_host(StrBytes::from(host))
                        .with_port(port)
                }
                None => FindCoordinatorResponse::default()
                    .with_error_code(COORDINATOR_NOT_AVAILABLE)
                    .with_node_id(broker_id(-1))
                    .with_host(StrBytes::from_static_str(""))
                    .with_port(-1),
            }
        }
        Err(error) => FindCoordinatorResponse::default()
            .with_error_code(error_code(error))
            .with_node_id(broker_id(-1))
            .with_host(StrBytes::from_static_str(""))
            .with_port(-1),
    };
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn join_group(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = JoinGroupRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let outcome = ctx
        .groups
        .join(JoinInput {
            group_id: request.group_id.to_string(),
            member_id: request.member_id.to_string(),
            instance_id: request.group_instance_id.map(|id| id.to_string()),
            client_id: req.client_id.clone().unwrap_or_default(),
            membership: Membership::Client {
                protocol_type: request.protocol_type.to_string(),
                protocols: request
                    .protocols
                    .into_iter()
                    .map(|protocol| JoinProtocol {
                        name: protocol.name.to_string(),
                        metadata: protocol.metadata,
                    })
                    .collect(),
            },
            session_timeout_ms: request.session_timeout_ms,
            rebalance_timeout_ms: request.rebalance_timeout_ms,
            require_known_member_id: req.api_version >= 4,
        })
        .await;
    let mut response = JoinGroupResponse::default()
        .with_generation_id(outcome.generation)
        .with_member_id(StrBytes::from(outcome.member_id));
    match outcome.result {
        Ok(Joined::Client {
            protocol_type,
            protocol_name,
            leader,
            members,
        }) => {
            let members = members
                .into_iter()
                .map(|member| {
                    let mut wire = JoinGroupResponseMember::default()
                        .with_member_id(StrBytes::from(member.member_id))
                        .with_metadata(member.metadata);
                    if req.api_version >= 5 {
                        wire = wire.with_group_instance_id(member.instance_id.map(StrBytes::from));
                    }
                    wire
                })
                .collect();
            response = response
                .with_error_code(NO_ERROR)
                .with_protocol_name(Some(StrBytes::from(protocol_name)))
                .with_leader(StrBytes::from(leader))
                .with_members(members);
            if req.api_version >= 7 {
                response = response.with_protocol_type(Some(StrBytes::from(protocol_type)));
            }
        }
        Ok(Joined::Subscribed { .. }) => {
            response = response.with_error_code(INCONSISTENT_GROUP_PROTOCOL);
        }
        Err(error) => {
            response = response.with_error_code(error_code(error));
        }
    }
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn sync_group(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = SyncGroupRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let result = ctx
        .groups
        .sync(SyncInput {
            group_id: request.group_id.to_string(),
            generation: request.generation_id,
            member_id: request.member_id.to_string(),
            instance_id: request.group_instance_id.map(|id| id.to_string()),
            assignments: request
                .assignments
                .into_iter()
                .map(|assignment| (assignment.member_id.to_string(), assignment.assignment))
                .collect(),
        })
        .await;
    let response = match result {
        Ok(outcome) => {
            let mut response = kafka_protocol::messages::SyncGroupResponse::default()
                .with_error_code(NO_ERROR)
                .with_assignment(outcome.assignment);
            if req.api_version >= 5 {
                response = response
                    .with_protocol_type(Some(StrBytes::from(outcome.protocol_type)))
                    .with_protocol_name(Some(StrBytes::from(outcome.protocol_name)));
            }
            response
        }
        Err(error) => kafka_protocol::messages::SyncGroupResponse::default()
            .with_error_code(error_code(error)),
    };
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn heartbeat(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = HeartbeatRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let result = ctx
        .groups
        .heartbeat(
            request.group_id.as_str(),
            MemberFence {
                generation: request.generation_id,
                member_id: request.member_id.as_str(),
                instance_id: request.group_instance_id.as_ref().map(StrBytes::as_str),
            },
        )
        .await;
    let response =
        HeartbeatResponse::default().with_error_code(result.err().map_or(NO_ERROR, error_code));
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn leave_group(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = LeaveGroupRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let identities: Vec<(String, Option<String>)> = if req.api_version <= 2 {
        vec![(request.member_id.to_string(), None)]
    } else {
        request
            .members
            .iter()
            .map(|member| {
                (
                    member.member_id.to_string(),
                    member.group_instance_id.as_ref().map(ToString::to_string),
                )
            })
            .collect()
    };
    let results = ctx
        .groups
        .leave(request.group_id.as_str(), &identities)
        .await;
    let top_level = if req.api_version <= 2 {
        results
            .first()
            .copied()
            .and_then(Result::err)
            .map_or(NO_ERROR, error_code)
    } else {
        NO_ERROR
    };
    let members = if req.api_version >= 3 {
        identities
            .into_iter()
            .zip(results)
            .map(|((member_id, instance_id), result)| {
                MemberResponse::default()
                    .with_member_id(StrBytes::from(member_id))
                    .with_group_instance_id(instance_id.map(StrBytes::from))
                    .with_error_code(result.err().map_or(NO_ERROR, error_code))
            })
            .collect()
    } else {
        Vec::new()
    };
    let response = LeaveGroupResponse::default()
        .with_error_code(top_level)
        .with_members(members);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn describe_groups(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = DescribeGroupsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let mut groups = Vec::with_capacity(request.groups.len());
    for group_id in request.groups {
        let wire = match ctx.groups.describe(group_id.as_str()).await {
            Ok(described) => {
                let members = described
                    .members
                    .into_iter()
                    .map(|member| {
                        let (metadata, assignment) = match member.role {
                            MemberRole::Client {
                                metadata,
                                assignment,
                            } => (metadata, assignment),
                            MemberRole::Subscribed { .. } => (Bytes::new(), Bytes::new()),
                        };
                        let mut wire = DescribedGroupMember::default()
                            .with_member_id(StrBytes::from(member.member_id))
                            .with_client_id(StrBytes::from(member.client_id))
                            .with_client_host(StrBytes::from_static_str(""))
                            .with_member_metadata(metadata)
                            .with_member_assignment(assignment);
                        if req.api_version >= 4 {
                            wire =
                                wire.with_group_instance_id(member.instance_id.map(StrBytes::from));
                        }
                        wire
                    })
                    .collect();
                DescribedGroup::default()
                    .with_error_code(NO_ERROR)
                    .with_group_id(group_id)
                    .with_group_state(StrBytes::from_static_str(described.state.as_str()))
                    .with_protocol_type(StrBytes::from(described.protocol_type))
                    .with_protocol_data(StrBytes::from(described.protocol_name))
                    .with_members(members)
            }
            Err(error) => DescribedGroup::default()
                .with_error_code(match error {
                    GroupError::GroupNotFound => NO_ERROR,
                    other => error_code(other),
                })
                .with_group_id(group_id)
                .with_group_state(StrBytes::from_static_str("Dead"))
                .with_protocol_type(StrBytes::from_static_str(""))
                .with_protocol_data(StrBytes::from_static_str("")),
        };
        groups.push(wire);
    }
    let response = kafka_protocol::messages::DescribeGroupsResponse::default().with_groups(groups);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn list_groups(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = ListGroupsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let states: Vec<&str> = request.states_filter.iter().map(StrBytes::as_str).collect();
    let types: Vec<&str> = request.types_filter.iter().map(StrBytes::as_str).collect();
    let groups = ctx
        .groups
        .list()
        .await
        .into_iter()
        .filter(|group| states.is_empty() || states.contains(&group.state.as_str()))
        .filter(|_| types.is_empty() || types.contains(&"classic"))
        .map(|group| {
            let mut wire = WireListedGroup::default()
                .with_group_id(kafka_protocol::messages::GroupId(StrBytes::from(
                    group.group_id,
                )))
                .with_protocol_type(StrBytes::from(group.protocol_type));
            if req.api_version >= 4 {
                wire = wire.with_group_state(StrBytes::from_static_str(group.state.as_str()));
            }
            if req.api_version >= 5 {
                wire = wire.with_group_type(StrBytes::from_static_str("classic"));
            }
            wire
        })
        .collect();
    let response = ListGroupsResponse::default()
        .with_error_code(NO_ERROR)
        .with_groups(groups);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn offset_commit(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = OffsetCommitRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let mut commits = Vec::new();
    let mut rejected: BTreeMap<(String, i32), i16> = BTreeMap::new();
    for topic in &request.topics {
        let stream = resolve_topic(ctx, topic.name.as_str()).await;
        for partition in &topic.partitions {
            let key = (topic.name.to_string(), partition.partition_index);
            match (&stream, u64::try_from(partition.committed_offset)) {
                (Ok(stream), Ok(position)) if partition.partition_index == 0 => {
                    commits.push(OffsetCommit {
                        stream: stream.clone(),
                        value: CommittedOffset {
                            position,
                            metadata: partition
                                .committed_metadata
                                .as_ref()
                                .map(ToString::to_string),
                        },
                    });
                }
                (Ok(_), Ok(_)) => {
                    rejected.insert(key, UNKNOWN_TOPIC_OR_PARTITION);
                }
                (Ok(_), Err(_)) => {
                    rejected.insert(key, INVALID_REQUEST);
                }
                (Err(code), _) => {
                    rejected.insert(key, *code);
                }
            }
        }
    }
    let fence = (request.generation_id_or_member_epoch >= 0).then(|| MemberFence {
        generation: request.generation_id_or_member_epoch,
        member_id: request.member_id.as_str(),
        instance_id: request.group_instance_id.as_ref().map(StrBytes::as_str),
    });
    let result = ctx
        .groups
        .commit_offsets(request.group_id.as_str(), fence, &commits)
        .await;
    let code = result.err().map_or(NO_ERROR, error_code);
    let topics = request
        .topics
        .iter()
        .map(|topic| {
            let partitions = topic
                .partitions
                .iter()
                .map(|partition| {
                    let key = (topic.name.to_string(), partition.partition_index);
                    OffsetCommitResponsePartition::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(rejected.get(&key).copied().unwrap_or(code))
                })
                .collect();
            OffsetCommitResponseTopic::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions)
        })
        .collect();
    let response = OffsetCommitResponse::default().with_topics(topics);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn offset_fetch(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = OffsetFetchRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let mut requested: Vec<(String, Vec<i32>, Option<String>)> = Vec::new();
    if let Some(topics) = &request.topics {
        for topic in topics {
            let stream = resolve_topic(ctx, topic.name.as_str()).await.ok();
            requested.push((
                topic.name.to_string(),
                topic.partition_indexes.clone(),
                stream,
            ));
        }
    }
    let streams: Vec<String> = requested
        .iter()
        .filter_map(|(_, _, stream)| stream.clone())
        .collect();
    let result = ctx
        .groups
        .fetch_offsets(
            request.group_id.as_str(),
            request.topics.is_some().then_some(streams.as_slice()),
        )
        .await;
    let (error_code, offsets) = match result {
        Ok(offsets) => (NO_ERROR, offsets),
        Err(error) => (error_code(error), BTreeMap::new()),
    };
    let topics = if request.topics.is_some() {
        requested
            .into_iter()
            .map(|(topic, partitions, stream)| {
                let committed = stream.as_ref().and_then(|stream| offsets.get(stream));
                OffsetFetchResponseTopic::default()
                    .with_name(topic_name(&topic))
                    .with_partitions(
                        partitions
                            .into_iter()
                            .map(|partition| {
                                fetched_partition(partition, committed.filter(|_| partition == 0))
                            })
                            .collect(),
                    )
            })
            .collect()
    } else {
        let mut topics = Vec::with_capacity(offsets.len());
        for (stream, committed) in &offsets {
            if let Ok(Some(meta)) = ctx.service.describe(stream).await
                && let Some(topic) = meta.kafka_topic
            {
                topics.push(
                    OffsetFetchResponseTopic::default()
                        .with_name(topic_name(&topic))
                        .with_partitions(vec![fetched_partition(0, Some(committed))]),
                );
            }
        }
        topics
    };
    let response = OffsetFetchResponse::default()
        .with_error_code(error_code)
        .with_topics(topics);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

fn fetched_partition(
    partition: i32,
    committed: Option<&CommittedOffset>,
) -> OffsetFetchResponsePartition {
    let base = OffsetFetchResponsePartition::default()
        .with_partition_index(partition)
        .with_committed_leader_epoch(-1)
        .with_error_code(NO_ERROR);
    match committed {
        Some(value) => base
            .with_committed_offset(value.position as i64)
            .with_metadata(value.metadata.clone().map(StrBytes::from)),
        None => base.with_committed_offset(-1).with_metadata(None),
    }
}
