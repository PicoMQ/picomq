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
use crate::group::{
    CommittedOffset, JoinInput, JoinProtocol, OffsetCommit, SyncInput, SyncOutcome,
};
use crate::handlers::common::{
    INVALID_REQUEST, NO_ERROR, UNKNOWN_TOPIC_OR_PARTITION, broker_id, encode_response,
    parse_host_port, topic_name,
};
use crate::handlers::{HandlerError, HandlerOutcome};
use picomq_server::alias::is_valid_topic as validate_topic_name;

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
        Err(INVALID_REQUEST)
    };
    let response = match result {
        Ok(endpoint) => {
            let (host, port) = parse_host_port(&endpoint.address);
            FindCoordinatorResponse::default()
                .with_error_code(NO_ERROR)
                .with_node_id(broker_id(endpoint.node_id))
                .with_host(StrBytes::from(host))
                .with_port(port)
        }
        Err(code) => FindCoordinatorResponse::default()
            .with_error_code(code)
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
            group_instance_id: request.group_instance_id.map(|id| id.to_string()),
            protocol_type: request.protocol_type.to_string(),
            protocols: request
                .protocols
                .into_iter()
                .map(|protocol| JoinProtocol {
                    name: protocol.name.to_string(),
                    metadata: protocol.metadata,
                })
                .collect(),
            session_timeout_ms: request.session_timeout_ms,
            rebalance_timeout_ms: request.rebalance_timeout_ms,
            client_id: req.client_id.clone().unwrap_or_default(),
            require_known_member_id: req.api_version >= 4,
        })
        .await;
    let members = outcome
        .members
        .into_iter()
        .map(|member| {
            let mut response = JoinGroupResponseMember::default()
                .with_member_id(StrBytes::from(member.member_id))
                .with_metadata(member.metadata);
            if req.api_version >= 5 {
                response =
                    response.with_group_instance_id(member.group_instance_id.map(StrBytes::from));
            }
            response
        })
        .collect();
    let mut response = JoinGroupResponse::default()
        .with_error_code(outcome.error_code)
        .with_generation_id(outcome.generation_id)
        .with_protocol_name(outcome.protocol_name.map(StrBytes::from))
        .with_leader(StrBytes::from(outcome.leader))
        .with_member_id(StrBytes::from(outcome.member_id))
        .with_members(members);
    if req.api_version >= 7 {
        response = response.with_protocol_type(outcome.protocol_type.map(StrBytes::from));
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
    let outcome = ctx
        .groups
        .sync(SyncInput {
            group_id: request.group_id.to_string(),
            generation_id: request.generation_id,
            member_id: request.member_id.to_string(),
            group_instance_id: request.group_instance_id.map(|id| id.to_string()),
            assignments: request
                .assignments
                .into_iter()
                .map(|assignment| (assignment.member_id.to_string(), assignment.assignment))
                .collect(),
        })
        .await;
    Ok(HandlerOutcome::Response(sync_response(req, outcome)))
}

fn sync_response(req: &RequestContext, outcome: SyncOutcome) -> super::ResponseFrame {
    let mut response = kafka_protocol::messages::SyncGroupResponse::default()
        .with_error_code(outcome.error_code)
        .with_assignment(outcome.assignment);
    if req.api_version >= 5 {
        response = response
            .with_protocol_type(outcome.protocol_type.map(StrBytes::from))
            .with_protocol_name(outcome.protocol_name.map(StrBytes::from));
    }
    encode_response(req.correlation_id, req.api_version, &response)
}

async fn heartbeat(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = HeartbeatRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let code = ctx
        .groups
        .heartbeat(
            request.group_id.as_str(),
            request.generation_id,
            request.member_id.as_str(),
            request.group_instance_id.as_ref().map(StrBytes::as_str),
        )
        .await;
    let response = HeartbeatResponse::default().with_error_code(code);
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
    let codes = ctx
        .groups
        .leave(request.group_id.as_str(), &identities)
        .await;
    let top_level = if req.api_version <= 2 {
        codes.first().copied().unwrap_or(NO_ERROR)
    } else {
        NO_ERROR
    };
    let members = if req.api_version >= 3 {
        identities
            .into_iter()
            .zip(codes)
            .map(|((member_id, instance_id), error_code)| {
                MemberResponse::default()
                    .with_member_id(StrBytes::from(member_id))
                    .with_group_instance_id(instance_id.map(StrBytes::from))
                    .with_error_code(error_code)
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
        let described = ctx.groups.describe(group_id.as_str()).await;
        let members = described
            .members
            .into_iter()
            .map(|member| {
                let mut wire = DescribedGroupMember::default()
                    .with_member_id(StrBytes::from(member.member_id))
                    .with_client_id(StrBytes::from(member.client_id))
                    .with_client_host(StrBytes::from_static_str(""))
                    .with_member_metadata(member.metadata)
                    .with_member_assignment(member.assignment);
                if req.api_version >= 4 {
                    wire =
                        wire.with_group_instance_id(member.group_instance_id.map(StrBytes::from));
                }
                wire
            })
            .collect();
        groups.push(
            DescribedGroup::default()
                .with_error_code(described.error_code)
                .with_group_id(group_id)
                .with_group_state(StrBytes::from(described.state))
                .with_protocol_type(StrBytes::from(described.protocol_type))
                .with_protocol_data(StrBytes::from(described.protocol_name))
                .with_members(members),
        );
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
    let states: Vec<String> = request
        .states_filter
        .iter()
        .map(ToString::to_string)
        .collect();
    let types: Vec<String> = request
        .types_filter
        .iter()
        .map(ToString::to_string)
        .collect();
    let groups = ctx
        .groups
        .list()
        .await
        .into_iter()
        .filter(|group| states.is_empty() || states.contains(&group.state))
        .filter(|_| types.is_empty() || types.iter().any(|kind| kind == "classic"))
        .map(|group| {
            let mut wire = WireListedGroup::default()
                .with_group_id(kafka_protocol::messages::GroupId(StrBytes::from(
                    group.group_id,
                )))
                .with_protocol_type(StrBytes::from(group.protocol_type));
            if req.api_version >= 4 {
                wire = wire.with_group_state(StrBytes::from(group.state));
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
    for topic in &request.topics {
        let name = topic.name.to_string();
        if !validate_topic_name(&name) {
            continue;
        }
        for partition in &topic.partitions {
            if partition.partition_index == 0 {
                commits.push(OffsetCommit {
                    topic: name.clone(),
                    partition: 0,
                    value: CommittedOffset {
                        offset: partition.committed_offset,
                        leader_epoch: partition.committed_leader_epoch,
                        metadata: partition
                            .committed_metadata
                            .as_ref()
                            .map(ToString::to_string),
                    },
                });
            }
        }
    }
    let code = ctx
        .groups
        .commit_offsets(
            request.group_id.as_str(),
            request.generation_id_or_member_epoch,
            request.member_id.as_str(),
            request.group_instance_id.as_ref().map(StrBytes::as_str),
            &commits,
        )
        .await;
    let topics = request
        .topics
        .into_iter()
        .map(|topic| {
            let valid_name = validate_topic_name(topic.name.as_str());
            let partitions = topic
                .partitions
                .into_iter()
                .map(|partition| {
                    let error_code = if !valid_name || partition.partition_index != 0 {
                        UNKNOWN_TOPIC_OR_PARTITION
                    } else {
                        code
                    };
                    OffsetCommitResponsePartition::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(error_code)
                })
                .collect();
            OffsetCommitResponseTopic::default()
                .with_name(topic.name)
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
    let requested: Option<Vec<(String, Vec<i32>)>> = request.topics.as_ref().map(|topics| {
        topics
            .iter()
            .map(|topic| (topic.name.to_string(), topic.partition_indexes.clone()))
            .collect()
    });
    let result = ctx
        .groups
        .fetch_offsets(request.group_id.as_str(), requested.as_deref())
        .await;
    let (error_code, values) = match result {
        Ok(values) => (NO_ERROR, values),
        Err(code) => (code, Default::default()),
    };
    let topics = values
        .into_iter()
        .map(|(name, partitions)| {
            OffsetFetchResponseTopic::default()
                .with_name(topic_name(&name))
                .with_partitions(
                    partitions
                        .into_iter()
                        .map(|(partition, value)| {
                            OffsetFetchResponsePartition::default()
                                .with_partition_index(partition)
                                .with_committed_offset(value.offset)
                                .with_committed_leader_epoch(value.leader_epoch)
                                .with_metadata(value.metadata.map(StrBytes::from))
                                .with_error_code(NO_ERROR)
                        })
                        .collect(),
                )
        })
        .collect();
    let response = OffsetFetchResponse::default()
        .with_error_code(error_code)
        .with_topics(topics);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}
