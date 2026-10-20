use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, Bytes};
use kafka_protocol::messages::delete_groups_response::{
    DeletableGroupResult, DeleteGroupsResponse,
};
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
use kafka_protocol::messages::offset_delete_response::{
    OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    ApiKey, ConsumerProtocolSubscription, DeleteGroupsRequest, DescribeGroupsRequest,
    FindCoordinatorRequest, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    LeaveGroupRequest, ListGroupsRequest, OffsetCommitRequest, OffsetDeleteRequest,
    OffsetFetchRequest, SyncGroupRequest,
};
use kafka_protocol::protocol::{Decodable, StrBytes};
use picomq_common::name::canonical;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    COORDINATOR_NOT_AVAILABLE, FENCED_INSTANCE_ID, GROUP_ID_NOT_FOUND, GROUP_MAX_SIZE_REACHED,
    GROUP_SUBSCRIBED_TO_TOPIC, ILLEGAL_GENERATION, INCONSISTENT_GROUP_PROTOCOL, INVALID_REQUEST,
    KAFKA_STORAGE_ERROR, MEMBER_ID_REQUIRED, NO_ERROR, NON_EMPTY_GROUP, NOT_COORDINATOR,
    REBALANCE_IN_PROGRESS, UNKNOWN_MEMBER_ID, broker_address, broker_id, encode_response,
    parse_host_port, partition_of, resolve_topic, topic_name,
};
use crate::handlers::{HandlerError, HandlerOutcome};
use picomq_server::{
    CommittedOffset, GroupError, JoinInput, JoinProtocol, Joined, MemberFence, MemberRole,
    Membership, OffsetCommit, Partition, SyncInput, Topic, TopicId,
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
        GroupError::NonEmpty => NON_EMPTY_GROUP,
        GroupError::MemberIdRequired => MEMBER_ID_REQUIRED,
        GroupError::CapacityExceeded => GROUP_MAX_SIZE_REACHED,
        GroupError::FencedInstance => FENCED_INSTANCE_ID,
        GroupError::Superseded => REBALANCE_IN_PROGRESS,
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
        key if key == ApiKey::DeleteGroups as i16 => delete_groups(ctx, req, body).await,
        key if key == ApiKey::OffsetDelete as i16 => offset_delete(ctx, req, body).await,
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
        // Kafka never answers a join that a newer join from the same member
        // replaced.
        Err(GroupError::Superseded) => std::future::pending().await,
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
        .coordinated()
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
        let resolved = resolve_topic(ctx, topic.name.as_str()).await;
        for partition in &topic.partitions {
            let key = (topic.name.to_string(), partition.partition_index);
            let target = match &resolved {
                Ok(resolved) => partition_of(resolved, partition.partition_index),
                Err(code) => Err(*code),
            };

            match (target, u64::try_from(partition.committed_offset)) {
                (Ok(target), Ok(position)) => commits.push(OffsetCommit {
                    partition: target,
                    value: CommittedOffset {
                        position,
                        leader_epoch: Some(partition.committed_leader_epoch)
                            .filter(|epoch| *epoch >= 0),
                        metadata: partition
                            .committed_metadata
                            .as_ref()
                            .map(ToString::to_string),
                    },
                }),
                (Ok(_), Err(_)) => {
                    rejected.insert(key, INVALID_REQUEST);
                }
                (Err(code), _) => {
                    rejected.insert(key, code);
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
    let mut requested: Vec<(String, Vec<i32>, Option<Topic>)> = Vec::new();
    if let Some(topics) = &request.topics {
        for topic in topics {
            let resolved = resolve_topic(ctx, topic.name.as_str()).await.ok();
            requested.push((
                topic.name.to_string(),
                topic.partition_indexes.clone(),
                resolved,
            ));
        }
    }

    let partitions: Vec<Partition> = requested
        .iter()
        .filter_map(|(_, indexes, topic)| topic.as_ref().map(|topic| (indexes, topic)))
        .flat_map(|(indexes, topic)| {
            indexes
                .iter()
                .filter_map(|index| partition_of(topic, *index).ok())
        })
        .collect();
    let result = ctx
        .groups
        .fetch_offsets(
            request.group_id.as_str(),
            request.topics.is_some().then_some(partitions.as_slice()),
        )
        .await;
    let (error_code, offsets) = match result {
        Ok(offsets) => (NO_ERROR, offsets),
        Err(error) => (error_code(error), BTreeMap::new()),
    };

    let topics = if request.topics.is_some() {
        requested
            .into_iter()
            .map(|(name, indexes, topic)| {
                let partitions = indexes
                    .into_iter()
                    .map(|index| {
                        let committed = topic
                            .as_ref()
                            .and_then(|topic| partition_of(topic, index).ok())
                            .and_then(|partition| offsets.get(&partition));
                        fetched_partition(index, committed)
                    })
                    .collect();

                OffsetFetchResponseTopic::default()
                    .with_name(topic_name(&name))
                    .with_partitions(partitions)
            })
            .collect()
    } else {
        let mut by_topic: BTreeMap<TopicId, Vec<(u32, &CommittedOffset)>> = BTreeMap::new();
        for (partition, committed) in &offsets {
            by_topic
                .entry(partition.topic)
                .or_default()
                .push((partition.index, committed));
        }

        let mut topics = Vec::with_capacity(by_topic.len());
        for (id, committed) in by_topic {
            let Ok(Some(topic)) = ctx.topics.by_id(&id).await else {
                continue;
            };
            let Some(name) = topic.name.kafka() else {
                continue;
            };

            let partitions = committed
                .into_iter()
                .map(|(index, value)| fetched_partition(index as i32, Some(value)))
                .collect();
            topics.push(
                OffsetFetchResponseTopic::default()
                    .with_name(topic_name(name))
                    .with_partitions(partitions),
            );
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
        .with_committed_leader_epoch(committed.and_then(|value| value.leader_epoch).unwrap_or(-1))
        .with_error_code(NO_ERROR);
    match committed {
        Some(value) => base
            .with_committed_offset(value.position as i64)
            .with_metadata(value.metadata.clone().map(StrBytes::from)),
        None => base.with_committed_offset(-1).with_metadata(None),
    }
}

async fn delete_groups(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = DeleteGroupsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let mut results = Vec::with_capacity(request.groups_names.len());
    for group in request.groups_names {
        let code = match ctx.groups.delete(group.as_str()).await {
            Ok(()) => NO_ERROR,
            Err(error) => error_code(error),
        };
        results.push(
            DeletableGroupResult::default()
                .with_group_id(group)
                .with_error_code(code),
        );
    }

    let response = DeleteGroupsResponse::default().with_results(results);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn offset_delete(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = OffsetDeleteRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let response = match subscribed_topics(ctx, request.group_id.as_str()).await {
        Ok(subscribed) => {
            let mut deletable = Vec::new();
            let mut topics = Vec::with_capacity(request.topics.len());
            for topic in &request.topics {
                let resolved = resolve_topic(ctx, topic.name.as_str()).await;
                let mut partitions = Vec::with_capacity(topic.partitions.len());
                for partition in &topic.partitions {
                    let index = partition.partition_index;
                    let code = match &resolved {
                        Err(code) => *code,
                        Ok(resolved) if subscribed.contains(resolved.name.canonical()) => {
                            GROUP_SUBSCRIBED_TO_TOPIC
                        }
                        Ok(resolved) => match partition_of(resolved, index) {
                            Ok(partition) => {
                                deletable.push(partition);
                                NO_ERROR
                            }
                            Err(code) => code,
                        },
                    };
                    partitions.push(
                        OffsetDeleteResponsePartition::default()
                            .with_partition_index(index)
                            .with_error_code(code),
                    );
                }
                topics.push(
                    OffsetDeleteResponseTopic::default()
                        .with_name(topic.name.clone())
                        .with_partitions(partitions),
                );
            }

            match ctx
                .groups
                .delete_offsets(request.group_id.as_str(), &deletable)
                .await
            {
                Ok(()) => OffsetDeleteResponse::default()
                    .with_error_code(NO_ERROR)
                    .with_topics(topics),
                Err(error) => OffsetDeleteResponse::default().with_error_code(error_code(error)),
            }
        }
        Err(code) => OffsetDeleteResponse::default().with_error_code(code),
    };

    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn subscribed_topics(ctx: &BrokerContext, group_id: &str) -> Result<BTreeSet<String>, i16> {
    let described = ctx.groups.describe(group_id).await.map_err(error_code)?;

    let mut subscribed = BTreeSet::new();
    for member in &described.members {
        match &member.role {
            MemberRole::Subscribed { subscription, .. } => {
                subscribed.extend(subscription.iter().map(|name| canonical(name)));
            }
            MemberRole::Client { metadata, .. } if described.protocol_type == "consumer" => {
                let mut buf = metadata.clone();
                let subscription = buf
                    .try_get_i16()
                    .ok()
                    .and_then(|version| {
                        ConsumerProtocolSubscription::decode(&mut buf, version.min(3)).ok()
                    })
                    .ok_or(INVALID_REQUEST)?;
                subscribed.extend(subscription.topics.iter().map(|topic| canonical(topic)));
            }
            MemberRole::Client { .. } => return Err(NON_EMPTY_GROUP),
        }
    }

    Ok(subscribed)
}
