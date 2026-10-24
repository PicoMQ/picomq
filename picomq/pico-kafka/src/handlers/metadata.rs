use kafka_protocol::messages::describe_cluster_response::{
    DescribeClusterBroker, DescribeClusterResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{DescribeClusterRequest, MetadataRequest};
use kafka_protocol::protocol::{Decodable, StrBytes};
use picomq_server::record::leader_epoch;
use picomq_server::topic::is_kafka_name;
use picomq_server::{CreateCommand, OwnershipService, Topic, partition_log};
use uuid::Uuid;

use picomq_auth::Operation;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    INVALID_TOPIC_EXCEPTION, NO_ERROR, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_TOPIC_OR_PARTITION,
    broker_id, brokers, encode_response, service_error_code, topic_name, topic_uuid,
};
use crate::handlers::topics::KAFKA_CREATED_CT;
use crate::handlers::{HandlerError, HandlerOutcome};

pub async fn handle(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = MetadataRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let brokers = brokers(&ctx.views.load().state, false)
        .into_iter()
        .map(|broker| {
            MetadataResponseBroker::default()
                .with_node_id(broker_id(broker.id))
                .with_host(StrBytes::from(broker.host))
                .with_port(broker.port)
                .with_rack(None)
        })
        .collect();

    let requested: Option<Vec<String>> = match &request.topics {
        None => None,
        Some(topics) if topics.is_empty() && req.api_version == 0 => None,
        Some(topics) => Some(
            topics
                .iter()
                .filter_map(|topic| topic.name.as_ref().map(|name| name.to_string()))
                .collect(),
        ),
    };

    let mut response_topics = Vec::new();
    match requested {
        None => {
            for topic in ctx.topics.kafka_topics() {
                let readable = req.allowed(
                    ctx.authorizer.as_deref(),
                    Operation::Head,
                    topic.name.kafka(),
                );
                if readable {
                    response_topics.push(build_topic(ctx, &topic, req.api_version).await);
                }
            }
        }
        Some(names) => {
            for name in names {
                response_topics.push(
                    match resolve_or_create(ctx, req, &name, request.allow_auto_topic_creation)
                        .await
                    {
                        Ok(topic) => build_topic(ctx, &topic, req.api_version).await,
                        Err(code) => topic_error(&name, code),
                    },
                );
            }
        }
    }

    let response = MetadataResponse::default()
        .with_brokers(brokers)
        .with_controller_id(broker_id(ctx.node_id))
        .with_cluster_id(Some(StrBytes::from(ctx.cluster_id.clone())))
        .with_topics(response_topics);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn resolve_or_create(
    ctx: &BrokerContext,
    req: &RequestContext,
    name: &str,
    auto_create: bool,
) -> Result<Topic, i16> {
    if !is_kafka_name(name) {
        return Err(INVALID_TOPIC_EXCEPTION);
    }
    let authorizer = ctx.authorizer.as_deref();
    if !req.allowed(authorizer, Operation::Head, Some(name)) {
        return Err(TOPIC_AUTHORIZATION_FAILED);
    }
    if let Some(topic) = ctx
        .topics
        .get_kafka(name)
        .await
        .map_err(|error| service_error_code(&error))?
    {
        return Ok(topic);
    }
    if !auto_create {
        return Err(UNKNOWN_TOPIC_OR_PARTITION);
    }
    if !req.allowed(authorizer, Operation::Create, Some(name)) {
        return Err(TOPIC_AUTHORIZATION_FAILED);
    }

    ctx.topics
        .create(CreateCommand::new(name, KAFKA_CREATED_CT))
        .await
        .map(|created| created.meta.topic)
        .map_err(|error| service_error_code(&error))
}

async fn build_topic(
    ctx: &BrokerContext,
    topic: &Topic,
    api_version: i16,
) -> MetadataResponseTopic {
    let name = topic.name.kafka().unwrap_or_default();

    let mut partitions = Vec::with_capacity(topic.partitions as usize);
    for index in 0..topic.partitions {
        let owner = match partition_log(topic, index) {
            Ok(log) => ctx.ownership.owner_of(&log).await,
            Err(error) => Err(error),
        };
        let owner = match owner {
            Ok(owner) => owner,
            Err(error) => return topic_error(name, service_error_code(&error)),
        };

        let leader = broker_id(owner.owner_node_id.unwrap_or(ctx.node_id));
        let mut partition = MetadataResponsePartition::default()
            .with_partition_index(index as i32)
            .with_leader_id(leader)
            .with_replica_nodes(vec![leader])
            .with_isr_nodes(vec![leader])
            .with_offline_replicas(Vec::new())
            .with_error_code(NO_ERROR);
        if api_version >= 7 {
            partition =
                partition.with_leader_epoch(u64::try_from(owner.epoch).map_or(-1, leader_epoch));
        }
        partitions.push(partition);
    }

    let topic_id = if api_version >= 10 {
        topic_uuid(topic.id.0)
    } else {
        Uuid::nil()
    };

    MetadataResponseTopic::default()
        .with_error_code(NO_ERROR)
        .with_name(Some(topic_name(name)))
        .with_is_internal(false)
        .with_topic_id(topic_id)
        .with_partitions(partitions)
}

fn topic_error(topic: &str, code: i16) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .with_error_code(code)
        .with_name(Some(topic_name(topic)))
}

pub async fn describe_cluster(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = DescribeClusterRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let brokers = brokers(&ctx.views.load().state, request.include_fenced_brokers)
        .into_iter()
        .map(|broker| {
            DescribeClusterBroker::default()
                .with_broker_id(broker_id(broker.id))
                .with_host(StrBytes::from(broker.host))
                .with_port(broker.port)
                .with_rack(None)
                .with_is_fenced(broker.fenced)
        })
        .collect();

    let response = DescribeClusterResponse::default()
        .with_error_code(NO_ERROR)
        .with_endpoint_type(request.endpoint_type)
        .with_cluster_id(StrBytes::from(ctx.cluster_id.clone()))
        .with_controller_id(broker_id(ctx.node_id))
        .with_brokers(brokers)
        .with_cluster_authorized_operations(i32::MIN);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}
