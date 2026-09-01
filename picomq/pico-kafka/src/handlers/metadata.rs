use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::MetadataRequest;
use kafka_protocol::protocol::{Decodable, StrBytes};
use picomq_server::{alias, CreateCommand, OwnershipService};
use uuid::Uuid;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    broker_id, encode_response, is_internal_topic, new_topic_id, parse_host_port,
    service_error_code, topic_name, topic_uuid, NO_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
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

    let mut brokers = Vec::new();
    let view = ctx.views.load();
    for (node_id, node) in view.state.nodes.iter() {
        let Some(host) = node
            .protocol_addresses
            .get(crate::PROTOCOL_NAME)
            .map(String::as_str)
            .filter(|a| !a.is_empty())
        else {
            continue;
        };
        let (host, port) = parse_host_port(host);
        brokers.push(
            MetadataResponseBroker::default()
                .with_node_id(broker_id(*node_id))
                .with_host(StrBytes::from(host))
                .with_port(port)
                .with_rack(None),
        );
    }

    let requested: Vec<String> = match &request.topics {
        Some(topics) if !topics.is_empty() => topics
            .iter()
            .filter_map(|topic| topic.name.as_ref().map(|name| name.to_string()))
            .collect(),
        _ => ctx
            .service
            .list_topics()
            .into_iter()
            .map(|(topic, _)| topic)
            .collect(),
    };

    let mut response_topics = Vec::with_capacity(requested.len());
    for topic in requested {
        let stream = match resolve_or_create(ctx, &topic, request.allow_auto_topic_creation).await {
            Ok(stream) => stream,
            Err(code) => {
                response_topics.push(topic_error(&topic, code));
                continue;
            }
        };
        match build_topic(ctx, &topic, &stream, req.api_version).await {
            Ok(response_topic) => response_topics.push(response_topic),
            Err(code) => response_topics.push(topic_error(&topic, code)),
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
    topic: &str,
    auto_create: bool,
) -> Result<String, i16> {
    if !alias::is_valid_topic(topic) {
        return Err(UNKNOWN_TOPIC_OR_PARTITION);
    }
    if let Some(stream) = ctx
        .service
        .lookup_by_topic(topic)
        .await
        .map_err(|error| service_error_code(&error))?
    {
        return Ok(stream);
    }
    if !auto_create {
        return Err(UNKNOWN_TOPIC_OR_PARTITION);
    }
    let stream = alias::stream_name_for_topic(topic);
    ctx.service
        .create(
            CreateCommand::with_external_id(&stream, KAFKA_CREATED_CT, new_topic_id())
                .with_kafka_topic(topic),
        )
        .await
        .map_err(|error| service_error_code(&error))?;
    Ok(stream)
}

async fn build_topic(
    ctx: &BrokerContext,
    topic: &str,
    name: &str,
    api_version: i16,
) -> Result<MetadataResponseTopic, i16> {
    let meta = ctx
        .service
        .describe(name)
        .await
        .map_err(|error| service_error_code(&error))?
        .ok_or(UNKNOWN_TOPIC_OR_PARTITION)?;
    let owner = ctx
        .ownership
        .owner_of(name)
        .await
        .map_err(|error| service_error_code(&error))?;
    let leader = owner.owner_node_id.unwrap_or(ctx.node_id);
    let mut partition = MetadataResponsePartition::default()
        .with_partition_index(0)
        .with_leader_id(broker_id(leader))
        .with_replica_nodes(vec![broker_id(leader)])
        .with_isr_nodes(vec![broker_id(leader)])
        .with_offline_replicas(Vec::new())
        .with_error_code(NO_ERROR);
    if api_version >= 7 {
        partition = partition.with_leader_epoch(-1);
    }
    let mut response_topic = MetadataResponseTopic::default()
        .with_error_code(NO_ERROR)
        .with_name(Some(topic_name(topic)))
        .with_is_internal(is_internal_topic(topic))
        .with_partitions(vec![partition]);
    if api_version >= 10 {
        response_topic = response_topic.with_topic_id(topic_uuid(meta.external_id));
    } else {
        response_topic = response_topic.with_topic_id(Uuid::nil());
    }
    Ok(response_topic)
}

fn topic_error(topic: &str, code: i16) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .with_error_code(code)
        .with_name(Some(topic_name(topic)))
}
