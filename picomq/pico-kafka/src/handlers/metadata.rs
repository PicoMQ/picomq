use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::MetadataRequest;
use kafka_protocol::protocol::{Decodable, StrBytes};
use pico_server::{CreateCommand, OwnershipService};
use uuid::Uuid;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    broker_id, encode_response, new_topic_id, parse_host_port, reject_sys_create,
    service_error_code, topic_name, topic_uuid, INVALID_REQUEST, NO_ERROR,
    UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::handlers::{HandlerError, HandlerOutcome};
use crate::topic::{
    is_catalog_name, is_internal_topic, kafka_content_type, stream_name, topic_from_stream,
    validate_topic_name, CATALOG_TOPIC,
};

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

    let topic_names: Vec<String> = match &request.topics {
        None => list_topic_names(ctx).await?,
        Some(topics) if topics.is_empty() => list_topic_names(ctx).await?,
        Some(topics) => topics
            .iter()
            .filter_map(|topic| topic.name.as_ref().map(|name| name.to_string()))
            .collect(),
    };

    let mut response_topics = Vec::with_capacity(topic_names.len());
    for topic in topic_names {
        if !validate_topic_name(&topic) {
            response_topics.push(topic_error(&topic, UNKNOWN_TOPIC_OR_PARTITION));
            continue;
        }
        let name = stream_name(&topic);
        if !is_catalog_name(&topic) && reject_sys_create(&name).is_err() {
            response_topics.push(topic_error(&topic, INVALID_REQUEST));
            continue;
        }
        match ctx.service.describe(&name).await {
            Ok(Some(_)) => {}
            Ok(None) if request.allow_auto_topic_creation && !is_catalog_name(&topic) => {
                if let Err(error) = create_topic(ctx, &name).await {
                    response_topics.push(topic_error(&topic, service_error_code(&error)));
                    continue;
                }
            }
            Ok(None) => {
                response_topics.push(topic_error(&topic, UNKNOWN_TOPIC_OR_PARTITION));
                continue;
            }
            Err(error) => {
                response_topics.push(topic_error(&topic, service_error_code(&error)));
                continue;
            }
        }
        match build_topic(ctx, &topic, &name, req.api_version).await {
            Ok(response_topic) => response_topics.push(response_topic),
            Err(code) => response_topics.push(topic_error(&topic, code)),
        }
    }

    // Any node serves admin requests, so the answering node is the controller.
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

/// All Kafka topic streams: kafka content type, single-segment `/{topic}`
/// names, reserved subtree excluded, plus the catalog topic when it exists.
async fn list_topic_names(ctx: &BrokerContext) -> Result<Vec<String>, HandlerError> {
    let list = ctx.service.list("/", None, 10_000).await?;
    let mut names: Vec<String> = list
        .streams
        .into_iter()
        .filter(|stream| stream.content_type == kafka_content_type())
        .filter_map(|stream| topic_from_stream(&stream.name).map(str::to_owned))
        .collect();
    if ctx
        .service
        .head(pico_server::CATALOG_STREAM)
        .await?
        .is_some()
    {
        names.push(CATALOG_TOPIC.to_owned());
    }
    Ok(names)
}

async fn create_topic(ctx: &BrokerContext, name: &str) -> Result<(), pico_server::ServiceError> {
    ctx.service
        .create(CreateCommand::with_external_id(
            name,
            kafka_content_type(),
            new_topic_id(),
        ))
        .await?;
    Ok(())
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
    if meta.content_type != kafka_content_type() && !is_catalog_name(topic) {
        // A stream created by another frontend is not a Kafka topic.
        return Err(INVALID_REQUEST);
    }
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
