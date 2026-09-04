//! CreateTopics and DeleteTopics.

use kafka_protocol::messages::create_topics_request::CreatableTopicConfig;
use kafka_protocol::messages::create_topics_response::{
    CreatableTopicResult, CreateTopicsResponse,
};
use kafka_protocol::messages::delete_topics_response::{
    DeletableTopicResult, DeleteTopicsResponse,
};
use kafka_protocol::messages::{CreateTopicsRequest, DeleteTopicsRequest};
use kafka_protocol::protocol::Decodable;
use picomq_server::{CreateCommand, alias};

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    INVALID_REQUEST, NO_ERROR, TOPIC_ALREADY_EXISTS, UNKNOWN_TOPIC_OR_PARTITION, encode_response,
    new_topic_id, resolve_topic, service_error_code, topic_name,
};
use crate::handlers::{HandlerError, HandlerOutcome};

pub(crate) const KAFKA_CREATED_CT: &str = "application/octet-stream";

pub const SCHEMA_CONFIG: &str = "pico.schema";
pub const SCHEMA_VALIDATE_CONFIG: &str = "pico.schema.validate";

pub async fn create(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = CreateTopicsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in request.topics {
        let name = topic.name.to_string();
        if !alias::is_valid_topic(&name) {
            topics.push(
                CreatableTopicResult::default()
                    .with_name(topic_name(&name))
                    .with_error_code(INVALID_REQUEST),
            );
            continue;
        }
        if topic.num_partitions != 1 || topic.replication_factor != 1 {
            topics.push(
                CreatableTopicResult::default()
                    .with_name(topic_name(&name))
                    .with_error_code(INVALID_REQUEST),
            );
            continue;
        }
        let (schema_name, schema_validate) = match schema_configs(&topic.configs) {
            Ok(parsed) => parsed,
            Err(code) => {
                topics.push(
                    CreatableTopicResult::default()
                        .with_name(topic_name(&name))
                        .with_error_code(code),
                );
                continue;
            }
        };
        let mut command = CreateCommand::with_external_id(
            alias::stream_name_for_topic(&name),
            KAFKA_CREATED_CT,
            new_topic_id(),
        )
        .with_kafka_topic(&name);
        if let Some(schema_name) = schema_name {
            command = command
                .with_schema_name(schema_name)
                .with_schema_validate(schema_validate);
        }
        match ctx.service.create(command).await {
            Ok(result) if result.created => topics.push(
                CreatableTopicResult::default()
                    .with_name(topic_name(&name))
                    .with_error_code(NO_ERROR)
                    .with_topic_config_error_code(NO_ERROR)
                    .with_num_partitions(1)
                    .with_replication_factor(1)
                    .with_topic_id(uuid::Uuid::from_bytes(result.meta.external_id)),
            ),
            Ok(_) => topics.push(
                CreatableTopicResult::default()
                    .with_name(topic_name(&name))
                    .with_error_code(TOPIC_ALREADY_EXISTS),
            ),
            Err(error) => topics.push(
                CreatableTopicResult::default()
                    .with_name(topic_name(&name))
                    .with_error_code(service_error_code(&error)),
            ),
        }
    }

    let response = CreateTopicsResponse::default().with_topics(topics);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

pub async fn delete(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = DeleteTopicsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let names: Vec<String> = if req.api_version >= 6 {
        request
            .topics
            .into_iter()
            .filter_map(|topic| topic.name.map(|name| name.to_string()))
            .collect()
    } else {
        request
            .topic_names
            .into_iter()
            .map(|name| name.to_string())
            .collect()
    };

    let mut responses = Vec::with_capacity(names.len());
    for name in names {
        let stream = match resolve_topic(ctx, &name).await {
            Ok(stream) => stream,
            Err(code) => {
                responses.push(
                    DeletableTopicResult::default()
                        .with_name(Some(topic_name(&name)))
                        .with_error_code(code),
                );
                continue;
            }
        };
        match ctx.service.delete(&stream).await {
            Ok(true) => responses.push(
                DeletableTopicResult::default()
                    .with_name(Some(topic_name(&name)))
                    .with_error_code(NO_ERROR),
            ),
            Ok(false) => responses.push(
                DeletableTopicResult::default()
                    .with_name(Some(topic_name(&name)))
                    .with_error_code(UNKNOWN_TOPIC_OR_PARTITION),
            ),
            Err(error) => responses.push(
                DeletableTopicResult::default()
                    .with_name(Some(topic_name(&name)))
                    .with_error_code(service_error_code(&error)),
            ),
        }
    }

    let response = DeleteTopicsResponse::default().with_responses(responses);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

fn schema_configs(configs: &[CreatableTopicConfig]) -> Result<(Option<String>, bool), i16> {
    let mut schema_name = None;
    let mut schema_validate = false;
    for config in configs {
        let Some(value) = config
            .value
            .as_ref()
            .map(|value| value.as_str())
            .filter(|value| !value.is_empty())
        else {
            if config.name.as_str() == SCHEMA_CONFIG
                || config.name.as_str() == SCHEMA_VALIDATE_CONFIG
            {
                return Err(INVALID_REQUEST);
            }
            continue;
        };
        match config.name.as_str() {
            SCHEMA_CONFIG => {
                if schema_name.is_some() {
                    return Err(INVALID_REQUEST);
                }
                schema_name = Some(value.to_owned());
            }
            SCHEMA_VALIDATE_CONFIG => match value {
                "true" => schema_validate = true,
                "false" => schema_validate = false,
                _ => return Err(INVALID_REQUEST),
            },
            _ => {}
        }
    }
    Ok((schema_name, schema_validate))
}
