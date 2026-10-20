//! CreateTopics and DeleteTopics.

use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::create_topics_response::{
    CreatableTopicResult, CreateTopicsResponse,
};
use kafka_protocol::messages::delete_topics_response::{
    DeletableTopicResult, DeleteTopicsResponse,
};
use kafka_protocol::messages::{CreateTopicsRequest, DeleteTopicsRequest};
use kafka_protocol::protocol::{Decodable, StrBytes};
use picomq_server::topic::is_kafka_name;
use picomq_server::{CreateCommand, MAX_NEW_PARTITIONS};
use uuid::Uuid;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    EXCESSIVE_PARTITIONS, Failure, INVALID_CONFIG, INVALID_PARTITIONS, INVALID_REPLICA_ASSIGNMENT,
    INVALID_REPLICATION_FACTOR, INVALID_REQUEST, NO_ERROR, POLICY_VIOLATION, TOPIC_ALREADY_EXISTS,
    UNKNOWN_TOPIC_OR_PARTITION, assigned_broker, encode_response, resolve_topic,
    service_error_code, service_failure, topic_name, topic_uuid,
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

    let requested: u64 = request.topics.iter().map(requested_partitions).sum();
    let oversized = requested > u64::from(MAX_NEW_PARTITIONS);

    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in &request.topics {
        let outcome = if oversized {
            Err((POLICY_VIOLATION, Some(EXCESSIVE_PARTITIONS.to_owned())))
        } else {
            create_topic(ctx, topic, request.validate_only).await
        };

        topics.push(match outcome {
            Ok(created) => created,
            Err((code, message)) => CreatableTopicResult::default()
                .with_name(topic.name.clone())
                .with_error_code(code)
                .with_error_message(message.map(StrBytes::from)),
        });
    }

    let response = CreateTopicsResponse::default().with_topics(topics);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn create_topic(
    ctx: &BrokerContext,
    topic: &CreatableTopic,
    validate_only: bool,
) -> Result<CreatableTopicResult, Failure> {
    let name = topic.name.to_string();
    if !is_kafka_name(&name) {
        return Err((
            INVALID_REQUEST,
            Some(format!("{name} is not a valid topic name")),
        ));
    }

    let (partitions, owners) = partitions(ctx, topic)?;
    let (schema_name, schema_validate) = schema_configs(&topic.configs)?;
    cleanup_policy(&topic.configs)?;

    let mut command = CreateCommand {
        partitions,
        owners,
        ..CreateCommand::new(&name, KAFKA_CREATED_CT)
    };
    if let Some(schema_name) = schema_name {
        command = command
            .with_schema_name(schema_name)
            .with_schema_validate(schema_validate);
    }

    if validate_only {
        command.validate().map_err(service_failure)?;
        if ctx
            .topics
            .get_kafka(&name)
            .await
            .map_err(service_failure)?
            .is_some()
        {
            return Err(already_exists(&name));
        }

        return Ok(created(topic, partitions, Uuid::nil()));
    }

    match ctx.topics.create(command).await {
        Ok(result) if result.created => Ok(created(
            topic,
            result.meta.topic.partitions,
            topic_uuid(result.meta.topic.id.0),
        )),
        Ok(_) => Err(already_exists(&name)),
        Err(error) => Err(service_failure(error)),
    }
}

fn requested_partitions(topic: &CreatableTopic) -> u64 {
    if topic.assignments.is_empty() {
        u64::try_from(topic.num_partitions).unwrap_or(1)
    } else {
        topic.assignments.len() as u64
    }
}

/// The partition count and, for a manual assignment, the broker each
/// partition goes to.
fn partitions(ctx: &BrokerContext, topic: &CreatableTopic) -> Result<(u32, Vec<i32>), Failure> {
    if topic.assignments.is_empty() {
        if !matches!(topic.replication_factor, 1 | -1) {
            return Err((
                INVALID_REPLICATION_FACTOR,
                Some("PicoMQ keeps one replica of each partition, use 1 or -1".to_owned()),
            ));
        }

        return match topic.num_partitions {
            -1 => Ok((1, Vec::new())),
            count if count > 0 => Ok((count as u32, Vec::new())),
            _ => Err((
                INVALID_PARTITIONS,
                Some("Number of partitions was set to an invalid non-positive value.".to_owned()),
            )),
        };
    }

    if topic.replication_factor != -1 {
        return Err((
            INVALID_REPLICATION_FACTOR,
            Some(
                "A manual partition assignment was specified, but replication factor was not set to -1."
                    .to_owned(),
            ),
        ));
    }
    if topic.num_partitions != -1 {
        return Err((
            INVALID_PARTITIONS,
            Some(
                "A manual partition assignment was specified, but numPartitions was not set to -1."
                    .to_owned(),
            ),
        ));
    }

    let mut owners = Vec::with_capacity(topic.assignments.len());
    for (position, assignment) in topic.assignments.iter().enumerate() {
        if assignment.partition_index != position as i32 {
            return Err((
                INVALID_REPLICA_ASSIGNMENT,
                Some("partitions should be a consecutive 0-based integer sequence".to_owned()),
            ));
        }
        owners.push(assigned_broker(ctx, &assignment.broker_ids)?);
    }

    Ok((owners.len() as u32, owners))
}

fn created(topic: &CreatableTopic, partitions: u32, id: Uuid) -> CreatableTopicResult {
    CreatableTopicResult::default()
        .with_name(topic.name.clone())
        .with_error_code(NO_ERROR)
        .with_topic_config_error_code(NO_ERROR)
        .with_num_partitions(partitions as i32)
        .with_replication_factor(1)
        .with_topic_id(id)
}

fn already_exists(name: &str) -> Failure {
    (
        TOPIC_ALREADY_EXISTS,
        Some(format!("Topic '{name}' already exists.")),
    )
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
        let topic = match resolve_topic(ctx, &name).await {
            Ok(topic) => topic,
            Err(code) => {
                responses.push(
                    DeletableTopicResult::default()
                        .with_name(Some(topic_name(&name)))
                        .with_error_code(code),
                );
                continue;
            }
        };
        match ctx.topics.delete(topic.name.spelling()).await {
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

fn schema_configs(configs: &[CreatableTopicConfig]) -> Result<(Option<String>, bool), Failure> {
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
                return Err(invalid_config(config.name.as_str()));
            }
            continue;
        };
        match config.name.as_str() {
            SCHEMA_CONFIG => {
                if schema_name.is_some() {
                    return Err(invalid_config(SCHEMA_CONFIG));
                }
                schema_name = Some(value.to_owned());
            }
            SCHEMA_VALIDATE_CONFIG => match value {
                "true" => schema_validate = true,
                "false" => schema_validate = false,
                _ => return Err(invalid_config(SCHEMA_VALIDATE_CONFIG)),
            },
            _ => {}
        }
    }
    Ok((schema_name, schema_validate))
}

/// PicoMQ never compacts, so a topic that asks for compaction is refused
/// rather than created without it.
fn cleanup_policy(configs: &[CreatableTopicConfig]) -> Result<(), Failure> {
    let compacted = configs
        .iter()
        .filter(|config| config.name.as_str() == "cleanup.policy")
        .filter_map(|config| config.value.as_ref())
        .any(|value| value.as_str() != "delete");
    if compacted {
        return Err((
            INVALID_CONFIG,
            Some("PicoMQ does not compact topics, cleanup.policy must be delete".to_owned()),
        ));
    }

    Ok(())
}

fn invalid_config(name: &str) -> Failure {
    (INVALID_REQUEST, Some(format!("invalid value for {name}")))
}
