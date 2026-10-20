use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult,
};
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;
use kafka_protocol::messages::{
    DescribeConfigsRequest, DescribeConfigsResponse, IncrementalAlterConfigsRequest,
    IncrementalAlterConfigsResponse,
};
use kafka_protocol::protocol::{Decodable, StrBytes};
use picomq_server::{Topic, UpdateTopicCommand};

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    Failure, INVALID_CONFIG, INVALID_REQUEST, NO_ERROR, encode_response, resolve_topic,
    service_failure,
};
use crate::handlers::topics::{SCHEMA_CONFIG, SCHEMA_VALIDATE_CONFIG};
use crate::handlers::{HandlerError, HandlerOutcome};

const TOPIC: i8 = 2;
const BROKER: i8 = 4;

const DYNAMIC_TOPIC_CONFIG: i8 = 1;
const DEFAULT_CONFIG: i8 = 5;

const BOOLEAN: i8 = 1;
const STRING: i8 = 2;
const LONG: i8 = 5;
const LIST: i8 = 7;

const SET: i8 = 0;
const DELETE: i8 = 1;

pub async fn describe(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = DescribeConfigsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let mut results = Vec::with_capacity(request.resources.len());
    for resource in &request.resources {
        let result = DescribeConfigsResult::default()
            .with_resource_type(resource.resource_type)
            .with_resource_name(resource.resource_name.clone());

        results.push(match configs(ctx, resource).await {
            Ok(configs) => result.with_error_code(NO_ERROR).with_configs(configs),
            Err((code, message)) => result
                .with_error_code(code)
                .with_error_message(message.map(StrBytes::from)),
        });
    }

    let response = DescribeConfigsResponse::default().with_results(results);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

pub async fn alter(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = IncrementalAlterConfigsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let mut responses = Vec::with_capacity(request.resources.len());
    for resource in &request.resources {
        let (code, message) = change(ctx, resource, request.validate_only)
            .await
            .err()
            .unwrap_or((NO_ERROR, None));

        responses.push(
            AlterConfigsResourceResponse::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(resource.resource_name.clone())
                .with_error_code(code)
                .with_error_message(message.map(StrBytes::from)),
        );
    }

    let response = IncrementalAlterConfigsResponse::default().with_responses(responses);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn configs(
    ctx: &BrokerContext,
    resource: &DescribeConfigsResource,
) -> Result<Vec<DescribeConfigsResourceResult>, Failure> {
    let name = resource.resource_name.as_str();
    let configs = match resource.resource_type {
        TOPIC => topic_configs(
            &resolve_topic(ctx, name)
                .await
                .map_err(|code| (code, None))?,
        ),
        BROKER if name == ctx.node_id.to_string() => Vec::new(),
        BROKER => {
            return Err((
                INVALID_REQUEST,
                Some(format!("broker {name} is not this node")),
            ));
        }
        other => {
            return Err((
                INVALID_REQUEST,
                Some(format!("resource type {other} has no configs")),
            ));
        }
    };

    Ok(configs
        .into_iter()
        .filter(|config| {
            resource
                .configuration_keys
                .as_ref()
                .is_none_or(|keys| keys.contains(&config.name))
        })
        .collect())
}

fn topic_configs(topic: &Topic) -> Vec<DescribeConfigsResourceResult> {
    let mut configs = vec![
        config("cleanup.policy", "delete", DEFAULT_CONFIG, LIST),
        config("retention.ms", "-1", DEFAULT_CONFIG, LONG),
        config("retention.bytes", "-1", DEFAULT_CONFIG, LONG),
    ];

    if let Some(schema) = &topic.schema_name {
        configs.push(config(SCHEMA_CONFIG, schema, DYNAMIC_TOPIC_CONFIG, STRING));
        configs.push(config(
            SCHEMA_VALIDATE_CONFIG,
            &topic.schema_validate.to_string(),
            DYNAMIC_TOPIC_CONFIG,
            BOOLEAN,
        ));
    }

    configs
}

fn config(name: &str, value: &str, source: i8, kind: i8) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult::default()
        .with_name(StrBytes::from(name.to_owned()))
        .with_value(Some(StrBytes::from(value.to_owned())))
        .with_read_only(source != DYNAMIC_TOPIC_CONFIG)
        .with_config_source(source)
        .with_is_sensitive(false)
        .with_config_type(kind)
        .with_documentation(None)
}

async fn change(
    ctx: &BrokerContext,
    resource: &AlterConfigsResource,
    validate_only: bool,
) -> Result<(), Failure> {
    if resource.resource_type != TOPIC {
        return Err((
            INVALID_REQUEST,
            Some("only topic configs can be changed".to_owned()),
        ));
    }
    let topic = resolve_topic(ctx, resource.resource_name.as_str())
        .await
        .map_err(|code| (code, None))?;

    let mut command = UpdateTopicCommand {
        name: topic.name.spelling().to_owned(),
        ..Default::default()
    };
    for config in &resource.configs {
        let value = config.value.as_ref().map(|value| value.as_str());
        match (config.name.as_str(), config.config_operation, value) {
            (SCHEMA_CONFIG, SET, Some(schema)) => {
                command.schema_name = Some(Some(schema.to_owned()))
            }
            (SCHEMA_CONFIG, DELETE, _) => command.schema_name = Some(None),
            (SCHEMA_VALIDATE_CONFIG, SET, Some(flag)) => {
                command.schema_validate = Some(flag.parse().map_err(|_| {
                    (
                        INVALID_CONFIG,
                        Some(format!("{SCHEMA_VALIDATE_CONFIG} must be true or false")),
                    )
                })?);
            }
            (SCHEMA_VALIDATE_CONFIG, DELETE, _) => command.schema_validate = Some(false),
            (name, _, _) => {
                return Err((
                    INVALID_CONFIG,
                    Some(format!("PicoMQ cannot change {name} this way")),
                ));
            }
        }
    }

    if command.schema_name.is_none() && command.schema_validate.is_none() {
        return Ok(());
    }
    command.validate().map_err(service_failure)?;
    if validate_only {
        return Ok(());
    }

    ctx.topics
        .update(command)
        .await
        .map(|_| ())
        .map_err(service_failure)
}
