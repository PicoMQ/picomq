use kafka_protocol::messages::CreatePartitionsRequest;
use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::create_partitions_response::{
    CreatePartitionsResponse, CreatePartitionsTopicResult,
};
use kafka_protocol::protocol::{Decodable, StrBytes};
use picomq_server::{MAX_NEW_PARTITIONS, UpdateTopicCommand};

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    EXCESSIVE_PARTITIONS, Failure, INVALID_PARTITIONS, INVALID_REPLICA_ASSIGNMENT, NO_ERROR,
    POLICY_VIOLATION, assigned_broker, encode_response, resolve_topic, service_failure,
};
use crate::handlers::{HandlerError, HandlerOutcome};

pub async fn create(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = CreatePartitionsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let mut results = Vec::with_capacity(request.topics.len());
    for topic in &request.topics {
        let (code, message) = grow(ctx, topic, request.validate_only)
            .await
            .err()
            .unwrap_or((NO_ERROR, None));

        results.push(
            CreatePartitionsTopicResult::default()
                .with_name(topic.name.clone())
                .with_error_code(code)
                .with_error_message(message.map(StrBytes::from)),
        );
    }

    let response = CreatePartitionsResponse::default().with_results(results);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn grow(
    ctx: &BrokerContext,
    requested: &CreatePartitionsTopic,
    validate_only: bool,
) -> Result<(), Failure> {
    let topic = resolve_topic(ctx, requested.name.as_str())
        .await
        .map_err(|code| (code, None))?;

    let count = match u32::try_from(requested.count) {
        Ok(count) if count > topic.partitions => count,
        _ => {
            let message = format!(
                "topic currently has {} partitions, which is not lower than the requested {}",
                topic.partitions, requested.count
            );
            return Err((INVALID_PARTITIONS, Some(message)));
        }
    };

    let added = count - topic.partitions;
    if added > MAX_NEW_PARTITIONS {
        return Err((POLICY_VIOLATION, Some(EXCESSIVE_PARTITIONS.to_owned())));
    }

    let mut owners = Vec::new();
    if let Some(assignments) = &requested.assignments {
        if assignments.len() != added as usize {
            let message = format!(
                "Attempted to add {added} additional partition(s), but only {} assignment(s) were specified.",
                assignments.len()
            );
            return Err((INVALID_REPLICA_ASSIGNMENT, Some(message)));
        }
        for assignment in assignments {
            owners.push(assigned_broker(ctx, &assignment.broker_ids)?);
        }
    }

    if validate_only {
        return Ok(());
    }

    ctx.topics
        .update(UpdateTopicCommand {
            name: topic.name.spelling().to_owned(),
            partitions: Some(count),
            owners,
            ..Default::default()
        })
        .await
        .map(|_| ())
        .map_err(service_failure)
}
