use bytes::Bytes;
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use kafka_protocol::protocol::Decodable;
use picomq_server::{SubmittedBatchAppend, Topic};

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    NO_ERROR, encode_response, leader_partition, resolve_topic, service_error_code, topic_name,
};
use crate::handlers::{HandlerError, HandlerOutcome};

pub async fn handle(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = ProduceRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    if request.transactional_id.is_some() {
        return Err(HandlerError::Protocol(
            "transactional produce is not supported".into(),
        ));
    }

    let mut pending = Vec::with_capacity(request.topic_data.len());
    for topic in request.topic_data {
        let topic_name_str = topic.name.to_string();
        let resolved = resolve_topic(ctx, &topic_name_str).await;

        let mut partitions = Vec::with_capacity(topic.partition_data.len());
        for partition in topic.partition_data {
            let index = partition.index;
            let submitted = match &resolved {
                Ok(resolved) => {
                    let records = partition.records.unwrap_or_default();
                    submit_partition(ctx, resolved, index, records).await
                }
                Err(code) => Err(*code),
            };
            partitions.push(match submitted {
                Ok(submitted) => PartitionSubmit::Pending(index, submitted),
                Err(code) => failed(index, code),
            });
        }
        pending.push((topic_name_str, partitions));
    }

    let topics = ctx.topics.clone();
    let acks = request.acks;
    let correlation_id = req.correlation_id;
    let api_version = req.api_version;
    Ok(HandlerOutcome::Deferred(Box::pin(async move {
        let mut topic_responses = Vec::with_capacity(pending.len());
        for (topic_name_str, partitions) in pending {
            let mut partition_responses = Vec::with_capacity(partitions.len());
            for partition in partitions {
                partition_responses.push(match partition {
                    PartitionSubmit::Ready(response) => response,
                    PartitionSubmit::Pending(index, submitted) => {
                        match topics.finish_batch(submitted).await {
                            Ok(result) => PartitionProduceResponse::default()
                                .with_index(index)
                                .with_error_code(NO_ERROR)
                                .with_base_offset(result.base_offset as i64)
                                .with_log_start_offset(result.log_start_offset as i64),
                            Err(error) => PartitionProduceResponse::default()
                                .with_index(index)
                                .with_error_code(service_error_code(&error)),
                        }
                    }
                });
            }
            topic_responses.push(
                TopicProduceResponse::default()
                    .with_name(topic_name(&topic_name_str))
                    .with_partition_responses(partition_responses),
            );
        }

        if acks == 0 {
            return Ok(HandlerOutcome::NoResponse);
        }

        let response = ProduceResponse::default().with_responses(topic_responses);
        Ok(HandlerOutcome::Response(encode_response(
            correlation_id,
            api_version,
            &response,
        )))
    })))
}

enum PartitionSubmit {
    Ready(PartitionProduceResponse),
    Pending(i32, SubmittedBatchAppend),
}

fn failed(index: i32, code: i16) -> PartitionSubmit {
    PartitionSubmit::Ready(
        PartitionProduceResponse::default()
            .with_index(index)
            .with_error_code(code),
    )
}

async fn submit_partition(
    ctx: &BrokerContext,
    topic: &Topic,
    index: i32,
    records: Bytes,
) -> Result<SubmittedBatchAppend, i16> {
    let partition = leader_partition(ctx, topic, index).await?;

    ctx.topics
        .submit_batch(topic, partition, records)
        .await
        .map_err(|error| {
            tracing::debug!(%error, topic = %topic.name, index, "produce rejected");
            service_error_code(&error)
        })
}
