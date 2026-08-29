use bytes::Bytes;
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::protocol::Decodable;
use pico_server::{AppendBatchCommand, BatchSpan, NumericProducer};

use crate::batch::decode_batches;
use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    encode_response, ensure_local_leader, service_error_code, topic_name, CORRUPT_MESSAGE,
    INVALID_REQUEST, NO_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::handlers::{HandlerError, HandlerOutcome};
use crate::topic::{is_catalog_name, stream_name, validate_topic_name};

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

    let mut topic_responses = Vec::with_capacity(request.topic_data.len());
    for topic in request.topic_data {
        let topic_name_str = topic.name.to_string();
        if let Err(code) = writable(&topic_name_str) {
            topic_responses.push(topic_error(&topic_name_str, code));
            continue;
        }
        let stream = stream_name(&topic_name_str);
        if let Err(code) = ensure_local_leader(ctx, &stream).await {
            topic_responses.push(topic_error(&topic_name_str, code));
            continue;
        }

        let mut partition_responses = Vec::new();
        for partition in topic.partition_data {
            if partition.index != 0 {
                partition_responses.push(
                    PartitionProduceResponse::default()
                        .with_index(partition.index)
                        .with_error_code(UNKNOWN_TOPIC_OR_PARTITION),
                );
                continue;
            }
            let records = partition.records.unwrap_or_default();
            let response = match produce_partition(ctx, &stream, records).await {
                Ok((base_offset, log_start_offset)) => PartitionProduceResponse::default()
                    .with_index(0)
                    .with_error_code(NO_ERROR)
                    .with_base_offset(base_offset)
                    .with_log_start_offset(log_start_offset),
                Err(code) => PartitionProduceResponse::default()
                    .with_index(0)
                    .with_error_code(code),
            };
            partition_responses.push(response);
        }
        topic_responses.push(
            TopicProduceResponse::default()
                .with_name(topic_name(&topic_name_str))
                .with_partition_responses(partition_responses),
        );
    }

    if request.acks == 0 {
        return Ok(HandlerOutcome::NoResponse);
    }

    let response = ProduceResponse::default().with_responses(topic_responses);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

fn writable(topic: &str) -> Result<(), i16> {
    if !validate_topic_name(topic) {
        return Err(UNKNOWN_TOPIC_OR_PARTITION);
    }
    if is_catalog_name(topic) {
        return Err(INVALID_REQUEST);
    }
    Ok(())
}

fn topic_error(topic: &str, code: i16) -> TopicProduceResponse {
    TopicProduceResponse::default()
        .with_name(topic_name(topic))
        .with_partition_responses(vec![PartitionProduceResponse::default()
            .with_index(0)
            .with_error_code(code)])
}

/// Returns `(base_offset, log_start_offset)` or a Kafka partition error code.
async fn produce_partition(
    ctx: &BrokerContext,
    stream: &str,
    records: Bytes,
) -> Result<(i64, i64), i16> {
    let batches = decode_batches(&records).map_err(|error| {
        tracing::debug!(%error, stream, "rejected produce payload");
        CORRUPT_MESSAGE
    })?;
    if batches
        .iter()
        .any(|batch| batch.info.transactional || batch.info.control)
    {
        return Err(INVALID_REQUEST);
    }
    let first = &batches[0].info;
    let producer = if first.producer_id >= 0 {
        // The service enforces the single-batch requirement.
        Some(NumericProducer {
            id: first.producer_id,
            epoch: first.producer_epoch,
            first_seq: first.base_sequence,
        })
    } else {
        None
    };
    let base_timestamp_ms = first.min_timestamp;
    let spans = batches
        .iter()
        .map(|batch| BatchSpan {
            patch_at: batch.payload_offset,
            record_count: batch.info.record_count.max(0) as u32,
        })
        .collect();
    let result = ctx
        .service
        .append_batch(AppendBatchCommand {
            name: stream.to_owned(),
            payload: records,
            batches: spans,
            producer,
            base_timestamp_ms,
        })
        .await
        .map_err(|error| {
            tracing::debug!(%error, stream, "append failed");
            service_error_code(&error)
        })?;
    Ok((result.base_offset as i64, result.log_start_offset as i64))
}
