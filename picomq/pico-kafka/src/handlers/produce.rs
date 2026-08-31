use bytes::Bytes;
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::protocol::Decodable;
use kafka_protocol::records::RecordBatchDecoder;
use picomq_server::{
    AppendBatchCommand, BatchSpan, NumericProducer, SchemaBatch, SchemaRecord, SubmittedBatchAppend,
};

use crate::batch::decode_batches;
use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    encode_response, ensure_local_leader, service_error_code, topic_name, CORRUPT_MESSAGE,
    INVALID_RECORD, INVALID_REQUEST, NO_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::handlers::{HandlerError, HandlerOutcome};
use crate::topic::{stream_name, validate_topic_name};

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

    let mut topics = Vec::with_capacity(request.topic_data.len());
    for topic in request.topic_data {
        let topic_name_str = topic.name.to_string();
        if !validate_topic_name(&topic_name_str) {
            topics.push((
                topic_name_str,
                vec![PartitionSubmit::Ready(
                    PartitionProduceResponse::default()
                        .with_index(0)
                        .with_error_code(UNKNOWN_TOPIC_OR_PARTITION),
                )],
            ));
            continue;
        }
        let stream = stream_name(&topic_name_str);
        if let Err(code) = ensure_local_leader(ctx, &stream).await {
            topics.push((
                topic_name_str,
                vec![PartitionSubmit::Ready(
                    PartitionProduceResponse::default()
                        .with_index(0)
                        .with_error_code(code),
                )],
            ));
            continue;
        }

        let mut partitions = Vec::new();
        for partition in topic.partition_data {
            if partition.index != 0 {
                partitions.push(PartitionSubmit::Ready(
                    PartitionProduceResponse::default()
                        .with_index(partition.index)
                        .with_error_code(UNKNOWN_TOPIC_OR_PARTITION),
                ));
                continue;
            }
            let records = partition.records.unwrap_or_default();
            partitions.push(match submit_partition(ctx, &stream, records).await {
                Ok(submitted) => PartitionSubmit::Pending(submitted),
                Err(code) => PartitionSubmit::Ready(
                    PartitionProduceResponse::default()
                        .with_index(0)
                        .with_error_code(code),
                ),
            });
        }
        topics.push((topic_name_str, partitions));
    }

    let service = ctx.service.clone();
    let acks = request.acks;
    let correlation_id = req.correlation_id;
    let api_version = req.api_version;
    Ok(HandlerOutcome::Deferred(Box::pin(async move {
        let mut topic_responses = Vec::with_capacity(topics.len());
        for (topic_name_str, partitions) in topics {
            let mut partition_responses = Vec::with_capacity(partitions.len());
            for partition in partitions {
                partition_responses.push(match partition {
                    PartitionSubmit::Ready(response) => response,
                    PartitionSubmit::Pending(submitted) => {
                        match service.finish_batch_append(submitted).await {
                            Ok(result) => PartitionProduceResponse::default()
                                .with_index(0)
                                .with_error_code(NO_ERROR)
                                .with_base_offset(result.base_offset as i64)
                                .with_log_start_offset(result.log_start_offset as i64),
                            Err(error) => PartitionProduceResponse::default()
                                .with_index(0)
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
    Pending(SubmittedBatchAppend),
}

async fn submit_partition(
    ctx: &BrokerContext,
    stream: &str,
    records: Bytes,
) -> Result<SubmittedBatchAppend, i16> {
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
    let schema_name = ctx
        .service
        .validation_schema_of(stream)
        .await
        .map_err(|error| {
            tracing::debug!(%error, stream, "schema bind lookup failed");
            service_error_code(&error)
        })?;
    if let Some(schema_name) = schema_name {
        let batch = schema_batch(stream, &records)?;
        if let Err(error) = ctx.service.validate_schema(&schema_name, &batch).await {
            tracing::debug!(%error, stream, %schema_name, "schema validation failed");
            return Err(INVALID_RECORD);
        }
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
    ctx.service
        .submit_batch_append(AppendBatchCommand {
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
        })
}

fn schema_batch(stream: &str, records: &Bytes) -> Result<SchemaBatch, i16> {
    let mut buf = records.clone();
    let sets = RecordBatchDecoder::decode_all(&mut buf).map_err(|error| {
        tracing::debug!(%error, stream, "rejected produce payload");
        CORRUPT_MESSAGE
    })?;
    let records = sets
        .iter()
        .flat_map(|set| set.records.iter())
        .map(|record| {
            let mut builder = SchemaRecord::builder();
            if let Some(key) = record.key.clone() {
                builder = builder.key(key);
            }
            if let Some(value) = record.value.clone() {
                builder = builder.value(value);
            }
            builder.build()
        })
        .collect();
    Ok(SchemaBatch { records })
}
