use kafka_protocol::messages::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::protocol::Decodable;

use picomq_server::record::decode_batches;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, NO_ERROR, UNKNOWN_TOPIC_OR_PARTITION, encode_response,
    ensure_local_leader, resolve_topic, service_error_code, topic_name,
};
use crate::handlers::{HandlerError, HandlerOutcome};

pub async fn handle(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = ListOffsetsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in request.topics {
        let topic_name_str = topic.name.to_string();
        let stream = match resolve_topic(ctx, &topic_name_str).await {
            Ok(stream) => stream,
            Err(code) => {
                topics.push(
                    ListOffsetsTopicResponse::default()
                        .with_name(topic_name(&topic_name_str))
                        .with_partitions(vec![
                            ListOffsetsPartitionResponse::default()
                                .with_partition_index(0)
                                .with_error_code(code),
                        ]),
                );
                continue;
            }
        };
        if let Err(code) = ensure_local_leader(ctx, &stream).await {
            topics.push(
                ListOffsetsTopicResponse::default()
                    .with_name(topic_name(&topic_name_str))
                    .with_partitions(vec![
                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(0)
                            .with_error_code(code),
                    ]),
            );
            continue;
        }

        let mut partitions = Vec::new();
        for partition in topic.partitions {
            if partition.partition_index != 0 {
                partitions.push(
                    ListOffsetsPartitionResponse::default()
                        .with_partition_index(partition.partition_index)
                        .with_error_code(UNKNOWN_TOPIC_OR_PARTITION),
                );
                continue;
            }
            partitions
                .push(list_partition(ctx, &stream, partition.timestamp, req.api_version).await?);
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(topic_name(&topic_name_str))
                .with_partitions(partitions),
        );
    }

    let response = ListOffsetsResponse::default().with_topics(topics);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn list_partition(
    ctx: &BrokerContext,
    stream: &str,
    timestamp: i64,
    api_version: i16,
) -> Result<ListOffsetsPartitionResponse, HandlerError> {
    let (code, offset, ts) = match list_offset(ctx, stream, timestamp).await {
        Ok((offset, ts)) => (NO_ERROR, offset, ts),
        Err(error) => (service_error_code(&error), -1, -1),
    };
    let mut response = ListOffsetsPartitionResponse::default()
        .with_partition_index(0)
        .with_error_code(code)
        .with_offset(offset)
        .with_timestamp(ts);
    if api_version >= 4 {
        response = response.with_leader_epoch(-1);
    }
    Ok(response)
}

async fn list_offset(
    ctx: &BrokerContext,
    stream: &str,
    timestamp: i64,
) -> Result<(i64, i64), picomq_server::ServiceError> {
    let watermarks = ctx.service.watermarks(stream).await?;
    Ok(match timestamp {
        EARLIEST_TIMESTAMP => (watermarks.log_start_offset as i64, timestamp),
        LATEST_TIMESTAMP => (watermarks.high_watermark as i64, timestamp),
        target => resolve_timestamp(ctx, stream, target, &watermarks).await?,
    })
}

async fn resolve_timestamp(
    ctx: &BrokerContext,
    stream: &str,
    target: i64,
    watermarks: &picomq_server::StreamWatermarks,
) -> Result<(i64, i64), picomq_server::ServiceError> {
    if watermarks.high_watermark <= watermarks.log_start_offset {
        return Ok((watermarks.high_watermark as i64, target));
    }
    let mut cursor = watermarks.log_start_offset;
    let mut last_match = (watermarks.high_watermark as i64, target);
    while cursor < watermarks.high_watermark {
        let read = ctx
            .service
            .read_batches(stream, cursor, 4 * 1024 * 1024)
            .await?;
        if read.batches.is_empty() {
            break;
        }
        for batch in read.batches {
            let records = decode_batches(&batch.payload).map_err(|error| {
                picomq_server::ServiceError::with_message(
                    picomq_server::ErrorKind::CorruptBatch,
                    None,
                    false,
                    error.to_string(),
                )
            })?;
            for record in records {
                let offset = record.offset.record_offset() as i64;
                if record.record.timestamp_ms >= target {
                    return Ok((offset, record.record.timestamp_ms));
                }
                last_match = (offset, record.record.timestamp_ms);
            }
        }
        cursor = read.next_offset;
    }
    Ok(last_match)
}
