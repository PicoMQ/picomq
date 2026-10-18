use kafka_protocol::messages::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::protocol::Decodable;

use picomq_server::ServiceError;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, MAX_TIMESTAMP, NO_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
    encode_response, ensure_local_leader, resolve_topic, service_error_code, topic_name,
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
                .push(list_partition(ctx, &stream, partition.timestamp, req.api_version).await);
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
) -> ListOffsetsPartitionResponse {
    let (code, offset, timestamp) = match list_offset(ctx, stream, timestamp).await {
        Ok((offset, timestamp)) => (NO_ERROR, offset, timestamp),
        Err(error) => (service_error_code(&error), -1, -1),
    };

    let response = ListOffsetsPartitionResponse::default()
        .with_partition_index(0)
        .with_error_code(code)
        .with_offset(offset)
        .with_timestamp(timestamp);

    if api_version >= 4 {
        response.with_leader_epoch(-1)
    } else {
        response
    }
}

async fn list_offset(
    ctx: &BrokerContext,
    stream: &str,
    timestamp: i64,
) -> Result<(i64, i64), ServiceError> {
    let found = match timestamp {
        EARLIEST_TIMESTAMP => {
            let watermarks = ctx.service.watermarks(stream).await?;
            return Ok((watermarks.log_start_offset as i64, timestamp));
        }
        LATEST_TIMESTAMP => {
            let watermarks = ctx.service.watermarks(stream).await?;
            return Ok((watermarks.high_watermark as i64, timestamp));
        }
        MAX_TIMESTAMP => ctx.service.record_with_max_timestamp(stream).await?,
        target => ctx.service.record_at_or_after(stream, target).await?,
    };

    Ok(found
        .map(|record| {
            (
                record.offset.record_offset() as i64,
                record.record.timestamp_ms,
            )
        })
        .unwrap_or((-1, -1)))
}
