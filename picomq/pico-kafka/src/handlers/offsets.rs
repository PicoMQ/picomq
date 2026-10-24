use kafka_protocol::messages::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_request::ListOffsetsPartition;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::protocol::Decodable;

use picomq_server::Topic;
use picomq_server::record::leader_epoch;

use picomq_auth::Operation;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, MAX_TIMESTAMP, NO_ERROR, encode_response, leader_epochs,
    leader_partition, leader_watermarks, resolve_topic, service_error_code, topic_name,
};
use crate::handlers::{HandlerError, HandlerOutcome};

const NOT_FOUND: (i64, i64, i32) = (-1, -1, -1);

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
        let resolved = resolve_topic(ctx, req, Operation::Head, &topic_name_str).await;

        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let index = partition.partition_index;
            let leader = match &resolved {
                Ok(resolved) => leader_partition(ctx, resolved, index)
                    .await
                    .map(|leader| (resolved, leader)),
                Err(code) => Err(*code),
            };
            partitions.push(match leader {
                Ok((topic, leader)) => {
                    list_partition(ctx, topic, leader, index, &partition, req.api_version).await
                }
                Err(code) => ListOffsetsPartitionResponse::default()
                    .with_partition_index(index)
                    .with_error_code(code),
            });
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
    topic: &Topic,
    partition: u32,
    index: i32,
    requested: &ListOffsetsPartition,
    api_version: i16,
) -> ListOffsetsPartitionResponse {
    let (code, (offset, timestamp, epoch)) =
        match list_offset(ctx, topic, partition, requested).await {
            Ok(found) => (NO_ERROR, found),
            Err(code) => (code, NOT_FOUND),
        };

    let response = ListOffsetsPartitionResponse::default()
        .with_partition_index(index)
        .with_error_code(code)
        .with_offset(offset)
        .with_timestamp(timestamp);

    if api_version >= 4 {
        response.with_leader_epoch(epoch)
    } else {
        response
    }
}

async fn list_offset(
    ctx: &BrokerContext,
    topic: &Topic,
    partition: u32,
    requested: &ListOffsetsPartition,
) -> Result<(i64, i64, i32), i16> {
    let watermarks =
        leader_watermarks(ctx, topic, partition, requested.current_leader_epoch).await?;
    let epochs = leader_epochs(ctx, topic, partition)?;
    let at = |offset: u64, timestamp: i64| (offset as i64, timestamp, epoch_at(&epochs, offset));

    let found = match requested.timestamp {
        EARLIEST_TIMESTAMP => return Ok(at(watermarks.log_start_offset, EARLIEST_TIMESTAMP)),
        LATEST_TIMESTAMP => {
            return Ok((
                watermarks.high_watermark as i64,
                LATEST_TIMESTAMP,
                leader_epoch(watermarks.epoch),
            ));
        }
        MAX_TIMESTAMP => ctx.topics.record_with_max_timestamp(topic, partition).await,
        target => {
            ctx.topics
                .record_at_or_after(topic, partition, target)
                .await
        }
    }
    .map_err(|error| service_error_code(&error))?;

    Ok(found.map_or(NOT_FOUND, |record| {
        at(record.offset.record_offset(), record.record.timestamp_ms)
    }))
}

fn epoch_at(epochs: &[(i32, u64)], offset: u64) -> i32 {
    epochs
        .iter()
        .take_while(|(_, start)| *start <= offset)
        .last()
        .map_or(-1, |(epoch, _)| *epoch)
}
