use std::time::Duration;

use futures::future::join_all;
use kafka_protocol::messages::DeleteRecordsRequest;
use kafka_protocol::messages::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
};
use kafka_protocol::protocol::Decodable;
use picomq_server::Topic;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    NO_ERROR, OFFSET_OUT_OF_RANGE, encode_response, leader_partition, resolve_topic,
    service_error_code,
};
use crate::handlers::{HandlerError, HandlerOutcome};

const HIGH_WATERMARK: i64 = -1;

pub async fn delete(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = DeleteRecordsRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;
    let timeout = Duration::from_millis(request.timeout_ms.max(0) as u64);

    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in &request.topics {
        let resolved = resolve_topic(ctx, topic.name.as_str()).await;
        let partitions = join_all(topic.partitions.iter().map(|partition| async {
            let (low_watermark, code) = match trim(
                ctx,
                &resolved,
                partition.partition_index,
                partition.offset,
                timeout,
            )
            .await
            {
                Ok(low_watermark) => (low_watermark, NO_ERROR),
                Err(code) => (-1, code),
            };

            DeleteRecordsPartitionResult::default()
                .with_partition_index(partition.partition_index)
                .with_low_watermark(low_watermark)
                .with_error_code(code)
        }))
        .await;

        topics.push(
            DeleteRecordsTopicResult::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions),
        );
    }

    let response = DeleteRecordsResponse::default().with_topics(topics);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn trim(
    ctx: &BrokerContext,
    topic: &Result<Topic, i16>,
    index: i32,
    offset: i64,
    timeout: Duration,
) -> Result<i64, i16> {
    let topic = topic.as_ref().map_err(|code| *code)?;
    let partition = leader_partition(ctx, topic, index).await?;
    let watermarks = ctx
        .topics
        .watermarks(topic, partition)
        .await
        .map_err(|error| service_error_code(&error))?;

    let target = match offset {
        HIGH_WATERMARK => watermarks.high_watermark,
        offset if offset >= 0 && offset as u64 <= watermarks.high_watermark => offset as u64,
        _ => return Err(OFFSET_OUT_OF_RANGE),
    };

    ctx.topics
        .delete_records(topic, partition, target, timeout)
        .await
        .map(|low_watermark| low_watermark as i64)
        .map_err(|error| service_error_code(&error))
}
