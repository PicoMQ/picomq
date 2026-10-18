//! Fetch: verbatim batch reads with Kafka long-poll.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::select_all;
use kafka_protocol::messages::FetchRequest;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::protocol::Decodable;
use picomq_server::{OffsetToken, StreamWatermarks, Topic, TopicId};
use uuid::Uuid;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    NO_ERROR, OFFSET_OUT_OF_RANGE, UNKNOWN_TOPIC_ID, concat_batches, encode_response,
    leader_partition, resolve_topic, service_error_code, topic_name,
};
use crate::handlers::{HandlerError, HandlerOutcome};

const FETCH_TOPIC_ID_VERSION: i16 = 13;

struct ResolvedTopic {
    name: String,
    topic_id: Uuid,
    partitions: Vec<FetchPartition>,
}

struct FetchPartition {
    index: i32,
    fetch_offset: i64,
    target: Result<(Topic, u32), i16>,
}

struct PartitionRead {
    data: PartitionData,
    bytes: usize,
    wait: Option<u64>,
}

pub async fn handle(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = Bytes::copy_from_slice(body);
    let request = FetchRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let deadline = Instant::now() + Duration::from_millis(request.max_wait_ms.max(0) as u64);
    let min_bytes = request.min_bytes.max(0) as usize;
    let max_bytes = if request.max_bytes > 0 {
        request.max_bytes as usize
    } else {
        usize::MAX
    };

    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in &request.topics {
        let partitions: Vec<(i32, i64)> = topic
            .partitions
            .iter()
            .map(|p| (p.partition, p.fetch_offset))
            .collect();
        topics.push(
            resolve_fetch_topic(
                ctx,
                req.api_version,
                &topic.topic,
                topic.topic_id,
                partitions,
            )
            .await,
        );
    }

    loop {
        let mut responses = Vec::with_capacity(topics.len());
        let mut total_bytes = 0usize;
        let mut any_error = false;
        let mut waits: Vec<(&Topic, u32, u64)> = Vec::new();

        for topic in &topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for requested in &topic.partitions {
                let (resolved, leader) = match &requested.target {
                    Ok((resolved, leader)) => (resolved, *leader),
                    Err(code) => {
                        any_error = true;
                        partitions.push(error_partition(requested.index, *code).data);
                        continue;
                    }
                };

                let budget = max_bytes.saturating_sub(total_bytes).max(1);
                let read = read_partition(ctx, resolved, leader, requested, budget).await;

                total_bytes += read.bytes;
                if read.data.error_code != NO_ERROR {
                    any_error = true;
                }
                if let Some(high_watermark) = read.wait
                    && !waits.iter().any(|(waiting, partition, _)| {
                        waiting.id == resolved.id && *partition == leader
                    })
                {
                    waits.push((resolved, leader, high_watermark));
                }
                partitions.push(read.data);
            }
            let mut response = FetchableTopicResponse::default().with_partitions(partitions);
            if req.api_version >= FETCH_TOPIC_ID_VERSION {
                response = response.with_topic_id(topic.topic_id);
            } else {
                response = response.with_topic(topic_name(&topic.name));
            }
            responses.push(response);
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if total_bytes >= min_bytes || any_error || waits.is_empty() || remaining.is_zero() {
            let mut response =
                kafka_protocol::messages::FetchResponse::default().with_responses(responses);
            if req.api_version >= 7 {
                response = response.with_session_id(0).with_error_code(NO_ERROR);
            }
            return Ok(HandlerOutcome::Response(encode_response(
                req.correlation_id,
                req.api_version,
                &response,
            )));
        }

        let waiters = waits.iter().map(|(topic, partition, high_watermark)| {
            let from = OffsetToken::of_record_offset(*high_watermark);
            Box::pin(
                ctx.topics
                    .wait_partition(topic, *partition, from, remaining),
            )
        });
        let _ = select_all(waiters).await;
    }
}

async fn resolve_fetch_topic(
    ctx: &BrokerContext,
    api_version: i16,
    requested_name: &kafka_protocol::messages::TopicName,
    requested_id: Uuid,
    requested: Vec<(i32, i64)>,
) -> ResolvedTopic {
    let (name, topic) = if api_version >= FETCH_TOPIC_ID_VERSION {
        let topic = match ctx.topics.by_id(&TopicId(*requested_id.as_bytes())).await {
            Ok(Some(topic)) if topic.name.kafka().is_some() => Ok(topic),
            Ok(_) => Err(UNKNOWN_TOPIC_ID),
            Err(error) => Err(service_error_code(&error)),
        };
        let name = topic
            .as_ref()
            .ok()
            .and_then(|topic| topic.name.kafka())
            .unwrap_or_default()
            .to_owned();
        (name, topic)
    } else {
        let name = requested_name.to_string();
        let topic = resolve_topic(ctx, &name).await;
        (name, topic)
    };

    let mut partitions = Vec::with_capacity(requested.len());
    for (index, fetch_offset) in requested {
        let target = match &topic {
            Ok(topic) => leader_partition(ctx, topic, index)
                .await
                .map(|leader| (topic.clone(), leader)),
            Err(code) => Err(*code),
        };
        partitions.push(FetchPartition {
            index,
            fetch_offset,
            target,
        });
    }

    ResolvedTopic {
        name,
        topic_id: requested_id,
        partitions,
    }
}

async fn read_partition(
    ctx: &BrokerContext,
    topic: &Topic,
    partition: u32,
    requested: &FetchPartition,
    max_bytes: usize,
) -> PartitionRead {
    let index = requested.index;
    if requested.fetch_offset < 0 {
        return error_partition(index, OFFSET_OUT_OF_RANGE);
    }

    let from = requested.fetch_offset as u64;
    let watermarks = match ctx.topics.watermarks(topic, partition).await {
        Ok(watermarks) => watermarks,
        Err(error) => return error_partition(index, service_error_code(&error)),
    };
    if from < watermarks.log_start_offset || from > watermarks.high_watermark {
        return PartitionRead {
            data: partition_data(index, OFFSET_OUT_OF_RANGE, &watermarks, None),
            bytes: 0,
            wait: None,
        };
    }
    if from == watermarks.high_watermark {
        return PartitionRead {
            data: partition_data(index, NO_ERROR, &watermarks, None),
            bytes: 0,
            wait: Some(watermarks.high_watermark),
        };
    }

    match ctx
        .topics
        .read_batches(topic, partition, from, max_bytes)
        .await
    {
        Ok(read) => {
            let payload = concat_batches(&read.batches);
            let bytes = payload.len();
            let watermarks = StreamWatermarks {
                log_start_offset: read.log_start_offset,
                high_watermark: read.high_watermark,
            };

            PartitionRead {
                data: partition_data(index, NO_ERROR, &watermarks, Some(payload)),
                bytes,
                wait: Some(read.high_watermark),
            }
        }
        Err(error) => error_partition(index, service_error_code(&error)),
    }
}

fn partition_data(
    index: i32,
    error_code: i16,
    watermarks: &StreamWatermarks,
    records: Option<Bytes>,
) -> PartitionData {
    PartitionData::default()
        .with_partition_index(index)
        .with_error_code(error_code)
        .with_high_watermark(watermarks.high_watermark as i64)
        .with_last_stable_offset(watermarks.high_watermark as i64)
        .with_log_start_offset(watermarks.log_start_offset as i64)
        .with_aborted_transactions(None)
        .with_records(Some(records.unwrap_or_default()))
}

fn error_partition(partition_index: i32, error_code: i16) -> PartitionRead {
    PartitionRead {
        data: PartitionData::default()
            .with_partition_index(partition_index)
            .with_error_code(error_code)
            .with_high_watermark(-1)
            .with_last_stable_offset(-1)
            .with_log_start_offset(-1)
            .with_aborted_transactions(None),
        bytes: 0,
        wait: None,
    }
}
