//! Fetch: verbatim batch reads with Kafka long-poll semantics. Waking is
//! event-driven off the per-stream waiter registry, never a poll loop.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::select_all;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::FetchRequest;
use kafka_protocol::protocol::Decodable;
use pico_server::OffsetToken;
use uuid::Uuid;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    concat_batches, encode_response, ensure_local_leader, service_error_code, topic_name, NO_ERROR,
    OFFSET_OUT_OF_RANGE, UNKNOWN_TOPIC_ID, UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::handlers::{HandlerError, HandlerOutcome};
use crate::topic::{stream_name, topic_from_stream, validate_topic_name};

/// Topics are addressed by name below v13 and by UUID from v13 on.
const FETCH_TOPIC_ID_VERSION: i16 = 13;

struct ResolvedTopic {
    /// Identity echoed back to the client (name pre-v13, UUID from v13).
    name: String,
    topic_id: Uuid,
    /// Stream to serve from, or the error code every partition reports.
    stream: Result<String, i16>,
    partitions: Vec<(i32, i64)>,
}

struct PartitionRead {
    data: PartitionData,
    bytes: usize,
    /// Stream and the high watermark observed, to park on for growth.
    wait: Option<(String, u64)>,
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
            resolve_topic(
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
        let mut waits: Vec<(String, u64)> = Vec::new();

        for topic in &topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for &(partition_index, fetch_offset) in &topic.partitions {
                let read = match &topic.stream {
                    Err(code) => error_partition(partition_index, *code),
                    Ok(_) if partition_index != 0 => {
                        error_partition(partition_index, UNKNOWN_TOPIC_OR_PARTITION)
                    }
                    Ok(stream) => {
                        let budget = max_bytes.saturating_sub(total_bytes).max(1);
                        read_partition(ctx, stream, fetch_offset, budget).await
                    }
                };
                total_bytes += read.bytes;
                if read.data.error_code != NO_ERROR {
                    any_error = true;
                }
                if let Some(wait) = read.wait {
                    if !waits.contains(&wait) {
                        waits.push(wait);
                    }
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

        // Park until any requested stream grows past the watermark we just
        // observed, then re-read. `wait_appended` is waiter-registry backed:
        // it returns immediately only when the stream already grew.
        let waiters = waits.iter().map(|(stream, high_watermark)| {
            let service = ctx.service.clone();
            let stream = stream.clone();
            let from = OffsetToken::of_record_offset(*high_watermark);
            Box::pin(async move { service.wait_appended(&stream, from, remaining).await })
        });
        let _ = select_all(waiters).await;
    }
}

async fn resolve_topic(
    ctx: &BrokerContext,
    api_version: i16,
    requested_name: &kafka_protocol::messages::TopicName,
    requested_id: Uuid,
    partitions: Vec<(i32, i64)>,
) -> ResolvedTopic {
    if api_version >= FETCH_TOPIC_ID_VERSION {
        let stream = match ctx
            .service
            .lookup_by_external_id(*requested_id.as_bytes())
            .await
        {
            Ok(Some(stream)) if topic_from_stream(&stream).is_some() => Ok(stream),
            Ok(_) => Err(UNKNOWN_TOPIC_ID),
            Err(error) => Err(service_error_code(&error)),
        };
        let name = stream
            .as_ref()
            .ok()
            .and_then(|s| topic_from_stream(s))
            .unwrap_or_default()
            .to_owned();
        let stream = match stream {
            Ok(stream) => ensure_leader(ctx, stream).await,
            Err(code) => Err(code),
        };
        return ResolvedTopic {
            name,
            topic_id: requested_id,
            stream,
            partitions,
        };
    }

    let name = requested_name.to_string();
    if !validate_topic_name(&name) {
        return ResolvedTopic {
            name,
            topic_id: requested_id,
            stream: Err(UNKNOWN_TOPIC_OR_PARTITION),
            partitions,
        };
    }
    let stream = ensure_leader(ctx, stream_name(&name)).await;
    ResolvedTopic {
        name,
        topic_id: requested_id,
        stream,
        partitions,
    }
}

async fn ensure_leader(ctx: &BrokerContext, stream: String) -> Result<String, i16> {
    ensure_local_leader(ctx, &stream).await?;
    Ok(stream)
}

async fn read_partition(
    ctx: &BrokerContext,
    stream: &str,
    fetch_offset: i64,
    max_bytes: usize,
) -> PartitionRead {
    if fetch_offset < 0 {
        return error_partition(0, OFFSET_OUT_OF_RANGE);
    }
    let from = fetch_offset as u64;
    let watermarks = match ctx.service.watermarks(stream).await {
        Ok(watermarks) => watermarks,
        Err(error) => return error_partition(0, service_error_code(&error)),
    };
    if from < watermarks.log_start_offset || from > watermarks.high_watermark {
        return PartitionRead {
            data: partition_data(OFFSET_OUT_OF_RANGE, &watermarks, None),
            bytes: 0,
            wait: None,
        };
    }
    if from == watermarks.high_watermark {
        return PartitionRead {
            data: partition_data(NO_ERROR, &watermarks, None),
            bytes: 0,
            wait: Some((stream.to_owned(), watermarks.high_watermark)),
        };
    }
    match ctx.service.read_batches(stream, from, max_bytes).await {
        Ok(read) => {
            let payload = concat_batches(&read.batches);
            let bytes = payload.len();
            PartitionRead {
                data: partition_data(
                    NO_ERROR,
                    &pico_server::StreamWatermarks {
                        log_start_offset: read.log_start_offset,
                        high_watermark: read.high_watermark,
                    },
                    Some(payload),
                ),
                bytes,
                wait: Some((stream.to_owned(), read.high_watermark)),
            }
        }
        Err(error) => error_partition(0, service_error_code(&error)),
    }
}

fn partition_data(
    error_code: i16,
    watermarks: &pico_server::StreamWatermarks,
    records: Option<Bytes>,
) -> PartitionData {
    PartitionData::default()
        .with_partition_index(0)
        .with_error_code(error_code)
        .with_high_watermark(watermarks.high_watermark as i64)
        .with_last_stable_offset(watermarks.high_watermark as i64)
        .with_log_start_offset(watermarks.log_start_offset as i64)
        .with_aborted_transactions(None)
        // Empty, not null. librdkafka only raises partition EOF on an empty
        // record set.
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
