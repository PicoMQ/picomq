use kafka_protocol::messages::OffsetForLeaderEpochRequest;
use kafka_protocol::messages::offset_for_leader_epoch_request::OffsetForLeaderPartition;
use kafka_protocol::messages::offset_for_leader_epoch_response::{
    EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
};
use kafka_protocol::protocol::Decodable;
use picomq_server::Topic;
use picomq_server::record::leader_epoch;

use picomq_auth::Operation;

use crate::broker::BrokerContext;
use crate::dispatch::RequestContext;
use crate::handlers::common::{
    NO_ERROR, encode_response, leader_epochs, leader_partition, leader_watermarks, resolve_topic,
};
use crate::handlers::{HandlerError, HandlerOutcome};

const UNDEFINED: (i32, i64) = (-1, -1);

pub async fn handle(
    ctx: &BrokerContext,
    req: &RequestContext,
    body: &[u8],
) -> Result<HandlerOutcome, HandlerError> {
    let mut body = bytes::Bytes::copy_from_slice(body);
    let request = OffsetForLeaderEpochRequest::decode(&mut body, req.api_version)
        .map_err(|error| HandlerError::Protocol(error.to_string()))?;

    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in &request.topics {
        let resolved = resolve_topic(ctx, req, Operation::Head, topic.topic.as_str()).await;

        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in &topic.partitions {
            let result = match &resolved {
                Ok(resolved) => end_offset(ctx, resolved, partition).await,
                Err(code) => Err(*code),
            };
            let ((epoch, end_offset), code) = match result {
                Ok(found) => (found, NO_ERROR),
                Err(code) => (UNDEFINED, code),
            };

            partitions.push(
                EpochEndOffset::default()
                    .with_partition(partition.partition)
                    .with_error_code(code)
                    .with_leader_epoch(epoch)
                    .with_end_offset(end_offset),
            );
        }

        topics.push(
            OffsetForLeaderTopicResult::default()
                .with_topic(topic.topic.clone())
                .with_partitions(partitions),
        );
    }

    let response = OffsetForLeaderEpochResponse::default().with_topics(topics);
    Ok(HandlerOutcome::Response(encode_response(
        req.correlation_id,
        req.api_version,
        &response,
    )))
}

async fn end_offset(
    ctx: &BrokerContext,
    topic: &Topic,
    requested: &OffsetForLeaderPartition,
) -> Result<(i32, i64), i16> {
    let partition = leader_partition(ctx, topic, requested.partition).await?;
    let watermarks =
        leader_watermarks(ctx, topic, partition, requested.current_leader_epoch).await?;
    Ok(epoch_end(
        &leader_epochs(ctx, topic, partition)?,
        leader_epoch(watermarks.epoch),
        watermarks.high_watermark,
        requested.leader_epoch,
    ))
}

fn epoch_end(epochs: &[(i32, u64)], current: i32, high_watermark: u64, wanted: i32) -> (i32, i64) {
    if wanted < 0 {
        return UNDEFINED;
    }
    if wanted == current {
        return (wanted, high_watermark as i64);
    }

    let Some((_, next_start)) = epochs.iter().find(|(epoch, _)| *epoch > wanted) else {
        return UNDEFINED;
    };
    let floor = epochs
        .iter()
        .take_while(|(epoch, _)| *epoch <= wanted)
        .last()
        .map_or(wanted, |(epoch, _)| *epoch);

    (floor, *next_start as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPOCHS: &[(i32, u64)] = &[(2, 10), (5, 30), (6, 30), (9, 40)];

    #[test]
    fn the_current_epoch_ends_at_the_high_watermark() {
        assert_eq!(epoch_end(EPOCHS, 9, 55, 9), (9, 55));
    }

    #[test]
    fn a_past_epoch_ends_where_the_next_one_starts() {
        assert_eq!(epoch_end(EPOCHS, 9, 55, 2), (2, 30));
        assert_eq!(epoch_end(EPOCHS, 9, 55, 5), (5, 30));
        assert_eq!(epoch_end(EPOCHS, 9, 55, 6), (6, 40));
    }

    #[test]
    fn an_unrecorded_epoch_takes_the_one_below_it() {
        assert_eq!(epoch_end(EPOCHS, 9, 55, 7), (6, 40));
        assert_eq!(epoch_end(EPOCHS, 9, 55, 1), (1, 10));
    }

    #[test]
    fn undefined_and_future_epochs_have_no_end() {
        assert_eq!(epoch_end(EPOCHS, 9, 55, -1), UNDEFINED);
        assert_eq!(epoch_end(EPOCHS, 9, 55, 12), UNDEFINED);
    }
}
