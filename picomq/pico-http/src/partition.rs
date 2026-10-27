use axum::http::Uri;
use picomq_protocol::pico::Q_PARTITION;
use picomq_server::{ErrorKind, LogOffsets, ServiceError, Topic, Topics};

use crate::http::{bad_request, parse_strict_u64, query_param};

pub(crate) fn requested(uri: &Uri) -> Result<Option<u32>, ServiceError> {
    parse_strict_u64(
        query_param(uri, Q_PARTITION)?.as_deref(),
        "invalid partition",
    )?
    .map(|index| u32::try_from(index).map_err(|_| bad_request("invalid partition")))
    .transpose()
}

pub(crate) async fn resolve(
    topics: &Topics,
    name: &str,
    requested: Option<u32>,
) -> Result<u32, ServiceError> {
    let topic = topics
        .get(name)
        .await?
        .ok_or_else(|| ServiceError::kind(ErrorKind::NotFound))?;

    choose(&topic, requested)
}

pub(crate) async fn offsets(
    topics: &Topics,
    name: &str,
    partition: u32,
) -> Result<Option<(Topic, LogOffsets)>, ServiceError> {
    let Some(topic) = topics.get(name).await? else {
        return Ok(None);
    };

    let offsets = topics.live_offsets(&topic, partition).await?;

    Ok(Some((topic, offsets)))
}

pub(crate) async fn head(
    topics: &Topics,
    name: &str,
    requested: Option<u32>,
) -> Result<Option<(Topic, Option<LogOffsets>)>, ServiceError> {
    let Some(topic) = topics.get(name).await? else {
        return Ok(None);
    };

    let offsets = match (requested, topic.partitions) {
        (None, partitions) if partitions > 1 => None,
        (requested, _) => {
            let index = choose(&topic, requested)?;
            Some(topics.live_offsets(&topic, index).await?)
        }
    };

    Ok(Some((topic, offsets)))
}

pub(crate) fn choose(topic: &Topic, requested: Option<u32>) -> Result<u32, ServiceError> {
    match requested {
        Some(index) if index < topic.partitions => Ok(index),
        Some(index) => Err(ServiceError::with_message(
            ErrorKind::NotFound,
            None,
            false,
            format!("no partition {index}, the topic has {}", topic.partitions),
        )),
        None if topic.partitions == 1 => Ok(0),
        None => Err(bad_request(format!(
            "the topic has {} partitions, name one with ?partition=",
            topic.partitions
        ))),
    }
}
