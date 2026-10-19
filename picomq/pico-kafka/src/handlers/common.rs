use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{BrokerId, ResponseHeader, TopicName};
use kafka_protocol::protocol::{Encodable, HeaderVersion, StrBytes};
use picomq_metadata::MetadataState;
use picomq_server::record::leader_epoch;
use picomq_server::{
    ErrorKind, Log, OwnershipService, Partition, ServiceError, StreamWatermarks, Topic,
};
use uuid::Uuid;

use crate::broker::BrokerContext;

#[derive(Debug, Clone)]
pub struct ResponseFrame(pub Bytes);

pub const UNKNOWN_SERVER_ERROR: i16 = -1;
pub const NO_ERROR: i16 = 0;
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
pub const CORRUPT_MESSAGE: i16 = 2;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const REQUEST_TIMED_OUT: i16 = 7;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const NOT_COORDINATOR: i16 = 16;
pub const ILLEGAL_GENERATION: i16 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
pub const INVALID_PARTITIONS: i16 = 37;
pub const INVALID_REPLICATION_FACTOR: i16 = 38;
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
pub const INVALID_CONFIG: i16 = 40;
pub const INVALID_REQUEST: i16 = 42;
pub const POLICY_VIOLATION: i16 = 44;
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
pub const KAFKA_STORAGE_ERROR: i16 = 56;
pub const INVALID_RECORD: i16 = 87;
pub const NON_EMPTY_GROUP: i16 = 68;
pub const GROUP_ID_NOT_FOUND: i16 = 69;
pub const MEMBER_ID_REQUIRED: i16 = 79;
pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
pub const FENCED_LEADER_EPOCH: i16 = 74;
pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
pub const FENCED_INSTANCE_ID: i16 = 82;
pub const GROUP_SUBSCRIBED_TO_TOPIC: i16 = 86;
pub const UNKNOWN_TOPIC_ID: i16 = 100;

pub const EXCESSIVE_PARTITIONS: &str = "Excessively large number of partitions per request.";

pub const EARLIEST_TIMESTAMP: i64 = -2;
pub const LATEST_TIMESTAMP: i64 = -1;
pub const MAX_TIMESTAMP: i64 = -3;

pub fn encode_response<T: Encodable + HeaderVersion>(
    correlation_id: i32,
    response_version: i16,
    body: &T,
) -> ResponseFrame {
    let mut buf = BytesMut::new();
    let header = ResponseHeader::default().with_correlation_id(correlation_id);
    header
        .encode(&mut buf, T::header_version(response_version))
        .expect("response header encode");
    body.encode(&mut buf, response_version)
        .expect("response body encode");
    ResponseFrame(buf.freeze())
}

pub fn topic_name(value: &str) -> TopicName {
    TopicName(StrBytes::from(value.to_owned()))
}

pub fn broker_id(id: i32) -> BrokerId {
    BrokerId(id)
}

pub fn topic_uuid(bytes: [u8; 16]) -> Uuid {
    Uuid::from_bytes(bytes)
}

pub type Failure = (i16, Option<String>);

pub fn service_failure(error: ServiceError) -> Failure {
    (service_error_code(&error), Some(error.message))
}

pub fn assigned_broker(ctx: &BrokerContext, brokers: &[BrokerId]) -> Result<i32, Failure> {
    let [broker] = brokers else {
        return Err((
            INVALID_REPLICA_ASSIGNMENT,
            Some(format!(
                "PicoMQ keeps one replica of each partition, {} were assigned",
                brokers.len()
            )),
        ));
    };

    if broker_address(&ctx.views.load().state, broker.0).is_none() {
        return Err((
            INVALID_REPLICA_ASSIGNMENT,
            Some(format!("broker {} is not registered", broker.0)),
        ));
    }

    Ok(broker.0)
}

pub fn service_error_code(error: &ServiceError) -> i16 {
    match error.kind {
        ErrorKind::NotFound => UNKNOWN_TOPIC_OR_PARTITION,
        ErrorKind::Conflict => TOPIC_ALREADY_EXISTS,
        ErrorKind::Transferring | ErrorKind::Unavailable => NOT_LEADER_OR_FOLLOWER,
        ErrorKind::BadRequest => INVALID_REQUEST,
        ErrorKind::CorruptBatch => CORRUPT_MESSAGE,
        ErrorKind::SchemaViolation => INVALID_RECORD,
        ErrorKind::Fenced => INVALID_PRODUCER_EPOCH,
        ErrorKind::SequenceGap => OUT_OF_ORDER_SEQUENCE_NUMBER,
        ErrorKind::Closed => POLICY_VIOLATION,
        ErrorKind::MatchFailed => INVALID_REQUEST,
        ErrorKind::Durability => KAFKA_STORAGE_ERROR,
        ErrorKind::Timeout => REQUEST_TIMED_OUT,
        ErrorKind::Internal => UNKNOWN_SERVER_ERROR,
    }
}

/// Advertised Kafka listener of `node_id`, `None` when the node is unknown.
pub fn broker_address(state: &MetadataState, node_id: i32) -> Option<&str> {
    state.get_node_protocol_address(node_id, crate::PROTOCOL_NAME)
}

pub struct Broker {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub fenced: bool,
}

pub fn brokers(state: &MetadataState, include_fenced: bool) -> Vec<Broker> {
    state
        .nodes
        .values()
        .filter(|node| include_fenced || !node.fenced)
        .filter_map(|node| {
            let (host, port) = parse_host_port(broker_address(state, node.node_id)?);
            Some(Broker {
                id: node.node_id,
                host,
                port,
                fenced: node.fenced,
            })
        })
        .collect()
}

pub fn parse_host_port(address: &str) -> (String, i32) {
    let address = address
        .strip_prefix("http://")
        .or_else(|| address.strip_prefix("https://"))
        .unwrap_or(address);
    if let Some(host) = address.strip_prefix('[')
        && let Some((host, port)) = host.split_once("]:")
    {
        return (host.to_owned(), port.parse().unwrap_or(9092));
    }
    match address.rsplit_once(':') {
        Some((host, port)) => (host.to_owned(), port.parse().unwrap_or(9092)),
        None => (address.to_owned(), 9092),
    }
}

pub async fn resolve_topic(ctx: &BrokerContext, topic: &str) -> Result<Topic, i16> {
    ctx.topics
        .get_kafka(topic)
        .await
        .map_err(|error| service_error_code(&error))?
        .ok_or(UNKNOWN_TOPIC_OR_PARTITION)
}

pub fn partition_of(topic: &Topic, index: i32) -> Result<Partition, i16> {
    match u32::try_from(index) {
        Ok(index) if index < topic.partitions => Ok(Partition::new(topic.id, index)),
        _ => Err(UNKNOWN_TOPIC_OR_PARTITION),
    }
}

pub async fn leader_partition(ctx: &BrokerContext, topic: &Topic, index: i32) -> Result<u32, i16> {
    let partition = partition_of(topic, index)?;
    let owner = ctx
        .ownership
        .owner_of(&Log::Partition(partition))
        .await
        .map_err(|_| NOT_LEADER_OR_FOLLOWER)?;

    if !owner.local {
        return Err(NOT_LEADER_OR_FOLLOWER);
    }

    Ok(partition.index)
}

pub async fn leader_watermarks(
    ctx: &BrokerContext,
    topic: &Topic,
    partition: u32,
    current_leader_epoch: i32,
) -> Result<StreamWatermarks, i16> {
    let watermarks = ctx
        .topics
        .watermarks(topic, partition)
        .await
        .map_err(|error| service_error_code(&error))?;

    let epoch = leader_epoch(watermarks.epoch);
    match current_leader_epoch {
        current if current < 0 || current == epoch => Ok(watermarks),
        current if current < epoch => Err(FENCED_LEADER_EPOCH),
        _ => Err(UNKNOWN_LEADER_EPOCH),
    }
}

pub fn leader_epochs(
    ctx: &BrokerContext,
    topic: &Topic,
    partition: u32,
) -> Result<Vec<(i32, u64)>, i16> {
    let epochs = ctx
        .topics
        .epochs(topic, partition)
        .map_err(|error| service_error_code(&error))?;

    Ok(epochs
        .into_iter()
        .map(|(epoch, start_offset)| (leader_epoch(epoch), start_offset))
        .collect())
}

pub fn is_internal_topic(topic: &str) -> bool {
    topic.starts_with('_')
}

pub fn concat_batches(batches: &[picomq_server::StreamBatch]) -> Bytes {
    if let [only] = batches {
        return only.payload.clone();
    }
    let len: usize = batches.iter().map(|batch| batch.payload.len()).sum();
    let mut out = BytesMut::with_capacity(len);
    for batch in batches {
        out.extend_from_slice(&batch.payload);
    }
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transferring_stream_tells_clients_to_refresh_metadata() {
        let error = ServiceError::kind(ErrorKind::Transferring);
        assert_eq!(service_error_code(&error), NOT_LEADER_OR_FOLLOWER);
        let taken = ServiceError::kind(ErrorKind::Conflict);
        assert_eq!(service_error_code(&taken), TOPIC_ALREADY_EXISTS);
    }
}
