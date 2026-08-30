use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{BrokerId, ResponseHeader, TopicName};
use kafka_protocol::protocol::{Encodable, HeaderVersion, StrBytes};
use pico_server::{ErrorKind, ServiceError};
use uuid::Uuid;

use crate::broker::BrokerContext;
use crate::topic;

#[derive(Debug, Clone)]
pub struct ResponseFrame(pub Bytes);

pub const NO_ERROR: i16 = 0;
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
pub const CORRUPT_MESSAGE: i16 = 2;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const NOT_COORDINATOR: i16 = 16;
pub const ILLEGAL_GENERATION: i16 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
pub const INVALID_REQUEST: i16 = 42;
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
pub const KAFKA_STORAGE_ERROR: i16 = 56;
pub const INVALID_RECORD: i16 = 87;
pub const GROUP_ID_NOT_FOUND: i16 = 69;
pub const MEMBER_ID_REQUIRED: i16 = 79;
pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
pub const FENCED_INSTANCE_ID: i16 = 82;
pub const UNKNOWN_TOPIC_ID: i16 = 100;

pub const EARLIEST_TIMESTAMP: i64 = -2;
pub const LATEST_TIMESTAMP: i64 = -1;

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

pub fn new_topic_id() -> [u8; 16] {
    *Uuid::new_v4().as_bytes()
}

pub fn service_error_code(error: &ServiceError) -> i16 {
    match error.kind {
        ErrorKind::NotFound => UNKNOWN_TOPIC_OR_PARTITION,
        ErrorKind::Conflict => TOPIC_ALREADY_EXISTS,
        ErrorKind::BadRequest => INVALID_REQUEST,
        // Idempotent-producer rejections carry the exact codes clients key
        // their retry and fencing behavior off.
        ErrorKind::Fenced => INVALID_PRODUCER_EPOCH,
        ErrorKind::SequenceGap => OUT_OF_ORDER_SEQUENCE_NUMBER,
        ErrorKind::Closed => UNKNOWN_TOPIC_OR_PARTITION,
        ErrorKind::Durability => KAFKA_STORAGE_ERROR,
        _ => INVALID_REQUEST,
    }
}

pub async fn ensure_local_leader(ctx: &BrokerContext, stream_name: &str) -> Result<(), i16> {
    use pico_server::OwnershipService;
    let owner = ctx
        .ownership
        .owner_of(stream_name)
        .await
        .map_err(|_| NOT_LEADER_OR_FOLLOWER)?;
    if owner.local {
        Ok(())
    } else {
        Err(NOT_LEADER_OR_FOLLOWER)
    }
}

/// Split an advertised address into host and port, tolerating an optional
/// scheme prefix and bracketed IPv6 literals. Defaults to Kafka's 9092.
pub fn parse_host_port(address: &str) -> (String, i32) {
    let address = address
        .strip_prefix("http://")
        .or_else(|| address.strip_prefix("https://"))
        .unwrap_or(address);
    if let Some(host) = address.strip_prefix('[') {
        if let Some((host, port)) = host.split_once("]:") {
            return (host.to_owned(), port.parse().unwrap_or(9092));
        }
    }
    match address.rsplit_once(':') {
        Some((host, port)) => (host.to_owned(), port.parse().unwrap_or(9092)),
        None => (address.to_owned(), 9092),
    }
}

pub fn reject_sys_create(name: &str) -> Result<(), i16> {
    if topic::is_sys_name(name) {
        Err(INVALID_REQUEST)
    } else {
        Ok(())
    }
}

pub fn concat_batches(batches: &[pico_server::StreamBatch]) -> Bytes {
    // The common case is a single stored batch: hand back the engine's
    // zero-copy Bytes untouched.
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
