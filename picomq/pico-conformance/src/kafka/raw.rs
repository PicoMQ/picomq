use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, ProduceRequest,
    ProduceResponse, RequestHeader, ResponseHeader, TopicName, VoteRequest,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TIMEOUT: Duration = Duration::from_secs(10);
const CLIENT: &str = "pico-conformance";
const CRC: usize = 17;

pub(super) enum Reply {
    Closed,
    Timeout,
    Frame(Bytes),
}

pub async fn future_api_versions(address: &str) -> String {
    let body = encode(&api_versions_request(), 3);

    match exchange(address, ApiKey::ApiVersions, 99, 2, body).await {
        Reply::Frame(mut frame) => {
            let Ok(response) = decode::<ApiVersionsResponse>(&mut frame, ApiKey::ApiVersions, 0)
            else {
                return "unreadable".to_owned();
            };

            let ranges = response
                .api_keys
                .iter()
                .map(|api| format!("{} {}..{}", api.api_key, api.min_version, api.max_version))
                .collect::<Vec<_>>()
                .join(", ");

            format!("error {}, apis [{ranges}]", response.error_code)
        }
        reply => describe(reply),
    }
}

pub async fn unsupported_version(address: &str) -> String {
    let body = encode(&MetadataRequest::default(), 12);

    describe(exchange(address, ApiKey::Metadata, 99, 2, body).await)
}

pub async fn controller_api(address: &str) -> String {
    let body = encode(&VoteRequest::default(), 0);

    describe(exchange(address, ApiKey::Vote, 0, 2, body).await)
}

pub async fn legacy_produce(address: &str, topic: &str, version: i16) -> String {
    let magic = if version < 2 { 0 } else { 1 };
    let set = message_set(magic, b"value");

    let mut body = BytesMut::new();
    body.put_i16(-1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i32(set.len() as i32);
    body.put_slice(&set);

    match exchange(address, ApiKey::Produce, version, 1, body.freeze()).await {
        Reply::Frame(mut frame) => {
            if ResponseHeader::decode(&mut frame, 0).is_err() {
                return "unreadable".to_owned();
            }

            first_produce_error(&mut frame)
                .map(|code| format!("error {code}"))
                .unwrap_or_else(|| "unreadable".to_owned())
        }
        reply => describe(reply),
    }
}

pub async fn corrupt_produce(address: &str, topic: &str) -> String {
    let mut batch = BytesMut::from(&record_batch()[..]);
    batch[CRC] ^= 0xff;

    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from(topic.to_owned())))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(batch.freeze())),
                ]),
        ]);

    match exchange(address, ApiKey::Produce, 9, 2, encode(&request, 9)).await {
        Reply::Frame(mut frame) => {
            match decode::<ProduceResponse>(&mut frame, ApiKey::Produce, 9) {
                Ok(response) => response
                    .responses
                    .first()
                    .and_then(|topic| topic.partition_responses.first())
                    .map(|partition| format!("error {}", partition.error_code))
                    .unwrap_or_else(|| "no partitions".to_owned()),
                Err(_) => "unreadable".to_owned(),
            }
        }
        reply => describe(reply),
    }
}

pub(super) fn record_batch() -> Bytes {
    let record = Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: NO_SEQUENCE,
        timestamp: 1,
        key: None,
        value: Some(Bytes::from_static(b"value")),
        headers: Default::default(),
    };

    let mut batch = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut batch,
        &[record],
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("record batch encodes");

    batch.freeze()
}

pub(super) fn decode<T: Decodable>(
    frame: &mut Bytes,
    api: ApiKey,
    version: i16,
) -> anyhow::Result<T> {
    ResponseHeader::decode(frame, api.response_header_version(version))?;

    T::decode(frame, version)
}

fn api_versions_request() -> ApiVersionsRequest {
    ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str(CLIENT))
        .with_client_software_version(StrBytes::from_static_str("1"))
}

pub(super) fn encode(message: &impl Encodable, version: i16) -> Bytes {
    let mut buf = BytesMut::new();

    message
        .encode(&mut buf, version)
        .expect("request encodes at a supported version");

    buf.freeze()
}

pub(super) async fn exchange(
    address: &str,
    api: ApiKey,
    version: i16,
    header_version: i16,
    body: Bytes,
) -> Reply {
    match Connection::open(address).await {
        Some(mut connection) => connection.send(api, version, header_version, body).await,
        None => Reply::Closed,
    }
}

pub(super) struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub async fn open(address: &str) -> Option<Self> {
        let connect = tokio::time::timeout(TIMEOUT, TcpStream::connect(address)).await;

        connect.ok()?.ok().map(|stream| Self { stream })
    }

    pub async fn send(
        &mut self,
        api: ApiKey,
        version: i16,
        header_version: i16,
        body: Bytes,
    ) -> Reply {
        let header = RequestHeader::default()
            .with_request_api_key(api as i16)
            .with_request_api_version(version)
            .with_correlation_id(1)
            .with_client_id(Some(StrBytes::from_static_str(CLIENT)));

        let mut request = BytesMut::new();
        header
            .encode(&mut request, header_version)
            .expect("request header encodes");
        request.put_slice(&body);

        self.send_bare(request.freeze()).await
    }

    pub async fn send_bare(&mut self, frame: Bytes) -> Reply {
        let round_trip = async {
            self.stream.write_i32(frame.len() as i32).await?;
            self.stream.write_all(&frame).await?;

            let size = self.stream.read_i32().await?;
            let mut frame = vec![0u8; size.max(0) as usize];
            self.stream.read_exact(&mut frame).await?;

            Ok::<_, std::io::Error>(Bytes::from(frame))
        };

        match tokio::time::timeout(TIMEOUT, round_trip).await {
            Ok(Ok(frame)) => Reply::Frame(frame),
            Ok(Err(_)) => Reply::Closed,
            Err(_) => Reply::Timeout,
        }
    }
}

pub(super) fn describe(reply: Reply) -> String {
    match reply {
        Reply::Closed => "closed".to_owned(),
        Reply::Timeout => "timeout".to_owned(),
        Reply::Frame(_) => "response".to_owned(),
    }
}

fn message_set(magic: u8, value: &[u8]) -> Bytes {
    let mut message = BytesMut::new();
    message.put_u8(magic);
    message.put_u8(0);

    if magic == 1 {
        message.put_i64(0);
    }

    message.put_i32(-1);
    message.put_i32(value.len() as i32);
    message.put_slice(value);

    let mut set = BytesMut::new();
    set.put_i64(0);
    set.put_i32(message.len() as i32 + 4);
    set.put_u32(crc32fast::hash(&message));
    set.put_slice(&message);

    set.freeze()
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_i16(value.len() as i16);
    buf.put_slice(value.as_bytes());
}

fn first_produce_error(frame: &mut Bytes) -> Option<i16> {
    if frame.remaining() < 4 || frame.get_i32() < 1 || frame.remaining() < 2 {
        return None;
    }

    let name = frame.get_i16().max(0) as usize;
    if frame.remaining() < name + 10 {
        return None;
    }
    frame.advance(name);

    if frame.get_i32() < 1 {
        return None;
    }
    frame.advance(4);

    Some(frame.get_i16())
}
