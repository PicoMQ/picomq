//! Kafka RecordBatch v2 encode/decode.

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::records::RecordBatchDecoder;

use crate::types::{NumericProducer, OffsetToken};

pub const BATCH_HEADER_PREFIX: usize = 12;
pub const BATCH_HEADER_LEN: usize = 61;
const MAGIC_V2: i8 = 2;
const ATTR_LOG_APPEND_TIME: i16 = 1 << 3;
const ATTR_TRANSACTIONAL: i16 = 1 << 4;
const ATTR_CONTROL: i16 = 1 << 5;
const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;
const NO_SEQUENCE: i32 = -1;
const NO_PARTITION_LEADER_EPOCH: i32 = -1;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogRecord {
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, Bytes)>,
}

impl LogRecord {
    pub fn value(value: impl Into<Bytes>) -> Self {
        Self {
            value: value.into(),
            ..Default::default()
        }
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn size_hint(&self) -> usize {
        self.key.as_ref().map_or(0, Bytes::len)
            + self.value.len()
            + self
                .headers
                .iter()
                .map(|(name, value)| name.len() + value.len())
                .sum::<usize>()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecord {
    pub offset: OffsetToken,
    pub record: LogRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHeader {
    pub base_offset: u64,
    pub record_count: u32,
    pub max_timestamp_ms: i64,
    pub log_append_time: bool,
    pub transactional_or_control: bool,
    pub producer: Option<NumericProducer>,
    pub len: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("record batch truncated")]
    Truncated,
    #[error("record batch magic {0} is not v2")]
    Magic(i8),
    #[error("record batch has no records")]
    Empty,
    #[error("record batch: {0}")]
    Decode(String),
}

pub fn encode_batch(base_offset: u64, timestamp_ms: i64, records: &[LogRecord]) -> Bytes {
    assert!(!records.is_empty(), "a batch needs at least one record");
    let mut buf = BytesMut::with_capacity(
        BATCH_HEADER_LEN + records.iter().map(|r| r.size_hint() + 16).sum::<usize>(),
    );
    buf.put_i64(base_offset as i64);
    buf.put_i32(0); // batchLength, patched below
    buf.put_i32(NO_PARTITION_LEADER_EPOCH);
    buf.put_i8(MAGIC_V2);
    buf.put_u32(0); // crc, patched below
    buf.put_i16(ATTR_LOG_APPEND_TIME);
    buf.put_i32(records.len() as i32 - 1);
    buf.put_i64(timestamp_ms);
    buf.put_i64(timestamp_ms);
    buf.put_i64(NO_PRODUCER_ID);
    buf.put_i16(NO_PRODUCER_EPOCH);
    buf.put_i32(NO_SEQUENCE);
    buf.put_i32(records.len() as i32);
    for (delta, record) in records.iter().enumerate() {
        put_record(&mut buf, delta as i32, record);
    }

    let batch_length = (buf.len() - BATCH_HEADER_PREFIX) as i32;
    buf[8..12].copy_from_slice(&batch_length.to_be_bytes());
    let crc = crc32c::crc32c(&buf[21..]);
    buf[17..21].copy_from_slice(&crc.to_be_bytes());
    buf.freeze()
}

fn put_record(buf: &mut BytesMut, offset_delta: i32, record: &LogRecord) {
    let mut body = BytesMut::with_capacity(record.size_hint() + 16);
    body.put_i8(0); // record attributes
    put_varlong(&mut body, 0); // timestampDelta: LogAppendTime is uniform
    put_varint(&mut body, offset_delta);
    match &record.key {
        Some(key) => {
            put_varint(&mut body, key.len() as i32);
            body.put_slice(key);
        }
        None => put_varint(&mut body, -1),
    }
    put_varint(&mut body, record.value.len() as i32);
    body.put_slice(&record.value);
    put_varint(&mut body, record.headers.len() as i32);
    for (name, value) in &record.headers {
        put_varint(&mut body, name.len() as i32);
        body.put_slice(name.as_bytes());
        put_varint(&mut body, value.len() as i32);
        body.put_slice(value);
    }
    put_varint(buf, body.len() as i32);
    buf.put_slice(&body);
}

fn put_varint(buf: &mut BytesMut, value: i32) {
    put_unsigned_varint(buf, ((value << 1) ^ (value >> 31)) as u32 as u64);
}

fn put_varlong(buf: &mut BytesMut, value: i64) {
    put_unsigned_varint(buf, ((value << 1) ^ (value >> 63)) as u64);
}

fn put_unsigned_varint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

pub fn batch_header(payload: &[u8]) -> Result<BatchHeader, RecordError> {
    if payload.len() < BATCH_HEADER_LEN {
        return Err(RecordError::Truncated);
    }
    let magic = payload[16] as i8;
    if magic != MAGIC_V2 {
        return Err(RecordError::Magic(magic));
    }
    let base_offset = i64::from_be_bytes(payload[0..8].try_into().expect("8 bytes"));
    let batch_length = i32::from_be_bytes(payload[8..12].try_into().expect("4 bytes"));
    if batch_length < 0 {
        return Err(RecordError::Truncated);
    }
    let len = BATCH_HEADER_PREFIX + batch_length as usize;
    if len > payload.len() {
        return Err(RecordError::Truncated);
    }
    let attributes = i16::from_be_bytes(payload[21..23].try_into().expect("2 bytes"));
    let max_timestamp_ms = i64::from_be_bytes(payload[35..43].try_into().expect("8 bytes"));
    let producer_id = i64::from_be_bytes(payload[43..51].try_into().expect("8 bytes"));
    let producer_epoch = i16::from_be_bytes(payload[51..53].try_into().expect("2 bytes"));
    let base_sequence = i32::from_be_bytes(payload[53..57].try_into().expect("4 bytes"));
    let record_count = i32::from_be_bytes(payload[57..61].try_into().expect("4 bytes"));
    if record_count <= 0 {
        return Err(RecordError::Empty);
    }
    let producer = (producer_id >= 0 && base_sequence >= 0).then_some(NumericProducer {
        id: producer_id,
        epoch: producer_epoch,
        first_seq: base_sequence,
    });
    Ok(BatchHeader {
        base_offset: base_offset.max(0) as u64,
        record_count: record_count as u32,
        max_timestamp_ms,
        log_append_time: attributes & ATTR_LOG_APPEND_TIME != 0,
        transactional_or_control: attributes & (ATTR_TRANSACTIONAL | ATTR_CONTROL) != 0,
        producer,
        len,
    })
}

pub fn patch_base_offset(payload: &mut [u8], at: usize, base_offset: u64) {
    payload[at..at + 8].copy_from_slice(&(base_offset as i64).to_be_bytes());
}

pub fn batch_headers(payload: &[u8]) -> Result<Vec<BatchHeader>, RecordError> {
    let mut headers = Vec::new();
    let mut pos = 0;
    while pos < payload.len() {
        let header = batch_header(&payload[pos..])?;
        pos += header.len;
        headers.push(header);
    }
    Ok(headers)
}

pub fn decode_batches(payload: &Bytes) -> Result<Vec<StreamRecord>, RecordError> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < payload.len() {
        let header = batch_header(&payload[pos..])?;
        let mut batch = payload.slice(pos..pos + header.len);
        pos += header.len;
        let set = RecordBatchDecoder::decode(&mut batch)
            .map_err(|error| RecordError::Decode(error.to_string()))?;
        out.reserve(set.records.len());
        for record in set.records {
            out.push(StreamRecord {
                offset: OffsetToken::of_record_offset(record.offset.max(0) as u64),
                record: LogRecord {
                    timestamp_ms: if header.log_append_time {
                        header.max_timestamp_ms
                    } else {
                        record.timestamp
                    },
                    key: record.key,
                    value: record.value.unwrap_or_default(),
                    headers: record
                        .headers
                        .into_iter()
                        .map(|(name, value)| (name.to_string(), value.unwrap_or_default()))
                        .collect(),
                },
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records() -> Vec<LogRecord> {
        vec![
            LogRecord::value("one")
                .with_key("k1")
                .with_header("h", Bytes::from_static(&[0xff, 0x00])),
            LogRecord::value(Bytes::new()),
            LogRecord::value("three")
                .with_header("a", "b")
                .with_header("c", ""),
        ]
    }

    #[test]
    fn round_trips_through_the_kafka_decoder() {
        let encoded = encode_batch(40, 1_700_000_000_000, &records());
        let header = batch_header(&encoded).unwrap();
        assert_eq!(header.base_offset, 40);
        assert_eq!(header.record_count, 3);
        assert_eq!(header.max_timestamp_ms, 1_700_000_000_000);
        assert!(header.log_append_time);
        assert_eq!(header.len, encoded.len());

        let decoded = decode_batches(&encoded).unwrap();
        assert_eq!(decoded.len(), 3);
        for (i, stream_record) in decoded.iter().enumerate() {
            assert_eq!(stream_record.offset.record_offset(), 40 + i as u64);
            assert_eq!(stream_record.record.timestamp_ms, 1_700_000_000_000);
        }
        let expected = records();
        assert_eq!(decoded[0].record.key, expected[0].key);
        assert_eq!(decoded[0].record.value, expected[0].value);
        assert_eq!(decoded[0].record.headers, expected[0].headers);
        assert_eq!(decoded[1].record.key, None);
        assert!(decoded[1].record.value.is_empty());
        assert_eq!(decoded[2].record.headers, expected[2].headers);
    }

    #[test]
    fn concatenated_batches_decode_in_order() {
        let mut payload = encode_batch(0, 1, &records()[..1]).to_vec();
        payload.extend_from_slice(&encode_batch(1, 2, &records()[1..]));
        let payload = Bytes::from(payload);
        let headers = batch_headers(&payload).unwrap();
        assert_eq!(headers.len(), 2);
        assert_eq!((headers[0].base_offset, headers[1].base_offset), (0, 1));
        let decoded = decode_batches(&payload).unwrap();
        let offsets: Vec<u64> = decoded.iter().map(|r| r.offset.record_offset()).collect();
        assert_eq!(offsets, [0, 1, 2]);
        assert_eq!(decoded[2].record.timestamp_ms, 2);
    }

    #[test]
    fn crc_is_verified_on_decode() {
        let mut corrupted = encode_batch(0, 1, &records()).to_vec();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0x01;
        assert!(matches!(
            decode_batches(&Bytes::from(corrupted)),
            Err(RecordError::Decode(_))
        ));
    }

    #[test]
    fn rejects_truncated_and_foreign_bytes() {
        assert!(matches!(
            batch_header(b"short"),
            Err(RecordError::Truncated)
        ));
        let mut bad_magic = encode_batch(0, 1, &records()).to_vec();
        bad_magic[16] = 1;
        assert!(matches!(
            batch_header(&bad_magic),
            Err(RecordError::Magic(1))
        ));
        let encoded = encode_batch(0, 1, &records());
        assert!(matches!(
            batch_header(&encoded[..encoded.len() - 1]),
            Err(RecordError::Truncated)
        ));
    }

    #[test]
    fn producer_identity_and_base_offset_patch() {
        let mut encoded = encode_batch(0, 1, &records()).to_vec();
        assert_eq!(batch_header(&encoded).unwrap().producer, None);

        encoded[43..51].copy_from_slice(&7i64.to_be_bytes());
        encoded[51..53].copy_from_slice(&3i16.to_be_bytes());
        encoded[53..57].copy_from_slice(&40i32.to_be_bytes());
        let crc = crc32c::crc32c(&encoded[21..]);
        encoded[17..21].copy_from_slice(&crc.to_be_bytes());
        patch_base_offset(&mut encoded, 0, 1234);

        let header = batch_header(&encoded).unwrap();
        assert_eq!(header.base_offset, 1234);
        assert_eq!(
            header.producer,
            Some(NumericProducer {
                id: 7,
                epoch: 3,
                first_seq: 40
            })
        );
        let decoded = decode_batches(&Bytes::from(encoded)).unwrap();
        assert_eq!(decoded[0].offset.record_offset(), 1234);

        let mut empty = encode_batch(0, 1, &records()).to_vec();
        empty[57..61].copy_from_slice(&0i32.to_be_bytes());
        assert!(matches!(batch_header(&empty), Err(RecordError::Empty)));
    }

    #[test]
    fn varints_are_zigzag() {
        let mut buf = BytesMut::new();
        put_varint(&mut buf, -1);
        assert_eq!(&buf[..], &[1]);
        buf.clear();
        put_varint(&mut buf, 300);
        assert_eq!(&buf[..], &[0xd8, 0x04]);
        buf.clear();
        put_varlong(&mut buf, i64::MIN);
        assert_eq!(buf.len(), 10);
    }
}
