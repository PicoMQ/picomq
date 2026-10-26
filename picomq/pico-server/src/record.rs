//! Kafka RecordBatch v2 encode/decode.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::compression::{self as codec, Decompressor};

use crate::types::{NumericProducer, OffsetToken};

pub const BATCH_HEADER_PREFIX: usize = 12;
pub const BATCH_HEADER_LEN: usize = 61;
const MAGIC_V2: i8 = 2;
const ATTR_COMPRESSION: i16 = 0x07;
const ATTR_LOG_APPEND_TIME: i16 = 1 << 3;
const ATTR_TRANSACTIONAL: i16 = 1 << 4;
const ATTR_CONTROL: i16 = 1 << 5;
const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;
const NO_SEQUENCE: i32 = -1;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogRecord {
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<Bytes>)>,
}

impl LogRecord {
    pub fn value(value: impl Into<Bytes>) -> Self {
        Self {
            value: Some(value.into()),
            ..Default::default()
        }
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push((name.into(), Some(value.into())));
        self
    }

    pub fn size_hint(&self) -> usize {
        self.key.as_ref().map_or(0, Bytes::len)
            + self.value.as_ref().map_or(0, Bytes::len)
            + self
                .headers
                .iter()
                .map(|(name, value)| name.len() + value.as_ref().map_or(0, Bytes::len))
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
    pub leader_epoch: i32,
    pub record_count: u32,
    pub first_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub compression: i16,
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
    #[error("record batch crc does not match its contents")]
    Crc,
    #[error("record batch: {0}")]
    Decode(String),
}

pub fn leader_epoch(stream_epoch: u64) -> i32 {
    i32::try_from(stream_epoch).unwrap_or(i32::MAX)
}

pub fn encode_batch(
    base_offset: u64,
    leader_epoch: i32,
    timestamp_ms: i64,
    producer: Option<NumericProducer>,
    records: &[LogRecord],
) -> Bytes {
    assert!(!records.is_empty(), "a batch needs at least one record");
    let mut buf = BytesMut::with_capacity(
        BATCH_HEADER_LEN + records.iter().map(|r| r.size_hint() + 16).sum::<usize>(),
    );
    buf.put_i64(base_offset as i64);
    buf.put_i32(0); // batchLength, patched below
    buf.put_i32(leader_epoch);
    buf.put_i8(MAGIC_V2);
    buf.put_u32(0); // crc, patched below
    buf.put_i16(ATTR_LOG_APPEND_TIME);
    buf.put_i32(records.len() as i32 - 1);
    buf.put_i64(timestamp_ms);
    buf.put_i64(timestamp_ms);
    match producer {
        Some(producer) => {
            buf.put_i64(producer.id);
            buf.put_i16(producer.epoch);
            buf.put_i32(producer.first_seq);
        }
        None => {
            buf.put_i64(NO_PRODUCER_ID);
            buf.put_i16(NO_PRODUCER_EPOCH);
            buf.put_i32(NO_SEQUENCE);
        }
    }
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
    match &record.value {
        Some(value) => {
            put_varint(&mut body, value.len() as i32);
            body.put_slice(value);
        }
        None => put_varint(&mut body, -1),
    }
    put_varint(&mut body, record.headers.len() as i32);
    for (name, value) in &record.headers {
        put_varint(&mut body, name.len() as i32);
        body.put_slice(name.as_bytes());
        match value {
            Some(value) => {
                put_varint(&mut body, value.len() as i32);
                body.put_slice(value);
            }
            None => put_varint(&mut body, -1),
        }
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
    let leader_epoch = i32::from_be_bytes(payload[12..16].try_into().expect("4 bytes"));
    if batch_length < 0 {
        return Err(RecordError::Truncated);
    }
    let len = BATCH_HEADER_PREFIX + batch_length as usize;
    if len > payload.len() {
        return Err(RecordError::Truncated);
    }
    let attributes = i16::from_be_bytes(payload[21..23].try_into().expect("2 bytes"));
    let first_timestamp_ms = i64::from_be_bytes(payload[27..35].try_into().expect("8 bytes"));
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
        leader_epoch,
        record_count: record_count as u32,
        first_timestamp_ms,
        max_timestamp_ms,
        compression: attributes & ATTR_COMPRESSION,
        log_append_time: attributes & ATTR_LOG_APPEND_TIME != 0,
        transactional_or_control: attributes & (ATTR_TRANSACTIONAL | ATTR_CONTROL) != 0,
        producer,
        len,
    })
}

pub fn verify_crc(batch: &[u8]) -> Result<(), RecordError> {
    if batch.len() < BATCH_HEADER_LEN {
        return Err(RecordError::Truncated);
    }

    let stored = u32::from_be_bytes(batch[17..21].try_into().expect("4 bytes"));
    if crc32c::crc32c(&batch[21..]) != stored {
        return Err(RecordError::Crc);
    }

    Ok(())
}

pub fn stamp(payload: &mut [u8], at: usize, base_offset: u64, leader_epoch: i32) {
    payload[at..at + 8].copy_from_slice(&(base_offset as i64).to_be_bytes());
    payload[at + 12..at + 16].copy_from_slice(&leader_epoch.to_be_bytes());
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
        let batch = batch_header(&payload[pos..])?;
        verify_crc(&payload[pos..pos + batch.len])?;
        let mut section = payload.slice(pos + BATCH_HEADER_LEN..pos + batch.len);
        pos += batch.len;

        let records = match batch.compression {
            0 => codec::None::decompress(&mut section, |buf| Ok(decode_records(buf, &batch))),
            1 => codec::Gzip::decompress(&mut section, |buf| Ok(decode_records(buf, &batch))),
            2 => codec::Snappy::decompress(&mut section, |buf| Ok(decode_records(buf, &batch))),
            3 => codec::Lz4::decompress(&mut section, |buf| Ok(decode_records(buf, &batch))),
            4 => codec::Zstd::decompress(&mut section, |buf| Ok(decode_records(buf, &batch))),
            other => return Err(RecordError::Decode(format!("unknown compression {other}"))),
        };
        out.extend(records.map_err(|error| RecordError::Decode(error.to_string()))??);
    }

    Ok(out)
}

fn decode_records(buf: &mut Bytes, batch: &BatchHeader) -> Result<Vec<StreamRecord>, RecordError> {
    let mut records = Vec::with_capacity(batch.record_count as usize);

    for _ in 0..batch.record_count {
        let length = usize::try_from(get_varint(buf)?)
            .map_err(|_| RecordError::Decode("negative record length".to_owned()))?;
        let mut encoded = split(buf, length)?;
        if !encoded.has_remaining() {
            return Err(RecordError::Truncated);
        }
        let _attributes = encoded.get_i8();

        let timestamp_delta = get_varlong(&mut encoded)?;
        let offset_delta = u64::try_from(get_varint(&mut encoded)?)
            .map_err(|_| RecordError::Decode("negative offset delta".to_owned()))?;
        let key = get_nullable(&mut encoded)?;
        let value = get_nullable(&mut encoded)?;

        let header_count = get_varint(&mut encoded)?;
        let mut headers = Vec::new();
        for _ in 0..header_count {
            let name = get_nullable(&mut encoded)?
                .ok_or_else(|| RecordError::Decode("header name is null".to_owned()))?;
            let name = String::from_utf8(name.to_vec())
                .map_err(|_| RecordError::Decode("header name is not UTF-8".to_owned()))?;
            headers.push((name, get_nullable(&mut encoded)?));
        }

        records.push(StreamRecord {
            offset: OffsetToken::of_record_offset(batch.base_offset + offset_delta),
            record: LogRecord {
                timestamp_ms: if batch.log_append_time {
                    batch.max_timestamp_ms
                } else {
                    batch.first_timestamp_ms + timestamp_delta
                },
                key,
                value,
                headers,
            },
        });
    }

    Ok(records)
}

fn get_nullable(buf: &mut Bytes) -> Result<Option<Bytes>, RecordError> {
    let Ok(length) = usize::try_from(get_varint(buf)?) else {
        return Ok(None);
    };
    split(buf, length).map(Some)
}

fn split(buf: &mut Bytes, length: usize) -> Result<Bytes, RecordError> {
    if length > buf.remaining() {
        return Err(RecordError::Truncated);
    }
    Ok(buf.split_to(length))
}

fn get_varint(buf: &mut Bytes) -> Result<i32, RecordError> {
    let raw = get_unsigned_varint(buf, 5)? as u32;
    Ok((raw >> 1) as i32 ^ -((raw & 1) as i32))
}

fn get_varlong(buf: &mut Bytes) -> Result<i64, RecordError> {
    let raw = get_unsigned_varint(buf, 10)?;
    Ok((raw >> 1) as i64 ^ -((raw & 1) as i64))
}

fn get_unsigned_varint(buf: &mut Bytes, max_bytes: u32) -> Result<u64, RecordError> {
    let mut value = 0u64;

    for index in 0..max_bytes {
        if !buf.has_remaining() {
            return Err(RecordError::Truncated);
        }
        let byte = buf.get_u8();
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }

    Err(RecordError::Decode("varint is too long".to_owned()))
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
    fn round_trips_through_the_decoder() {
        let encoded = encode_batch(40, 5, 1_700_000_000_000, None, &records());
        let header = batch_header(&encoded).unwrap();
        assert_eq!(header.base_offset, 40);
        assert_eq!(header.leader_epoch, 5);
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
        assert_eq!(decoded[1].record.value, Some(Bytes::new()));
        assert_eq!(decoded[2].record.headers, expected[2].headers);
    }

    #[test]
    fn repeated_header_names_keep_every_value_in_order() {
        let record = LogRecord::value("v")
            .with_header("trace", "a")
            .with_header("span", "x")
            .with_header("trace", "b");
        let encoded = encode_batch(0, 1, 1, None, std::slice::from_ref(&record));

        let decoded = decode_batches(&encoded).unwrap();
        assert_eq!(decoded[0].record.headers, record.headers);
    }

    #[test]
    fn a_null_header_name_is_refused() {
        let record = LogRecord::value("v").with_header("h", "x");
        let mut encoded = encode_batch(0, 1, 1, None, &[record]).to_vec();
        let name = encoded
            .windows(2)
            .rposition(|window| window == [0x02, b'h'])
            .unwrap();
        encoded[name] = 0x01;
        let crc = crc32c::crc32c(&encoded[21..]);
        encoded[17..21].copy_from_slice(&crc.to_be_bytes());

        let error = decode_batches(&Bytes::from(encoded)).unwrap_err();
        assert_eq!(error.to_string(), "record batch: header name is null");
    }

    #[test]
    fn compressed_create_time_batches_decode_like_the_kafka_decoder() {
        use kafka_protocol::indexmap::IndexMap;
        use kafka_protocol::protocol::StrBytes;
        use kafka_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        };

        let written: Vec<Record> = (0..3)
            .map(|n| Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: 10 + n,
                sequence: -1,
                timestamp: 1_000 - n * 7,
                key: (n != 1).then(|| Bytes::from(format!("k{n}"))),
                value: Some(Bytes::from(format!("value {n}"))),
                headers: IndexMap::from([(
                    StrBytes::from_static_str("h"),
                    Some(Bytes::from(format!("{n}"))),
                )]),
            })
            .collect();

        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let mut encoded = BytesMut::new();
            RecordBatchEncoder::encode(
                &mut encoded,
                &written,
                &RecordEncodeOptions {
                    version: 2,
                    compression,
                },
            )
            .unwrap();

            let decoded = decode_batches(&encoded.freeze()).unwrap();
            assert_eq!(decoded.len(), 3, "{compression:?}");
            for (record, stream_record) in written.iter().zip(&decoded) {
                assert_eq!(stream_record.offset.record_offset(), record.offset as u64);
                assert_eq!(stream_record.record.timestamp_ms, record.timestamp);
                assert_eq!(stream_record.record.key, record.key);
                assert_eq!(stream_record.record.value, record.value);
                assert_eq!(
                    stream_record.record.headers,
                    [("h".to_owned(), record.headers[0].clone())]
                );
            }
        }
    }

    #[test]
    fn concatenated_batches_decode_in_order() {
        let mut payload = encode_batch(0, 1, 1, None, &records()[..1]).to_vec();
        payload.extend_from_slice(&encode_batch(1, 1, 2, None, &records()[1..]));
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
        let mut corrupted = encode_batch(0, 1, 1, None, &records()).to_vec();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0x01;
        assert!(matches!(
            decode_batches(&Bytes::from(corrupted)),
            Err(RecordError::Crc)
        ));
    }

    #[test]
    fn verify_crc_accepts_valid_and_rejects_flipped_batches() {
        let encoded = encode_batch(0, 1, 1, None, &records()).to_vec();
        assert!(verify_crc(&encoded).is_ok());

        let mut patched = encoded.clone();
        stamp(&mut patched, 0, 99, 4);
        assert!(verify_crc(&patched).is_ok());

        let mut corrupted = encoded;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0x01;
        assert!(matches!(verify_crc(&corrupted), Err(RecordError::Crc)));
    }

    #[test]
    fn rejects_truncated_and_foreign_bytes() {
        assert!(matches!(
            batch_header(b"short"),
            Err(RecordError::Truncated)
        ));
        let mut bad_magic = encode_batch(0, 1, 1, None, &records()).to_vec();
        bad_magic[16] = 1;
        assert!(matches!(
            batch_header(&bad_magic),
            Err(RecordError::Magic(1))
        ));
        let encoded = encode_batch(0, 1, 1, None, &records());
        assert!(matches!(
            batch_header(&encoded[..encoded.len() - 1]),
            Err(RecordError::Truncated)
        ));
    }

    #[test]
    fn producer_identity_and_stamp() {
        let mut encoded = encode_batch(0, 1, 1, None, &records()).to_vec();
        assert_eq!(batch_header(&encoded).unwrap().producer, None);

        encoded[43..51].copy_from_slice(&7i64.to_be_bytes());
        encoded[51..53].copy_from_slice(&3i16.to_be_bytes());
        encoded[53..57].copy_from_slice(&40i32.to_be_bytes());
        let crc = crc32c::crc32c(&encoded[21..]);
        encoded[17..21].copy_from_slice(&crc.to_be_bytes());
        stamp(&mut encoded, 0, 1234, 6);

        let header = batch_header(&encoded).unwrap();
        assert_eq!(header.base_offset, 1234);
        assert_eq!(header.leader_epoch, 6);
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

        let mut empty = encode_batch(0, 1, 1, None, &records()).to_vec();
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
