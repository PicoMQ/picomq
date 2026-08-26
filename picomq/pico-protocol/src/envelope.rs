//! Pico protocol record model: envelopes, sequenced records, and the three
//! wire codecs (envelope, binary batch, JSON).
//!
//! `SequencedRecord`,
//! `RecordEnvelopeCodec`, `BatchCodec`, `JsonCodec`. All integers are
//!
//! Envelope (v1): `u8 version | i64 timestamp | headers | body...` where
//! headers = `u32 count` then per header `u32 name_len | name | u32 value_len

use std::collections::BTreeMap;

use base64::Engine as _;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde_json::{json, Map, Value};

use crate::error::CodecError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordEnvelope {
    pub timestamp: i64,
    pub headers: BTreeMap<String, String>,
    pub body: Bytes,
}

impl RecordEnvelope {
    pub fn new(timestamp: i64, headers: BTreeMap<String, String>, body: Bytes) -> Self {
        Self {
            timestamp,
            headers,
            body,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedRecord {
    pub seq: u64,
    pub envelope: RecordEnvelope,
}

const ENVELOPE_VERSION: u8 = 1;
const BATCH_VERSION: u8 = 1;

fn malformed(message: impl Into<String>) -> CodecError {
    CodecError::new(message)
}

// ---------------------------------------------------------------------------
// RecordEnvelopeCodec
// ---------------------------------------------------------------------------

pub fn encode_envelope(envelope: &RecordEnvelope) -> Bytes {
    let mut buf =
        BytesMut::with_capacity(1 + 8 + headers_size(&envelope.headers) + envelope.body.len());
    buf.put_u8(ENVELOPE_VERSION);
    buf.put_i64(envelope.timestamp);
    put_headers(&mut buf, &envelope.headers);
    buf.put_slice(&envelope.body);
    buf.freeze()
}

pub fn decode_envelope(payload: &[u8]) -> Result<RecordEnvelope, CodecError> {
    let mut buf = payload;
    check_version(&mut buf, ENVELOPE_VERSION, "record envelope")?;
    let timestamp = get_i64(&mut buf)?;
    let headers = get_headers(&mut buf)?;
    Ok(RecordEnvelope::new(
        timestamp,
        headers,
        Bytes::copy_from_slice(buf),
    ))
}

pub fn decode_envelope_timestamp(payload: &[u8]) -> Result<i64, CodecError> {
    let mut buf = payload;
    check_version(&mut buf, ENVELOPE_VERSION, "record envelope")?;
    get_i64(&mut buf)
}

fn headers_size(headers: &BTreeMap<String, String>) -> usize {
    4 + headers
        .iter()
        .map(|(k, v)| 8 + k.len() + v.len())
        .sum::<usize>()
}

fn put_headers(buf: &mut BytesMut, headers: &BTreeMap<String, String>) {
    buf.put_u32(headers.len() as u32);
    for (name, value) in headers {
        buf.put_u32(name.len() as u32);
        buf.put_slice(name.as_bytes());
        buf.put_u32(value.len() as u32);
        buf.put_slice(value.as_bytes());
    }
}

fn get_headers(buf: &mut &[u8]) -> Result<BTreeMap<String, String>, CodecError> {
    let count = get_u32(buf)?;
    let mut headers = BTreeMap::new();
    for _ in 0..count {
        let name = get_string(buf)?;
        let value = get_string(buf)?;
        headers.insert(name, value);
    }
    Ok(headers)
}

// ---------------------------------------------------------------------------
// BatchCodec (binary)
// ---------------------------------------------------------------------------

pub fn encode_batch_append(records: &[RecordEnvelope]) -> Bytes {
    let size: usize = 5 + records
        .iter()
        .map(|r| 4 + headers_size(&r.headers) + 4 + r.body.len())
        .sum::<usize>();
    let mut buf = BytesMut::with_capacity(size);
    buf.put_u8(BATCH_VERSION);
    buf.put_u32(records.len() as u32);
    for record in records {
        put_headers(&mut buf, &record.headers);
        buf.put_u32(record.body.len() as u32);
        buf.put_slice(&record.body);
    }
    buf.freeze()
}

pub fn decode_batch_append(payload: &[u8]) -> Result<Vec<RecordEnvelope>, CodecError> {
    let mut buf = payload;
    check_version(&mut buf, BATCH_VERSION, "batch")?;
    let count = get_u32(&mut buf)?;
    let mut records = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let headers = get_headers(&mut buf)?;
        let body = get_bytes(&mut buf)?;
        records.push(RecordEnvelope::new(0, headers, body));
    }
    Ok(records)
}

pub fn encode_batch_read(records: &[SequencedRecord]) -> Bytes {
    let size: usize = 5 + records
        .iter()
        .map(|r| 16 + 4 + headers_size(&r.envelope.headers) + 4 + r.envelope.body.len())
        .sum::<usize>();
    let mut buf = BytesMut::with_capacity(size);
    buf.put_u8(BATCH_VERSION);
    buf.put_u32(records.len() as u32);
    for record in records {
        buf.put_u64(record.seq);
        buf.put_i64(record.envelope.timestamp);
        put_headers(&mut buf, &record.envelope.headers);
        buf.put_u32(record.envelope.body.len() as u32);
        buf.put_slice(&record.envelope.body);
    }
    buf.freeze()
}

pub fn decode_batch_read(payload: &[u8]) -> Result<Vec<SequencedRecord>, CodecError> {
    let mut buf = payload;
    check_version(&mut buf, BATCH_VERSION, "batch")?;
    let count = get_u32(&mut buf)?;
    let mut records = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let seq = get_u64(&mut buf)?;
        let timestamp = get_i64(&mut buf)?;
        let headers = get_headers(&mut buf)?;
        let body = get_bytes(&mut buf)?;
        records.push(SequencedRecord {
            seq,
            envelope: RecordEnvelope::new(timestamp, headers, body),
        });
    }
    Ok(records)
}

// ---------------------------------------------------------------------------
// JsonCodec
// ---------------------------------------------------------------------------

pub fn encode_json_read(records: &[SequencedRecord]) -> Bytes {
    let array: Vec<Value> = records
        .iter()
        .map(|record| {
            let mut node = Map::new();
            node.insert("seq".into(), json!(record.seq));
            node.insert("timestamp".into(), json!(record.envelope.timestamp));
            if !record.envelope.headers.is_empty() {
                node.insert("headers".into(), json!(record.envelope.headers));
            }
            put_json_body(&mut node, &record.envelope.body);
            Value::Object(node)
        })
        .collect();
    Bytes::from(serde_json::to_vec(&array).expect("json encode"))
}

pub fn decode_json_append(payload: &[u8]) -> Result<Vec<RecordEnvelope>, CodecError> {
    let root: Value = serde_json::from_slice(payload).map_err(|_| malformed("invalid JSON"))?;
    let Some(array) = root.get("records").and_then(Value::as_array) else {
        return Err(malformed("records must be a non-empty array"));
    };
    if array.is_empty() {
        return Err(malformed("records must be a non-empty array"));
    }
    array
        .iter()
        .map(|node| Ok(RecordEnvelope::new(0, json_headers(node), json_body(node)?)))
        .collect()
}

fn put_json_body(node: &mut Map<String, Value>, body: &[u8]) {
    match std::str::from_utf8(body) {
        Ok(text) => node.insert("body".into(), json!(text)),
        Err(_) => node.insert(
            "body_b64".into(),
            json!(base64::engine::general_purpose::STANDARD.encode(body)),
        ),
    };
}

fn json_body(node: &Value) -> Result<Bytes, CodecError> {
    if let Some(b64) = node.get("body_b64").and_then(Value::as_str) {
        return base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map(Bytes::from)
            .map_err(|_| malformed("invalid body_b64"));
    }
    Ok(node
        .get("body")
        .and_then(Value::as_str)
        .map(|text| Bytes::copy_from_slice(text.as_bytes()))
        .unwrap_or_default())
}

fn json_headers(node: &Value) -> BTreeMap<String, String> {
    let Some(headers) = node.get("headers").and_then(Value::as_object) else {
        return BTreeMap::new();
    };
    headers
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| v.to_string()),
            )
        })
        .collect()
}

// ---- decode helpers ----

fn check_version(buf: &mut &[u8], expected: u8, what: &str) -> Result<(), CodecError> {
    if buf.is_empty() {
        return Err(malformed(format!("truncated {what}")));
    }
    let version = buf.get_u8();
    if version != expected {
        return Err(malformed(format!("unknown {what} version {version}")));
    }
    Ok(())
}

fn get_u32(buf: &mut &[u8]) -> Result<u32, CodecError> {
    if buf.len() < 4 {
        return Err(malformed("truncated payload"));
    }
    Ok(buf.get_u32())
}

fn get_u64(buf: &mut &[u8]) -> Result<u64, CodecError> {
    if buf.len() < 8 {
        return Err(malformed("truncated payload"));
    }
    Ok(buf.get_u64())
}

fn get_i64(buf: &mut &[u8]) -> Result<i64, CodecError> {
    if buf.len() < 8 {
        return Err(malformed("truncated payload"));
    }
    Ok(buf.get_i64())
}

fn get_bytes(buf: &mut &[u8]) -> Result<Bytes, CodecError> {
    let len = get_u32(buf)? as usize;
    if buf.len() < len {
        return Err(malformed("truncated payload"));
    }
    let out = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(out)
}

fn get_string(buf: &mut &[u8]) -> Result<String, CodecError> {
    let bytes = get_bytes(buf)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| malformed("invalid UTF-8 in headers"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Byte-level golden test against the Java format (hand-assembled from
    /// `RecordEnvelopeCodec#encode`: version, BE i64 timestamp, header table,
    /// raw body).
    #[test]
    fn envelope_bytes_match_java_layout() {
        let envelope = RecordEnvelope::new(7, headers(&[("a", "b")]), Bytes::from_static(b"xy"));
        let encoded = encode_envelope(&envelope);
        let expected: Vec<u8> = [
            &[1u8][..],          // version
            &7i64.to_be_bytes(), // timestamp
            &1u32.to_be_bytes(), // header count
            &1u32.to_be_bytes(),
            b"a", // name
            &1u32.to_be_bytes(),
            b"b",  // value
            b"xy", // body
        ]
        .concat();
        assert_eq!(&encoded[..], &expected[..]);
        assert_eq!(decode_envelope(&encoded).unwrap(), envelope);
        assert_eq!(decode_envelope_timestamp(&encoded).unwrap(), 7);
    }

    #[test]
    fn batch_append_and_read_round_trip() {
        let records = vec![
            RecordEnvelope::new(0, headers(&[("k", "v")]), Bytes::from_static(b"one")),
            RecordEnvelope::new(0, BTreeMap::new(), Bytes::from_static(b"two")),
        ];
        let decoded = decode_batch_append(&encode_batch_append(&records)).unwrap();
        assert_eq!(decoded, records);

        let sequenced = vec![
            SequencedRecord {
                seq: 4,
                envelope: RecordEnvelope::new(
                    9,
                    headers(&[("k", "v")]),
                    Bytes::from_static(b"one"),
                ),
            },
            SequencedRecord {
                seq: 5,
                envelope: RecordEnvelope::new(9, BTreeMap::new(), Bytes::new()),
            },
        ];
        assert_eq!(
            decode_batch_read(&encode_batch_read(&sequenced)).unwrap(),
            sequenced
        );
    }

    #[test]
    fn json_read_and_append() {
        let records = vec![SequencedRecord {
            seq: 1,
            envelope: RecordEnvelope::new(5, headers(&[("h", "1")]), Bytes::from_static(b"text")),
        }];
        let json: Value = serde_json::from_slice(&encode_json_read(&records)).unwrap();
        assert_eq!(json[0]["seq"], 1);
        assert_eq!(json[0]["timestamp"], 5);
        assert_eq!(json[0]["headers"]["h"], "1");
        assert_eq!(json[0]["body"], "text");

        // Non-UTF-8 body goes out as body_b64 and comes back byte-identical.
        let binary = vec![SequencedRecord {
            seq: 2,
            envelope: RecordEnvelope::new(5, BTreeMap::new(), Bytes::from_static(&[0xff, 0xfe])),
        }];
        let json: Value = serde_json::from_slice(&encode_json_read(&binary)).unwrap();
        assert!(json[0].get("body").is_none());
        let decoded = decode_json_append(
            format!(
                r#"{{"records":[{{"body_b64":"{}"}}]}}"#,
                json[0]["body_b64"].as_str().unwrap()
            )
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(&decoded[0].body[..], &[0xff, 0xfe]);

        assert!(decode_json_append(br#"{"records":[]}"#).is_err());
        assert!(decode_json_append(b"not json").is_err());
    }

    #[test]
    fn truncation_and_bad_version_rejected() {
        let envelope = RecordEnvelope::new(1, BTreeMap::new(), Bytes::from_static(b"x"));
        let mut bytes = encode_envelope(&envelope).to_vec();
        bytes[0] = 9;
        assert!(decode_envelope(&bytes).is_err());
        assert!(decode_envelope(&[]).is_err());
        assert!(decode_batch_append(&[1, 0, 0]).is_err());
    }
}
