//! Pico wire records: binary batch and JSON encodings.

use base64::Engine as _;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde_json::{json, Map, Value};

use crate::error::CodecError;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PicoRecord {
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub headers: Vec<(String, Bytes)>,
    pub body: Bytes,
}

impl PicoRecord {
    pub fn new(body: impl Into<Bytes>) -> Self {
        Self {
            body: body.into(),
            ..Default::default()
        }
    }

    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn header_str(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .and_then(|(_, v)| std::str::from_utf8(v).ok())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedRecord {
    pub seq: u64,
    pub record: PicoRecord,
}

const BATCH_VERSION: u8 = 1;

fn malformed(message: impl Into<String>) -> CodecError {
    CodecError::new(message)
}

fn record_size(record: &PicoRecord) -> usize {
    4 + record.key.as_ref().map_or(0, Bytes::len)
        + 4
        + record
            .headers
            .iter()
            .map(|(k, v)| 8 + k.len() + v.len())
            .sum::<usize>()
        + 4
        + record.body.len()
}

fn put_record(buf: &mut BytesMut, record: &PicoRecord) {
    match &record.key {
        Some(key) => {
            buf.put_i32(key.len() as i32);
            buf.put_slice(key);
        }
        None => buf.put_i32(-1),
    }
    buf.put_u32(record.headers.len() as u32);
    for (name, value) in &record.headers {
        buf.put_u32(name.len() as u32);
        buf.put_slice(name.as_bytes());
        buf.put_u32(value.len() as u32);
        buf.put_slice(value);
    }
    buf.put_u32(record.body.len() as u32);
    buf.put_slice(&record.body);
}

fn get_record(buf: &mut &[u8], timestamp: i64) -> Result<PicoRecord, CodecError> {
    let key_len = get_i32(buf)?;
    let key = if key_len < 0 {
        None
    } else {
        Some(get_exact(buf, key_len as usize)?)
    };
    let count = get_u32(buf)?;
    let mut headers = Vec::with_capacity(count.min(1024) as usize);
    for _ in 0..count {
        let name = get_string(buf)?;
        let value = get_bytes(buf)?;
        headers.push((name, value));
    }
    let body = get_bytes(buf)?;
    Ok(PicoRecord {
        timestamp,
        key,
        headers,
        body,
    })
}

pub fn encode_batch_append(records: &[PicoRecord]) -> Bytes {
    let size = 5 + records.iter().map(record_size).sum::<usize>();
    let mut buf = BytesMut::with_capacity(size);
    buf.put_u8(BATCH_VERSION);
    buf.put_u32(records.len() as u32);
    for record in records {
        put_record(&mut buf, record);
    }
    buf.freeze()
}

pub fn decode_batch_append(payload: &[u8]) -> Result<Vec<PicoRecord>, CodecError> {
    let mut buf = payload;
    check_version(&mut buf, "batch")?;
    let count = get_u32(&mut buf)?;
    let mut records = Vec::with_capacity(count.min(4096) as usize);
    for _ in 0..count {
        records.push(get_record(&mut buf, 0)?);
    }
    if !buf.is_empty() {
        return Err(malformed("trailing bytes after batch"));
    }
    Ok(records)
}

pub fn encode_batch_read(records: &[SequencedRecord]) -> Bytes {
    let size = 5 + records
        .iter()
        .map(|r| 16 + record_size(&r.record))
        .sum::<usize>();
    let mut buf = BytesMut::with_capacity(size);
    buf.put_u8(BATCH_VERSION);
    buf.put_u32(records.len() as u32);
    for record in records {
        buf.put_u64(record.seq);
        buf.put_i64(record.record.timestamp);
        put_record(&mut buf, &record.record);
    }
    buf.freeze()
}

pub fn decode_batch_read(payload: &[u8]) -> Result<Vec<SequencedRecord>, CodecError> {
    let mut buf = payload;
    check_version(&mut buf, "batch")?;
    let count = get_u32(&mut buf)?;
    let mut records = Vec::with_capacity(count.min(4096) as usize);
    for _ in 0..count {
        let seq = get_u64(&mut buf)?;
        let timestamp = get_i64(&mut buf)?;
        records.push(SequencedRecord {
            seq,
            record: get_record(&mut buf, timestamp)?,
        });
    }
    Ok(records)
}

pub fn encode_json_read(records: &[SequencedRecord]) -> Bytes {
    let array: Vec<Value> = records
        .iter()
        .map(|record| {
            let mut node = Map::new();
            node.insert("seq".into(), json!(record.seq));
            node.insert("timestamp".into(), json!(record.record.timestamp));
            put_json_record(&mut node, &record.record);
            Value::Object(node)
        })
        .collect();
    Bytes::from(serde_json::to_vec(&array).expect("json encode"))
}

pub fn decode_json_append(payload: &[u8]) -> Result<Vec<PicoRecord>, CodecError> {
    let root: Value = serde_json::from_slice(payload).map_err(|_| malformed("invalid JSON"))?;
    let Some(array) = root.get("records").and_then(Value::as_array) else {
        return Err(malformed("records must be a non-empty array"));
    };
    if array.is_empty() {
        return Err(malformed("records must be a non-empty array"));
    }
    array.iter().map(json_record).collect()
}

fn put_json_record(node: &mut Map<String, Value>, record: &PicoRecord) {
    if let Some(key) = &record.key {
        put_json_bytes(node, "key", key);
    }
    let mut text = Map::new();
    let mut binary = Map::new();
    for (name, value) in &record.headers {
        match std::str::from_utf8(value) {
            Ok(v) => text.insert(name.clone(), json!(v)),
            Err(_) => binary.insert(
                name.clone(),
                json!(base64::engine::general_purpose::STANDARD.encode(value)),
            ),
        };
    }
    if !text.is_empty() {
        node.insert("headers".into(), Value::Object(text));
    }
    if !binary.is_empty() {
        node.insert("headers_b64".into(), Value::Object(binary));
    }
    put_json_bytes(node, "body", &record.body);
}

fn put_json_bytes(node: &mut Map<String, Value>, field: &str, bytes: &[u8]) {
    match std::str::from_utf8(bytes) {
        Ok(text) => node.insert(field.into(), json!(text)),
        Err(_) => node.insert(
            format!("{field}_b64"),
            json!(base64::engine::general_purpose::STANDARD.encode(bytes)),
        ),
    };
}

fn json_record(node: &Value) -> Result<PicoRecord, CodecError> {
    if !node.is_object() {
        return Err(malformed("record must be an object"));
    }
    let key = match (node.get("key"), node.get("key_b64")) {
        (None, None) => None,
        (key, b64) => Some(json_bytes(key, b64, "key")?),
    };
    let mut headers = Vec::new();
    if let Some(text) = node.get("headers") {
        let Some(text) = text.as_object() else {
            return Err(malformed("headers must be an object"));
        };
        for (name, value) in text {
            let value = match value {
                Value::String(s) => Bytes::copy_from_slice(s.as_bytes()),
                other => Bytes::from(other.to_string()),
            };
            headers.push((name.clone(), value));
        }
    }
    if let Some(binary) = node.get("headers_b64") {
        let Some(binary) = binary.as_object() else {
            return Err(malformed("headers_b64 must be an object"));
        };
        for (name, value) in binary {
            let Some(b64) = value.as_str() else {
                return Err(malformed("headers_b64 values must be strings"));
            };
            let value = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|_| malformed(format!("invalid base64 in headers_b64.{name}")))?;
            headers.push((name.clone(), Bytes::from(value)));
        }
    }
    let body = json_bytes(node.get("body"), node.get("body_b64"), "body")?;
    Ok(PicoRecord {
        timestamp: 0,
        key,
        headers,
        body,
    })
}

fn json_bytes(text: Option<&Value>, b64: Option<&Value>, field: &str) -> Result<Bytes, CodecError> {
    if let Some(b64) = b64 {
        let Some(b64) = b64.as_str() else {
            return Err(malformed(format!("{field}_b64 must be a string")));
        };
        return base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map(Bytes::from)
            .map_err(|_| malformed(format!("invalid {field}_b64")));
    }
    match text {
        None | Some(Value::Null) => Ok(Bytes::new()),
        Some(Value::String(s)) => Ok(Bytes::copy_from_slice(s.as_bytes())),
        Some(_) => Err(malformed(format!("{field} must be a string"))),
    }
}

fn check_version(buf: &mut &[u8], what: &str) -> Result<(), CodecError> {
    if buf.is_empty() {
        return Err(malformed(format!("truncated {what}")));
    }
    let version = buf.get_u8();
    if version != BATCH_VERSION {
        return Err(malformed(format!("unknown {what} version {version}")));
    }
    Ok(())
}

fn get_i32(buf: &mut &[u8]) -> Result<i32, CodecError> {
    if buf.len() < 4 {
        return Err(malformed("truncated payload"));
    }
    Ok(buf.get_i32())
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

fn get_exact(buf: &mut &[u8], len: usize) -> Result<Bytes, CodecError> {
    if buf.len() < len {
        return Err(malformed("truncated payload"));
    }
    let out = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(out)
}

fn get_bytes(buf: &mut &[u8]) -> Result<Bytes, CodecError> {
    let len = get_u32(buf)? as usize;
    get_exact(buf, len)
}

fn get_string(buf: &mut &[u8]) -> Result<String, CodecError> {
    let bytes = get_bytes(buf)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| malformed("invalid UTF-8 in header name"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records() -> Vec<PicoRecord> {
        vec![
            PicoRecord::new("one")
                .with_key("k1")
                .with_header("h", "v")
                .with_header("h", Bytes::from_static(&[0xff, 0x00])),
            PicoRecord::new(Bytes::new()),
            PicoRecord::new(Bytes::from_static(&[0xfe])).with_key(Bytes::new()),
        ]
    }

    #[test]
    fn binary_batches_round_trip_losslessly() {
        let decoded = decode_batch_append(&encode_batch_append(&records())).unwrap();
        assert_eq!(decoded, records());
        assert_eq!(
            decoded[2].key,
            Some(Bytes::new()),
            "empty key is not no key"
        );

        let sequenced: Vec<SequencedRecord> = records()
            .into_iter()
            .enumerate()
            .map(|(i, record)| SequencedRecord {
                seq: 40 + i as u64,
                record: record.with_timestamp(7),
            })
            .collect();
        assert_eq!(
            decode_batch_read(&encode_batch_read(&sequenced)).unwrap(),
            sequenced
        );
    }

    #[test]
    fn json_read_and_append() {
        let sequenced = vec![SequencedRecord {
            seq: 1,
            record: records()[0].clone().with_timestamp(5),
        }];
        let json: Value = serde_json::from_slice(&encode_json_read(&sequenced)).unwrap();
        assert_eq!(json[0]["seq"], 1);
        assert_eq!(json[0]["timestamp"], 5);
        assert_eq!(json[0]["key"], "k1");
        assert_eq!(json[0]["headers"]["h"], "v");
        assert_eq!(json[0]["headers_b64"]["h"], "/wA=");
        assert_eq!(json[0]["body"], "one");

        let binary = vec![SequencedRecord {
            seq: 2,
            record: PicoRecord::new(Bytes::from_static(&[0xff, 0xfe]))
                .with_key(Bytes::from_static(&[0x80])),
        }];
        let json: Value = serde_json::from_slice(&encode_json_read(&binary)).unwrap();
        assert!(json[0].get("body").is_none());
        assert_eq!(json[0]["key_b64"], "gA==");
        let decoded = decode_json_append(
            format!(
                r#"{{"records":[{{"key_b64":"gA==","body_b64":"{}","headers":{{"a":"1"}},"headers_b64":{{"b":"/w=="}}}}]}}"#,
                json[0]["body_b64"].as_str().unwrap()
            )
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(&decoded[0].body[..], &[0xff, 0xfe]);
        assert_eq!(decoded[0].key.as_deref(), Some(&[0x80][..]));
        assert_eq!(decoded[0].header_str("a"), Some("1"));
        assert_eq!(
            decoded[0].headers[1],
            ("b".to_owned(), Bytes::from_static(&[0xff]))
        );

        let plain = decode_json_append(br#"{"records":[{"body":"x"}]}"#).unwrap();
        assert_eq!(plain[0].key, None);
        assert_eq!(&plain[0].body[..], b"x");

        assert!(decode_json_append(br#"{"records":[]}"#).is_err());
        assert!(decode_json_append(br#"{"records":[{"body":1}]}"#).is_err());
        assert!(decode_json_append(br#"{"records":[{"key_b64":"!!"}]}"#).is_err());
        assert!(decode_json_append(b"not json").is_err());
    }

    #[test]
    fn truncation_and_bad_version_rejected() {
        let encoded = encode_batch_append(&records());
        assert!(decode_batch_append(&[]).is_err());
        assert!(decode_batch_append(&[9, 0, 0, 0, 0]).is_err());
        for len in 1..encoded.len() {
            assert!(
                decode_batch_append(&encoded[..len]).is_err(),
                "truncated at {len}"
            );
        }
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(decode_batch_append(&trailing).is_err());
    }
}
