use bytes::Bytes;
use http::{HeaderMap, Method};
use serde_json::{Map, Value, json};

use crate::error::{CodecError, ErrorKind, WireError};
use crate::record::{
    PicoRecord, SequencedRecord, decode_batch_read, encode_batch_append, encode_json_read,
};
use crate::wire::{
    WireRequest, header_i64, header_string, header_u64, stream_path, truthy, urlencode,
};

pub use crate::wire::Producer;

pub const H_START_SEQ: &str = "Pico-Start-Seq";
pub const H_NEXT_SEQ: &str = "Pico-Next-Seq";
pub const H_TIMESTAMP: &str = "Pico-Timestamp";
pub const H_MATCH_SEQ: &str = "Pico-Match-Seq";
pub const H_TRIM_SEQ: &str = "Pico-Trim-Seq";
pub const H_TTL: &str = "Pico-TTL";
pub const H_EXPIRES_AT: &str = "Pico-Expires-At";
pub const H_CLOSED: &str = "Pico-Closed";
pub const H_SCHEMA: &str = "Pico-Schema";
pub const H_SCHEMA_VALIDATE: &str = "Pico-Schema-Validate";
pub const H_UP_TO_DATE: &str = "Pico-Up-To-Date";
pub const H_CURSOR: &str = "Pico-Cursor";
pub const H_PRODUCER_ID: &str = "Pico-Producer-Id";
pub const H_PRODUCER_EPOCH: &str = "Pico-Producer-Epoch";
pub const H_PRODUCER_SEQ: &str = "Pico-Producer-Seq";
pub const H_EXPECTED_SEQ: &str = "Pico-Expected-Seq";
pub const H_RECEIVED_SEQ: &str = "Pico-Received-Seq";
/// Record key for a single-record append; batch bodies carry keys inline.
pub const H_KEY: &str = "Pico-Key";
/// The stream's Kafka topic alias (create request and metadata responses).
pub const H_KAFKA_TOPIC: &str = "Pico-Kafka-Topic";
pub const CT_BATCH_JSON: &str = "application/vnd.picomq.batch+json";
pub const CT_BATCH_BINARY: &str = "application/vnd.picomq.batch";
pub const CT_JSON: &str = "application/json";
pub const CT_EVENT_STREAM: &str = "text/event-stream";
pub const DEFAULT_CT: &str = "application/octet-stream";
pub const Q_SEQ: &str = "seq";
pub const Q_COUNT: &str = "count";
pub const Q_BYTES: &str = "bytes";
pub const Q_FORMAT: &str = "format";
pub const Q_LIVE: &str = "live";
pub const Q_PREFIX: &str = "prefix";
pub const Q_LIMIT: &str = "limit";
pub const Q_START_AFTER: &str = "start_after";
pub const Q_CURSOR: &str = "cursor";
pub const FORMAT_JSON: &str = "json";
pub const FORMAT_BINARY: &str = "binary";
pub const FORMAT_RAW: &str = "raw";
pub const LIVE_LONG_POLL: &str = "long-poll";
pub const LIVE_SSE: &str = "sse";
pub const SEQ_NOW: &str = "now";
pub const SEQ_BEGINNING: &str = "0";
pub const E_NOT_FOUND: &str = "not_found";
pub const E_BAD_REQUEST: &str = "bad_request";
pub const E_SCHEMA_VIOLATION: &str = "schema_violation";
pub const E_FENCED: &str = "fenced";
pub const E_SEQUENCE_GAP: &str = "sequence_gap";
pub const E_MATCH_FAILED: &str = "match_failed";
pub const E_CONFLICT: &str = "conflict";
pub const E_CLOSED: &str = "closed";
pub const E_DURABILITY: &str = "durability";
pub const E_UNAUTHENTICATED: &str = "unauthenticated";
pub const E_PERMISSION_DENIED: &str = "permission_denied";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorBody {
    pub code: String,
    pub message: Option<String>,
    pub next_seq: Option<u64>,
}

impl ErrorBody {
    pub fn encode(&self) -> Bytes {
        let mut node = Map::new();
        node.insert("error".into(), json!(self.code));
        if let Some(message) = &self.message {
            node.insert("message".into(), json!(message));
        }
        if let Some(next_seq) = self.next_seq {
            node.insert("next_seq".into(), json!(next_seq));
        }
        Bytes::from(serde_json::to_vec(&Value::Object(node)).expect("json encode"))
    }

    pub fn decode(status: u16, body: &str) -> Self {
        let parsed: Value = serde_json::from_str(body).unwrap_or(Value::Null);
        let code = parsed["error"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("http_{status}"));
        let message = parsed["message"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| Some(body.to_owned()).filter(|b| !b.is_empty() && parsed.is_null()));
        Self {
            code,
            message,
            next_seq: parsed["next_seq"].as_u64(),
        }
    }
}

pub fn error_kind(status: u16, code: &str, closed: bool) -> ErrorKind {
    match status {
        400 => ErrorKind::BadRequest,
        401 => ErrorKind::Unauthenticated,
        403 if code == E_PERMISSION_DENIED => ErrorKind::PermissionDenied,
        403 => ErrorKind::StaleEpoch,
        404 => ErrorKind::NotFound,
        409 if closed || code == E_CLOSED => ErrorKind::Closed,
        409 | 412 => ErrorKind::Conflict,
        410 => ErrorKind::OffsetGone,
        _ => ErrorKind::Other,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub name: String,
    pub content_type: Option<String>,
    pub start_seq: u64,
    pub next_seq: u64,
    pub closed: bool,
    pub ttl_seconds: Option<u64>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Listing {
    pub streams: Vec<StreamEntry>,
    pub has_more: bool,
}

impl Listing {
    pub fn encode(&self) -> Bytes {
        let streams: Vec<Value> = self
            .streams
            .iter()
            .map(|entry| {
                let mut node = Map::new();
                node.insert("name".into(), json!(entry.name));
                if let Some(content_type) = &entry.content_type {
                    node.insert("content_type".into(), json!(content_type));
                }
                node.insert("start_seq".into(), json!(entry.start_seq));
                node.insert("next_seq".into(), json!(entry.next_seq));
                node.insert("closed".into(), json!(entry.closed));
                if let Some(ttl) = entry.ttl_seconds {
                    node.insert("ttl".into(), json!(ttl));
                }
                if let Some(expires_at) = &entry.expires_at {
                    node.insert("expires_at".into(), json!(expires_at));
                }
                Value::Object(node)
            })
            .collect();
        let body = json!({ "streams": streams, "has_more": self.has_more });
        Bytes::from(serde_json::to_vec(&body).expect("json encode"))
    }

    pub fn decode(payload: &[u8]) -> Result<Self, CodecError> {
        let root: Value =
            serde_json::from_slice(payload).map_err(|_| CodecError::new("invalid JSON"))?;
        let streams = root["streams"]
            .as_array()
            .map(|entries| {
                entries
                    .iter()
                    .map(|node| StreamEntry {
                        name: node["name"].as_str().unwrap_or_default().to_owned(),
                        content_type: node["content_type"].as_str().map(str::to_owned),
                        start_seq: node["start_seq"].as_u64().unwrap_or(0),
                        next_seq: node["next_seq"].as_u64().unwrap_or(0),
                        closed: node["closed"].as_bool().unwrap_or(false),
                        ttl_seconds: node["ttl"].as_u64(),
                        expires_at: node["expires_at"].as_str().map(str::to_owned),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            streams,
            has_more: root["has_more"].as_bool().unwrap_or(false),
        })
    }
}

pub fn sse_data_event(records: &[SequencedRecord], next_seq: u64) -> Bytes {
    let json = encode_json_read(records);
    let json = String::from_utf8(json.to_vec()).expect("json is utf-8");
    let mut out = format!("event: data\nid: {next_seq}\n");
    for line in crate::sse::lines(&json) {
        out.push_str("data:");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    Bytes::from(out)
}

pub fn sse_control_event(next_seq: u64, up_to_date: bool, closed: bool) -> Bytes {
    let mut node = Map::new();
    node.insert("next_seq".into(), json!(next_seq));
    node.insert("up_to_date".into(), json!(up_to_date));
    if closed {
        node.insert("closed".into(), json!(true));
    }
    let json = serde_json::to_string(&Value::Object(node)).expect("json encode");
    Bytes::from(format!("event: control\nid: {next_seq}\ndata:{json}\n\n"))
}

#[derive(Debug, Clone)]
pub struct CreateRequest<'a> {
    pub stream: &'a str,
    pub content_type: &'a str,
    pub ttl_seconds: Option<u64>,
    pub expires_at: Option<&'a str>,
    pub closed: bool,
    pub schema: Option<&'a str>,
    pub schema_validate: bool,
    pub kafka_topic: Option<&'a str>,
}

impl<'a> CreateRequest<'a> {
    pub fn new(stream: &'a str, content_type: &'a str) -> Self {
        Self {
            stream,
            content_type,
            ttl_seconds: None,
            expires_at: None,
            closed: false,
            schema: None,
            schema_validate: false,
            kafka_topic: None,
        }
    }

    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::PUT, stream_path(self.stream), &[200, 201])
            .header("content-type", self.content_type)
            .header_opt(H_TTL, self.ttl_seconds)
            .header_opt(H_EXPIRES_AT, self.expires_at)
            .flag(H_CLOSED, self.closed)
            .header_opt(H_SCHEMA, self.schema)
            .flag(H_SCHEMA_VALIDATE, self.schema_validate)
            .header_opt(H_KAFKA_TOPIC, self.kafka_topic)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateResponse {
    pub created: bool,
    pub content_type: Option<String>,
    pub next_seq: Option<u64>,
    pub closed: bool,
}

impl CreateResponse {
    pub fn decode(status: u16, headers: &HeaderMap) -> Self {
        Self {
            created: status == 201,
            content_type: header_string(headers, "content-type"),
            next_seq: header_u64(headers, H_NEXT_SEQ),
            closed: truthy(headers, H_CLOSED),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendRequest<'a> {
    pub stream: &'a str,
    pub records: &'a [PicoRecord],
    pub producer: Option<Producer<'a>>,
    pub match_seq: Option<u64>,
    pub close: bool,
}

impl<'a> AppendRequest<'a> {
    pub fn new(stream: &'a str, records: &'a [PicoRecord]) -> Self {
        Self {
            stream,
            records,
            producer: None,
            match_seq: None,
            close: false,
        }
    }

    pub fn encode(&self) -> WireRequest {
        let mut request = WireRequest::new(Method::POST, stream_path(self.stream), &[200]);
        if !self.records.is_empty() {
            request = request
                .header("content-type", CT_BATCH_BINARY)
                .body(encode_batch_append(self.records));
        }
        if let Some(producer) = &self.producer {
            request = request
                .header(H_PRODUCER_ID, producer.id)
                .header(H_PRODUCER_EPOCH, producer.epoch.to_string())
                .header(H_PRODUCER_SEQ, producer.seq.to_string());
        }
        request
            .header_opt(H_MATCH_SEQ, self.match_seq)
            .flag(H_CLOSED, self.close)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResponse {
    pub start_seq: Option<u64>,
    pub next_seq: Option<u64>,
    pub timestamp: Option<i64>,
    pub producer_epoch: Option<u64>,
    pub producer_seq: Option<u64>,
    pub closed: bool,
}

impl AppendResponse {
    pub fn decode(headers: &HeaderMap) -> Self {
        Self {
            start_seq: header_u64(headers, H_START_SEQ),
            next_seq: header_u64(headers, H_NEXT_SEQ),
            timestamp: header_i64(headers, H_TIMESTAMP),
            producer_epoch: header_u64(headers, H_PRODUCER_EPOCH),
            producer_seq: header_u64(headers, H_PRODUCER_SEQ),
            closed: truthy(headers, H_CLOSED),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TrimRequest<'a> {
    pub stream: &'a str,
    pub seq: u64,
}

impl TrimRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::POST, stream_path(self.stream), &[200])
            .header(H_TRIM_SEQ, self.seq.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrimResponse {
    pub start_seq: Option<u64>,
}

impl TrimResponse {
    pub fn decode(headers: &HeaderMap) -> Self {
        Self {
            start_seq: header_u64(headers, H_START_SEQ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HeadRequest<'a> {
    pub stream: &'a str,
}

impl HeadRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::HEAD, stream_path(self.stream), &[200, 404])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadResponse {
    pub content_type: Option<String>,
    pub start_seq: Option<u64>,
    pub next_seq: Option<u64>,
    pub closed: bool,
    pub ttl_seconds: Option<u64>,
    pub expires_at: Option<String>,
    pub schema: Option<String>,
    pub kafka_topic: Option<String>,
}

impl HeadResponse {
    pub fn decode(status: u16, headers: &HeaderMap) -> Option<Self> {
        if status == 404 {
            return None;
        }
        Some(Self {
            content_type: header_string(headers, "content-type"),
            start_seq: header_u64(headers, H_START_SEQ),
            next_seq: header_u64(headers, H_NEXT_SEQ),
            closed: truthy(headers, H_CLOSED),
            ttl_seconds: header_u64(headers, H_TTL),
            expires_at: header_string(headers, H_EXPIRES_AT),
            schema: header_string(headers, H_SCHEMA),
            kafka_topic: header_string(headers, H_KAFKA_TOPIC),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReadRequest<'a> {
    pub stream: &'a str,
    pub seq: &'a str,
    pub format: &'static str,
    pub count: u64,
    pub bytes: u64,
    pub live: Option<&'static str>,
}

impl<'a> ReadRequest<'a> {
    pub fn new(stream: &'a str, seq: &'a str) -> Self {
        Self {
            stream,
            seq,
            format: FORMAT_BINARY,
            count: 0,
            bytes: 0,
            live: None,
        }
    }

    pub fn encode(&self) -> WireRequest {
        let mut query = format!(
            "?{Q_FORMAT}={}&{Q_SEQ}={}",
            self.format,
            urlencode(self.seq)
        );
        if self.count > 0 {
            query.push_str(&format!("&{Q_COUNT}={}", self.count));
        }
        if self.bytes > 0 {
            query.push_str(&format!("&{Q_BYTES}={}", self.bytes));
        }
        if let Some(live) = self.live {
            query.push_str(&format!("&{Q_LIVE}={live}"));
        }
        let ok: &'static [u16] = if self.live.is_some() {
            &[200, 204]
        } else {
            &[200]
        };
        WireRequest::new(
            Method::GET,
            format!("{}{query}", stream_path(self.stream)),
            ok,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResponse {
    pub records: Vec<SequencedRecord>,
    pub next_seq: Option<u64>,
    pub up_to_date: bool,
    pub closed: bool,
    pub no_content: bool,
}

impl ReadResponse {
    pub fn decode(status: u16, headers: &HeaderMap, body: &[u8]) -> Result<Self, CodecError> {
        let no_content = status == 204;
        let records = if no_content || body.is_empty() {
            Vec::new()
        } else {
            decode_batch_read(body)?
        };
        Ok(Self {
            records,
            next_seq: header_u64(headers, H_NEXT_SEQ),
            up_to_date: truthy(headers, H_UP_TO_DATE),
            closed: truthy(headers, H_CLOSED),
            no_content,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ListRequest<'a> {
    pub prefix: &'a str,
    pub limit: u64,
    pub start_after: Option<&'a str>,
}

impl ListRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        let mut query = format!("?{Q_PREFIX}={}", urlencode(self.prefix));
        if self.limit > 0 {
            query.push_str(&format!("&{Q_LIMIT}={}", self.limit));
        }
        if let Some(start_after) = self.start_after {
            query.push_str(&format!("&{Q_START_AFTER}={}", urlencode(start_after)));
        }
        WireRequest::new(Method::GET, format!("/{query}"), &[200])
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CloseRequest<'a> {
    pub stream: &'a str,
}

impl CloseRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::POST, stream_path(self.stream), &[200]).flag(H_CLOSED, true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseResponse {
    pub next_seq: Option<u64>,
}

impl CloseResponse {
    pub fn decode(headers: &HeaderMap) -> Self {
        Self {
            next_seq: header_u64(headers, H_NEXT_SEQ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DeleteRequest<'a> {
    pub stream: &'a str,
}

impl DeleteRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::DELETE, stream_path(self.stream), &[204, 404])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteResponse {
    pub found: bool,
}

impl DeleteResponse {
    pub fn decode(status: u16) -> Self {
        Self {
            found: status != 404,
        }
    }
}

pub fn decode_error(status: u16, headers: &HeaderMap, body: &str) -> WireError {
    let closed = truthy(headers, H_CLOSED);
    let error = ErrorBody::decode(status, body);
    WireError {
        status,
        kind: error_kind(status, &error.code, closed),
        code: error.code,
        message: error.message,
        next: error.next_seq.map(|v| v.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_body_round_trip() {
        let full = ErrorBody {
            code: "conflict".to_owned(),
            message: Some("stream exists".to_owned()),
            next_seq: Some(7),
        };
        let encoded = full.encode();
        assert_eq!(
            ErrorBody::decode(409, std::str::from_utf8(&encoded).unwrap()),
            full
        );

        let plain = ErrorBody::decode(500, "boom");
        assert_eq!(plain.code, "http_500");
        assert_eq!(plain.message.as_deref(), Some("boom"));
        assert_eq!(plain.next_seq, None);

        let bare = ErrorBody::decode(404, "");
        assert_eq!(bare.code, "http_404");
        assert_eq!(bare.message, None);
    }

    #[test]
    fn listing_round_trip() {
        let listing = Listing {
            streams: vec![StreamEntry {
                name: "/a".to_owned(),
                content_type: Some("application/json".to_owned()),
                start_seq: 1,
                next_seq: 5,
                closed: false,
                ttl_seconds: Some(60),
                expires_at: Some("2026-01-01T00:00:00Z".to_owned()),
            }],
            has_more: true,
        };
        assert_eq!(Listing::decode(&listing.encode()).unwrap(), listing);
        assert_eq!(Listing::decode(b"{}").unwrap(), Listing::default());
        assert!(Listing::decode(b"not json").is_err());
    }

    #[test]
    fn sse_events() {
        let control = sse_control_event(3, true, true);
        assert_eq!(
            std::str::from_utf8(&control).unwrap(),
            "event: control\nid: 3\ndata:{\"closed\":true,\"next_seq\":3,\"up_to_date\":true}\n\n"
        );
        let data = sse_data_event(&[], 3);
        assert!(
            std::str::from_utf8(&data)
                .unwrap()
                .starts_with("event: data\nid: 3\ndata:[]")
        );
    }
}
