use base64::Engine as _;
use bytes::Bytes;
use http::{HeaderMap, Method};
use serde_json::{json, Map, Value};

use crate::error::{CodecError, ErrorKind, WireError};
use crate::mime::{is_json, mime_of};
use crate::wire::{header_string, header_u64, stream_path, truthy, urlencode, WireRequest};

pub use crate::wire::Producer;

pub const H_STREAM_NEXT_OFFSET: &str = "Stream-Next-Offset";
pub const H_STREAM_UP_TO_DATE: &str = "Stream-Up-To-Date";
pub const H_STREAM_TTL: &str = "Stream-TTL";
pub const H_STREAM_EXPIRES_AT: &str = "Stream-Expires-At";
pub const H_STREAM_CLOSED: &str = "Stream-Closed";
pub const H_STREAM_SCHEMA: &str = "Stream-Schema";
pub const H_STREAM_SCHEMA_VALIDATE: &str = "Stream-Schema-Validate";
pub const H_STREAM_CURSOR: &str = "Stream-Cursor";
pub const H_STREAM_SSE_DATA_ENCODING: &str = "Stream-SSE-Data-Encoding";
pub const H_STREAM_SEQ: &str = "Stream-Seq";
pub const H_PRODUCER_ID: &str = "Producer-Id";
pub const H_PRODUCER_EPOCH: &str = "Producer-Epoch";
pub const H_PRODUCER_SEQ: &str = "Producer-Seq";
pub const H_PRODUCER_EXPECTED_SEQ: &str = "Producer-Expected-Seq";
pub const H_PRODUCER_RECEIVED_SEQ: &str = "Producer-Received-Seq";
pub const Q_OFFSET: &str = "offset";
pub const Q_LIVE: &str = "live";
pub const Q_CURSOR: &str = "cursor";
pub const LIVE_LONG_POLL: &str = "long-poll";
pub const LIVE_SSE: &str = "sse";
pub const OFFSET_BEGINNING: &str = "-1";
pub const OFFSET_NOW: &str = "now";

pub fn error_kind(
    status: u16,
    closed: bool,
    fenced: bool,
    sequence_conflict: bool,
) -> (ErrorKind, &'static str) {
    match status {
        400 => (ErrorKind::BadRequest, "bad_request"),
        401 => (ErrorKind::Unauthenticated, "unauthenticated"),
        403 if fenced => (ErrorKind::StaleEpoch, "stale_epoch"),
        403 => (ErrorKind::PermissionDenied, "permission_denied"),
        404 => (ErrorKind::NotFound, "not_found"),
        409 if closed => (ErrorKind::Closed, "closed"),
        409 if sequence_conflict => (ErrorKind::Conflict, "sequence_conflict"),
        409 => (ErrorKind::Conflict, "conflict"),
        410 => (ErrorKind::OffsetGone, "offset_gone"),
        _ => (ErrorKind::Other, "request_failed"),
    }
}

pub fn encode_json_array(payloads: &[Bytes]) -> Bytes {
    let mut body = Vec::with_capacity(2 + payloads.iter().map(|p| p.len() + 1).sum::<usize>());
    body.push(b'[');
    for (i, payload) in payloads.iter().enumerate() {
        if i > 0 {
            body.push(b',');
        }
        body.extend_from_slice(payload);
    }
    body.push(b']');
    Bytes::from(body)
}

/// Splits a DS request body into the messages it carries. JSON streams take
/// a top-level array as one message per element (compacted); any other JSON
/// value is a single message. Non-JSON bodies are one message as-is. An empty
/// array is allowed only for `create` (a stream with no initial messages).
pub fn split_body(
    content_type: &str,
    body: &Bytes,
    create: bool,
) -> Result<Vec<Bytes>, CodecError> {
    if !is_json(&mime_of(Some(content_type))) {
        return Ok(vec![body.clone()]);
    }
    let node: Value = serde_json::from_slice(body).map_err(|_| CodecError::new("invalid JSON"))?;
    let compact = |value: &Value| {
        serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|_| CodecError::new("invalid JSON"))
    };
    match &node {
        Value::Array(items) if items.is_empty() && create => Ok(Vec::new()),
        Value::Array(items) if items.is_empty() => {
            Err(CodecError::new("empty JSON array not allowed"))
        }
        Value::Array(items) => items.iter().map(compact).collect(),
        other => Ok(vec![compact(other)?]),
    }
}

fn concatenated(payloads: &[Bytes]) -> Bytes {
    let mut out = Vec::with_capacity(payloads.iter().map(|p| p.len()).sum());
    for payload in payloads {
        out.extend_from_slice(payload);
    }
    Bytes::from(out)
}

pub struct SseEncoder {
    json: bool,
    pub base64: bool,
}

impl SseEncoder {
    pub fn new(content_type: &str) -> Self {
        let mime = mime_of(Some(content_type));
        let json = is_json(&mime);
        Self {
            json,
            base64: !json && !mime.starts_with("text/"),
        }
    }

    pub fn data_event(&self, payloads: &[Bytes]) -> Bytes {
        let mut out = String::from("event: data\n");
        if self.base64 {
            out.push_str("data:");
            out.push_str(&base64::engine::general_purpose::STANDARD.encode(concatenated(payloads)));
            out.push('\n');
        } else {
            let text = if self.json {
                String::from_utf8_lossy(&encode_json_array(payloads)).into_owned()
            } else {
                String::from_utf8_lossy(&concatenated(payloads)).into_owned()
            };
            for line in crate::sse::lines(&text) {
                out.push_str("data:");
                out.push_str(line);
                out.push('\n');
            }
        }
        out.push('\n');
        Bytes::from(out)
    }

    pub fn control_event(
        &self,
        next_offset: &str,
        cursor: Option<u64>,
        up_to_date: bool,
        closed: bool,
    ) -> Bytes {
        let mut node = Map::new();
        node.insert("streamNextOffset".into(), json!(next_offset));
        if !closed {
            if let Some(cursor) = cursor {
                node.insert("streamCursor".into(), json!(cursor.to_string()));
            }
        }
        node.insert("upToDate".into(), json!(up_to_date));
        if closed {
            node.insert("streamClosed".into(), json!(true));
        }
        let json = serde_json::to_string(&Value::Object(node)).expect("json encode");
        Bytes::from(format!("event: control\ndata:{json}\n\n"))
    }
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
    pub initial_body: Bytes,
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
            initial_body: Bytes::new(),
        }
    }

    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::PUT, stream_path(self.stream), &[200, 201])
            .header("content-type", self.content_type)
            .header_opt(H_STREAM_TTL, self.ttl_seconds)
            .header_opt(H_STREAM_EXPIRES_AT, self.expires_at)
            .flag(H_STREAM_CLOSED, self.closed)
            .header_opt(H_STREAM_SCHEMA, self.schema)
            .flag(H_STREAM_SCHEMA_VALIDATE, self.schema_validate)
            .body(self.initial_body.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateResponse {
    pub created: bool,
    pub content_type: Option<String>,
    pub next_offset: Option<String>,
    pub closed: bool,
}

impl CreateResponse {
    pub fn decode(status: u16, headers: &HeaderMap) -> Self {
        Self {
            created: status == 201,
            content_type: header_string(headers, "content-type"),
            next_offset: header_string(headers, H_STREAM_NEXT_OFFSET),
            closed: truthy(headers, H_STREAM_CLOSED),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendRequest<'a> {
    pub stream: &'a str,
    pub content_type: &'a str,
    pub body: Bytes,
    pub stream_seq: Option<&'a str>,
    pub producer: Option<Producer<'a>>,
    pub close: bool,
}

impl<'a> AppendRequest<'a> {
    pub fn new(stream: &'a str, content_type: &'a str, body: Bytes) -> Self {
        Self {
            stream,
            content_type,
            body,
            stream_seq: None,
            producer: None,
            close: false,
        }
    }

    pub fn encode(&self) -> WireRequest {
        let mut request = WireRequest::new(Method::POST, stream_path(self.stream), &[200, 204])
            .header("content-type", self.content_type)
            .body(self.body.clone());
        if let Some(producer) = &self.producer {
            request = request
                .header(H_PRODUCER_ID, producer.id)
                .header(H_PRODUCER_EPOCH, producer.epoch.to_string())
                .header(H_PRODUCER_SEQ, producer.seq.to_string());
        }
        request
            .header_opt(H_STREAM_SEQ, self.stream_seq)
            .flag(H_STREAM_CLOSED, self.close)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResponse {
    pub next_offset: Option<String>,
    pub producer_epoch: Option<u64>,
    pub producer_seq: Option<u64>,
    pub closed: bool,
}

impl AppendResponse {
    pub fn decode(headers: &HeaderMap) -> Self {
        Self {
            next_offset: header_string(headers, H_STREAM_NEXT_OFFSET),
            producer_epoch: header_u64(headers, H_PRODUCER_EPOCH),
            producer_seq: header_u64(headers, H_PRODUCER_SEQ),
            closed: truthy(headers, H_STREAM_CLOSED),
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
    pub next_offset: Option<String>,
    pub closed: bool,
    pub ttl_seconds: Option<u64>,
    pub expires_at: Option<String>,
    pub schema: Option<String>,
}

impl HeadResponse {
    pub fn decode(status: u16, headers: &HeaderMap) -> Option<Self> {
        if status == 404 {
            return None;
        }
        Some(Self {
            content_type: header_string(headers, "content-type"),
            next_offset: header_string(headers, H_STREAM_NEXT_OFFSET),
            closed: truthy(headers, H_STREAM_CLOSED),
            ttl_seconds: header_u64(headers, H_STREAM_TTL),
            expires_at: header_string(headers, H_STREAM_EXPIRES_AT),
            schema: header_string(headers, H_STREAM_SCHEMA),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReadRequest<'a> {
    pub stream: &'a str,
    pub offset: &'a str,
    pub live: Option<&'static str>,
    pub cursor: Option<&'a str>,
}

impl<'a> ReadRequest<'a> {
    pub fn new(stream: &'a str, offset: &'a str) -> Self {
        Self {
            stream,
            offset,
            live: None,
            cursor: None,
        }
    }

    pub fn encode(&self) -> WireRequest {
        let mut query = format!("?{Q_OFFSET}={}", urlencode(self.offset));
        if let Some(live) = self.live {
            query.push_str(&format!("&{Q_LIVE}={live}"));
        }
        if let Some(cursor) = self.cursor {
            query.push_str(&format!("&{Q_CURSOR}={}", urlencode(cursor)));
        }
        WireRequest::new(
            Method::GET,
            format!("{}{query}", stream_path(self.stream)),
            &[200, 204],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResponse {
    pub next_offset: Option<String>,
    pub up_to_date: bool,
    pub closed: bool,
    pub no_content: bool,
    pub cursor: Option<String>,
}

impl ReadResponse {
    pub fn decode(status: u16, headers: &HeaderMap) -> Self {
        Self {
            next_offset: header_string(headers, H_STREAM_NEXT_OFFSET),
            up_to_date: truthy(headers, H_STREAM_UP_TO_DATE),
            closed: truthy(headers, H_STREAM_CLOSED),
            no_content: status == 204,
            cursor: header_string(headers, H_STREAM_CURSOR),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CloseRequest<'a> {
    pub stream: &'a str,
}

impl CloseRequest<'_> {
    pub fn encode(&self) -> WireRequest {
        WireRequest::new(Method::POST, stream_path(self.stream), &[200, 204])
            .flag(H_STREAM_CLOSED, true)
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
    let closed = truthy(headers, H_STREAM_CLOSED);
    let epoch = header_string(headers, H_PRODUCER_EPOCH);
    let expected_seq = header_string(headers, H_PRODUCER_EXPECTED_SEQ);
    let received_seq = header_string(headers, H_PRODUCER_RECEIVED_SEQ);
    let next = header_string(headers, H_STREAM_NEXT_OFFSET);

    let (kind, code) = error_kind(
        status,
        closed,
        epoch.is_some(),
        expected_seq.is_some() || received_seq.is_some(),
    );
    let mut message = Some(body.to_owned()).filter(|b| !b.is_empty());
    if kind == ErrorKind::StaleEpoch {
        if let Some(epoch) = epoch {
            message = Some(format!(
                "{} (current epoch {epoch})",
                message.unwrap_or_else(|| "stale producer epoch".to_owned())
            ));
        }
    }
    if let (Some(expected_seq), Some(received_seq)) = (&expected_seq, &received_seq) {
        message = Some(format!(
            "{} (expected seq {expected_seq}, received {received_seq})",
            message.unwrap_or_else(|| "producer sequence gap".to_owned())
        ));
    }

    WireError {
        status,
        kind,
        code: code.to_owned(),
        message,
        next,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_array_body() {
        let payloads = [Bytes::from_static(b"{\"a\":1}"), Bytes::from_static(b"2")];
        assert_eq!(&encode_json_array(&payloads)[..], b"[{\"a\":1},2]");
        assert_eq!(&encode_json_array(&[])[..], b"[]");
    }

    #[test]
    fn json_bodies_split_per_element() {
        let body = Bytes::from_static(b"[ {\"a\": 1}, 2 ]");
        let split = split_body("application/json", &body, false).unwrap();
        assert_eq!(
            split,
            [Bytes::from_static(b"{\"a\":1}"), Bytes::from_static(b"2")]
        );
        assert_eq!(
            split_body("application/json", &Bytes::from_static(b"{\"a\":1}"), false).unwrap(),
            [Bytes::from_static(b"{\"a\":1}")]
        );
        assert_eq!(
            split_body("application/json", &Bytes::from_static(b"[]"), true).unwrap(),
            Vec::<Bytes>::new()
        );
        assert!(split_body("application/json", &Bytes::from_static(b"[]"), false).is_err());
        assert!(split_body("application/json", &Bytes::from_static(b"nope"), false).is_err());
        assert_eq!(
            split_body("text/plain", &Bytes::from_static(b"[1,2]"), false).unwrap(),
            [Bytes::from_static(b"[1,2]")]
        );
    }

    #[test]
    fn sse_encodings_per_content_type() {
        let payloads = [Bytes::from_static(b"one\ntwo")];

        let text = SseEncoder::new("text/plain");
        assert!(!text.base64);
        assert_eq!(
            std::str::from_utf8(&text.data_event(&payloads)).unwrap(),
            "event: data\ndata:one\ndata:two\n\n"
        );

        let json = SseEncoder::new("application/json");
        assert_eq!(
            std::str::from_utf8(&json.data_event(&[Bytes::from_static(b"1")])).unwrap(),
            "event: data\ndata:[1]\n\n"
        );

        let binary = SseEncoder::new("application/octet-stream");
        assert!(binary.base64);
        assert_eq!(
            std::str::from_utf8(&binary.data_event(&[Bytes::from_static(&[0xff])])).unwrap(),
            "event: data\ndata:/w==\n\n"
        );

        assert_eq!(
            std::str::from_utf8(&text.control_event("42", Some(7), true, false)).unwrap(),
            "event: control\ndata:{\"streamCursor\":\"7\",\"streamNextOffset\":\"42\",\"upToDate\":true}\n\n"
        );
        assert_eq!(
            std::str::from_utf8(&text.control_event("42", Some(7), true, true)).unwrap(),
            "event: control\ndata:{\"streamClosed\":true,\"streamNextOffset\":\"42\",\"upToDate\":true}\n\n"
        );
    }
}
