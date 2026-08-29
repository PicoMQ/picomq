//! The Durable Streams open protocol frontend.
//!
//! `ProtocolConverter`,
//! and the DS `SseEncoder`, on the exact wire vocabulary of the spec
//! (`Stream-*` / `Producer-*` headers are protocol constants and are NOT
//! rebranded):
//!
//! - `PUT /name`   create, optionally with an initial body (idempotent)

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use base64::Engine as _;
use bytes::Bytes;
use serde_json::{json, Map, Value};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;

use pico_auth::{Audience, Authorizer};
use pico_protocol::ds::{
    H_PRODUCER_EPOCH, H_PRODUCER_EXPECTED_SEQ, H_PRODUCER_ID, H_PRODUCER_RECEIVED_SEQ,
    H_PRODUCER_SEQ, H_STREAM_CLOSED, H_STREAM_CURSOR, H_STREAM_EXPIRES_AT, H_STREAM_NEXT_OFFSET,
    H_STREAM_SEQ, H_STREAM_SSE_DATA_ENCODING, H_STREAM_TTL, H_STREAM_UP_TO_DATE,
};
use pico_server::framing::{is_json, mime_of};
use pico_server::ownership::OwnershipService;
use pico_server::types::Producer;
use pico_server::{
    AppendCommand, CreateCommand, ErrorKind, OffsetToken, ReadResult, S3StreamService, ServiceError,
};

use crate::http::{
    bad_request, base_response, cursor, etag, format_instant, header_str, parse_instant_header,
    parse_strict_u64_header, query_param, set_header, truthy,
};
use crate::route::{route, stream_name, RoutingMode};

const CT_EVENT_STREAM: &str = "text/event-stream";
const CT_TEXT: &str = "text/plain; charset=utf-8";
const DEFAULT_CT: &str = "application/octet-stream";
const CACHE_CATCH_UP: &str = "public, max-age=60, stale-while-revalidate=300";
const DEFAULT_MAX_CHUNK_SIZE: usize = 64 * 1024;
const DEFAULT_MAX_REQUEST_SIZE: usize = 32 * 1024 * 1024;

/// The Durable Streams frontend over one node's service + ownership pair.
///
/// Defaults: 25s long poll, 55s SSE cap, 64 KiB chunks, 32 MiB request bodies.
pub struct DsFrontend {
    service: Arc<S3StreamService>,
    ownership: Arc<dyn OwnershipService>,
    authorizer: Option<Arc<Authorizer>>,
    mode: RoutingMode,
    long_poll_timeout: Duration,
    sse_max_duration: Duration,
    max_chunk_size: usize,
    max_request_size: usize,
}

impl DsFrontend {
    pub fn new(
        service: Arc<S3StreamService>,
        ownership: Arc<dyn OwnershipService>,
        mode: RoutingMode,
    ) -> Self {
        Self::with_tuning(
            service,
            ownership,
            mode,
            Duration::from_secs(25),
            Duration::from_secs(55),
            DEFAULT_MAX_CHUNK_SIZE,
            DEFAULT_MAX_REQUEST_SIZE,
        )
    }

    pub fn with_tuning(
        service: Arc<S3StreamService>,
        ownership: Arc<dyn OwnershipService>,
        mode: RoutingMode,
        long_poll_timeout: Duration,
        sse_max_duration: Duration,
        max_chunk_size: usize,
        max_request_size: usize,
    ) -> Self {
        Self {
            service,
            ownership,
            authorizer: None,
            mode,
            long_poll_timeout,
            sse_max_duration,
            max_chunk_size: if max_chunk_size > 0 {
                max_chunk_size
            } else {
                DEFAULT_MAX_CHUNK_SIZE
            },
            max_request_size: if max_request_size > 0 {
                max_request_size
            } else {
                DEFAULT_MAX_REQUEST_SIZE
            },
        }
    }

    /// `None` disables the gate; requests pass through unauthenticated.
    pub fn with_authorizer(mut self, authorizer: Option<Arc<Authorizer>>) -> Self {
        self.authorizer = authorizer;
        self
    }

    pub fn router(self: Arc<Self>) -> Router {
        let limit = self.max_request_size;
        Router::new()
            .fallback(any(dispatch))
            .layer(axum::extract::DefaultBodyLimit::max(limit))
            .with_state(self)
    }
}

async fn dispatch(
    State(frontend): State<Arc<DsFrontend>>,
    request: axum::extract::Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let permit = match crate::auth::gate(
        frontend.authorizer.as_deref(),
        Audience::DurableStreams,
        &parts.method,
        &parts.uri,
        &parts.headers,
    )
    .await
    {
        Ok(permit) => permit,
        Err(response) => return *response,
    };
    let name = match permit {
        Some(permit) => permit.stream_name,
        None => stream_name(&parts.uri),
    };
    if let Some(response) = route(
        frontend.ownership.as_ref(),
        frontend.mode,
        &parts.method,
        &parts.uri,
        &name,
    )
    .await
    {
        return response;
    }
    let body = match axum::body::to_bytes(body, frontend.max_request_size).await {
        Ok(body) => body,
        Err(_) => return fail(413, "content too large"),
    };
    frontend
        .handle(parts.method, parts.uri, parts.headers, body, name)
        .await
}

impl DsFrontend {
    async fn handle(
        &self,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
        name: String,
    ) -> Response {
        let result = match method {
            Method::OPTIONS => Ok(options()),
            Method::PUT => self.put(&uri, &headers, body, name).await,
            Method::POST => self.post(&uri, &headers, body, name).await,
            Method::DELETE => match self.service.delete(&name).await {
                Ok(deleted) => Ok(respond(if deleted { 204 } else { 404 }, None, false)),
                Err(e) => Err(e),
            },
            Method::HEAD => self.head(&name).await,
            Method::GET => self.get(&uri, &headers, &name).await,
            _ => Ok(fail(405, "method not allowed")),
        };
        result.unwrap_or_else(service_error_response)
    }

    async fn put(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        body: Bytes,
        name: String,
    ) -> Result<Response, ServiceError> {
        let ttl_seconds = parse_strict_u64_header(headers, H_STREAM_TTL, "invalid Stream-TTL")?;
        let expires_at_ms =
            parse_instant_header(headers, H_STREAM_EXPIRES_AT, "invalid Stream-Expires-At")?;
        if ttl_seconds.is_some() && expires_at_ms.is_some() {
            return Err(bad_request("Stream-TTL and Stream-Expires-At both set"));
        }

        let content_type = header_str(headers, header::CONTENT_TYPE.as_str())
            .filter(|v| !v.is_empty())
            .unwrap_or(DEFAULT_CT)
            .to_owned();
        let result = self
            .service
            .create(CreateCommand {
                name,
                content_type,
                ttl_seconds,
                expires_at_ms,
                closed: truthy(headers, H_STREAM_CLOSED),
                initial_payload: body,
                external_id: None,
                internal: false,
            })
            .await?;
        let meta = result.meta;

        let mut response = respond(
            if result.created { 201 } else { 200 },
            Some(&meta.next_offset),
            meta.closed,
        );
        set_header(
            &mut response,
            header::CONTENT_TYPE.as_str(),
            &meta.content_type,
        );
        if result.created {
            let mut location = self
                .ownership
                .local_node()
                .advertised_address
                .trim_end_matches('/')
                .to_owned();
            location.push_str(uri.path());
            set_header(&mut response, header::LOCATION.as_str(), &location);
        }
        Ok(response)
    }

    async fn post(
        &self,
        _uri: &Uri,
        headers: &HeaderMap,
        body: Bytes,
        name: String,
    ) -> Result<Response, ServiceError> {
        let close = truthy(headers, H_STREAM_CLOSED);
        let producer = producer_of(headers)?;
        let has_producer = producer.is_some();
        let content_type = header_str(headers, header::CONTENT_TYPE.as_str()).map(str::to_owned);

        if body.is_empty() && !close {
            return Err(bad_request("Empty body"));
        }
        if !body.is_empty() && !content_type.as_deref().is_some_and(|ct| !ct.is_empty()) {
            return Err(bad_request("missing Content-Type"));
        }

        let result = self
            .service
            .append(
                AppendCommand {
                    name,
                    payloads: if body.is_empty() {
                        Vec::new()
                    } else {
                        vec![body]
                    },
                    content_type: content_type.clone(),
                    stream_seq: header_str(headers, H_STREAM_SEQ).map(str::to_owned),
                    match_seq: None,
                    producer,
                    close_after: close,
                    atomic: true,
                    internal: false,
                }
                .normalized(),
            )
            .await?;

        let producer_appended = has_producer && result.applied;
        let mut response = respond(
            if producer_appended { 200 } else { 204 },
            Some(&result.next_offset),
            result.closed,
        );
        if let Some(ct) = content_type.filter(|ct| !ct.is_empty()) {
            set_header(&mut response, header::CONTENT_TYPE.as_str(), &ct);
        }
        if let Some(epoch) = result.producer_epoch {
            set_header(&mut response, H_PRODUCER_EPOCH, &epoch.to_string());
        }
        if let Some(seq) = result.producer_seq {
            set_header(&mut response, H_PRODUCER_SEQ, &seq.to_string());
        }
        Ok(response)
    }

    async fn head(&self, name: &str) -> Result<Response, ServiceError> {
        let Some(meta) = self.service.head(name).await? else {
            return Ok(respond(404, None, false));
        };
        let mut response = respond(200, Some(&meta.next_offset), meta.closed);
        set_header(
            &mut response,
            header::CONTENT_TYPE.as_str(),
            &meta.content_type,
        );
        if let Some(ttl) = meta.ttl_seconds {
            set_header(&mut response, H_STREAM_TTL, &ttl.to_string());
        }
        if let Some(expires_at_ms) = meta.expires_at_ms {
            set_header(
                &mut response,
                H_STREAM_EXPIRES_AT,
                &format_instant(expires_at_ms),
            );
        }
        Ok(response)
    }

    async fn get(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        name: &str,
    ) -> Result<Response, ServiceError> {
        let live = query_param(uri, "live").filter(|v| !v.is_empty());
        let offset_raw = query_param(uri, "offset");
        let offset_now = offset_raw
            .as_deref()
            .is_some_and(|raw| raw.eq_ignore_ascii_case("now"));
        let offset = self
            .parse_offset(name, offset_raw.as_deref(), live.is_some())
            .await?;
        let cursor_raw = query_param(uri, "cursor");

        match live.as_deref() {
            None => {
                let out = self
                    .service
                    .read(name, offset, self.max_chunk_size, 0)
                    .await?;
                self.write_read(headers, name, offset, out, false, offset_now)
            }
            Some("long-poll") => {
                self.long_poll(headers, name, offset, cursor_raw.as_deref())
                    .await
            }
            Some("sse") => self.sse(name, offset, cursor_raw.as_deref()).await,
            Some(_) => Err(bad_request("invalid live mode")),
        }
    }

    async fn long_poll(
        &self,
        headers: &HeaderMap,
        name: &str,
        offset: OffsetToken,
        cursor_raw: Option<&str>,
    ) -> Result<Response, ServiceError> {
        let cursor = cursor(cursor_raw);
        let out = self
            .service
            .read(name, offset, self.max_chunk_size, 0)
            .await?;
        if !(out.records.is_empty() && out.up_to_date) || out.closed {
            let mut response = self.write_read(headers, name, offset, out, true, false)?;
            set_header(&mut response, H_STREAM_CURSOR, &cursor.to_string());
            return Ok(response);
        }

        if self
            .service
            .wait_appended(name, offset, self.long_poll_timeout)
            .await?
        {
            let out = self
                .service
                .read(name, offset, self.max_chunk_size, 0)
                .await?;
            let mut response = self.write_read(headers, name, offset, out, true, false)?;
            set_header(&mut response, H_STREAM_CURSOR, &cursor.to_string());
            return Ok(response);
        }

        let Some(meta) = self.service.head(name).await? else {
            return Ok(respond(404, None, false));
        };
        let mut response = respond(204, Some(&meta.next_offset), meta.closed);
        set_header(&mut response, H_STREAM_CURSOR, &cursor.to_string());
        set_header(&mut response, H_STREAM_UP_TO_DATE, "true");
        Ok(response)
    }

    /// SSE tail per the DS spec: JSON streams emit a JSON-array data event,
    /// text streams raw lines, binary streams base64 (announced via
    /// `Stream-SSE-Data-Encoding`).
    async fn sse(
        &self,
        name: &str,
        start: OffsetToken,
        cursor_raw: Option<&str>,
    ) -> Result<Response, ServiceError> {
        let Some(meta) = self.service.head(name).await? else {
            return Ok(respond(404, None, false));
        };
        let encoder = SseEncoder::new(&meta.content_type);
        let cursor = cursor(cursor_raw);

        let mut response = base_response(200);
        set_header(
            &mut response,
            header::CONTENT_TYPE.as_str(),
            CT_EVENT_STREAM,
        );
        set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-cache");
        if encoder.base64 {
            set_header(&mut response, H_STREAM_SSE_DATA_ENCODING, "base64");
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(8);
        let service = self.service.clone();
        let name = name.to_owned();
        let max_chunk_size = self.max_chunk_size;
        let deadline = tokio::time::Instant::now() + self.sse_max_duration;
        tokio::spawn(async move {
            let mut offset = start;
            let mut announced_caught_up = false;
            while tokio::time::Instant::now() < deadline {
                let Ok(read) = service.read(&name, offset, max_chunk_size, 0).await else {
                    return;
                };

                if !read.records.is_empty() {
                    if tx.send(encoder.data_event(&read)).await.is_err() {
                        return;
                    }
                    offset = read.next_offset;
                    let closed_at_tail = read.closed && read.up_to_date;
                    let control = encoder.control_event(
                        &offset.value(),
                        if closed_at_tail { None } else { Some(cursor) },
                        read.up_to_date,
                        closed_at_tail,
                    );
                    if tx.send(control).await.is_err() || closed_at_tail {
                        return;
                    }
                    announced_caught_up = read.up_to_date;
                    continue;
                }

                if read.closed {
                    let _ = tx
                        .send(encoder.control_event(&read.next_offset.value(), None, true, true))
                        .await;
                    return;
                }

                if !announced_caught_up {
                    let control =
                        encoder.control_event(&read.next_offset.value(), Some(cursor), true, false);
                    if tx.send(control).await.is_err() {
                        return;
                    }
                    announced_caught_up = true;
                }

                if service
                    .wait_appended(&name, offset, Duration::from_secs(1))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        *response.body_mut() =
            Body::from_stream(ReceiverStream::new(rx).map(Ok::<_, std::convert::Infallible>));
        Ok(response)
    }

    fn write_read(
        &self,
        headers: &HeaderMap,
        name: &str,
        start: OffsetToken,
        out: ReadResult,
        live: bool,
        offset_now: bool,
    ) -> Result<Response, ServiceError> {
        let empty_tail = out.records.is_empty() && out.up_to_date;
        let json = is_json(&mime_of(Some(&out.content_type)));

        let mut response = base_response(if empty_tail && live { 204 } else { 200 });
        set_header(
            &mut response,
            header::CACHE_CONTROL.as_str(),
            if live || offset_now {
                "no-store"
            } else {
                CACHE_CATCH_UP
            },
        );
        set_header(
            &mut response,
            H_STREAM_NEXT_OFFSET,
            &out.next_offset.value(),
        );
        if out.closed && out.up_to_date {
            set_header(&mut response, H_STREAM_CLOSED, "true");
        }
        if out.up_to_date {
            set_header(&mut response, H_STREAM_UP_TO_DATE, "true");
        }
        let content_type = if out.content_type.is_empty() {
            DEFAULT_CT
        } else {
            &out.content_type
        };
        set_header(&mut response, header::CONTENT_TYPE.as_str(), content_type);

        if !live && !offset_now {
            let etag = etag(name, &start, &out.next_offset, out.closed && empty_tail);
            set_header(&mut response, header::ETAG.as_str(), &etag);
            if header_str(headers, header::IF_NONE_MATCH.as_str()) == Some(etag.as_str()) {
                *response.status_mut() = StatusCode::NOT_MODIFIED;
                return Ok(response);
            }
        }

        if !(empty_tail && live) {
            let body = if json {
                json_array_body(&out)
            } else {
                out.concatenated()
            };
            *response.body_mut() = Body::from(body);
        }
        Ok(response)
    }

    async fn parse_offset(
        &self,
        name: &str,
        raw: Option<&str>,
        required: bool,
    ) -> Result<OffsetToken, ServiceError> {
        let Some(raw) = raw else {
            if required {
                return Err(bad_request("offset required"));
            }
            return Ok(OffsetToken::beginning());
        };
        if raw.eq_ignore_ascii_case("now") {
            return self
                .service
                .head(name)
                .await?
                .map(|meta| meta.next_offset)
                .ok_or_else(|| ServiceError::kind(ErrorKind::NotFound));
        }
        OffsetToken::parse(Some(raw))
    }
}

fn options() -> Response {
    let mut response = base_response(204);
    set_header(
        &mut response,
        "Access-Control-Allow-Methods",
        "GET, PUT, POST, DELETE, HEAD, OPTIONS",
    );
    set_header(
        &mut response,
        "Access-Control-Allow-Headers",
        "content-type, authorization, If-None-Match, Stream-Seq, Stream-TTL, Stream-Expires-At, \
         Stream-Closed, Producer-Id, Producer-Epoch, Producer-Seq",
    );
    set_header(
        &mut response,
        "Access-Control-Expose-Headers",
        "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Stream-Closed, Producer-Epoch, \
         Producer-Seq, Producer-Expected-Seq, Producer-Received-Seq, etag, content-type",
    );
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    response
}

fn service_error_response(e: ServiceError) -> Response {
    match e.kind {
        ErrorKind::NotFound => respond(404, None, false),
        ErrorKind::BadRequest | ErrorKind::MatchFailed => fail(400, &e.message),
        ErrorKind::Fenced => {
            let mut response = fail(403, &e.message);
            if let Some(epoch) = e.producer_epoch {
                set_header(&mut response, H_PRODUCER_EPOCH, &epoch.to_string());
            }
            response
        }
        ErrorKind::SequenceGap => {
            let mut response = fail(409, &e.message);
            if let Some(expected) = e.expected_seq {
                set_header(
                    &mut response,
                    H_PRODUCER_EXPECTED_SEQ,
                    &expected.to_string(),
                );
            }
            if let Some(received) = e.received_seq {
                set_header(
                    &mut response,
                    H_PRODUCER_RECEIVED_SEQ,
                    &received.to_string(),
                );
            }
            response
        }
        ErrorKind::Conflict | ErrorKind::Closed => respond(409, e.next_offset.as_ref(), e.closed),
        ErrorKind::Durability => fail(500, &e.message),
    }
}

fn producer_of(headers: &HeaderMap) -> Result<Option<Producer>, ServiceError> {
    let id = header_str(headers, H_PRODUCER_ID);
    let epoch = header_str(headers, H_PRODUCER_EPOCH);
    let seq = header_str(headers, H_PRODUCER_SEQ);
    if id.is_none() && epoch.is_none() && seq.is_none() {
        return Ok(None);
    }
    let (Some(id), Some(epoch), Some(seq)) = (id, epoch, seq) else {
        return Err(bad_request(
            "All producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided \
             together",
        ));
    };
    if id.is_empty() {
        return Err(bad_request("Invalid Producer-Id: must not be empty"));
    }
    let epoch = parse_producer_long(
        epoch,
        "Invalid Producer-Epoch: must be a non-negative integer",
    )?;
    let seq = parse_producer_long(seq, "Invalid Producer-Seq: must be a non-negative integer")?;
    Ok(Some(Producer::new(id, epoch, seq)?))
}

fn parse_producer_long(raw: &str, message: &str) -> Result<u64, ServiceError> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad_request(message.to_owned()));
    }
    raw.parse::<u64>()
        .map_err(|_| bad_request(message.to_owned()))
}

fn respond(status: u16, next: Option<&OffsetToken>, closed: bool) -> Response {
    let mut response = base_response(status);
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    if let Some(next) = next {
        set_header(&mut response, H_STREAM_NEXT_OFFSET, &next.value());
    }
    if closed {
        set_header(&mut response, H_STREAM_CLOSED, "true");
    }
    response
}

pub(crate) fn fail(status: u16, message: &str) -> Response {
    let mut response = base_response(status);
    set_header(&mut response, header::CONTENT_TYPE.as_str(), CT_TEXT);
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    let message = if message.is_empty() { "error" } else { message };
    *response.body_mut() = Body::from(message.to_owned());
    response
}

fn json_array_body(out: &ReadResult) -> Bytes {
    let mut body = Vec::with_capacity(
        2 + out
            .records
            .iter()
            .map(|r| r.payload.len() + 1)
            .sum::<usize>(),
    );
    body.push(b'[');
    for (i, record) in out.records.iter().enumerate() {
        if i > 0 {
            body.push(b',');
        }
        body.extend_from_slice(&record.payload);
    }
    body.push(b']');
    Bytes::from(body)
}

struct SseEncoder {
    json: bool,
    base64: bool,
}

impl SseEncoder {
    fn new(content_type: &str) -> Self {
        let mime = mime_of(Some(content_type));
        let json = is_json(&mime);
        Self {
            json,
            base64: !json && !mime.starts_with("text/"),
        }
    }

    fn data_event(&self, read: &ReadResult) -> Bytes {
        let mut out = String::from("event: data\n");
        if self.base64 {
            out.push_str("data:");
            out.push_str(&base64::engine::general_purpose::STANDARD.encode(read.concatenated()));
            out.push('\n');
        } else {
            let text = if self.json {
                String::from_utf8_lossy(&json_array_body(read)).into_owned()
            } else {
                String::from_utf8_lossy(&read.concatenated()).into_owned()
            };
            for line in crate::http::sse_lines(&text) {
                out.push_str("data:");
                out.push_str(line);
                out.push('\n');
            }
        }
        out.push('\n');
        Bytes::from(out)
    }

    fn control_event(
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
