//! The Pico protocol: PicoMQ's native HTTP API. All custom headers use the
//! `Pico-*` prefix.
//!
//! - `PUT /name`          create (idempotent, 201 created / 200 exists / 409 CT clash)
//! - `POST /name`         append (single body, JSON batch, or binary batch) or
//!   trim when `Pico-Trim-Seq` is set

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::Response;
use axum::routing::any;
use bytes::Bytes;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;

use picomq_auth::{Audience, Authorizer};
use picomq_protocol::mime::{mime_equals, mime_of};
use picomq_protocol::pico::{
    CT_BATCH_BINARY, CT_BATCH_JSON, CT_EVENT_STREAM, CT_JSON, DEFAULT_CT, E_BAD_REQUEST, E_CLOSED,
    E_CONFLICT, E_DURABILITY, E_FENCED, E_MATCH_FAILED, E_NOT_FOUND, E_SCHEMA_VIOLATION,
    E_SEQUENCE_GAP, ErrorBody, FORMAT_BINARY, FORMAT_JSON, FORMAT_RAW, H_CLOSED, H_CURSOR,
    H_EXPECTED_SEQ, H_EXPIRES_AT, H_KAFKA_TOPIC, H_KEY, H_MATCH_SEQ, H_NEXT_SEQ, H_PRODUCER_EPOCH,
    H_PRODUCER_ID, H_PRODUCER_SEQ, H_RECEIVED_SEQ, H_SCHEMA, H_SCHEMA_VALIDATE, H_START_SEQ,
    H_TIMESTAMP, H_TRIM_SEQ, H_TTL, H_UP_TO_DATE, LIVE_LONG_POLL, LIVE_SSE, Listing, Q_BYTES,
    Q_COUNT, Q_CURSOR, Q_FORMAT, Q_LIMIT, Q_LIVE, Q_PREFIX, Q_SEQ, Q_START_AFTER, SEQ_NOW,
    StreamEntry, sse_control_event, sse_data_event,
};
use picomq_protocol::record::{
    PicoRecord, SequencedRecord, decode_batch_append, decode_json_append, encode_batch_read,
    encode_json_read,
};
use picomq_server::ownership::OwnershipService;
use picomq_server::{
    AppendCommand, CreateCommand, ErrorKind, LogRecord, OffsetToken, ReadResult, S3StreamService,
    ServiceError, StreamMeta, StreamRecord,
};

use crate::auth::Permit;
use crate::http::{
    bad_request, base_response, codec_error, cursor, etag, format_instant, header_str,
    parse_instant_header, parse_strict_u64, parse_strict_u64_header, query_param, set_header,
    truthy,
};
use crate::route::{RoutingMode, route, stream_name};

const CACHE_CATCH_UP: &str = "public, max-age=60, stale-while-revalidate=300";

const MAX_READ_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_CHUNK_SIZE: usize = 64 * 1024;
const DEFAULT_MAX_REQUEST_SIZE: usize = 32 * 1024 * 1024;

/// The Pico frontend over one node's service + ownership pair.
///
/// Defaults: 25s long poll, 55s SSE cap, 64 KiB chunks, 32 MiB request bodies.
pub struct PicoFrontend {
    service: Arc<S3StreamService>,
    ownership: Arc<dyn OwnershipService>,
    authorizer: Option<Arc<Authorizer>>,
    mode: RoutingMode,
    long_poll_timeout: Duration,
    sse_max_duration: Duration,
    max_chunk_size: usize,
    max_request_size: usize,
}

impl PicoFrontend {
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

    /// `None` disables the gate, requests pass through unauthenticated.
    pub fn with_authorizer(mut self, authorizer: Option<Arc<Authorizer>>) -> Self {
        self.authorizer = authorizer;
        self
    }

    /// (`app.addHttpHandler(..., router::handle)`).
    pub fn router(self: Arc<Self>) -> Router {
        let limit = self.max_request_size;
        Router::new()
            .fallback(any(dispatch))
            .layer(axum::extract::DefaultBodyLimit::max(limit))
            .with_state(self)
    }
}

async fn dispatch(
    State(frontend): State<Arc<PicoFrontend>>,
    request: axum::extract::Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let permit = match crate::auth::gate(
        frontend.authorizer.as_deref(),
        Audience::Pico,
        &parts.method,
        &parts.uri,
        &parts.headers,
    )
    .await
    {
        Ok(permit) => permit,
        Err(response) => return *response,
    };
    let name = match &permit {
        Some(permit) => permit.stream_name.clone(),
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
        Err(_) => return error(413, "internal", "content too large", None),
    };
    frontend
        .handle(parts.method, parts.uri, parts.headers, body, name, permit)
        .await
}

impl PicoFrontend {
    async fn handle(
        &self,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
        name: String,
        permit: Option<Permit>,
    ) -> Response {
        let result = match method {
            Method::OPTIONS => Ok(options()),
            Method::PUT => self.put(&uri, &headers, &body, &name).await,
            Method::POST => self.post(&uri, &headers, &body, &name).await,
            Method::DELETE => self.delete(&name).await,
            Method::HEAD => self.head(&name).await,
            Method::GET => self.get(&uri, &headers, &name, permit.as_ref()).await,
            _ => Ok(error(405, "method_not_allowed", "method not allowed", None)),
        };
        result.unwrap_or_else(service_error_response)
    }

    async fn put(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &Bytes,
        name: &str,
    ) -> Result<Response, ServiceError> {
        if stream_name(uri) == "/" {
            return Ok(error(
                400,
                E_BAD_REQUEST,
                "cannot create the root stream",
                None,
            ));
        }
        if !body.is_empty() {
            return Ok(error(
                400,
                E_BAD_REQUEST,
                "create takes no body, append with POST",
                None,
            ));
        }

        let ttl_seconds = parse_strict_u64_header(headers, H_TTL, "invalid Pico-TTL")?;
        let expires_at_ms = parse_instant_header(headers, H_EXPIRES_AT, "invalid Pico-Expires-At")?;
        if ttl_seconds.is_some() && expires_at_ms.is_some() {
            return Err(bad_request("Pico-TTL and Pico-Expires-At both set"));
        }

        let content_type = header_str(headers, header::CONTENT_TYPE.as_str())
            .filter(|v| !v.is_empty())
            .unwrap_or(DEFAULT_CT)
            .to_owned();
        let result = self
            .service
            .create(CreateCommand {
                name: name.to_owned(),
                content_type: content_type.clone(),
                ttl_seconds,
                expires_at_ms,
                closed: truthy(headers, H_CLOSED),
                initial_records: Vec::new(),
                external_id: None,
                internal: false,
                schema_name: header_str(headers, H_SCHEMA)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
                schema_validate: truthy(headers, H_SCHEMA_VALIDATE),
                kafka_topic: header_str(headers, H_KAFKA_TOPIC)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
            })
            .await?;
        let meta = result.meta;

        if !result.created && !mime_equals(Some(&meta.content_type), Some(&content_type)) {
            return Ok(error(
                409,
                E_CONFLICT,
                &format!("stream exists with content type {}", meta.content_type),
                Some(&meta.next_offset),
            ));
        }

        let mut response = respond(
            if result.created { 201 } else { 200 },
            Some(&meta.next_offset),
            meta.closed,
        );
        write_meta(&mut response, &meta);
        if result.created {
            set_header(&mut response, header::LOCATION.as_str(), uri.path());
        }
        Ok(response)
    }

    async fn head(&self, name: &str) -> Result<Response, ServiceError> {
        let Some(meta) = self.service.head(name).await? else {
            return Ok(respond(404, None, false));
        };
        let mut response = respond(200, Some(&meta.next_offset), meta.closed);
        write_meta(&mut response, &meta);
        Ok(response)
    }

    async fn post(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        body: &Bytes,
        name: &str,
    ) -> Result<Response, ServiceError> {
        if stream_name(uri) == "/" {
            return Ok(error(400, E_BAD_REQUEST, "no stream in path", None));
        }
        if let Some(trim_seq) =
            parse_strict_u64_header(headers, H_TRIM_SEQ, "invalid Pico-Trim-Seq")?
        {
            if !body.is_empty() {
                return Err(bad_request("trim takes no body"));
            }
            let start = self.service.trim(name, trim_seq).await?;
            let mut response = respond(200, None, false);
            set_header(&mut response, H_START_SEQ, &start.to_string());
            return Ok(response);
        }
        self.append(name, headers, body).await
    }

    async fn append(
        &self,
        name: &str,
        headers: &HeaderMap,
        body: &Bytes,
    ) -> Result<Response, ServiceError> {
        let close = truthy(headers, H_CLOSED);
        let match_seq = parse_strict_u64_header(headers, H_MATCH_SEQ, "invalid Pico-Match-Seq")?;
        let producer = producer_of(headers)?;
        if body.is_empty() && !close {
            return Err(bad_request("empty body"));
        }

        let records = if body.is_empty() {
            Vec::new()
        } else {
            decode_records(headers, body)?
        };
        let count = records.len() as u64;

        let result = self
            .service
            .append(AppendCommand {
                name: name.to_owned(),
                records,
                content_type: None,
                stream_seq: None,
                match_seq,
                producer,
                close_after: close,
            })
            .await?;

        let mut response = respond(200, Some(&result.next_offset), result.closed);
        if let (true, Some(timestamp)) = (result.applied && count > 0, result.timestamp_ms) {
            let start = result.next_offset.record_offset() - count;
            set_header(&mut response, H_START_SEQ, &start.to_string());
            set_header(&mut response, H_TIMESTAMP, &timestamp.to_string());
        }
        if let Some(epoch) = result.producer_epoch {
            set_header(&mut response, H_PRODUCER_EPOCH, &epoch.to_string());
        }
        if let Some(seq) = result.producer_seq {
            set_header(&mut response, H_PRODUCER_SEQ, &seq.to_string());
        }
        Ok(response)
    }

    async fn delete(&self, name: &str) -> Result<Response, ServiceError> {
        let deleted = self.service.delete(name).await?;
        Ok(respond(if deleted { 204 } else { 404 }, None, false))
    }

    async fn get(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        name: &str,
        permit: Option<&Permit>,
    ) -> Result<Response, ServiceError> {
        if stream_name(uri) == "/" {
            return self.list(uri, permit).await;
        }
        let seq = self.parse_seq(uri, headers, name).await?;
        match query_param(uri, Q_LIVE) {
            None => {
                let out = self.read(name, seq, uri).await?;
                self.write_read(uri, headers, name, seq, out, false)
            }
            Some(mode) if mode == LIVE_LONG_POLL => self.long_poll(uri, headers, name, seq).await,
            Some(mode) if mode == LIVE_SSE => self.sse(name, seq).await,
            Some(mode) => Err(bad_request(format!("invalid live mode: {mode}"))),
        }
    }

    async fn list(&self, uri: &Uri, permit: Option<&Permit>) -> Result<Response, ServiceError> {
        let prefix = match permit {
            Some(permit) => permit.stream_name.clone(),
            None => query_param(uri, Q_PREFIX)
                .filter(|p| !p.is_empty())
                .unwrap_or_else(|| "/".into()),
        };
        let start_after = match (permit, query_param(uri, Q_START_AFTER)) {
            (Some(permit), Some(after)) => Some(
                permit
                    .principal
                    .scope
                    .resolve_stream_name(&after)
                    .map_err(|_| bad_request("invalid start_after"))?,
            ),
            (_, after) => after,
        };
        let limit = parse_strict_u64(query_param(uri, Q_LIMIT).as_deref(), "invalid limit")?;
        let result = self
            .service
            .list(&prefix, start_after.as_deref(), limit.unwrap_or(0) as usize)
            .await?;

        let listing = Listing {
            streams: result
                .streams
                .iter()
                .map(|meta| {
                    let name = match permit {
                        Some(permit) => permit.principal.scope.strip_stream_name(&meta.name),
                        None => &meta.name,
                    };
                    StreamEntry {
                        name: name.to_owned(),
                        content_type: Some(meta.content_type.clone()),
                        start_seq: meta.start_offset.record_offset(),
                        next_seq: meta.next_offset.record_offset(),
                        closed: meta.closed,
                        ttl_seconds: meta.ttl_seconds,
                        expires_at: meta.expires_at_ms.map(format_instant),
                    }
                })
                .collect(),
            has_more: result.has_more,
        };

        let mut response = base_response(200);
        set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
        set_header(&mut response, header::CONTENT_TYPE.as_str(), CT_JSON);
        *response.body_mut() = Body::from(listing.encode());
        Ok(response)
    }

    async fn read(
        &self,
        name: &str,
        seq: OffsetToken,
        uri: &Uri,
    ) -> Result<ReadResult, ServiceError> {
        let count = parse_strict_u64(query_param(uri, Q_COUNT).as_deref(), "invalid count")?;
        let bytes = parse_strict_u64(query_param(uri, Q_BYTES).as_deref(), "invalid bytes")?;
        let cap = self.max_chunk_size.max(MAX_READ_BYTES);
        let max_bytes = match bytes {
            None => self.max_chunk_size,
            Some(bytes) => (bytes as usize).min(cap),
        };
        self.service
            .read(name, seq, max_bytes, count.unwrap_or(0) as usize)
            .await
    }

    async fn long_poll(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        name: &str,
        seq: OffsetToken,
    ) -> Result<Response, ServiceError> {
        let cursor = cursor(query_param(uri, Q_CURSOR).as_deref());
        let out = self.read(name, seq, uri).await?;
        if !(out.records.is_empty() && out.up_to_date) || out.closed {
            let mut response = self.write_read(uri, headers, name, seq, out, true)?;
            set_header(&mut response, H_CURSOR, &cursor.to_string());
            return Ok(response);
        }

        if self
            .service
            .wait_appended(name, seq, self.long_poll_timeout)
            .await?
        {
            let out = self.read(name, seq, uri).await?;
            let mut response = self.write_read(uri, headers, name, seq, out, true)?;
            set_header(&mut response, H_CURSOR, &cursor.to_string());
            return Ok(response);
        }

        let Some(meta) = self.service.head(name).await? else {
            return Ok(respond(404, None, false));
        };
        let mut response = respond(204, Some(&meta.next_offset), meta.closed);
        set_header(&mut response, H_CURSOR, &cursor.to_string());
        set_header(&mut response, H_UP_TO_DATE, "true");
        Ok(response)
    }

    async fn sse(&self, name: &str, start: OffsetToken) -> Result<Response, ServiceError> {
        if self.service.head(name).await?.is_none() {
            return Ok(respond(404, None, false));
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(8);
        let service = self.service.clone();
        let name = name.to_owned();
        let max_chunk_size = self.max_chunk_size;
        let deadline = tokio::time::Instant::now() + self.sse_max_duration;
        tokio::spawn(async move {
            let mut seq = start;
            let mut announced_caught_up = false;
            while tokio::time::Instant::now() < deadline {
                let Ok(read) = service.read(&name, seq, max_chunk_size, 0).await else {
                    return;
                };

                if !read.records.is_empty() {
                    seq = read.next_offset;
                    let closed_at_tail = read.closed && read.up_to_date;
                    let records = to_sequenced(&read.records);
                    if tx
                        .send(sse_data_event(&records, seq.record_offset()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let control =
                        sse_control_event(seq.record_offset(), read.up_to_date, closed_at_tail);
                    if tx.send(control).await.is_err() || closed_at_tail {
                        return;
                    }
                    announced_caught_up = read.up_to_date;
                    continue;
                }

                if read.closed {
                    let _ = tx
                        .send(sse_control_event(
                            read.next_offset.record_offset(),
                            true,
                            true,
                        ))
                        .await;
                    return;
                }

                if !announced_caught_up {
                    let control = sse_control_event(read.next_offset.record_offset(), true, false);
                    if tx.send(control).await.is_err() {
                        return;
                    }
                    announced_caught_up = true;
                }

                if service
                    .wait_appended(&name, seq, Duration::from_secs(1))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        let mut response = base_response(200);
        set_header(
            &mut response,
            header::CONTENT_TYPE.as_str(),
            CT_EVENT_STREAM,
        );
        set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-cache");
        *response.body_mut() =
            Body::from_stream(ReceiverStream::new(rx).map(Ok::<_, std::convert::Infallible>));
        Ok(response)
    }

    fn write_read(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        name: &str,
        start: OffsetToken,
        out: ReadResult,
        live: bool,
    ) -> Result<Response, ServiceError> {
        let format = query_param(uri, Q_FORMAT)
            .filter(|f| !f.is_empty())
            .unwrap_or_else(|| FORMAT_JSON.into());
        let empty_tail = out.records.is_empty() && out.up_to_date;

        let mut response = base_response(if empty_tail && live { 204 } else { 200 });
        set_header(
            &mut response,
            header::CACHE_CONTROL.as_str(),
            if live { "no-store" } else { CACHE_CATCH_UP },
        );
        set_header(
            &mut response,
            H_NEXT_SEQ,
            &out.next_offset.record_offset().to_string(),
        );
        if out.up_to_date {
            set_header(&mut response, H_UP_TO_DATE, "true");
        }
        if out.closed && out.up_to_date {
            set_header(&mut response, H_CLOSED, "true");
        }

        if !live {
            let etag = etag(
                &format!("{name}:{format}"),
                &start,
                &out.next_offset,
                out.closed && empty_tail,
            );
            set_header(&mut response, header::ETAG.as_str(), &etag);
            if header_str(headers, header::IF_NONE_MATCH.as_str()) == Some(etag.as_str()) {
                *response.status_mut() = StatusCode::NOT_MODIFIED;
                return Ok(response);
            }
        }

        if empty_tail && live {
            return Ok(response);
        }

        let records = to_sequenced(&out.records);
        match format.as_str() {
            FORMAT_JSON => {
                set_header(&mut response, header::CONTENT_TYPE.as_str(), CT_JSON);
                *response.body_mut() = Body::from(encode_json_read(&records));
            }
            FORMAT_BINARY => {
                set_header(
                    &mut response,
                    header::CONTENT_TYPE.as_str(),
                    CT_BATCH_BINARY,
                );
                *response.body_mut() = Body::from(encode_batch_read(&records));
            }
            FORMAT_RAW => {
                set_header(
                    &mut response,
                    header::CONTENT_TYPE.as_str(),
                    &out.content_type,
                );
                *response.body_mut() = Body::from(out.concatenated_values());
            }
            other => return Err(bad_request(format!("invalid format: {other}"))),
        }
        Ok(response)
    }

    async fn parse_seq(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        name: &str,
    ) -> Result<OffsetToken, ServiceError> {
        let Some(raw) = query_param(uri, Q_SEQ) else {
            if let Some(last_event_id) =
                header_str(headers, "last-event-id").filter(|v| !v.is_empty())
            {
                return OffsetToken::parse(Some(last_event_id));
            }
            return Ok(OffsetToken::beginning());
        };
        if raw.eq_ignore_ascii_case(SEQ_NOW) {
            return self
                .service
                .head(name)
                .await?
                .map(|meta| meta.next_offset)
                .ok_or_else(|| ServiceError::kind(ErrorKind::NotFound));
        }
        OffsetToken::parse(Some(&raw))
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
        "content-type, authorization, If-None-Match, Pico-Match-Seq, Pico-Trim-Seq, Pico-TTL, \
         Pico-Expires-At, Pico-Closed, Pico-Producer-Id, Pico-Producer-Epoch, Pico-Producer-Seq",
    );
    set_header(
        &mut response,
        "Access-Control-Expose-Headers",
        "Pico-Start-Seq, Pico-Next-Seq, Pico-Timestamp, Pico-Cursor, Pico-Up-To-Date, Pico-Closed, \
         Pico-Producer-Epoch, Pico-Producer-Seq, Pico-Expected-Seq, Pico-Received-Seq, etag, \
         content-type",
    );
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    response
}

fn service_error_response(e: ServiceError) -> Response {
    match e.kind {
        ErrorKind::NotFound => error(404, E_NOT_FOUND, "no such stream", None),
        ErrorKind::BadRequest | ErrorKind::CorruptBatch => {
            error(400, E_BAD_REQUEST, &e.message, None)
        }
        ErrorKind::SchemaViolation => error(400, E_SCHEMA_VIOLATION, &e.message, None),
        ErrorKind::Fenced => {
            let mut response = error(403, E_FENCED, &e.message, None);
            if let Some(epoch) = e.producer_epoch {
                set_header(&mut response, H_PRODUCER_EPOCH, &epoch.to_string());
            }
            response
        }
        ErrorKind::SequenceGap => {
            let mut response = error(409, E_SEQUENCE_GAP, &e.message, None);
            if let Some(expected) = e.expected_seq {
                set_header(&mut response, H_EXPECTED_SEQ, &expected.to_string());
            }
            if let Some(received) = e.received_seq {
                set_header(&mut response, H_RECEIVED_SEQ, &received.to_string());
            }
            response
        }
        ErrorKind::MatchFailed => error(412, E_MATCH_FAILED, &e.message, e.next_offset.as_ref()),
        ErrorKind::Conflict => error(409, E_CONFLICT, &e.message, e.next_offset.as_ref()),
        ErrorKind::Closed => {
            let mut response = error(409, E_CLOSED, "stream is closed", e.next_offset.as_ref());
            set_header(&mut response, H_CLOSED, "true");
            response
        }
        ErrorKind::Durability => error(500, E_DURABILITY, &e.message, None),
    }
}

/// A batch body carries full records; any other body is one record whose
/// value is the body (optionally keyed via `Pico-Key`).
fn decode_records(headers: &HeaderMap, body: &Bytes) -> Result<Vec<LogRecord>, ServiceError> {
    let mime = mime_of(header_str(headers, header::CONTENT_TYPE.as_str()));
    let records = if mime == CT_BATCH_JSON {
        decode_json_append(body).map_err(codec_error)?
    } else if mime == CT_BATCH_BINARY {
        decode_batch_append(body).map_err(codec_error)?
    } else {
        let mut record = PicoRecord::new(body.clone());
        if let Some(key) = header_str(headers, H_KEY) {
            record.key = Some(Bytes::copy_from_slice(key.as_bytes()));
        }
        vec![record]
    };
    Ok(records.into_iter().map(to_log_record).collect())
}

fn to_log_record(record: PicoRecord) -> LogRecord {
    LogRecord {
        timestamp_ms: 0,
        key: record.key,
        value: record.body,
        headers: record.headers,
    }
}

fn producer_of(
    headers: &HeaderMap,
) -> Result<Option<picomq_server::types::Producer>, ServiceError> {
    let id = header_str(headers, H_PRODUCER_ID);
    let epoch = header_str(headers, H_PRODUCER_EPOCH);
    let seq = header_str(headers, H_PRODUCER_SEQ);
    if id.is_none() && epoch.is_none() && seq.is_none() {
        return Ok(None);
    }
    let (Some(id), Some(epoch), Some(seq)) = (id.filter(|v| !v.is_empty()), epoch, seq) else {
        return Err(bad_request(
            "all producer headers (Pico-Producer-Id, Pico-Producer-Epoch, Pico-Producer-Seq) \
             must be provided together",
        ));
    };
    let epoch = parse_strict_u64(Some(epoch), "invalid Pico-Producer-Epoch")?
        .ok_or_else(|| bad_request("invalid Pico-Producer-Epoch"))?;
    let seq = parse_strict_u64(Some(seq), "invalid Pico-Producer-Seq")?
        .ok_or_else(|| bad_request("invalid Pico-Producer-Seq"))?;
    Ok(Some(picomq_server::types::Producer::new(id, epoch, seq)?))
}

fn write_meta(response: &mut Response, meta: &StreamMeta) {
    set_header(response, header::CONTENT_TYPE.as_str(), &meta.content_type);
    set_header(
        response,
        H_START_SEQ,
        &meta.start_offset.record_offset().to_string(),
    );
    if let Some(ttl) = meta.ttl_seconds {
        set_header(response, H_TTL, &ttl.to_string());
    }
    if let Some(expires_at_ms) = meta.expires_at_ms {
        set_header(response, H_EXPIRES_AT, &format_instant(expires_at_ms));
    }
    if let Some(schema_name) = &meta.schema_name {
        set_header(response, H_SCHEMA, schema_name);
    }
    if let Some(topic) = &meta.kafka_topic {
        set_header(response, H_KAFKA_TOPIC, topic);
    }
}

fn to_sequenced(records: &[StreamRecord]) -> Vec<SequencedRecord> {
    records
        .iter()
        .map(|record| SequencedRecord {
            seq: record.offset.record_offset(),
            record: PicoRecord {
                timestamp: record.record.timestamp_ms,
                key: record.record.key.clone(),
                headers: record.record.headers.clone(),
                body: record.record.value.clone(),
            },
        })
        .collect()
}

fn respond(status: u16, next: Option<&OffsetToken>, closed: bool) -> Response {
    let mut response = base_response(status);
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    if let Some(next) = next {
        set_header(&mut response, H_NEXT_SEQ, &next.record_offset().to_string());
    }
    if closed {
        set_header(&mut response, H_CLOSED, "true");
    }
    response
}

pub(crate) fn error(
    status: u16,
    code: &str,
    message: &str,
    next: Option<&OffsetToken>,
) -> Response {
    let mut response = base_response(status);
    set_header(&mut response, header::CONTENT_TYPE.as_str(), CT_JSON);
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    let next_seq = next.map(|next| next.record_offset());
    if let Some(next_seq) = next_seq {
        set_header(&mut response, H_NEXT_SEQ, &next_seq.to_string());
    }
    let body = ErrorBody {
        code: code.to_owned(),
        message: Some(message.to_owned()).filter(|m| !m.is_empty()),
        next_seq,
    };
    *response.body_mut() = Body::from(body.encode());
    response
}
