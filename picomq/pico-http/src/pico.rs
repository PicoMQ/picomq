//! The Pico protocol: PicoMQ's native HTTP API. All custom headers use the
//! `Pico-*` prefix.
//!
//! - `PUT /name`          create (idempotent, 201 created / 200 exists / 409 CT clash)
//! - `POST /name`         append (single body, JSON batch, or binary batch) or
//!   trim when `Pico-Trim-Seq` is set

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use serde_json::{json, Map, Value};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;

use pico_auth::{Audience, Authorizer};
use pico_protocol::envelope::{
    decode_batch_append, decode_envelope, decode_json_append, encode_batch_read, encode_envelope,
    encode_json_read, RecordEnvelope, SequencedRecord,
};
use pico_protocol::pico::{
    CT_BATCH_BINARY, CT_BATCH_JSON, CT_CORE, CT_CORE_PARAM, CT_EVENT_STREAM, CT_JSON, DEFAULT_CT,
    H_CLOSED, H_CURSOR, H_EXPECTED_SEQ, H_EXPIRES_AT, H_MATCH_SEQ, H_NEXT_SEQ, H_PRODUCER_EPOCH,
    H_PRODUCER_ID, H_PRODUCER_SEQ, H_RECEIVED_SEQ, H_START_SEQ, H_TIMESTAMP, H_TRIM_SEQ, H_TTL,
    H_UP_TO_DATE,
};
use pico_server::framing::{mime_equals, mime_of};
use pico_server::ownership::OwnershipService;
use pico_server::{
    AppendCommand, CreateCommand, ErrorKind, OffsetToken, ReadResult, S3StreamService,
    ServiceError, StreamMeta,
};

use crate::auth::Permit;
use crate::http::{
    bad_request, base_response, codec_error, cursor, etag, format_instant, header_str,
    parse_instant_header, parse_strict_u64, parse_strict_u64_header, query_param, set_header,
    truthy,
};
use crate::route::{route, stream_name, RoutingMode};
use crate::timestamps::StreamTimestamps;

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
    timestamps: StreamTimestamps,
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
        let timestamps = StreamTimestamps::new(service.clone());
        Self {
            service,
            ownership,
            timestamps,
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
                "bad_request",
                "cannot create the root stream",
                None,
            ));
        }
        if !body.is_empty() {
            return Ok(error(
                400,
                "bad_request",
                "create takes no body, append with POST",
                None,
            ));
        }

        let ttl_seconds = parse_strict_u64_header(headers, H_TTL, "invalid Pico-TTL")?;
        let expires_at_ms = parse_instant_header(headers, H_EXPIRES_AT, "invalid Pico-Expires-At")?;
        if ttl_seconds.is_some() && expires_at_ms.is_some() {
            return Err(bad_request("Pico-TTL and Pico-Expires-At both set"));
        }

        let user_ct = header_str(headers, header::CONTENT_TYPE.as_str())
            .filter(|v| !v.is_empty())
            .unwrap_or(DEFAULT_CT)
            .to_owned();
        let result = self
            .service
            .create(CreateCommand {
                name: name.to_owned(),
                content_type: engine_ct(&user_ct),
                ttl_seconds,
                expires_at_ms,
                closed: truthy(headers, H_CLOSED),
                initial_payload: Bytes::new(),
                external_id: None,
                internal: false,
            })
            .await?;
        let meta = result.meta;

        if !result.created && !mime_equals(Some(&user_ct_of(&meta.content_type)), Some(&user_ct)) {
            return Ok(error(
                409,
                "conflict",
                &format!(
                    "stream exists with content type {}",
                    user_ct_of(&meta.content_type)
                ),
                Some(&meta.next_offset),
            ));
        }

        let mut response = respond(
            if result.created { 201 } else { 200 },
            Some(&meta.next_offset),
            meta.closed,
        );
        set_header(
            &mut response,
            header::CONTENT_TYPE.as_str(),
            &user_ct_of(&meta.content_type),
        );
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
            return Ok(error(400, "bad_request", "no stream in path", None));
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
        let timestamp = if records.is_empty() {
            None
        } else {
            Some(self.timestamps.next(name).await?)
        };
        let payloads: Vec<Bytes> = records
            .iter()
            .map(|record| {
                encode_envelope(&RecordEnvelope::new(
                    timestamp.expect("timestamp set when records exist"),
                    record.headers.clone(),
                    record.body.clone(),
                ))
            })
            .collect();

        let result = self
            .service
            .append(AppendCommand {
                name: name.to_owned(),
                payloads,
                content_type: Some(CT_CORE.to_owned()),
                stream_seq: None,
                match_seq,
                producer,
                close_after: close,
                atomic: true,
            })
            .await?;

        if result.applied {
            if let Some(timestamp) = timestamp {
                self.timestamps.record(name, timestamp);
            }
        }

        let mut response = respond(200, Some(&result.next_offset), result.closed);
        if result.applied && !records.is_empty() {
            let start = result.next_offset.record_offset() - records.len() as u64;
            set_header(&mut response, H_START_SEQ, &start.to_string());
            set_header(
                &mut response,
                H_TIMESTAMP,
                &timestamp
                    .expect("timestamp set when records exist")
                    .to_string(),
            );
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
        if deleted {
            self.timestamps.invalidate(name);
        }
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
        match query_param(uri, "live") {
            None => {
                let out = self.read(name, seq, uri).await?;
                self.write_read(uri, headers, name, seq, out, false)
            }
            Some(mode) if mode == "long-poll" => self.long_poll(uri, headers, name, seq).await,
            Some(mode) if mode == "sse" => self.sse(name, seq).await,
            Some(mode) => Err(bad_request(format!("invalid live mode: {mode}"))),
        }
    }

    async fn list(&self, uri: &Uri, permit: Option<&Permit>) -> Result<Response, ServiceError> {
        let prefix = match permit {
            Some(permit) => permit.stream_name.clone(),
            None => query_param(uri, "prefix")
                .filter(|p| !p.is_empty())
                .unwrap_or_else(|| "/".into()),
        };
        let start_after = match (permit, query_param(uri, "start_after")) {
            (Some(permit), Some(after)) => Some(
                permit
                    .principal
                    .scope
                    .resolve_stream_name(&after)
                    .map_err(|_| bad_request("invalid start_after"))?,
            ),
            (_, after) => after,
        };
        let limit = parse_strict_u64(query_param(uri, "limit").as_deref(), "invalid limit")?;
        let result = self
            .service
            .list(&prefix, start_after.as_deref(), limit.unwrap_or(0) as usize)
            .await?;

        let streams: Vec<Value> = result
            .streams
            .iter()
            .map(|meta| {
                let name = match permit {
                    Some(permit) => permit.principal.scope.strip_stream_name(&meta.name),
                    None => &meta.name,
                };
                let mut node = Map::new();
                node.insert("name".into(), json!(name));
                node.insert("content_type".into(), json!(user_ct_of(&meta.content_type)));
                node.insert("start_seq".into(), json!(meta.start_offset.record_offset()));
                node.insert("next_seq".into(), json!(meta.next_offset.record_offset()));
                node.insert("closed".into(), json!(meta.closed));
                if let Some(ttl) = meta.ttl_seconds {
                    node.insert("ttl".into(), json!(ttl));
                }
                if let Some(expires_at_ms) = meta.expires_at_ms {
                    node.insert("expires_at".into(), json!(format_instant(expires_at_ms)));
                }
                Value::Object(node)
            })
            .collect();
        let body = json!({ "streams": streams, "has_more": result.has_more });

        let mut response = base_response(200);
        set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
        set_header(&mut response, header::CONTENT_TYPE.as_str(), CT_JSON);
        *response.body_mut() = Body::from(serde_json::to_vec(&body).expect("json encode"));
        Ok(response)
    }

    async fn read(
        &self,
        name: &str,
        seq: OffsetToken,
        uri: &Uri,
    ) -> Result<ReadResult, ServiceError> {
        let count = parse_strict_u64(query_param(uri, "count").as_deref(), "invalid count")?;
        let bytes = parse_strict_u64(query_param(uri, "bytes").as_deref(), "invalid bytes")?;
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
        let cursor = cursor(query_param(uri, "cursor").as_deref());
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
                    let records = match to_sequenced(&read.records) {
                        Ok(records) => records,
                        Err(_) => return,
                    };
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
        let format = query_param(uri, "format")
            .filter(|f| !f.is_empty())
            .unwrap_or_else(|| "json".into());
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

        let records = to_sequenced(&out.records)?;
        match format.as_str() {
            "json" => {
                set_header(&mut response, header::CONTENT_TYPE.as_str(), CT_JSON);
                *response.body_mut() = Body::from(encode_json_read(&records));
            }
            "binary" => {
                set_header(
                    &mut response,
                    header::CONTENT_TYPE.as_str(),
                    CT_BATCH_BINARY,
                );
                *response.body_mut() = Body::from(encode_batch_read(&records));
            }
            "raw" => {
                set_header(
                    &mut response,
                    header::CONTENT_TYPE.as_str(),
                    &user_ct_of(&out.content_type),
                );
                let mut body = Vec::new();
                for record in &records {
                    body.extend_from_slice(&record.envelope.body);
                }
                *response.body_mut() = Body::from(body);
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
        let Some(raw) = query_param(uri, "seq") else {
            if let Some(last_event_id) =
                header_str(headers, "last-event-id").filter(|v| !v.is_empty())
            {
                return OffsetToken::parse(Some(last_event_id));
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
        OffsetToken::parse(Some(&raw))
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

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
        ErrorKind::NotFound => error(404, "not_found", "no such stream", None),
        ErrorKind::BadRequest => error(400, "bad_request", &e.message, None),
        ErrorKind::Fenced => {
            let mut response = error(403, "fenced", &e.message, None);
            if let Some(epoch) = e.producer_epoch {
                set_header(&mut response, H_PRODUCER_EPOCH, &epoch.to_string());
            }
            response
        }
        ErrorKind::SequenceGap => {
            let mut response = error(409, "sequence_gap", &e.message, None);
            if let Some(expected) = e.expected_seq {
                set_header(&mut response, H_EXPECTED_SEQ, &expected.to_string());
            }
            if let Some(received) = e.received_seq {
                set_header(&mut response, H_RECEIVED_SEQ, &received.to_string());
            }
            response
        }
        ErrorKind::MatchFailed => error(412, "match_failed", &e.message, e.next_offset.as_ref()),
        ErrorKind::Conflict => error(409, "conflict", &e.message, e.next_offset.as_ref()),
        ErrorKind::Closed => {
            let mut response = error(409, "closed", "stream is closed", e.next_offset.as_ref());
            set_header(&mut response, H_CLOSED, "true");
            response
        }
        ErrorKind::Durability => error(500, "durability", &e.message, None),
    }
}

fn decode_records(headers: &HeaderMap, body: &Bytes) -> Result<Vec<RecordEnvelope>, ServiceError> {
    let mime = mime_of(header_str(headers, header::CONTENT_TYPE.as_str()));
    if mime == CT_BATCH_JSON {
        return decode_json_append(body).map_err(codec_error);
    }
    if mime == CT_BATCH_BINARY {
        return decode_batch_append(body).map_err(codec_error);
    }
    Ok(vec![RecordEnvelope::new(
        0,
        Default::default(),
        body.clone(),
    )])
}

fn producer_of(headers: &HeaderMap) -> Result<Option<pico_server::types::Producer>, ServiceError> {
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
    Ok(Some(pico_server::types::Producer::new(id, epoch, seq)?))
}

pub fn engine_ct(user_ct: &str) -> String {
    format!("{CT_CORE}; {CT_CORE_PARAM}={user_ct}")
}

pub fn user_ct_of(engine_ct: &str) -> String {
    let Some((_, params)) = engine_ct.split_once(';') else {
        return DEFAULT_CT.to_owned();
    };
    let params = params.trim();
    match params.strip_prefix(&format!("{CT_CORE_PARAM}=")) {
        Some(user) => user.to_owned(),
        None => DEFAULT_CT.to_owned(),
    }
}

fn write_meta(response: &mut Response, meta: &StreamMeta) {
    set_header(
        response,
        header::CONTENT_TYPE.as_str(),
        &user_ct_of(&meta.content_type),
    );
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
}

fn to_sequenced(
    records: &[pico_server::StreamRecord],
) -> Result<Vec<SequencedRecord>, ServiceError> {
    records
        .iter()
        .map(|record| {
            Ok(SequencedRecord {
                seq: record.offset.record_offset(),
                envelope: decode_envelope(&record.payload).map_err(codec_error)?,
            })
        })
        .collect()
}

fn sse_data_event(records: &[SequencedRecord], next_seq: u64) -> Bytes {
    let json = encode_json_read(records);
    let json = String::from_utf8(json.to_vec()).expect("json is utf-8");
    let mut out = format!("event: data\nid: {next_seq}\n");
    for line in crate::http::sse_lines(&json) {
        out.push_str("data:");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    Bytes::from(out)
}

fn sse_control_event(next_seq: u64, up_to_date: bool, closed: bool) -> Bytes {
    let mut node = Map::new();
    node.insert("next_seq".into(), json!(next_seq));
    node.insert("up_to_date".into(), json!(up_to_date));
    if closed {
        node.insert("closed".into(), json!(true));
    }
    let json = serde_json::to_string(&Value::Object(node)).expect("json encode");
    Bytes::from(format!("event: control\nid: {next_seq}\ndata:{json}\n\n"))
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

/// JSON `{error, message?, next_seq?}`.
pub(crate) fn error(
    status: u16,
    code: &str,
    message: &str,
    next: Option<&OffsetToken>,
) -> Response {
    let mut response = base_response(status);
    set_header(&mut response, header::CONTENT_TYPE.as_str(), CT_JSON);
    set_header(&mut response, header::CACHE_CONTROL.as_str(), "no-store");
    let mut node = Map::new();
    node.insert("error".into(), json!(code));
    if !message.is_empty() {
        node.insert("message".into(), json!(message));
    }
    if let Some(next) = next {
        set_header(&mut response, H_NEXT_SEQ, &next.record_offset().to_string());
        node.insert("next_seq".into(), json!(next.record_offset()));
    }
    *response.body_mut() =
        Body::from(serde_json::to_vec(&Value::Object(node)).expect("json encode"));
    response
}
