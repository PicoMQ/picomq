use std::time::Duration;

use axum::extract::{Path, Request, State};
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use picomq_auth::Operation;
use picomq_schema::{MessageKind, Schema, SchemaFormat};
use picomq_server::{Log, OffsetToken, Page, ServiceError, StreamRecord, Topic, partition_log};
use serde_json::{Value, json};

use super::{AdminState, authorized_topic};
use crate::common::{error_response, service_error};
use crate::http::query_param;
use crate::paging::Paging;

const PREVIEW_BYTES: usize = 4 * 1024;
const RECORD_BYTES: usize = 1 << 20;
const READ_BYTES: usize = 1 << 20;
const SCAN_BYTES: usize = 16 << 20;
const MAX_WAIT_MS: u64 = 5_000;
const FILTERS: [&str; 4] = ["key", "header", "headerValue", "value"];

pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/admin/topics/{name}/partitions/{index}/records", get(list))
        .route(
            "/admin/topics/{name}/partitions/{index}/records/{offset}",
            get(record),
        )
}

struct Filter {
    key: Option<String>,
    header: Option<String>,
    header_value: Option<String>,
    value: Option<String>,
}

#[derive(Clone, Copy)]
enum Encoding {
    Utf8,
    Base64,
    Decoded(SchemaFormat),
}

struct Field {
    data: Bytes,
    encoding: Encoding,
    size: usize,
}

async fn list(
    State(state): State<AdminState>,
    Path((name, index)): Path<(String, u32)>,
    request: Request,
) -> Response {
    let topic = match authorized_topic(&state, request.headers(), Operation::Read, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };
    match state.remote_owner(&topic, index).await {
        Ok(Some(owner)) => return state.forward_to(owner, request).await,
        Ok(None) => {}
        Err(response) => return *response,
    }

    let paging = match Paging::from_query(request.uri(), &FILTERS) {
        Ok(paging) => paging,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let filter = match Filter::of(&paging) {
        Ok(filter) => filter,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let wait = match wait_of(request.uri()) {
        Ok(wait) => wait,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let log = match partition_log(&topic, index) {
        Ok(log) => log,
        Err(error) => return service_error(error),
    };

    let schema = schema_of(&state, &topic).await;
    let start = match start_of(&state, &topic, index, &paging, request.uri()).await {
        Ok(start) => start,
        Err(response) => return *response,
    };
    let scanned = scan(
        &state,
        &log,
        start,
        paging.limit,
        &filter,
        schema.as_ref(),
        wait,
    )
    .await;

    match scanned {
        Ok(page) => paging.reply(page, |_, record| {
            record_json(record, schema.as_ref(), PREVIEW_BYTES)
        }),
        Err(error) => service_error(error),
    }
}

async fn record(
    State(state): State<AdminState>,
    Path((name, index, offset)): Path<(String, u32, u64)>,
    request: Request,
) -> Response {
    let topic = match authorized_topic(&state, request.headers(), Operation::Read, &name).await {
        Ok(topic) => topic,
        Err(response) => return *response,
    };
    match state.remote_owner(&topic, index).await {
        Ok(Some(owner)) => return state.forward_to(owner, request).await,
        Ok(None) => {}
        Err(response) => return *response,
    }
    let log = match partition_log(&topic, index) {
        Ok(log) => log,
        Err(error) => return service_error(error),
    };

    let schema = schema_of(&state, &topic).await;
    match read_one(&state, &log, offset).await {
        Ok(Some(record)) => {
            let body = record_json(&record, schema.as_ref(), RECORD_BYTES);
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(None) => error_response(
            StatusCode::NOT_FOUND,
            format!("no record at offset {offset}"),
        ),
        Err(error) => service_error(error),
    }
}

async fn start_of(
    state: &AdminState,
    topic: &Topic,
    index: u32,
    paging: &Paging,
    uri: &Uri,
) -> Result<u64, Box<Response>> {
    let bad = |message: &str| Box::new(error_response(StatusCode::BAD_REQUEST, message));
    let failed = |error: ServiceError| Box::new(service_error(error));

    if let Some(after) = &paging.after {
        return after
            .parse::<u64>()
            .ok()
            .and_then(|after| after.checked_add(1))
            .ok_or_else(|| bad("the cursor is not valid"));
    }

    let offset = query_param(uri, "offset").map_err(failed)?;
    let timestamp = query_param(uri, "timestamp").map_err(failed)?;
    match (offset, timestamp) {
        (Some(_), Some(_)) => Err(bad("expected an offset or a timestamp, not both")),
        (Some(offset), None) => offset
            .parse::<u64>()
            .map_err(|_| bad("offset must be a non-negative integer")),
        (None, Some(timestamp)) => {
            let timestamp = timestamp
                .parse::<i64>()
                .map_err(|_| bad("timestamp must be milliseconds since the epoch"))?;
            let topics = state.node.topics();
            match topics.record_at_or_after(topic, index, timestamp).await {
                Ok(Some(record)) => Ok(record.offset.record_offset()),
                Ok(None) => topics
                    .watermarks(topic, index)
                    .await
                    .map(|watermarks| watermarks.high_watermark)
                    .map_err(failed),
                Err(error) => Err(failed(error)),
            }
        }
        (None, None) => Ok(0),
    }
}

async fn scan(
    state: &AdminState,
    log: &Log,
    start: u64,
    limit: usize,
    filter: &Filter,
    schema: Option<&Schema>,
    wait: Duration,
) -> Result<Page<u64, StreamRecord>, ServiceError> {
    let service = state.node.service();
    let watermarks = service.watermarks(log).await?;
    let mut from = start.clamp(watermarks.log_start_offset, watermarks.high_watermark);
    if from == watermarks.high_watermark && !wait.is_zero() {
        service
            .wait_appended(log, OffsetToken::of_record_offset(from), wait)
            .await?;
    }

    let budget = picomq_server::page::fetch_count(limit);
    let mut items = Vec::new();
    let mut scanned = 0;
    let mut bytes = 0;
    let mut last = None;
    let mut full = false;
    let mut exhausted = false;

    while !full && !exhausted {
        let read = service
            .read(log, OffsetToken::of_record_offset(from), READ_BYTES, 0)
            .await?;
        let count = read.records.len();
        exhausted = read.up_to_date || count == 0;

        for (position, record) in read.records.into_iter().enumerate() {
            let offset = record.offset.record_offset();
            scanned += 1;
            bytes += record.record.size_hint();
            last = Some(offset);

            if filter.matches(&record, schema) {
                items.push((offset, record));
            }
            full = items.len() == limit || scanned == budget || bytes >= SCAN_BYTES;
            if full {
                exhausted = read.up_to_date && position + 1 == count;
                break;
            }
        }

        from = read.next_offset.record_offset();
    }

    let next = last.filter(|_| !exhausted || !wait.is_zero());
    Ok(Page { items, next })
}

async fn read_one(
    state: &AdminState,
    log: &Log,
    offset: u64,
) -> Result<Option<StreamRecord>, ServiceError> {
    let service = state.node.service();
    let watermarks = service.watermarks(log).await?;
    if offset < watermarks.log_start_offset || offset >= watermarks.high_watermark {
        return Ok(None);
    }

    let read = service
        .read(log, OffsetToken::of_record_offset(offset), 1, 1)
        .await?;
    Ok(read
        .records
        .into_iter()
        .find(|record| record.offset.record_offset() == offset))
}

async fn schema_of(state: &AdminState, topic: &Topic) -> Option<Schema> {
    let name = topic.schema_name.as_deref()?;
    let registry = state.node.topics().schema_registry()?.clone();
    registry.schema(name).await.ok().flatten()
}

fn wait_of(uri: &Uri) -> Result<Duration, String> {
    match query_param(uri, "wait").map_err(|error| error.to_string())? {
        None => Ok(Duration::ZERO),
        Some(raw) => raw
            .parse::<u64>()
            .ok()
            .filter(|ms| *ms <= MAX_WAIT_MS)
            .map(Duration::from_millis)
            .ok_or_else(|| format!("wait must be between 0 and {MAX_WAIT_MS} milliseconds")),
    }
}

impl Filter {
    fn of(paging: &Paging) -> Result<Self, String> {
        let given = |name| paging.filter(name).map(str::to_owned);
        let filter = Self {
            key: given("key"),
            header: given("header"),
            header_value: given("headerValue"),
            value: given("value"),
        };

        if filter.header_value.is_some() && filter.header.is_none() {
            return Err("headerValue needs a header".to_owned());
        }
        Ok(filter)
    }

    fn matches(&self, record: &StreamRecord, schema: Option<&Schema>) -> bool {
        let record = &record.record;

        self.key.as_deref().is_none_or(|needle| {
            record.key.as_ref().is_some_and(|key| {
                contains(&schema_field(key, schema, MessageKind::Key).data, needle)
            })
        }) && self.header.as_deref().is_none_or(|name| {
            record.headers.iter().any(|(header, value)| {
                header == name
                    && self.header_value.as_deref().is_none_or(|needle| {
                        value.as_ref().is_some_and(|value| contains(value, needle))
                    })
            })
        }) && self.value.as_deref().is_none_or(|needle| {
            record.value.as_ref().is_some_and(|value| {
                contains(
                    &schema_field(value, schema, MessageKind::Value).data,
                    needle,
                )
            })
        })
    }
}

impl Encoding {
    fn name(self) -> &'static str {
        match self {
            Self::Utf8 => "utf8",
            Self::Base64 => "base64",
            Self::Decoded(SchemaFormat::Avro) => "avro",
            Self::Decoded(SchemaFormat::Json) => "json",
            Self::Decoded(SchemaFormat::Protobuf) => "protobuf",
        }
    }
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    needle.is_empty()
        || haystack
            .windows(needle.len())
            .any(|window| window == needle.as_bytes())
}

fn schema_field(bytes: &Bytes, schema: Option<&Schema>, kind: MessageKind) -> Field {
    let Some((schema, json)) =
        schema.and_then(|schema| Some((schema, schema.decode(kind, bytes).ok()??)))
    else {
        return raw_field(bytes);
    };

    Field {
        data: Bytes::from(json.to_string()),
        encoding: Encoding::Decoded(schema.format()),
        size: bytes.len(),
    }
}

fn raw_field(bytes: &Bytes) -> Field {
    let encoding = match std::str::from_utf8(bytes) {
        Ok(_) => Encoding::Utf8,
        Err(_) => Encoding::Base64,
    };

    Field {
        data: bytes.clone(),
        encoding,
        size: bytes.len(),
    }
}

fn record_json(record: &StreamRecord, schema: Option<&Schema>, limit: usize) -> Value {
    let stored = &record.record;
    let key = stored
        .key
        .as_ref()
        .map(|key| field_json(&schema_field(key, schema, MessageKind::Key), limit));
    let value = stored
        .value
        .as_ref()
        .map(|value| field_json(&schema_field(value, schema, MessageKind::Value), limit));
    let headers: Vec<Value> = stored
        .headers
        .iter()
        .map(|(name, value)| {
            let value = value
                .as_ref()
                .map(|value| field_json(&raw_field(value), limit));
            json!({ "name": name, "value": value })
        })
        .collect();

    json!({
        "offset": record.offset.record_offset(),
        "timestamp": stored.timestamp_ms,
        "key": key,
        "headers": headers,
        "value": value,
    })
}

fn field_json(field: &Field, limit: usize) -> Value {
    let shown = field.data.len().min(limit);
    let data = match field.encoding {
        Encoding::Base64 => STANDARD.encode(&field.data[..shown]),
        Encoding::Utf8 | Encoding::Decoded(_) => {
            let text = std::str::from_utf8(&field.data).unwrap_or_default();
            let mut end = shown;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            text[..end].to_owned()
        }
    };

    json!({
        "data": data,
        "encoding": field.encoding.name(),
        "size": field.size,
        "truncated": field.data.len() > limit,
    })
}
