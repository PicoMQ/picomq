// Modified from Apache Iggy for PicoMQ.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::common::{
    DEFAULT_V3_CURSOR_FIELD, PayloadFormat, Row, RowContext, V3SourceConfig, V3State,
    apply_query_params, is_timestamp_after, parse_jsonl_rows, validate_cursor,
};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use picomq_connector_sdk::{Error, ProducedMessage, Schema, now_millis};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde_json::json;
use tracing::warn;

pub(crate) const DEFAULT_STUCK_CAP_FACTOR: u32 = 10;
pub(crate) const MAX_STUCK_CAP_FACTOR: u32 = 100;

const MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024 * 1024;

const QUERY_FORMAT_JSONL: &str = "jsonl";

fn build_query(base: &str, query: &str, db: &str) -> Result<(Url, serde_json::Value), Error> {
    let url = Url::parse(&format!("{base}/api/v3/query_sql"))
        .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;
    let body = json!({
        "db":     db,
        "q":      query,
        "format": QUERY_FORMAT_JSONL
    });
    Ok((url, body))
}

pub(crate) async fn run_query(
    client: &ClientWithMiddleware,
    config: &V3SourceConfig,
    auth: &str,
    cursor: &str,
    effective_batch: u32,
    offset: u64,
) -> Result<String, Error> {
    validate_cursor(cursor)?;
    let q = apply_query_params(
        &config.query,
        cursor,
        &effective_batch.to_string(),
        &offset.to_string(),
    );
    let base = config.url.trim_end_matches('/');
    let (url, body) = build_query(base, &q, &config.db)?;

    let mut response = client
        .post(url)
        .header("Authorization", auth)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| Error::Storage(format!("InfluxDB V3 query failed: {e}")))?;

    let status = response.status();
    if status.is_success() {
        if response
            .content_length()
            .is_some_and(|n| n as usize > MAX_RESPONSE_BODY_BYTES)
        {
            return Err(Error::Storage(format!(
                "InfluxDB V3 response body exceeds {MAX_RESPONSE_BODY_BYTES} byte cap; \
                 reduce batch_size to avoid OOM"
            )));
        }
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| Error::Storage(format!("Failed to read V3 response: {e}")))?
        {
            buf.extend_from_slice(&chunk);
            if buf.len() > MAX_RESPONSE_BODY_BYTES {
                return Err(Error::Storage(format!(
                    "InfluxDB V3 response body exceeded {MAX_RESPONSE_BODY_BYTES} byte cap \
                     while streaming; reduce batch_size to avoid OOM"
                )));
            }
        }
        return String::from_utf8(buf)
            .map_err(|e| Error::Storage(format!("V3 response body is not valid UTF-8: {e}")));
    }

    let body_text = response
        .text()
        .await
        .unwrap_or_else(|_| "failed to read response body".to_string());

    if status.as_u16() == 404 {
        if body_text.to_lowercase().contains("database not found") {
            return Ok(String::new());
        }
        return Err(Error::PermanentHttpError(format!(
            "InfluxDB V3 query failed with status {status}: {body_text}"
        )));
    }

    if picomq_connector_sdk::retry::is_transient_status(status) {
        Err(Error::Storage(format!(
            "InfluxDB V3 query failed with status {status}: {body_text}"
        )))
    } else {
        Err(Error::PermanentHttpError(format!(
            "InfluxDB V3 query failed with status {status}: {body_text}"
        )))
    }
}

fn build_payload(
    row: &Row,
    payload_column: Option<&str>,
    payload_format: PayloadFormat,
    include_metadata: bool,
    cursor_field: &str,
) -> Result<Vec<u8>, Error> {
    if let Some(col) = payload_column {
        let raw = row
            .get(col)
            .cloned()
            .ok_or_else(|| Error::InvalidRecordValue(format!("Missing payload column '{col}'")))?;
        return match payload_format {
            PayloadFormat::Json => serde_json::to_vec(&raw)
                .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}"))),
            PayloadFormat::Text => match raw {
                serde_json::Value::String(s) => Ok(s.into_bytes()),
                other => serde_json::to_vec(&other)
                    .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}"))),
            },
            PayloadFormat::Raw => {
                let s = raw.as_str().ok_or_else(|| {
                    Error::InvalidRecordValue(format!(
                        "Payload column '{col}' must be a string value for Raw format"
                    ))
                })?;
                general_purpose::STANDARD.decode(s.as_bytes()).map_err(|e| {
                    Error::InvalidRecordValue(format!("Failed to decode payload as base64: {e}"))
                })
            }
        };
    }

    struct RowView<'a> {
        row: &'a Row,
        cursor_field: &'a str,
        include_metadata: bool,
    }
    impl serde::Serialize for RowView<'_> {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeMap;
            let mut map = s.serialize_map(None)?;
            for (k, v) in self
                .row
                .iter()
                .filter(|(k, _)| self.include_metadata || k.as_ref() != self.cursor_field)
            {
                map.serialize_entry(k.as_ref(), v)?;
            }
            map.end()
        }
    }
    serde_json::to_vec(&RowView {
        row,
        cursor_field,
        include_metadata,
    })
    .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}")))
}

pub(crate) fn next_stuck_batch_size(current: u32, base: u32, cap_factor: u32) -> Option<u32> {
    let cap = base.saturating_mul(cap_factor);
    if current >= cap {
        None
    } else {
        Some(current.saturating_mul(2).min(cap))
    }
}

pub(crate) struct PollResult {
    pub messages: Vec<ProducedMessage>,
    pub new_state: V3State,
    pub schema: Schema,
    pub trip_circuit_breaker: bool,
    pub is_stuck: bool,
}

fn normalize_v3_timestamp(ts: &str) -> Result<(std::borrow::Cow<'_, str>, DateTime<Utc>), Error> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
        return Ok((std::borrow::Cow::Borrowed(ts), dt.with_timezone(&Utc)));
    }
    let with_z = format!("{ts}Z");
    match chrono::DateTime::parse_from_rfc3339(&with_z) {
        Ok(dt) => Ok((std::borrow::Cow::Owned(with_z), dt.with_timezone(&Utc))),
        Err(_) => Err(Error::InvalidRecordValue(format!(
            "cursor field contains {ts:?} which is not a valid RFC 3339 timestamp \
             (tried appending 'Z', still invalid)"
        ))),
    }
}

#[derive(Debug)]
pub(crate) struct RowProcessingResult {
    pub messages: Vec<ProducedMessage>,
    pub max_cursor: Option<String>,
    pub rows_at_max_cursor: u64,
    pub penultimate_cursor: Option<String>,
    pub safe_message_count: usize,
}

pub(crate) fn process_rows(
    rows: &[Row],
    ctx: &RowContext<'_>,
    row_offset_base: u64,
) -> Result<RowProcessingResult, Error> {
    let mut messages = Vec::with_capacity(rows.len());
    let mut max_cursor: Option<String> = None;
    let mut max_cursor_parsed: Option<DateTime<Utc>> = None;
    let mut rows_at_max_cursor = 0u64;
    let mut penultimate_cursor: Option<String> = None;
    let mut safe_message_count = 0usize;
    for row in rows.iter() {
        let db_pos = row_offset_base as u128 + messages.len() as u128;
        let raw_cv = row
            .get(ctx.cursor_field)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::InvalidRecordValue(format!(
                    "Row missing '{}' cursor field, message ID would be non-deterministic \
                     on re-delivery, breaking deduplication. \
                     Ensure your query selects the cursor column.",
                    ctx.cursor_field
                ))
            })?;
        let (cv_owned, cv_dt) = normalize_v3_timestamp(raw_cv)?;
        let nanos_i128 =
            cv_dt.timestamp() as i128 * 1_000_000_000 + cv_dt.timestamp_subsec_nanos() as i128;
        let this_row_id = (nanos_i128 as u128).wrapping_add(db_pos);
        match max_cursor_parsed {
            Some(cur_dt) if cv_dt > cur_dt => {
                penultimate_cursor = max_cursor.take();
                max_cursor = Some(cv_owned.into_owned());
                max_cursor_parsed = Some(cv_dt);
                rows_at_max_cursor = 1;
                safe_message_count = messages.len();
            }
            Some(cur_dt) if cv_dt == cur_dt => {
                rows_at_max_cursor += 1;
            }
            Some(_) => {}
            None => {
                max_cursor = Some(cv_owned.into_owned());
                max_cursor_parsed = Some(cv_dt);
                rows_at_max_cursor = 1;
            }
        }

        let payload = build_payload(
            row,
            ctx.payload_col,
            ctx.payload_format,
            ctx.include_metadata,
            ctx.cursor_field,
        )?;
        messages.push(ProducedMessage {
            key: Some(this_row_id.to_string().into_bytes()),
            timestamp: Some(ctx.now_millis),
            headers: None,
            payload,
        });
    }

    Ok(RowProcessingResult {
        messages,
        max_cursor,
        rows_at_max_cursor,
        penultimate_cursor,
        safe_message_count,
    })
}

pub(crate) async fn poll(
    client: &ClientWithMiddleware,
    config: &V3SourceConfig,
    auth: &str,
    state: &V3State,
    payload_format: PayloadFormat,
    include_metadata: bool,
) -> Result<PollResult, Error> {
    let cursor = state
        .last_timestamp
        .clone()
        .or_else(|| config.initial_offset.clone())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());

    let base_batch = config.batch_size.unwrap_or(500).max(1);

    let cap_factor = config
        .stuck_batch_cap_factor
        .unwrap_or(DEFAULT_STUCK_CAP_FACTOR);

    let effective_batch = if state.effective_batch_size == 0 {
        base_batch
    } else if cap_factor == 0 {
        state.effective_batch_size
    } else {
        state
            .effective_batch_size
            .min(base_batch.saturating_mul(cap_factor))
    };

    let response_data = run_query(
        client,
        config,
        auth,
        &cursor,
        effective_batch,
        state.last_timestamp_row_offset,
    )
    .await?;
    let rows = parse_jsonl_rows(&response_data)?;

    let ctx = RowContext {
        cursor_field: config
            .cursor_field
            .as_deref()
            .unwrap_or(DEFAULT_V3_CURSOR_FIELD),
        current_cursor: &cursor,
        include_metadata,
        payload_col: config.payload_column.as_deref(),
        payload_format,
        now_millis: now_millis(),
    };

    let result = process_rows(&rows, &ctx, state.last_timestamp_row_offset)?;

    let schema = if ctx.payload_col.is_some() {
        ctx.payload_format.schema()
    } else {
        Schema::Json
    };

    let full_batch = rows.len() as u32 == effective_batch;
    let all_same_timestamp = result.rows_at_max_cursor >= effective_batch as u64;

    if cap_factor > 0
        && full_batch
        && !all_same_timestamp
        && result.rows_at_max_cursor > 1
        && let Some(penultimate) = result.penultimate_cursor
    {
        let safe_count = result.safe_message_count;
        if safe_count == 0 {
            return Err(Error::InvalidState);
        } else {
            let max_ts = result.max_cursor.as_deref().unwrap_or("unknown");
            warn!(
                "InfluxDB V3 source, full batch of {} rows has mixed timestamps; \
                     emitting {} safe rows and advancing cursor to {} \
                     (rows at {} deferred to next poll)",
                rows.len(),
                safe_count,
                penultimate,
                max_ts,
            );
            let mut messages = result.messages;
            messages.truncate(safe_count);
            let msg_count = messages.len() as u64;
            return Ok(PollResult {
                messages,
                new_state: V3State {
                    last_timestamp: Some(penultimate),
                    processed_rows: state.processed_rows + msg_count,
                    effective_batch_size: base_batch,
                    last_timestamp_row_offset: 0,
                    stuck_cursor: None,
                },
                schema,
                trip_circuit_breaker: false,
                is_stuck: false,
            });
        }
    }

    let stuck = cap_factor > 0 && all_same_timestamp;

    if stuck {
        return match next_stuck_batch_size(effective_batch, base_batch, cap_factor) {
            Some(next_batch) => {
                warn!(
                    "InfluxDB V3 source, all {} rows share timestamp {}; \
                     inflating batch size {} → {} (cap={}×{}={})",
                    rows.len(),
                    result.max_cursor.as_deref().unwrap_or("unknown"),
                    effective_batch,
                    next_batch,
                    cap_factor,
                    base_batch,
                    base_batch.saturating_mul(cap_factor)
                );
                let msg_count = result.messages.len() as u64;
                Ok(PollResult {
                    messages: result.messages,
                    new_state: V3State {
                        last_timestamp: state.last_timestamp.clone(),
                        processed_rows: state.processed_rows + msg_count,
                        effective_batch_size: next_batch,
                        last_timestamp_row_offset: state
                            .last_timestamp_row_offset
                            .saturating_add(result.rows_at_max_cursor),
                        stuck_cursor: result.max_cursor,
                    },
                    schema,
                    trip_circuit_breaker: false,
                    is_stuck: true,
                })
            }
            None => {
                warn!(
                    "InfluxDB V3 source, stuck-timestamp cap reached at batch size {effective_batch}; \
                     tripping circuit breaker to prevent an infinite loop"
                );
                Ok(PollResult {
                    messages: vec![],
                    new_state: V3State {
                        last_timestamp: state.last_timestamp.clone(),
                        processed_rows: state.processed_rows,
                        effective_batch_size: base_batch,
                        last_timestamp_row_offset: state.last_timestamp_row_offset,
                        stuck_cursor: result.max_cursor,
                    },
                    schema,
                    trip_circuit_breaker: true,
                    is_stuck: false,
                })
            }
        };
    }

    let old_dt = state.last_timestamp.as_deref().and_then(|s| {
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    if let (Some(new), Some(_)) = (
        result.max_cursor.as_deref(),
        state.last_timestamp.as_deref(),
    ) && !old_dt.is_some_and(|dt| is_timestamp_after(new, dt))
    {
        warn!("V3 source: max_cursor did not advance past saved cursor; keeping old value");
        let (messages, effective_batch_size, trip_circuit_breaker, is_stuck) =
            if state.stuck_cursor.is_some() {
                (result.messages, state.effective_batch_size, false, true)
            } else {
                (vec![], base_batch, true, false)
            };
        let processed_rows = state.processed_rows + messages.len() as u64;
        return Ok(PollResult {
            messages,
            new_state: V3State {
                last_timestamp: state.last_timestamp.clone(),
                processed_rows,
                effective_batch_size,
                last_timestamp_row_offset: state.last_timestamp_row_offset,
                stuck_cursor: state.stuck_cursor.clone(),
            },
            schema,
            trip_circuit_breaker,
            is_stuck,
        });
    }

    let processed_rows = state.processed_rows + result.messages.len() as u64;

    let advanced_cursor = match (
        result.max_cursor.as_deref(),
        state.last_timestamp.as_deref(),
    ) {
        (Some(_), _) => result.max_cursor,
        _ if state.last_timestamp_row_offset > 0 && state.stuck_cursor.is_some() => {
            warn!(
                "Advancing cursor past stuck_cursor={:?} on empty follow-up batch. \
                 Any backdated writes at this timestamp inserted after this poll \
                 will be silently skipped. Set stuck_batch_cap_factor=0 to disable \
                 stuck detection if your workload has out-of-order ingestion.",
                state.stuck_cursor.as_deref()
            );
            state.stuck_cursor.clone()
        }
        _ => state.last_timestamp.clone(),
    };

    let new_state = V3State {
        last_timestamp: advanced_cursor,
        processed_rows,
        effective_batch_size: base_batch,
        last_timestamp_row_offset: 0,
        stuck_cursor: None,
    };

    Ok(PollResult {
        messages: result.messages,
        new_state,
        schema,
        trip_circuit_breaker: false,
        is_stuck: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Row, RowContext};
    use std::sync::Arc;

    fn row(pairs: &[(&str, &str)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    Arc::<str>::from(*k),
                    serde_json::Value::String(v.to_string()),
                )
            })
            .collect()
    }

    const T1: &str = "2024-01-01T00:00:00Z";
    const T2: &str = "2024-01-01T00:00:01Z";
    const T3: &str = "2024-01-01T00:00:02Z";

    fn ctx(current_cursor: &str, now_millis: u64) -> RowContext<'_> {
        RowContext {
            cursor_field: "time",
            current_cursor,
            include_metadata: true,
            payload_col: None,
            payload_format: PayloadFormat::Json,
            now_millis,
        }
    }

    #[test]
    fn process_rows_empty_returns_empty() {
        let result = process_rows(&[], &ctx(T1, 1000), 0).unwrap();
        assert!(result.messages.is_empty());
        assert!(result.max_cursor.is_none());
        assert_eq!(
            result.rows_at_max_cursor, 0,
            "empty slice has no rows at max cursor"
        );
    }

    #[test]
    fn process_rows_single_row_advances_cursor() {
        let rows = vec![row(&[("time", T1), ("val", "1")])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.max_cursor.as_deref(), Some(T1));
    }

    #[test]
    fn process_rows_advances_to_latest_timestamp() {
        let rows = vec![
            row(&[("time", T1)]),
            row(&[("time", T3)]),
            row(&[("time", T2)]),
        ];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some(T3));
        assert_eq!(result.messages.len(), 3);
    }

    #[test]
    fn process_rows_tied_timestamps_do_not_regress_cursor() {
        let rows = vec![
            row(&[("time", T2)]),
            row(&[("time", T1)]),
            row(&[("time", T2)]),
        ];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.max_cursor.as_deref(), Some(T2));
    }

    #[test]
    fn process_rows_row_without_cursor_field_returns_error() {
        let rows = vec![row(&[("val", "1")])];
        let err = process_rows(&rows, &ctx(T1, 1000), 0).unwrap_err();
        assert!(
            matches!(err, Error::InvalidRecordValue(_)),
            "expected InvalidRecordValue when cursor column is absent, got {err:?}"
        );
    }

    #[test]
    fn process_rows_all_rows_missing_cursor_field_returns_error() {
        let rows = vec![
            row(&[("val", "1")]),
            row(&[("val", "2")]),
            row(&[("val", "3")]),
        ];
        let err = process_rows(&rows, &ctx(T1, 1000), 0).unwrap_err();
        assert!(
            matches!(err, Error::InvalidRecordValue(_)),
            "expected InvalidRecordValue when cursor column is absent, got {err:?}"
        );
    }

    #[test]
    fn process_rows_message_ids_are_some_and_unique() {
        let rows = vec![row(&[("time", T1)]), row(&[("time", T2)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert!(result.messages[0].key.is_some());
        assert!(result.messages[1].key.is_some());
        assert_ne!(result.messages[0].key, result.messages[1].key);
    }

    #[test]
    fn process_rows_message_timestamps_use_now_millis() {
        let rows = vec![row(&[("time", T1)])];
        let result = process_rows(&rows, &ctx(T1, 888_888), 0).unwrap();
        assert_eq!(result.messages[0].timestamp, Some(888_888));
    }

    #[test]
    fn process_rows_text_payload_format() {
        use base64::{Engine as _, engine::general_purpose};
        let encoded = general_purpose::STANDARD.encode(b"hello");
        let rows = vec![row(&[("time", T1), ("payload", &encoded)])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("payload"),
                payload_format: PayloadFormat::Text,
                now_millis: 1000,
            },
            0,
        )
        .unwrap();
        assert_eq!(result.messages.len(), 1);
    }

    #[test]
    fn process_rows_rows_at_max_cursor_counts_rows_sharing_max_timestamp() {
        let rows = vec![row(&[("time", T1)]), row(&[("time", T1)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.rows_at_max_cursor, 2);
    }

    #[test]
    fn process_rows_rows_at_max_cursor_resets_when_cursor_advances() {
        let rows = vec![row(&[("time", T1)]), row(&[("time", T2)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.rows_at_max_cursor, 1);
        assert_eq!(result.max_cursor.as_deref(), Some(T2));
    }

    #[test]
    fn process_rows_rows_at_max_cursor_zero_for_empty_slice() {
        let result = process_rows(&[], &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.rows_at_max_cursor, 0);
    }

    #[test]
    fn process_rows_penultimate_cursor_set_on_mixed_timestamps() {
        let rows = vec![row(&[("time", T1)]), row(&[("time", T2)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.penultimate_cursor.as_deref(), Some(T1));
        assert_eq!(result.safe_message_count, 1);
        assert_eq!(result.max_cursor.as_deref(), Some(T2));
    }

    #[test]
    fn process_rows_penultimate_cursor_none_for_single_timestamp() {
        let rows = vec![row(&[("time", T1)]), row(&[("time", T1)])];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert!(result.penultimate_cursor.is_none());
        assert_eq!(result.safe_message_count, 0);
    }

    #[test]
    fn process_rows_penultimate_tracks_second_highest_across_three_timestamps() {
        let rows = vec![
            row(&[("time", T1)]),
            row(&[("time", T2)]),
            row(&[("time", T3)]),
        ];
        let result = process_rows(&rows, &ctx(T1, 1000), 0).unwrap();
        assert_eq!(result.penultimate_cursor.as_deref(), Some(T2));
        assert_eq!(result.safe_message_count, 2);
        assert_eq!(result.max_cursor.as_deref(), Some(T3));
    }

    #[test]
    fn process_rows_penultimate_cursor_none_for_empty_slice() {
        let result = process_rows(&[], &ctx(T1, 1000), 0).unwrap();
        assert!(result.penultimate_cursor.is_none());
        assert_eq!(result.safe_message_count, 0);
    }

    #[test]
    fn next_stuck_batch_size_doubles_until_cap() {
        assert_eq!(next_stuck_batch_size(500, 500, 10), Some(1000));
        assert_eq!(next_stuck_batch_size(1000, 500, 10), Some(2000));
        assert_eq!(next_stuck_batch_size(4000, 500, 10), Some(5000));
        assert_eq!(next_stuck_batch_size(5000, 500, 10), None);
    }

    #[test]
    fn normalize_already_valid_rfc3339_unchanged() {
        assert_eq!(
            normalize_v3_timestamp("2024-01-01T00:00:00.123Z")
                .unwrap()
                .0,
            "2024-01-01T00:00:00.123Z"
        );
        assert_eq!(
            normalize_v3_timestamp("2024-01-01T00:00:00Z").unwrap().0,
            "2024-01-01T00:00:00Z"
        );
    }

    #[test]
    fn normalize_no_tz_nanoseconds_appends_z_only() {
        let (result, _) = normalize_v3_timestamp("2026-04-26T02:32:20.526360865").unwrap();
        assert_eq!(result, "2026-04-26T02:32:20.526360865Z");
    }

    #[test]
    fn normalize_no_tz_milliseconds_appends_z() {
        let (result, _) = normalize_v3_timestamp("2026-04-26T02:32:20.526").unwrap();
        assert_eq!(result, "2026-04-26T02:32:20.526Z");
    }

    #[test]
    fn normalize_rfc3339_sub_ms_precision_returned_unchanged() {
        let (result, _) = normalize_v3_timestamp("2026-04-26T02:32:20.526360865Z").unwrap();
        assert_eq!(result, "2026-04-26T02:32:20.526360865Z");
    }

    #[test]
    fn normalize_invalid_returns_err() {
        assert!(normalize_v3_timestamp("not-a-timestamp").is_err());
    }

    #[test]
    fn process_rows_accepts_influxdb3_no_tz_timestamps() {
        let rows = vec![
            row(&[("time", "2026-04-26T02:32:20.526360865"), ("val", "1")]),
            row(&[("time", "2026-04-26T02:32:21.000000000"), ("val", "2")]),
        ];
        let c = ctx("2026-04-26T02:32:19.000Z", 0);
        let result = process_rows(&rows, &c, 0).expect("should not fail on bare timestamps");
        assert_eq!(result.messages.len(), 2);
        assert_eq!(
            result.max_cursor.as_deref(),
            Some("2026-04-26T02:32:21.000000000Z")
        );
    }

    #[test]
    fn process_rows_sub_ms_timestamps_have_distinct_cursors() {
        let rows = vec![
            row(&[("time", "2026-04-26T02:32:20.526360000"), ("val", "a")]),
            row(&[("time", "2026-04-26T02:32:20.526361000"), ("val", "b")]),
            row(&[("time", "2026-04-26T02:32:20.526362000"), ("val", "c")]),
        ];
        let c = ctx("2026-04-26T02:32:19.000Z", 0);
        let result = process_rows(&rows, &c, 0).expect("should succeed");
        assert_eq!(
            result.max_cursor.as_deref(),
            Some("2026-04-26T02:32:20.526362000Z")
        );
        assert_eq!(result.rows_at_max_cursor, 1);
    }

    #[test]
    fn process_rows_message_ids_stable_across_repoll() {
        let rows = vec![
            row(&[("time", T1), ("val", "a")]),
            row(&[("time", T2), ("val", "b")]),
        ];
        let c = ctx(T1, 1000);
        let first = process_rows(&rows, &c, 0).unwrap();
        let second = process_rows(&rows, &c, 0).unwrap();
        assert_eq!(
            first.messages[0].key, second.messages[0].key,
            "row at T1 must have the same ID on re-poll"
        );
        assert_eq!(
            first.messages[1].key, second.messages[1].key,
            "row at T2 must have the same ID on re-poll"
        );
    }

    #[test]
    fn process_rows_rows_with_same_timestamp_get_distinct_stable_ids() {
        let rows = vec![
            row(&[("time", T1), ("val", "first")]),
            row(&[("time", T1), ("val", "second")]),
        ];
        let c = ctx("1970-01-01T00:00:00Z", 0);
        let result = process_rows(&rows, &c, 0).unwrap();
        assert_ne!(
            result.messages[0].key, result.messages[1].key,
            "two rows at the same timestamp must have distinct IDs"
        );
        let result2 = process_rows(&rows, &c, 0).unwrap();
        assert_eq!(result.messages[0].key, result2.messages[0].key);
        assert_eq!(result.messages[1].key, result2.messages[1].key);
    }

    #[test]
    fn process_rows_offset_base_prevents_id_collision_across_batches() {
        let rows_a = vec![
            row(&[("time", T1), ("val", "R0")]),
            row(&[("time", T1), ("val", "R1")]),
        ];
        let rows_b = vec![
            row(&[("time", T1), ("val", "R2")]),
            row(&[("time", T1), ("val", "R3")]),
        ];
        let c = ctx("1970-01-01T00:00:00Z", 0);

        let result_a = process_rows(&rows_a, &c, 0).unwrap();
        let result_b = process_rows(&rows_b, &c, 2).unwrap();

        assert_ne!(
            result_a.messages[0].key, result_b.messages[0].key,
            "R0 and R2 share batch position 0 but differ by offset, must not collide"
        );
        assert_ne!(
            result_a.messages[1].key, result_b.messages[1].key,
            "R1 and R3 share batch position 1 but differ by offset, must not collide"
        );
        assert_ne!(result_a.messages[0].key, result_a.messages[1].key);
        assert_ne!(result_b.messages[0].key, result_b.messages[1].key);
    }

    #[test]
    fn process_rows_payload_column_json_format_serializes_value() {
        let rows = vec![row(&[("time", T1), ("data", r#"{"k":1}"#)])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("data"),
                payload_format: PayloadFormat::Json,
                now_millis: 0,
            },
            0,
        )
        .unwrap();
        assert_eq!(result.messages.len(), 1);
        assert!(!result.messages[0].payload.is_empty());
    }

    #[test]
    fn process_rows_payload_column_raw_decodes_base64() {
        use base64::{Engine as _, engine::general_purpose};
        let encoded = general_purpose::STANDARD.encode(b"raw-bytes");
        let rows = vec![row(&[("time", T1), ("blob", &encoded)])];
        let result = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("blob"),
                payload_format: PayloadFormat::Raw,
                now_millis: 0,
            },
            0,
        )
        .unwrap();
        assert_eq!(result.messages[0].payload, b"raw-bytes");
    }

    #[test]
    fn process_rows_payload_column_raw_non_string_value_returns_error() {
        use crate::common::Row;
        let mut row: Row = Row::default();
        row.insert(
            Arc::<str>::from("time"),
            serde_json::Value::String(T1.to_string()),
        );
        row.insert(
            Arc::<str>::from("blob"),
            serde_json::Value::Number(42.into()),
        );
        let err = process_rows(
            &[row],
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("blob"),
                payload_format: PayloadFormat::Raw,
                now_millis: 0,
            },
            0,
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::InvalidRecordValue(_)),
            "non-string value for Raw format must return InvalidRecordValue: {err:?}"
        );
    }

    #[test]
    fn process_rows_payload_column_raw_invalid_base64_returns_error() {
        let rows = vec![row(&[("time", T1), ("blob", "!!!invalid!!!")])];
        let err = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("blob"),
                payload_format: PayloadFormat::Raw,
                now_millis: 0,
            },
            0,
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidRecordValue(_)));
    }

    #[test]
    fn process_rows_missing_payload_column_returns_error() {
        let rows = vec![row(&[("time", T1), ("other", "value")])];
        let err = process_rows(
            &rows,
            &RowContext {
                cursor_field: "time",
                current_cursor: T1,
                include_metadata: true,
                payload_col: Some("missing_col"),
                payload_format: PayloadFormat::Json,
                now_millis: 0,
            },
            0,
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidRecordValue(_)));
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use axum::Router;
    use axum::extract::Request;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use secrecy::SecretString;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    async fn start_server(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://127.0.0.1:{port}")
    }

    fn make_client() -> ClientWithMiddleware {
        let raw = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        picomq_connector_sdk::retry::build_retry_client(
            raw,
            1,
            Duration::from_millis(1),
            Duration::from_millis(10),
            "test",
        )
    }

    fn make_config(url: &str) -> V3SourceConfig {
        V3SourceConfig {
            url: url.to_string(),
            db: "test_db".to_string(),
            token: SecretString::from("test_token"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset".to_string(),
            poll_interval: None,
            batch_size: Some(10),
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: Some("1ms".to_string()),
            timeout: Some("5s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("10ms".to_string()),
            retry_max_delay: Some("10ms".to_string()),
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        }
    }

    const CURSOR: &str = "1970-01-01T00:00:00Z";

    #[tokio::test]
    async fn run_query_returns_jsonl_body_on_200() {
        let jsonl = r#"{"time":"2024-01-01T00:00:00Z","val":1}"#;
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await
        .unwrap();
        assert!(result.contains("val"));
        assert!(result.contains("2024-01-01"));
    }

    #[tokio::test]
    async fn run_query_empty_body_on_200() {
        let app = Router::new().route("/api/v3/query_sql", post(|| async { (StatusCode::OK, "") }));
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn run_query_404_database_not_found_returns_empty_string() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { (StatusCode::NOT_FOUND, "database not found") }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn run_query_404_other_body_returns_permanent_error() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { (StatusCode::NOT_FOUND, "table not found") }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    }

    #[tokio::test]
    async fn run_query_500_returns_transient_error() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await;
        assert!(matches!(result, Err(Error::Storage(_))));
    }

    #[tokio::test]
    async fn run_query_400_returns_permanent_error() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { StatusCode::BAD_REQUEST }),
        );
        let base = start_server(app).await;
        let result = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            CURSOR,
            10,
            0,
        )
        .await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    }

    #[tokio::test]
    async fn run_query_sends_bearer_authorization_header() {
        let captured: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured.clone();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move |headers: HeaderMap| {
                let cap = cap2.clone();
                async move {
                    *cap.lock().await = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    StatusCode::OK
                }
            }),
        );
        let base = start_server(app).await;
        let _ = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer my_token",
            CURSOR,
            10,
            0,
        )
        .await;
        assert_eq!(*captured.lock().await, "Bearer my_token");
    }

    #[tokio::test]
    async fn run_query_request_body_contains_db_and_substituted_cursor() {
        let captured_body: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured_body.clone();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move |request: Request| {
                let cap = cap2.clone();
                async move {
                    let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
                        .await
                        .unwrap();
                    *cap.lock().await = String::from_utf8_lossy(&bytes).to_string();
                    StatusCode::OK
                }
            }),
        );
        let base = start_server(app).await;
        let cursor = "2024-06-01T00:00:00Z";
        let _ = run_query(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            cursor,
            10,
            0,
        )
        .await;
        let body = captured_body.lock().await;
        assert!(body.contains("test_db"), "body should include db: {body}");
        assert!(body.contains(cursor), "body should include cursor: {body}");
        assert!(
            !body.contains("$cursor"),
            "raw placeholder must not appear: {body}"
        );
    }

    #[tokio::test]
    async fn poll_returns_messages_for_jsonl_response() {
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"val\":1}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"val\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 2);
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:02Z")
        );
        assert!(!result.trip_circuit_breaker);
        assert_eq!(result.schema, Schema::Json);
    }

    #[tokio::test]
    async fn poll_advances_cursor_to_latest_out_of_order_timestamp() {
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"v\":1}\n\
                     {\"time\":\"2024-01-01T00:00:03Z\",\"v\":3}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"v\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 3);
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:03Z")
        );
    }

    #[tokio::test]
    async fn poll_empty_jsonl_returns_no_messages() {
        let app = Router::new().route("/api/v3/query_sql", post(|| async { (StatusCode::OK, "") }));
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert!(result.messages.is_empty());
        assert!(!result.trip_circuit_breaker);
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:00Z")
        );
    }

    #[tokio::test]
    async fn poll_detects_stuck_batch_and_doubles_batch_size() {
        let t = "2024-01-01T00:00:00Z";
        let jsonl: String = (0..10)
            .map(|i| format!("{{\"time\":\"{t}\",\"val\":{i}}}\n"))
            .collect();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some(t.to_string()),
            effective_batch_size: 10,
            processed_rows: 0,
            last_timestamp_row_offset: 0,
            stuck_cursor: None,
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            result.messages.len(),
            10,
            "stuck batch must emit rows, not discard them"
        );
        assert_eq!(result.new_state.effective_batch_size, 20, "should double");
        assert!(!result.trip_circuit_breaker);
        assert!(result.is_stuck, "inflating batch must signal is_stuck");
        assert_eq!(result.new_state.last_timestamp.as_deref(), Some(t));
    }

    #[tokio::test]
    async fn poll_trips_circuit_breaker_when_stuck_cap_reached() {
        let t = "2024-01-01T00:00:00Z";
        let jsonl: String = (0..20)
            .map(|i| format!("{{\"time\":\"{t}\",\"val\":{i}}}\n"))
            .collect();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let config = V3SourceConfig {
            stuck_batch_cap_factor: Some(2),
            ..make_config(&base)
        };
        let state = V3State {
            last_timestamp: Some(t.to_string()),
            effective_batch_size: 20,
            processed_rows: 0,
            last_timestamp_row_offset: 0,
            stuck_cursor: None,
        };
        let result = poll(
            &make_client(),
            &config,
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert!(result.trip_circuit_breaker, "must trip when at cap");
        assert!(result.messages.is_empty());
    }

    #[tokio::test]
    async fn poll_small_batch_all_same_timestamp_is_not_stuck() {
        let t2 = "2024-01-01T00:00:01Z";
        let jsonl = format!(
            "{{\"time\":\"{t2}\",\"val\":0}}\n\
             {{\"time\":\"{t2}\",\"val\":1}}\n\
             {{\"time\":\"{t2}\",\"val\":2}}\n"
        );
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            effective_batch_size: 10,
            last_timestamp_row_offset: 0,
            processed_rows: 0,
            stuck_cursor: None,
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 3, "all 3 rows must be emitted");
        assert!(!result.trip_circuit_breaker);
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(t2),
            "cursor must advance to t2"
        );
        assert_eq!(
            result.new_state.last_timestamp_row_offset, 0,
            "offset must reset to 0 after cursor advance"
        );
    }

    #[tokio::test]
    async fn poll_zero_cap_factor_full_batch_advances_cursor() {
        let t1 = "2024-01-01T00:00:01Z";
        let jsonl = (0..10)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let mut cfg = make_config(&base);
        cfg.stuck_batch_cap_factor = Some(0);
        let state = V3State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            effective_batch_size: 10,
            last_timestamp_row_offset: 0,
            processed_rows: 0,
            stuck_cursor: None,
        };
        let result = poll(
            &make_client(),
            &cfg,
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 10, "all 10 rows must be emitted");
        assert!(
            !result.trip_circuit_breaker,
            "CB must not trip when cap_factor=0"
        );
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(t1),
            "cursor must advance to t1"
        );
    }

    #[tokio::test]
    async fn poll_resets_effective_batch_size_on_cursor_advance() {
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"v\":1}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"v\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            effective_batch_size: 5000,
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            result.new_state.effective_batch_size, 10,
            "should reset to base"
        );
        assert_eq!(result.messages.len(), 2);
    }

    #[tokio::test]
    async fn poll_accumulates_processed_rows_in_state() {
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"v\":1}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"v\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            processed_rows: 7,
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.new_state.processed_rows, 9);
    }

    #[tokio::test]
    async fn poll_propagates_transient_http_error() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let base = start_server(app).await;
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await;
        assert!(matches!(result, Err(Error::Storage(_))));
    }

    #[tokio::test]
    async fn poll_permanent_http_error_propagates() {
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(|| async { StatusCode::BAD_REQUEST }),
        );
        let base = start_server(app).await;
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await;
        assert!(matches!(result, Err(Error::PermanentHttpError(_))));
    }

    const BASE: &str = "http://localhost:8181";

    #[test]
    fn build_query_url_path_and_body_fields() {
        let (url, body) = build_query(BASE, "SELECT * FROM cpu LIMIT 10", "sensors").unwrap();
        assert!(
            url.path().ends_with("/api/v3/query_sql"),
            "wrong path: {}",
            url.path()
        );
        assert!(
            url.query().is_none_or(|q| !q.contains("org=")),
            "org must not appear in URL"
        );
        assert_eq!(body["db"].as_str(), Some("sensors"));
        assert_eq!(body["format"].as_str(), Some("jsonl"));
        assert!(body["q"].as_str().unwrap().contains("SELECT"));
    }

    #[test]
    fn build_query_format_is_always_jsonl() {
        let (_, body) = build_query(BASE, "SELECT 1", "db").unwrap();
        assert_eq!(body["format"].as_str(), Some("jsonl"));
    }

    #[test]
    fn build_query_invalid_base_returns_error() {
        assert!(build_query("not-a-url", "SELECT 1", "db").is_err());
    }

    #[tokio::test]
    async fn poll_non_stuck_advance_resets_row_offset_to_zero() {
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"v\":1}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"v\":2}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some("1970-01-01T00:00:00Z".to_string()),
            last_timestamp_row_offset: 7,
            stuck_cursor: Some("2024-01-01T00:00:01Z".to_string()),
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            result.new_state.last_timestamp_row_offset, 0,
            "offset must reset to 0 after cursor advances"
        );
        assert_eq!(result.messages.len(), 2);
    }

    #[tokio::test]
    async fn poll_stuck_first_poll_sets_offset_and_inflation_resolves_stuck() {
        let t0 = "2024-01-01T00:00:00Z";
        let t1 = "2024-01-01T00:00:01Z";
        let jsonl_stuck: String = (0..10)
            .map(|i| format!("{{\"time\":\"{t0}\",\"val\":{i}}}\n"))
            .collect();
        let jsonl_advance: String = (0..5)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}\n"))
            .collect();

        let app1 = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl_stuck) }),
        );
        let base1 = start_server(app1).await;

        let state1 = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 10,
            last_timestamp_row_offset: 0,
            processed_rows: 0,
            stuck_cursor: None,
        };
        let r1 = poll(
            &make_client(),
            &make_config(&base1),
            "Bearer tok",
            &state1,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(r1.messages.len(), 10, "first stuck poll must emit messages");
        assert!(
            r1.new_state.last_timestamp_row_offset > 0,
            "first stuck poll must set offset > 0"
        );
        assert_eq!(
            r1.new_state.effective_batch_size, 20,
            "batch must double on stuck"
        );

        let app2 = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl_advance) }),
        );
        let base2 = start_server(app2).await;
        let r2 = poll(
            &make_client(),
            &make_config(&base2),
            "Bearer tok",
            &r1.new_state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(r2.messages.len(), 5, "advancing poll must emit messages");
        assert_eq!(
            r2.new_state.last_timestamp_row_offset, 0,
            "offset must reset to 0 after cursor advances"
        );
        assert_eq!(
            r2.new_state.last_timestamp.as_deref(),
            Some(t1),
            "cursor must advance to t1"
        );
    }

    #[tokio::test]
    async fn poll_cursor_does_not_advance_when_new_is_not_after_saved() {
        let saved_ts = "2024-01-01T00:00:02Z";
        let jsonl = format!(
            "{{\"time\":\"{saved_ts}\",\"val\":1}}\n\
             {{\"time\":\"{saved_ts}\",\"val\":2}}\n"
        );
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some(saved_ts.to_string()),
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(saved_ts),
            "cursor must not regress when new max == saved cursor"
        );
    }

    #[tokio::test]
    async fn poll_cursor_non_advancing_preserves_stuck_sequence_state() {
        let t0 = "2024-01-01T00:00:00Z";
        let t1 = "2024-01-01T00:00:01Z";
        let jsonl = format!(
            "{{\"time\":\"{t0}\",\"val\":1}}\n\
             {{\"time\":\"{t0}\",\"val\":2}}\n"
        );
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 20,
            last_timestamp_row_offset: 10,
            stuck_cursor: Some(t1.to_string()),
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(t0),
            "cursor must stay at t0"
        );
        assert_eq!(
            result.new_state.last_timestamp_row_offset, 10,
            "offset must be preserved, not reset to 0"
        );
        assert_eq!(
            result.new_state.stuck_cursor.as_deref(),
            Some(t1),
            "stuck_cursor must be preserved for livelock resolution"
        );
        assert_eq!(
            result.new_state.effective_batch_size, 20,
            "inflated batch size must be preserved"
        );
    }

    #[tokio::test]
    async fn poll_stuck_batch_emits_messages() {
        let t0 = "2024-01-01T00:00:00Z";
        let t1 = "2024-01-01T00:00:01Z";
        let jsonl: String = (0..10)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}\n"))
            .collect();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let state = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 10,
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            result.messages.len(),
            10,
            "stuck batch must emit rows, not discard them"
        );
        assert_eq!(
            result.new_state.last_timestamp.as_deref(),
            Some(t0),
            "cursor must not advance when stuck"
        );
        assert_eq!(
            result.new_state.effective_batch_size, 20,
            "batch must double"
        );
        assert!(!result.trip_circuit_breaker);
    }

    #[tokio::test]
    async fn poll_livelock_resolved_when_empty_batch_follows_stuck() {
        let t0 = "2024-01-01T00:00:00Z";
        let t1 = "2024-01-01T00:00:01Z";
        let jsonl_t1: String = (0..10)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}\n"))
            .collect();
        let cc = Arc::new(Mutex::new(0u32));
        let cc2 = cc.clone();
        let rows2 = jsonl_t1.clone();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || {
                let counter = cc2.clone();
                let rows = rows2.clone();
                async move {
                    let mut n = counter.lock().await;
                    *n += 1;
                    if *n == 1 {
                        (StatusCode::OK, rows)
                    } else {
                        (StatusCode::OK, String::new())
                    }
                }
            }),
        );
        let base = start_server(app).await;
        let state1 = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 10,
            ..V3State::default()
        };
        let r1 = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &state1,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            r1.new_state.last_timestamp_row_offset, 10,
            "offset must accumulate after stuck poll"
        );
        let r2 = poll(
            &make_client(),
            &make_config(&base),
            "Bearer tok",
            &r1.new_state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            r2.new_state.last_timestamp.as_deref(),
            Some(t1),
            "cursor must advance to T1 after empty follow-up confirms all rows at T1 seen; livelock not allowed"
        );
        assert_eq!(
            r2.new_state.last_timestamp_row_offset, 0,
            "offset must reset after cursor advance"
        );
        assert_eq!(
            r2.new_state.effective_batch_size, 10,
            "batch must reset to base"
        );
    }

    #[tokio::test]
    async fn poll_cb_trip_preserves_stuck_cursor() {
        let t0 = "2024-01-01T00:00:00Z";
        let t1 = "2024-01-01T00:00:01Z";
        let jsonl: String = (0..20)
            .map(|i| format!("{{\"time\":\"{t1}\",\"val\":{i}}}\n"))
            .collect();
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let config = V3SourceConfig {
            stuck_batch_cap_factor: Some(2),
            ..make_config(&base)
        };
        let state = V3State {
            last_timestamp: Some(t0.to_string()),
            effective_batch_size: 20,
            last_timestamp_row_offset: 10,
            processed_rows: 10,
            stuck_cursor: None,
        };
        let result = poll(
            &make_client(),
            &config,
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert!(result.trip_circuit_breaker, "must trip CB when at cap");
        assert!(result.messages.is_empty());
        assert_eq!(
            result.new_state.stuck_cursor.as_deref(),
            Some(t1),
            "stuck_cursor must be the stuck timestamp so cooldown poll can advance past it"
        );
        assert_eq!(
            result.new_state.effective_batch_size, 10,
            "CB trip must reset batch to base"
        );
    }

    #[tokio::test]
    async fn poll_v3_zero_batch_size_is_floored_to_one() {
        let t1 = "2024-01-01T00:00:01Z";
        let jsonl = format!("{{\"time\":\"{t1}\",\"val\":1}}\n");
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let mut config = make_config(&base);
        config.batch_size = Some(0);
        let state = V3State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            effective_batch_size: 0,
            ..V3State::default()
        };
        let result = poll(
            &make_client(),
            &config,
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();
        assert!(
            !result.trip_circuit_breaker,
            "batch_size=0 must floor to 1, not trip CB immediately"
        );
        assert_eq!(result.messages.len(), 1, "row must be emitted");
    }

    #[tokio::test]
    async fn poll_with_payload_column_returns_raw_schema() {
        let jsonl = "{\"time\":\"2024-01-01T00:00:01Z\",\"data\":\"aGVsbG8=\"}\n\
                     {\"time\":\"2024-01-01T00:00:02Z\",\"data\":\"d29ybGQ=\"}\n";
        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move || async move { (StatusCode::OK, jsonl) }),
        );
        let base = start_server(app).await;
        let config = V3SourceConfig {
            payload_column: Some("data".to_string()),
            ..make_config(&base)
        };
        let state = V3State::default();
        let result = poll(
            &make_client(),
            &config,
            "Bearer tok",
            &state,
            PayloadFormat::Raw,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.messages.len(), 2, "both rows must produce messages");
        assert_eq!(result.schema, Schema::Raw);
        assert_eq!(&result.messages[0].payload, b"hello");
        assert_eq!(&result.messages[1].payload, b"world");
    }

    #[tokio::test]
    async fn poll_clamps_persisted_effective_batch_size_to_new_cap() {
        let captured_body: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured_body.clone();

        let app = Router::new().route(
            "/api/v3/query_sql",
            post(move |request: Request| {
                let cap = cap2.clone();
                async move {
                    let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
                        .await
                        .unwrap();
                    *cap.lock().await = String::from_utf8_lossy(&bytes).to_string();
                    (StatusCode::OK, "")
                }
            }),
        );
        let base = start_server(app).await;

        let mut cfg = make_config(&base);
        cfg.batch_size = Some(10);
        cfg.stuck_batch_cap_factor = Some(2);

        let state = V3State {
            last_timestamp: Some("1970-01-01T00:00:00Z".to_string()),
            effective_batch_size: 5000,
            last_timestamp_row_offset: 0,
            processed_rows: 0,
            stuck_cursor: None,
        };

        let _ = poll(
            &make_client(),
            &cfg,
            "Bearer tok",
            &state,
            PayloadFormat::Json,
            true,
        )
        .await
        .unwrap();

        let body = captured_body.lock().await;
        assert!(
            body.contains("LIMIT 20") || body.contains("limit 20"),
            "expected outbound query to be clamped to LIMIT 20, got body: {body}"
        );
        assert!(
            !body.contains("LIMIT 5000") && !body.contains("limit 5000"),
            "must not send un-clamped LIMIT 5000, got body: {body}"
        );
    }
}
