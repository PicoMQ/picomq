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

use std::time::Duration;

use bytes::Bytes;
use picomq_connector_sdk::Error;
use rand::RngExt;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Deserialize;
use tracing::{debug, error, info, warn};

use crate::schema::{Column, parse_type};

const USER_HEADER: &str = "X-ClickHouse-User";
const KEY_HEADER: &str = "X-ClickHouse-Key";

#[derive(Debug)]
pub(crate) struct ClickHouseClient {
    inner: reqwest::Client,
    base_url: String,
    database: String,
    format_name: String,
    insert_url: String,
}

impl ClickHouseClient {
    pub fn new(
        base_url: String,
        database: String,
        format_name: String,
        username: &str,
        password: &str,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let mut auth_headers = HeaderMap::new();
        auth_headers.insert(
            USER_HEADER,
            HeaderValue::from_str(username)
                .map_err(|e| Error::InitError(format!("Invalid username header value: {e}")))?,
        );
        let mut key_value = HeaderValue::from_str(password)
            .map_err(|e| Error::InitError(format!("Invalid password header value: {e}")))?;
        key_value.set_sensitive(true);
        auth_headers.insert(KEY_HEADER, key_value);

        let inner = reqwest::Client::builder()
            .timeout(timeout)
            .default_headers(auth_headers)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build HTTP client: {e}")))?;

        let insert_url = format!(
            "{}/?database={}&date_time_input_format=best_effort",
            base_url,
            urlencoded(&database),
        );

        Ok(ClickHouseClient {
            inner,
            base_url,
            database,
            format_name,
            insert_url,
        })
    }

    pub fn insert_query(&self, table: &str) -> String {
        format!(
            "INSERT INTO `{}`.`{}` FORMAT {}",
            escape_backtick(&self.database),
            escape_backtick(table),
            self.format_name,
        )
    }

    pub async fn ping(&self) -> Result<(), Error> {
        let url = format!("{}/ping", self.base_url);
        let response = self
            .inner
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::InitError(format!("Ping failed: {e}")))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!("ClickHouse ping returned HTTP {status}: {body}");
            Err(Error::InitError(format!(
                "ClickHouse ping returned HTTP {status}: {body}"
            )))
        }
    }

    pub async fn fetch_schema(&self, table: &str) -> Result<Vec<Column>, Error> {
        let query = format!(
            "SELECT name, type, default_kind FROM system.columns \
             WHERE database = '{}' AND table = '{}' \
             ORDER BY position \
             FORMAT JSONEachRow",
            escape_single_quote(&self.database),
            escape_single_quote(table),
        );

        let body = self.run_query(&query).await?;
        let columns = parse_schema_body(&body)?;

        if columns.is_empty() {
            error!(
                "Table '{table}' not found or has no columns in database '{}'",
                self.database
            );
            return Err(Error::InitError(format!(
                "Table '{table}' not found in database '{}'",
                self.database
            )));
        }

        info!(
            "Fetched schema for table '{table}': {} columns",
            columns.len()
        );
        Ok(columns)
    }

    pub async fn insert(
        &self,
        table: &str,
        deduplication_token: &str,
        body: Vec<u8>,
        max_retries: u32,
        retry_delay: Duration,
    ) -> Result<(), Error> {
        if body.is_empty() {
            debug!("insert called with empty body, skipping");
            return Ok(());
        }

        let query = self.insert_query(table);
        let body = Bytes::from(body);
        let mut attempts = 0u32;
        loop {
            let result = self
                .inner
                .post(&self.insert_url)
                .header(CONTENT_TYPE, "application/octet-stream")
                .query(&[
                    ("query", query.as_str()),
                    ("insert_deduplication_token", deduplication_token),
                ])
                .body(body.clone())
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        debug!(
                            "Inserted {} bytes into {}.{table} FORMAT {}",
                            body.len(),
                            self.database,
                            self.format_name
                        );
                        return Ok(());
                    }

                    let body_text = response.text().await.unwrap_or_default();

                    if is_retryable_status(status) {
                        attempts += 1;
                        if attempts >= max_retries {
                            error!(
                                "Insert failed after {attempts} attempts (HTTP {status}): {body_text}"
                            );
                            return Err(Error::CannotStoreData(format!(
                                "HTTP {status}: {body_text}"
                            )));
                        }
                        warn!(
                            "Retryable HTTP {status} on attempt {attempts}/{max_retries}: {body_text}"
                        );
                        tokio::time::sleep(jittered_backoff(retry_delay, attempts)).await;
                    } else {
                        error!("ClickHouse insert error HTTP {status}: {body_text}");
                        return Err(Error::PermanentHttpError(format!(
                            "HTTP {status}: {body_text}"
                        )));
                    }
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_retries {
                        error!("Insert failed after {attempts} attempts: {e}");
                        return Err(Error::CannotStoreData(format!(
                            "Network error after {attempts} attempts: {e}"
                        )));
                    }
                    warn!("Network error on attempt {attempts}/{max_retries}: {e}. Retrying...");
                    tokio::time::sleep(jittered_backoff(retry_delay, attempts)).await;
                }
            }
        }
    }

    async fn run_query(&self, query: &str) -> Result<String, Error> {
        let url = format!("{}/?database={}", self.base_url, urlencoded(&self.database));
        let response = self
            .inner
            .post(&url)
            .body(query.to_owned())
            .send()
            .await
            .map_err(|e| Error::InitError(format!("Query failed: {e}")))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| Error::InitError(format!("Failed to read response: {e}")))?;

        if !status.is_success() {
            error!("Query returned HTTP {status}: {body}");
            return Err(Error::InitError(format!("HTTP {status}: {body}")));
        }
        Ok(body)
    }
}

#[derive(Deserialize)]
struct SchemaRow {
    name: String,
    r#type: String,
    default_kind: Option<String>,
}

fn parse_schema_body(body: &str) -> Result<Vec<Column>, Error> {
    let mut columns = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let row: SchemaRow = serde_json::from_str(line).map_err(|e| {
            error!("Failed to parse schema row '{line}': {e}");
            Error::InitError(format!("Schema parse error: {e}"))
        })?;

        let default_kind = row.default_kind.as_deref().unwrap_or("");
        match default_kind {
            "MATERIALIZED" | "ALIAS" | "EPHEMERAL" => continue,
            _ => {}
        }

        let ch_type = parse_type(&row.r#type)?;
        columns.push(Column {
            name: row.name,
            ch_type,
            has_default: default_kind == "DEFAULT",
        });
    }
    Ok(columns)
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT
    ) || status.is_server_error()
}

pub(crate) fn jittered_backoff(base: Duration, attempt: u32) -> Duration {
    const MAX: Duration = Duration::from_secs(60);
    let cap = base.saturating_mul(2u32.saturating_pow(attempt)).min(MAX);
    let cap_ms = cap.as_millis() as u64;
    Duration::from_millis(rand::rng().random_range(0..=cap_ms))
}

fn escape_single_quote(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

fn escape_backtick(s: &str) -> String {
    s.replace('\\', "\\\\").replace('`', "``")
}

fn urlencoded(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(ch),
            other => {
                let mut buf = [0u8; 4];
                for byte in other.encode_utf8(&mut buf).bytes() {
                    out.push_str(&format!("%{byte:02X}"));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_single_quote_plain() {
        assert_eq!(escape_single_quote("hello"), "hello");
    }

    #[test]
    fn escape_single_quote_doubles_quote() {
        assert_eq!(escape_single_quote("it's"), "it''s");
    }

    #[test]
    fn escape_single_quote_trailing_backslash_cannot_break_out() {
        let result = escape_single_quote("foo\\");
        assert_eq!(result, "foo\\\\");
        assert!(result.matches('\\').count().is_multiple_of(2));
    }

    #[test]
    fn escape_single_quote_backslash_quote_pair() {
        assert_eq!(escape_single_quote("a\\'b"), "a\\\\''b");
    }

    #[test]
    fn escape_backtick_plain() {
        assert_eq!(escape_backtick("my_table"), "my_table");
    }

    #[test]
    fn escape_backtick_doubles_backtick() {
        assert_eq!(escape_backtick("ta`ble"), "ta``ble");
    }

    #[test]
    fn escape_backtick_trailing_backslash_cannot_break_out() {
        let result = escape_backtick("innocent\\");
        assert_eq!(result, "innocent\\\\");
        assert!(result.matches('\\').count().is_multiple_of(2));
    }

    fn test_client(database: &str, format_name: &str) -> ClickHouseClient {
        ClickHouseClient::new(
            "http://localhost:8123".to_owned(),
            database.to_owned(),
            format_name.to_owned(),
            "user",
            "pass",
            Duration::from_secs(10),
        )
        .expect("client construction failed")
    }

    #[test]
    fn given_plain_config_new_should_precompute_insert_url() {
        let client = test_client("mydb", "JSONEachRow");
        assert_eq!(
            client.insert_url,
            "http://localhost:8123/?database=mydb&date_time_input_format=best_effort"
        );
    }

    #[test]
    fn given_plain_table_insert_query_should_quote_database_and_table() {
        let client = test_client("mydb", "JSONEachRow");
        assert_eq!(
            client.insert_query("events"),
            "INSERT INTO `mydb`.`events` FORMAT JSONEachRow"
        );
    }

    #[test]
    fn given_database_needing_encoding_new_should_percent_encode_insert_url() {
        let client = test_client("my db", "JSONEachRow");
        assert!(
            client.insert_url.contains("database=my%20db"),
            "URL was: {}",
            client.insert_url
        );
    }

    #[test]
    fn given_materialized_alias_ephemeral_columns_parse_schema_body_should_drop_them() {
        let body = concat!(
            r#"{"name":"id","type":"UInt64","default_kind":""}"#,
            "\n",
            r#"{"name":"m","type":"UInt64","default_kind":"MATERIALIZED"}"#,
            "\n",
            r#"{"name":"a","type":"UInt64","default_kind":"ALIAS"}"#,
            "\n",
            r#"{"name":"e","type":"UInt64","default_kind":"EPHEMERAL"}"#,
            "\n",
            r#"{"name":"name","type":"String","default_kind":""}"#,
        );
        let columns = parse_schema_body(body).unwrap();
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name"]);
        assert!(columns.iter().all(|c| !c.has_default));
    }

    #[test]
    fn given_default_column_parse_schema_body_should_flag_has_default() {
        let body = concat!(
            r#"{"name":"id","type":"UInt64","default_kind":""}"#,
            "\n",
            r#"{"name":"created","type":"DateTime","default_kind":"DEFAULT"}"#,
        );
        let columns = parse_schema_body(body).unwrap();
        assert_eq!(columns.len(), 2);
        assert!(!columns[0].has_default);
        assert!(columns[1].has_default);
    }

    #[test]
    fn given_names_with_backticks_insert_query_should_double_escape() {
        let client = test_client("my`db", "JSONEachRow");
        assert_eq!(
            client.insert_query("ta`ble"),
            "INSERT INTO `my``db`.`ta``ble` FORMAT JSONEachRow"
        );
    }
}
