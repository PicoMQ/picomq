use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use humantime::Duration as HumanDuration;
use picomq_connector_sdk::destination::DestinationTemplate;
use picomq_connector_sdk::retry::{exponential_backoff, jitter};
use picomq_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use reqwest::{Method, StatusCode, header};
use secrecy::zeroize::Zeroizing;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use simd_json::{OwnedValue, StaticNode};
use std::io::Write as _;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, error, info, warn};

sink_connector!(DorisSink);

const DEFAULT_LABEL_PREFIX: &str = "picomq";
const DEFAULT_BATCH_SIZE: u32 = 1000;
const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_CONNECT_TIMEOUT: &str = "5s";
const MAX_REDIRECTS: u8 = 5;
const MAX_LABEL_PREFIX_LEN: usize = 16;
const MAX_LABEL_NAME_LEN: usize = 16;
const LABEL_HASH_HEX_LEN: usize = 16;
const MAX_RESPONSE_LOG_BYTES: usize = 4096;
const DEFAULT_MAX_RETRIES: u32 = 3;
const MAX_RETRIES_WARNING_THRESHOLD: u32 = 10;
const DEFAULT_RETRY_DELAY: &str = "200ms";
const DEFAULT_MAX_RETRY_DELAY: &str = "5s";
const CSV_COLUMN_SEPARATOR: u8 = 0x01;
const CSV_LINE_DELIMITER: u8 = 0x02;
const CSV_ENCLOSE: u8 = b'"';
const CSV_ESCAPE: u8 = b'\\';
const CSV_NULL_MARKER: &[u8] = b"\\N";
const CSV_COLUMN_SEPARATOR_HEADER: &str = "\\x01";
const CSV_LINE_DELIMITER_HEADER: &str = "\\x02";
const CSV_ENCLOSE_HEADER: &str = "\"";
const CSV_ESCAPE_HEADER: &str = "\\";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    #[default]
    Json,
    Csv,
}

#[derive(Debug)]
pub struct DorisSink {
    id: u32,
    config: DorisSinkConfig,
    auth_header: header::HeaderValue,
    connected: Option<Connected>,
}

#[derive(Debug)]
struct Connected {
    client: reqwest::Client,
    base_url: reqwest::Url,
    max_filter_ratio_header: Option<header::HeaderValue>,
    columns_header: Option<header::HeaderValue>,
    where_header: Option<header::HeaderValue>,
    allow_insecure_redirect: bool,
    allowed_redirect_hosts: Option<Vec<String>>,
    max_retries: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
    format: Format,
    csv_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct DorisSinkConfig {
    pub fe_url: String,
    pub database: String,
    pub table: DestinationTemplate,
    pub username: String,
    pub password: SecretString,
    pub label_prefix: Option<String>,
    pub max_filter_ratio: Option<f64>,
    pub columns: Option<String>,
    #[serde(rename = "where")]
    pub where_clause: Option<String>,
    pub output_format: Option<Format>,
    pub timeout: Option<String>,
    pub connect_timeout: Option<String>,
    pub batch_size: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub allow_insecure_redirect: Option<bool>,
    pub allowed_redirect_hosts: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct StreamLoadResponse {
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Message")]
    #[serde(default)]
    message: String,
    #[serde(rename = "NumberLoadedRows")]
    #[serde(default)]
    number_loaded_rows: u64,
    #[serde(rename = "NumberFilteredRows")]
    #[serde(default)]
    number_filtered_rows: u64,
    #[serde(rename = "ExistingJobStatus")]
    #[serde(default)]
    existing_job_status: Option<String>,
}

impl DorisSink {
    pub fn new(id: u32, config: DorisSinkConfig) -> Self {
        let credential = Zeroizing::new(format!(
            "{}:{}",
            config.username,
            config.password.expose_secret()
        ));
        let encoded = Zeroizing::new(general_purpose::STANDARD.encode(credential.as_bytes()));
        let auth_value = Zeroizing::new(format!("Basic {}", encoded.as_str()));
        let mut auth_header = header::HeaderValue::from_str(&auth_value)
            .expect("Basic auth header is always valid ASCII");
        auth_header.set_sensitive(true);

        DorisSink {
            id,
            config,
            auth_header,
            connected: None,
        }
    }

    fn build_client(&self) -> Result<reqwest::Client, Error> {
        let timeout = parse_request_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        let connect_timeout = parse_request_duration(
            self.config.connect_timeout.as_deref(),
            DEFAULT_CONNECT_TIMEOUT,
        );
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .connect_timeout(connect_timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build Doris HTTP client: {e}")))
    }

    async fn send_stream_load(
        &self,
        connected: &Connected,
        table: &str,
        label: &str,
        body: Bytes,
    ) -> Result<StreamLoadResponse, Error> {
        let mut url = stream_load_url(&connected.base_url, &self.config.database, table);
        let mut redirects = 0u8;

        loop {
            let mut request = connected
                .client
                .request(Method::PUT, url.clone())
                .header(header::AUTHORIZATION, self.auth_header.clone())
                .header(header::EXPECT, "100-continue")
                .header("label", label)
                .body(body.clone());

            request = match connected.format {
                Format::Json => request
                    .header("format", "json")
                    .header("strip_outer_array", "true"),
                Format::Csv => request
                    .header("format", "csv")
                    .header("column_separator", CSV_COLUMN_SEPARATOR_HEADER)
                    .header("line_delimiter", CSV_LINE_DELIMITER_HEADER)
                    .header("enclose", CSV_ENCLOSE_HEADER)
                    .header("escape", CSV_ESCAPE_HEADER),
            };

            if let Some(value) = &connected.max_filter_ratio_header {
                request = request.header("max_filter_ratio", value.clone());
            }
            if let Some(value) = &connected.columns_header {
                request = request.header("columns", value.clone());
            }
            if let Some(value) = &connected.where_header {
                request = request.header("where", value.clone());
            }

            let response = request.send().await.map_err(|e| {
                error!("Doris sink ID {} HTTP request failed: {e}", self.id);
                Error::HttpRequestFailed(e.to_string())
            })?;

            let status = response.status();
            if matches!(
                status,
                StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
            ) {
                redirects += 1;
                if redirects > MAX_REDIRECTS {
                    return Err(Error::PermanentHttpError(format!(
                        "Doris sink ID {} exceeded max redirects ({MAX_REDIRECTS})",
                        self.id
                    )));
                }
                let Some(location) = response
                    .headers()
                    .get(header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                else {
                    return Err(Error::PermanentHttpError(format!(
                        "Doris sink ID {} got {status} with no Location header",
                        self.id
                    )));
                };
                let target = reqwest::Url::parse(location).map_err(|e| {
                    Error::PermanentHttpError(format!(
                        "Doris sink ID {} got {status} with non-absolute or unparsable Location '{location}': {e}",
                        self.id
                    ))
                })?;
                connected.validate_redirect(&target, self.id)?;
                debug!("Doris sink ID {} following redirect to {target}", self.id);
                url = target;
                continue;
            }

            let is_success = status.is_success();
            let response_text = match response.text().await {
                Ok(text) => text,
                Err(e) if is_success => {
                    warn!(
                        "Doris sink ID {} failed to read 2xx response body: {e}; treating as retryable",
                        self.id
                    );
                    return Err(Error::CannotStoreData(format!(
                        "Doris sink ID {} could not read 2xx Stream Load response body: {e}",
                        self.id
                    )));
                }
                Err(e) => {
                    warn!(
                        "Doris sink ID {} failed to read response body: {e}",
                        self.id
                    );
                    String::new()
                }
            };
            let response_for_log = truncate_for_log(&response_text, MAX_RESPONSE_LOG_BYTES);

            if !is_success {
                let msg = format!(
                    "Doris sink ID {} stream load returned HTTP {status}: {response_for_log}",
                    self.id
                );
                error!("{msg}");
                return Err(match status {
                    StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS => {
                        Error::CannotStoreData(msg)
                    }
                    s if s.is_server_error() => Error::CannotStoreData(msg),
                    _ => Error::PermanentHttpError(msg),
                });
            }

            return parse_stream_load_response(&response_text);
        }
    }

    async fn load_batch(
        &self,
        table: &str,
        label: &str,
        body: Bytes,
    ) -> Result<StreamLoadResponse, Error> {
        let connected = self.connected.as_ref().ok_or_else(|| {
            Error::InitError(format!(
                "Doris sink ID {} called before open() - not connected",
                self.id
            ))
        })?;

        let mut attempt = 0u32;
        loop {
            let error = match self
                .send_stream_load(connected, table, label, body.clone())
                .await
                .and_then(|response| classify_status(self.id, &response).map(|()| response))
            {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };

            attempt += 1;
            if attempt >= connected.max_retries || !is_transient_error(&error) {
                return Err(error);
            }

            let delay = jitter(exponential_backoff(
                connected.retry_delay,
                attempt - 1,
                connected.max_retry_delay,
            ))
            .min(connected.max_retry_delay);
            warn!(
                "Doris sink ID {} transient Stream Load failure on attempt {attempt}/{} (label={label}): {error}; retrying in {delay:?}",
                self.id, connected.max_retries
            );
            tokio::time::sleep(delay).await;
        }
    }
}

impl Connected {
    fn validate_redirect(&self, target: &reqwest::Url, id: u32) -> Result<(), Error> {
        let scheme = target.scheme();
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id}: refusing redirect to non-HTTP(S) scheme '{scheme}'"
            )));
        }

        let downgraded = self.base_url.scheme().eq_ignore_ascii_case("https")
            && !target.scheme().eq_ignore_ascii_case("https");
        if downgraded && !self.allow_insecure_redirect {
            return Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id}: refusing redirect that downgrades {} -> {} \
                 (would leak credentials in cleartext; set allow_insecure_redirect=true \
                 to permit a known-insecure FE -> BE topology)",
                self.base_url.scheme(),
                target.scheme(),
            )));
        }

        if let Some(allowed) = self.allowed_redirect_hosts.as_deref()
            && !allowed.is_empty()
            && !redirect_target_allowed(allowed, target)
        {
            return Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id}: redirect target '{}:{}' is not in allowed_redirect_hosts",
                target.host_str().unwrap_or(""),
                target
                    .port_or_known_default()
                    .map(|p| p.to_string())
                    .unwrap_or_default(),
            )));
        }

        Ok(())
    }
}

fn redirect_target_allowed(allowed: &[String], target: &reqwest::Url) -> bool {
    let raw_host = target.host_str().unwrap_or("");
    let host = strip_brackets(raw_host);
    let port = target.port_or_known_default();
    allowed.iter().any(|entry| match split_host_port(entry) {
        (entry_host, Some(entry_port)) => {
            entry_host.eq_ignore_ascii_case(host) && Some(entry_port) == port
        }
        (entry_host, None) => entry_host.eq_ignore_ascii_case(host),
    })
}

fn strip_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

fn split_host_port(entry: &str) -> (&str, Option<u16>) {
    if let Some(rest) = entry.strip_prefix('[') {
        if let Some((host, after)) = rest.split_once(']') {
            let port = after.strip_prefix(':').and_then(|p| p.parse::<u16>().ok());
            return (host, port);
        }
        return (entry, None);
    }
    if entry.matches(':').count() > 1 {
        return (entry, None);
    }
    if let Some((host, port)) = entry.rsplit_once(':')
        && !port.is_empty()
        && let Ok(port) = port.parse::<u16>()
    {
        (host, Some(port))
    } else {
        (entry, None)
    }
}

fn parse_duration(input: Option<&str>, default: &str) -> Duration {
    let raw = input.unwrap_or(default);
    HumanDuration::from_str(raw)
        .map(|d| *d)
        .unwrap_or_else(|e| {
            warn!("Invalid duration '{raw}': {e}, using default '{default}'");
            *HumanDuration::from_str(default).expect("default duration must be valid")
        })
}

fn parse_request_duration(input: Option<&str>, default: &str) -> Duration {
    let raw = input.unwrap_or(default);
    let parsed = parse_duration(input, default);
    if parsed.is_zero() {
        warn!(
            "Duration '{raw}' is zero, which would time out every request immediately; \
             using default '{default}'"
        );
        return *HumanDuration::from_str(default).expect("default duration must be valid");
    }
    parsed
}

fn stream_load_url(base_url: &reqwest::Url, database: &str, table: &str) -> reqwest::Url {
    let mut url = base_url.clone();
    url.set_path(&format!("/api/{database}/{table}/_stream_load"));
    url
}

fn parse_fe_url(id: u32, fe_url: &str) -> Result<reqwest::Url, Error> {
    let url = reqwest::Url::parse(fe_url).map_err(|e| {
        Error::InvalidConfigValue(format!(
            "Doris sink ID {id} has invalid fe_url '{fe_url}': {e}"
        ))
    })?;
    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(Error::InvalidConfigValue(format!(
            "Doris sink ID {id} fe_url '{fe_url}' must use http or https, got '{scheme}'"
        )));
    }
    Ok(url)
}

fn resolve_table(template: &DestinationTemplate, topic: &str, id: u32) -> Result<String, Error> {
    let resolved = template.resolve(topic)?;
    let table = if template.is_static() {
        resolved
    } else {
        sanitize_identifier(&resolved)
    };
    validate_identifier(&table, "table", id)?;
    Ok(table)
}

fn sanitize_identifier(raw: &str) -> String {
    let mut sanitized: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        sanitized.insert(0, '_');
    }
    sanitized
}

fn effective_batch_size(configured: Option<u32>) -> usize {
    configured.unwrap_or(DEFAULT_BATCH_SIZE).max(1) as usize
}

fn effective_max_retries(configured: Option<u32>) -> u32 {
    configured.unwrap_or(DEFAULT_MAX_RETRIES).max(1)
}

fn should_warn_for_retry_count(max_retries: u32) -> bool {
    max_retries > MAX_RETRIES_WARNING_THRESHOLD
}

fn csv_column_names(columns: &str) -> Vec<String> {
    columns
        .split(',')
        .map(str::trim)
        .take_while(|name| !name.is_empty() && !name.contains('='))
        .map(str::to_string)
        .collect()
}

fn build_request_body(
    format: Format,
    rows: &[&OwnedValue],
    csv_columns: &[String],
) -> Result<Bytes, Error> {
    match format {
        Format::Json => serialize_json_batch(rows),
        Format::Csv => build_csv_body(rows, csv_columns).map(Bytes::from),
    }
}

fn build_csv_body(rows: &[&OwnedValue], columns: &[String]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(rows.len() * columns.len() * 16);
    for &row in rows {
        let OwnedValue::Object(object) = row else {
            return Err(Error::InvalidRecordValue(
                "Doris CSV format requires each message to be a JSON object".to_string(),
            ));
        };
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(CSV_COLUMN_SEPARATOR);
            }
            match object.get(column.as_str()) {
                None => out.extend_from_slice(CSV_NULL_MARKER),
                Some(value) => encode_csv_field(value, &mut out),
            }
        }
        out.push(CSV_LINE_DELIMITER);
    }
    Ok(out)
}

fn encode_csv_field(value: &OwnedValue, out: &mut Vec<u8>) {
    match value {
        OwnedValue::Static(StaticNode::Null) => out.extend_from_slice(CSV_NULL_MARKER),
        OwnedValue::Static(StaticNode::Bool(flag)) => {
            out.extend_from_slice(if *flag { b"true" } else { b"false" });
        }
        OwnedValue::Static(StaticNode::I64(number)) => {
            let _ = write!(out, "{number}");
        }
        OwnedValue::Static(StaticNode::U64(number)) => {
            let _ = write!(out, "{number}");
        }
        OwnedValue::Static(StaticNode::F64(number)) => {
            let _ = write!(out, "{number}");
        }
        OwnedValue::String(text) => encode_csv_enclosed(text.as_bytes(), out),
        nested => match simd_json::to_string(nested) {
            Ok(json) => encode_csv_enclosed(json.as_bytes(), out),
            Err(_) => out.extend_from_slice(CSV_NULL_MARKER),
        },
    }
}

fn encode_csv_enclosed(bytes: &[u8], out: &mut Vec<u8>) {
    out.push(CSV_ENCLOSE);
    for &byte in bytes {
        if byte == CSV_ESCAPE || byte == CSV_ENCLOSE {
            out.push(CSV_ESCAPE);
        }
        out.push(byte);
    }
    out.push(CSV_ENCLOSE);
}

fn validated_header(field: &str, value: &str, id: u32) -> Result<header::HeaderValue, Error> {
    header::HeaderValue::from_str(value).map_err(|e| {
        Error::InvalidConfigValue(format!(
            "Doris sink ID {id}: '{field}' header value is invalid (must be visible ASCII, no CR/LF): {e}"
        ))
    })
}

fn sanitize_segment(value: &str, max_len: usize) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(max_len)
        .collect()
}

fn identity_hash(prefix: &str, table: &str, topic: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [prefix, table, topic] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let hash = hasher.finalize().to_hex();
    hash.as_str()[..LABEL_HASH_HEX_LEN].to_string()
}

#[doc(hidden)]
pub fn build_label(
    prefix: &str,
    table: &str,
    topic: &str,
    partition: i32,
    first_offset: u64,
    last_offset: u64,
) -> String {
    format!(
        "{}-{}-{}-{}-{}-{}",
        sanitize_segment(prefix, MAX_LABEL_PREFIX_LEN),
        sanitize_segment(topic, MAX_LABEL_NAME_LEN),
        identity_hash(prefix, table, topic),
        partition,
        first_offset,
        last_offset,
    )
}

fn truncate_for_log(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...(truncated, total {} bytes)", &s[..end], s.len())
}

fn serialize_json_batch<T>(batch: &T) -> Result<Bytes, Error>
where
    T: Serialize + ?Sized,
{
    simd_json::to_vec(batch)
        .map(Bytes::from)
        .map_err(|e| Error::Serialization(format!("Failed to serialize batch for Doris: {e}")))
}

fn parse_stream_load_response(body: &str) -> Result<StreamLoadResponse, Error> {
    if body.is_empty() {
        return Err(Error::CannotStoreData(
            "Doris Stream Load returned an empty 2xx response body".to_string(),
        ));
    }

    serde_json::from_str(body).map_err(|e| {
        Error::PermanentHttpError(format!(
            "Failed to parse Doris stream load response: {e}. Body: {}",
            truncate_for_log(body, MAX_RESPONSE_LOG_BYTES)
        ))
    })
}

fn validate_identifier(name: &str, field: &str, id: u32) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::InvalidConfigValue(format!(
            "Doris sink ID {id}: {field} must not be empty"
        )));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(Error::InvalidConfigValue(format!(
            "Doris sink ID {id}: {field} '{name}' must match [A-Za-z0-9_]+ (picomq's stricter subset of Doris identifiers, used as a path-traversal guard)"
        )));
    }
    Ok(())
}

fn is_transient_error(error: &Error) -> bool {
    matches!(
        error,
        Error::CannotStoreData(_) | Error::HttpRequestFailed(_)
    )
}

fn classify_status(id: u32, response: &StreamLoadResponse) -> Result<(), Error> {
    match response.status.as_str() {
        "Success" => Ok(()),
        "Label Already Exists" => match response.existing_job_status.as_deref() {
            Some("FINISHED") => {
                info!(
                    "Doris sink ID {id} confirmed duplicate label belongs to a FINISHED job; treating as success"
                );
                Ok(())
            }
            Some(existing_status @ ("RUNNING" | "CANCELLED")) => {
                Err(Error::CannotStoreData(format!(
                    "Doris sink ID {id} found duplicate label with retryable existing job status '{}': {}",
                    existing_status, response.message
                )))
            }
            Some(existing_status) => Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id} found duplicate label with unsupported existing job status '{existing_status}': {}",
                response.message
            ))),
            None => Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id} found duplicate label without ExistingJobStatus: {}",
                response.message
            ))),
        },
        "Publish Timeout" => {
            warn!(
                "Doris sink ID {id} stream load committed but publish visibility timed out; treating as success: {}",
                response.message
            );
            Ok(())
        }
        "Fail" => Err(Error::PermanentHttpError(format!(
            "Doris sink ID {id} stream load failed: {}",
            response.message
        ))),
        other => Err(Error::PermanentHttpError(format!(
            "Doris sink ID {id} stream load returned unexpected status '{other}': {}",
            response.message
        ))),
    }
}

#[async_trait]
impl Sink for DorisSink {
    async fn open(&mut self) -> Result<(), Error> {
        validate_identifier(&self.config.database, "database", self.id)?;
        if self.config.table.is_static() {
            validate_identifier(&self.config.table.to_string(), "table", self.id)?;
        }

        let base_url = parse_fe_url(self.id, &self.config.fe_url)?;

        if self.config.password.expose_secret().is_empty() {
            warn!(
                "Doris sink ID {} is configured with an empty password for user '{}'; \
                 this is accepted but is usually a misconfiguration.",
                self.id, self.config.username
            );
        }

        if base_url.scheme().eq_ignore_ascii_case("http") {
            let host = base_url.host_str().unwrap_or("");
            let is_loopback = host == "localhost"
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback());
            if !is_loopback {
                warn!(
                    "Doris sink ID {} is configured with http:// to non-loopback host '{}'; \
                     credentials and message data will be transmitted in cleartext. \
                     Use https:// in production.",
                    self.id, host
                );
            }
        }

        let max_filter_ratio_header = match self.config.max_filter_ratio {
            Some(ratio) => {
                if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
                    return Err(Error::InvalidConfigValue(format!(
                        "Doris sink ID {}: max_filter_ratio must be a finite value in [0.0, 1.0], got {ratio}",
                        self.id
                    )));
                }
                Some(validated_header(
                    "max_filter_ratio",
                    &ratio.to_string(),
                    self.id,
                )?)
            }
            None => None,
        };
        let columns_header = match self.config.columns.as_deref() {
            Some(columns) => Some(validated_header("columns", columns, self.id)?),
            None => None,
        };
        let where_header = match self.config.where_clause.as_deref() {
            Some(where_clause) => Some(validated_header("where", where_clause, self.id)?),
            None => None,
        };

        let retry_delay = parse_duration(self.config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let max_retry_delay = parse_duration(
            self.config.max_retry_delay.as_deref(),
            DEFAULT_MAX_RETRY_DELAY,
        );
        let max_retries = effective_max_retries(self.config.max_retries);
        if should_warn_for_retry_count(max_retries) {
            warn!(
                "Doris sink ID {} configured max_retries={max_retries}, above the warning threshold {MAX_RETRIES_WARNING_THRESHOLD}; the value is honored, but an unavailable FE can keep each chunk in consume() for tens of minutes or hours and delay graceful shutdown",
                self.id
            );
        }
        let (retry_delay, max_retry_delay) = if retry_delay > max_retry_delay {
            warn!(
                "Doris sink ID {}: retry_delay ({retry_delay:?}) exceeds max_retry_delay ({max_retry_delay:?}); clamping base to the cap",
                self.id
            );
            (max_retry_delay, max_retry_delay)
        } else {
            (retry_delay, max_retry_delay)
        };

        let format = self.config.output_format.unwrap_or_default();
        let csv_columns = match format {
            Format::Json => Vec::new(),
            Format::Csv => {
                let columns = self
                    .config
                    .columns
                    .as_deref()
                    .map(csv_column_names)
                    .unwrap_or_default();
                if columns.is_empty() {
                    return Err(Error::InvalidConfigValue(format!(
                        "Doris sink ID {}: format=\"csv\" requires a non-empty `columns` listing \
                         the source columns in order (CSV is positional, unlike name-mapped JSON)",
                        self.id
                    )));
                }
                columns
            }
        };

        self.connected = Some(Connected {
            client: self.build_client()?,
            base_url,
            max_filter_ratio_header,
            columns_header,
            where_header,
            allow_insecure_redirect: self.config.allow_insecure_redirect.unwrap_or(false),
            allowed_redirect_hosts: self.config.allowed_redirect_hosts.clone(),
            max_retries,
            retry_delay,
            max_retry_delay,
            format,
            csv_columns,
        });

        info!(
            "Opened Doris sink ID {} for {}.{} at {}",
            self.id, self.config.database, self.config.table, self.config.fe_url
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let total = messages.len();
        let table = resolve_table(&self.config.table, &topic_metadata.topic, self.id)?;
        debug!(
            "Doris sink ID {} received {total} messages from topic {} for {}.{table}",
            self.id, topic_metadata.topic, self.config.database
        );

        let batch_size = effective_batch_size(self.config.batch_size);
        let label_prefix = self
            .config
            .label_prefix
            .as_deref()
            .unwrap_or(DEFAULT_LABEL_PREFIX);
        let connected = self.connected.as_ref();
        let mut first_error: Option<Error> = None;

        for chunk in messages.chunks(batch_size) {
            let json_values: Vec<&simd_json::OwnedValue> = chunk
                .iter()
                .map(|m| match &m.payload {
                    Payload::Json(value) => Ok(value),
                    _ => {
                        error!(
                            "Doris sink ID {} received non-JSON payload (schema={}); aborting poll",
                            self.id, messages_metadata.schema
                        );
                        Err(Error::InvalidPayloadType)
                    }
                })
                .collect::<Result<_, _>>()?;

            let Some((first_msg, last_msg)) = chunk.first().zip(chunk.last()) else {
                continue;
            };

            let connected = connected.ok_or_else(|| {
                Error::InitError(format!(
                    "Doris sink ID {} called before open() - not connected",
                    self.id
                ))
            })?;
            let body =
                match build_request_body(connected.format, &json_values, &connected.csv_columns) {
                    Ok(body) => body,
                    Err(error) => {
                        error!(
                            "Doris sink ID {} failed to serialize batch: {error}",
                            self.id
                        );
                        first_error.get_or_insert(error);
                        continue;
                    }
                };

            let label = build_label(
                label_prefix,
                &table,
                &topic_metadata.topic,
                messages_metadata.partition,
                first_msg.offset,
                last_msg.offset,
            );

            match self.load_batch(&table, &label, body).await {
                Ok(response) => {
                    if response.number_filtered_rows > 0 {
                        warn!(
                            "Doris sink ID {} loaded {} rows but FILTERED {} rows for {}.{} (label={label}); \
                             likely schema drift upstream",
                            self.id,
                            response.number_loaded_rows,
                            response.number_filtered_rows,
                            self.config.database,
                            table,
                        );
                    } else {
                        debug!(
                            "Doris sink ID {} loaded {} rows into {}.{} (label={label})",
                            self.id, response.number_loaded_rows, self.config.database, table,
                        );
                    }
                }
                Err(error) => {
                    error!(
                        "Doris sink ID {} batch failed (label={label}): {error}",
                        self.id
                    );
                    first_error.get_or_insert(error);
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Doris sink ID {} closed.", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> DorisSinkConfig {
        DorisSinkConfig {
            fe_url: "http://localhost:8030".into(),
            database: "test_db".into(),
            table: "test_tbl".parse().unwrap(),
            username: "root".into(),
            password: SecretString::from("pw"),
            label_prefix: None,
            max_filter_ratio: None,
            columns: None,
            where_clause: None,
            output_format: None,
            timeout: None,
            connect_timeout: None,
            batch_size: None,
            allow_insecure_redirect: None,
            allowed_redirect_hosts: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
        }
    }

    fn stream_load_response(status: &str, existing_job_status: Option<&str>) -> StreamLoadResponse {
        StreamLoadResponse {
            status: status.into(),
            message: String::new(),
            number_loaded_rows: 0,
            number_filtered_rows: 0,
            existing_job_status: existing_job_status.map(String::from),
        }
    }

    #[test]
    fn stream_load_url_is_well_formed() {
        let url = stream_load_url(
            &parse_fe_url(1, "http://localhost:8030").unwrap(),
            "test_db",
            "test_tbl",
        );
        assert_eq!(
            url.as_str(),
            "http://localhost:8030/api/test_db/test_tbl/_stream_load"
        );
    }

    #[test]
    fn stream_load_url_handles_trailing_slash() {
        let url = stream_load_url(
            &parse_fe_url(1, "http://localhost:8030/").unwrap(),
            "test_db",
            "test_tbl",
        );
        assert_eq!(
            url.as_str(),
            "http://localhost:8030/api/test_db/test_tbl/_stream_load"
        );
    }

    #[test]
    fn stream_load_url_rejects_garbage_fe_url() {
        assert!(matches!(
            parse_fe_url(1, "not a url"),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn stream_load_url_rejects_non_http_scheme() {
        for fe_url in ["file:///etc", "ftp://host/path", "ws://host:8030"] {
            assert!(
                matches!(parse_fe_url(1, fe_url), Err(Error::InvalidConfigValue(_))),
                "expected {fe_url} to be rejected at startup",
            );
        }
    }

    #[test]
    fn label_is_deterministic() {
        let a = build_label("picomq", "test_tbl", "orders", 7, 100, 199);
        let b = build_label("picomq", "test_tbl", "orders", 7, 100, 199);
        assert_eq!(a, b);
        let parts: Vec<&str> = a.split('-').collect();
        assert_eq!(parts.len(), 6);
        assert_eq!(parts[0], "picomq");
        assert_eq!(parts[1], "orders");
        assert_eq!(parts[2].len(), LABEL_HASH_HEX_LEN);
        assert!(parts[2].chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(parts[3], "7");
        assert_eq!(parts[4], "100");
        assert_eq!(parts[5], "199");
    }

    #[test]
    fn label_sanitizes_illegal_chars() {
        let label = build_label("picomq", "test_tbl", "orders.v1/inbound", 0, 0, 0);
        assert!(!label.contains('.'));
        assert!(!label.contains('/'));
    }

    #[test]
    fn label_disambiguates_names_that_sanitize_identically() {
        assert_ne!(
            build_label("picomq", "test_tbl", "orders.v1", 0, 0, 0),
            build_label("picomq", "test_tbl", "orders_v1", 0, 0, 0),
            "labels must NOT collide for names that sanitize to the same string"
        );
    }

    #[test]
    fn label_disambiguates_prefixes_that_sanitize_identically() {
        let a = build_label("prod_events_us_east_1", "test_tbl", "orders", 0, 0, 0);
        let b = build_label("prod_events_us_east_2", "test_tbl", "orders", 0, 0, 0);
        assert_eq!(
            a.split('-').next(),
            b.split('-').next(),
            "precondition: sanitized prefixes should collide at 16 chars"
        );
        assert_ne!(
            a, b,
            "labels must NOT collide for prefixes that sanitize to the same string"
        );
    }

    #[test]
    fn label_disambiguates_target_tables_in_same_database() {
        let first = build_label("picomq", "orders", "created", 0, 0, 99);
        let second = build_label("picomq", "orders_archive", "created", 0, 0, 99);

        assert_ne!(
            first, second,
            "Doris labels are database-scoped, so the target table must affect the label"
        );
    }

    #[test]
    fn identity_hash_is_not_aliased_by_boundary_shift() {
        assert_ne!(
            identity_hash("picomq", "test_tbl", "abc"),
            identity_hash("picomq", "test_tb", "labc")
        );
        assert_ne!(
            identity_hash("ab", "ctest_tbl", "topic"),
            identity_hash("a", "bctest_tbl", "topic")
        );
    }

    #[test]
    fn label_stays_under_doris_128_char_cap() {
        let prefix = "p".repeat(100);
        let topic = "t".repeat(100);
        let label = build_label(&prefix, "test_tbl", &topic, i32::MAX, u64::MAX, u64::MAX);
        assert!(
            label.len() <= 128,
            "label exceeds Doris's 128-char cap: {} chars: {label}",
            label.len()
        );
    }

    #[test]
    fn effective_batch_size_floors_at_one() {
        assert_eq!(effective_batch_size(Some(0)), 1);
        assert_eq!(effective_batch_size(None), DEFAULT_BATCH_SIZE as usize);
        assert_eq!(effective_batch_size(Some(500)), 500);
    }

    #[test]
    fn effective_max_retries_uses_default_and_floors_at_one() {
        assert_eq!(effective_max_retries(None), DEFAULT_MAX_RETRIES);
        assert_eq!(effective_max_retries(Some(0)), 1);
        assert_eq!(effective_max_retries(Some(1)), 1);
        assert_eq!(effective_max_retries(Some(5)), 5);
        assert_eq!(
            effective_max_retries(Some(MAX_RETRIES_WARNING_THRESHOLD + 1)),
            MAX_RETRIES_WARNING_THRESHOLD + 1
        );
    }

    #[test]
    fn retry_count_warning_starts_above_threshold() {
        assert!(!should_warn_for_retry_count(MAX_RETRIES_WARNING_THRESHOLD));
        assert!(should_warn_for_retry_count(
            MAX_RETRIES_WARNING_THRESHOLD + 1
        ));
    }

    #[test]
    fn classify_success_returns_ok() {
        let mut response = stream_load_response("Success", None);
        response.number_loaded_rows = 10;
        assert!(classify_status(1, &response).is_ok());
    }

    #[test]
    fn classify_finished_duplicate_returns_ok() {
        let response = stream_load_response("Label Already Exists", Some("FINISHED"));
        assert!(classify_status(1, &response).is_ok());
    }

    #[test]
    fn classify_running_or_cancelled_duplicate_is_transient() {
        for existing_status in ["RUNNING", "CANCELLED"] {
            let response = stream_load_response("Label Already Exists", Some(existing_status));
            assert!(matches!(
                classify_status(1, &response),
                Err(Error::CannotStoreData(_))
            ));
        }
    }

    #[test]
    fn classify_unconfirmed_duplicate_is_permanent() {
        for existing_status in [None, Some(""), Some("PRECOMMITTED"), Some("UNKNOWN")] {
            let response = stream_load_response("Label Already Exists", existing_status);
            assert!(matches!(
                classify_status(1, &response),
                Err(Error::PermanentHttpError(_))
            ));
        }
    }

    #[test]
    fn classify_publish_timeout_returns_ok() {
        let mut response = stream_load_response("Publish Timeout", None);
        response.message = "publish visibility delayed".into();
        assert!(classify_status(1, &response).is_ok());
    }

    #[test]
    fn classify_fail_is_permanent() {
        let mut response = stream_load_response("Fail", None);
        response.message = "schema mismatch".into();
        assert!(matches!(
            classify_status(1, &response).unwrap_err(),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn classify_unknown_status_is_permanent() {
        let response = stream_load_response("Future Doris Status", None);
        assert!(matches!(
            classify_status(1, &response),
            Err(Error::PermanentHttpError(_))
        ));
    }

    #[test]
    fn parse_stream_load_response_handles_minimal_json() {
        let body = r#"{"Status":"Success"}"#;
        let r = parse_stream_load_response(body).unwrap();
        assert_eq!(r.status, "Success");
        assert_eq!(r.number_loaded_rows, 0);
    }

    #[test]
    fn parse_stream_load_response_treats_empty_body_as_transient() {
        assert!(matches!(
            parse_stream_load_response("").unwrap_err(),
            Error::CannotStoreData(_)
        ));
    }

    #[test]
    fn parse_stream_load_response_rejects_nonempty_garbage_as_permanent() {
        let body = "not json";
        assert!(matches!(
            parse_stream_load_response(body).unwrap_err(),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn serialize_json_batch_maps_local_failure_to_serialization_error() {
        let invalid_json_map = std::collections::BTreeMap::from([(true, 1)]);
        let error = serialize_json_batch(&invalid_json_map).unwrap_err();

        assert!(matches!(&error, Error::Serialization(_)));
        assert!(!is_transient_error(&error));
    }

    #[test]
    fn resolve_table_sanitizes_templated_topic() {
        let template: DestinationTemplate = "events_{topic}".parse().unwrap();
        assert_eq!(
            resolve_table(&template, "orders.created-v2", 1).unwrap(),
            "events_orders_created_v2"
        );
        let leading_digit: DestinationTemplate = "{topic_segment[0]}".parse().unwrap();
        assert_eq!(
            resolve_table(&leading_digit, "2024.orders", 1).unwrap(),
            "_2024"
        );
    }

    #[test]
    fn resolve_table_rejects_invalid_static_name() {
        let template = DestinationTemplate::literal("../admin");
        assert!(resolve_table(&template, "orders", 1).is_err());
    }

    #[test]
    fn validate_identifier_rejects_path_traversal() {
        assert!(validate_identifier("../admin", "database", 1).is_err());
        assert!(validate_identifier("foo/bar", "table", 1).is_err());
        assert!(validate_identifier("", "database", 1).is_err());
        assert!(validate_identifier("ok_name_1", "database", 1).is_ok());
    }

    #[test]
    fn truncate_for_log_caps_long_input() {
        let long = "x".repeat(10_000);
        let truncated = truncate_for_log(&long, 100);
        assert!(truncated.len() <= 100 + "...(truncated, total 10000 bytes)".len());
        assert!(truncated.contains("(truncated"));
    }

    #[test]
    fn truncate_for_log_passes_short_input_through() {
        let short = "hello";
        assert_eq!(truncate_for_log(short, 100), "hello");
    }

    #[test]
    fn parse_duration_parses_zero_and_falls_back_for_invalid_input() {
        assert_eq!(parse_duration(Some("10s"), "30s"), Duration::from_secs(10));
        assert_eq!(parse_duration(None, "30s"), Duration::from_secs(30));
        assert_eq!(
            parse_duration(Some("not_a_duration"), "30s"),
            Duration::from_secs(30)
        );
        assert_eq!(parse_duration(Some("0ms"), "5s"), Duration::ZERO);
    }

    #[test]
    fn parse_request_duration_rejects_zero() {
        assert_eq!(
            parse_request_duration(Some("0s"), "30s"),
            Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn open_rejects_out_of_range_max_filter_ratio() {
        for ratio in [1.5_f64, -0.1_f64, f64::INFINITY, f64::NAN] {
            let mut cfg = make_config();
            cfg.max_filter_ratio = Some(ratio);
            let mut sink = DorisSink::new(1, cfg);
            assert!(
                matches!(sink.open().await, Err(Error::InvalidConfigValue(_))),
                "expected InvalidConfigValue for max_filter_ratio={ratio}",
            );
        }
    }

    #[tokio::test]
    async fn open_accepts_in_range_max_filter_ratio() {
        for ratio in [0.0_f64, 0.5_f64, 1.0_f64] {
            let mut cfg = make_config();
            cfg.max_filter_ratio = Some(ratio);
            let mut sink = DorisSink::new(1, cfg);
            assert!(
                sink.open().await.is_ok(),
                "expected open() to accept max_filter_ratio={ratio}",
            );
        }
    }

    fn url(s: &str) -> reqwest::Url {
        reqwest::Url::parse(s).unwrap()
    }

    fn opened_connection(sink: &DorisSink) -> &Connected {
        sink.connected.as_ref().expect("sink should be open")
    }

    fn connected(
        base: &str,
        allow_insecure: bool,
        allowed_hosts: Option<Vec<String>>,
    ) -> Connected {
        Connected {
            client: reqwest::Client::new(),
            base_url: url(base),
            max_filter_ratio_header: None,
            columns_header: None,
            where_header: None,
            allow_insecure_redirect: allow_insecure,
            allowed_redirect_hosts: allowed_hosts,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_delay: Duration::from_millis(1),
            max_retry_delay: Duration::from_millis(5),
            format: Format::Json,
            csv_columns: Vec::new(),
        }
    }

    #[test]
    fn redirect_refuses_https_to_http_downgrade_by_default() {
        let err = connected("https://fe.doris:8030", false, None)
            .validate_redirect(&url("http://attacker.evil/"), 1);
        assert!(matches!(err, Err(Error::PermanentHttpError(_))));
    }

    #[test]
    fn redirect_allows_downgrade_when_opted_in() {
        assert!(
            connected("https://fe.doris:8030", true, None)
                .validate_redirect(&url("http://be.doris:8040/"), 1)
                .is_ok()
        );
    }

    #[test]
    fn redirect_allows_cross_host_same_scheme() {
        assert!(
            connected("https://fe.doris:8030", false, None)
                .validate_redirect(&url("https://be.doris:8040/"), 1)
                .is_ok()
        );
        assert!(
            connected("http://fe.doris:8030", false, None)
                .validate_redirect(&url("http://be.doris:8040/"), 1)
                .is_ok()
        );
    }

    #[test]
    fn redirect_refuses_non_http_scheme() {
        for target in [
            "ftp://be.doris/",
            "file:///etc/passwd",
            "gopher://be.doris/",
        ] {
            assert!(
                matches!(
                    connected("http://fe.doris:8030", false, None)
                        .validate_redirect(&url(target), 1),
                    Err(Error::PermanentHttpError(_))
                ),
                "scheme of {target} should be refused"
            );
        }
    }

    #[test]
    fn redirect_enforces_host_allowlist_when_set() {
        let allowed = vec!["be1.doris".to_string(), "be2.doris".to_string()];
        assert!(matches!(
            connected("http://fe.doris:8030", false, Some(allowed.clone()))
                .validate_redirect(&url("http://attacker.evil:8040/"), 1),
            Err(Error::PermanentHttpError(_))
        ));
        assert!(
            connected("http://fe.doris:8030", false, Some(allowed))
                .validate_redirect(&url("http://be2.doris:8040/"), 1)
                .is_ok()
        );
    }

    #[test]
    fn redirect_allowlist_matches_ipv6_targets() {
        let target = "http://[::1]:8040/api/db/tbl/_stream_load";
        for entry in ["::1", "[::1]", "[::1]:8040"] {
            assert!(
                connected("http://fe.doris:8030", false, Some(vec![entry.to_string()]))
                    .validate_redirect(&url(target), 1)
                    .is_ok(),
                "IPv6 allowlist entry {entry:?} should match {target}"
            );
        }
        assert!(matches!(
            connected(
                "http://fe.doris:8030",
                false,
                Some(vec!["[::1]:8040".to_string()])
            )
            .validate_redirect(&url("http://[::1]:6379/exfil"), 1),
            Err(Error::PermanentHttpError(_))
        ));
        assert!(matches!(
            connected("http://fe.doris:8030", false, Some(vec!["::1".to_string()]))
                .validate_redirect(&url("http://[fe80::1]:8040/"), 1),
            Err(Error::PermanentHttpError(_))
        ));
    }

    #[test]
    fn redirect_allowlist_pins_port_when_specified() {
        let allowed = vec!["be.doris:8040".to_string()];
        assert!(
            connected("http://fe.doris:8030", false, Some(allowed.clone()))
                .validate_redirect(&url("http://be.doris:8040/"), 1)
                .is_ok()
        );
        assert!(matches!(
            connected("http://fe.doris:8030", false, Some(allowed))
                .validate_redirect(&url("http://be.doris:6379/exfil"), 1),
            Err(Error::PermanentHttpError(_))
        ));
    }

    #[test]
    fn auth_header_is_basic_b64() {
        let sink = DorisSink::new(1, make_config());
        assert_eq!(sink.auth_header.to_str().unwrap(), "Basic cm9vdDpwdw==");
        assert!(sink.auth_header.is_sensitive());
    }

    fn text_msg(offset: u64) -> ConsumedMessage {
        ConsumedMessage {
            offset,
            timestamp: 0,
            key: None,
            headers: None,
            payload: Payload::Text("not json".into()),
        }
    }

    fn json_msg(offset: u64) -> ConsumedMessage {
        let mut bytes = br#"{"k":1}"#.to_vec();
        let value = simd_json::to_owned_value(&mut bytes).unwrap();
        ConsumedMessage {
            offset,
            timestamp: 0,
            key: None,
            headers: None,
            payload: Payload::Json(value),
        }
    }

    fn topic_meta() -> TopicMetadata {
        TopicMetadata {
            topic: "orders".into(),
        }
    }

    fn messages_meta() -> MessagesMetadata {
        MessagesMetadata {
            partition: 0,
            current_offset: 0,
            schema: picomq_connector_sdk::Schema::Json,
        }
    }

    #[tokio::test]
    async fn consume_aborts_on_first_non_json_payload() {
        let sink = DorisSink::new(1, make_config());
        let result = sink
            .consume(&topic_meta(), messages_meta(), vec![text_msg(0)])
            .await;
        assert!(
            matches!(result, Err(Error::InvalidPayloadType)),
            "expected InvalidPayloadType, got {result:?}",
        );
    }

    #[tokio::test]
    async fn consume_aborts_on_non_json_in_mixed_batch() {
        let sink = DorisSink::new(1, make_config());
        let result = sink
            .consume(
                &topic_meta(),
                messages_meta(),
                vec![json_msg(0), text_msg(1)],
            )
            .await;
        assert!(
            matches!(result, Err(Error::InvalidPayloadType)),
            "expected InvalidPayloadType, got {result:?}",
        );
    }

    #[tokio::test]
    async fn open_rejects_columns_header_with_control_chars() {
        let mut cfg = make_config();
        cfg.columns = Some("c1,\nc2".into());
        let mut sink = DorisSink::new(1, cfg);
        assert!(
            matches!(sink.open().await, Err(Error::InvalidConfigValue(_))),
            "expected InvalidConfigValue for a columns header with a newline",
        );
    }

    #[tokio::test]
    async fn redirect_rebuilds_full_request_on_be() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let expected_auth = format!("Basic {}", general_purpose::STANDARD.encode("root:pw"));
        let be_url = format!("{}/be/_stream_load", server.uri());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(307).insert_header("Location", be_url.as_str()))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PUT"))
            .and(path("/be/_stream_load"))
            .and(header("authorization", expected_auth.as_str()))
            .and(header("format", "json"))
            .and(header("strip_outer_array", "true"))
            .and(header("expect", "100-continue"))
            .and(header("label", "picomq-test-label"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Success",
                "Message": "OK",
                "NumberLoadedRows": 1,
                "NumberFilteredRows": 0,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");

        let result = sink
            .send_stream_load(
                opened_connection(&sink),
                "test_tbl",
                "picomq-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Ok(r) if r.status == "Success"),
            "expected Ok(Success) after redirect, got {result:?}",
        );
    }

    #[tokio::test]
    async fn redirect_loop_is_permanent_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        let self_url = format!("{}/api/test_db/test_tbl/_stream_load", server.uri());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(307).insert_header("Location", self_url.as_str()))
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .send_stream_load(
                opened_connection(&sink),
                "test_tbl",
                "picomq-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError on redirect loop, got {result:?}",
        );
    }

    #[tokio::test]
    async fn redirect_without_location_is_permanent_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(307))
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .send_stream_load(
                opened_connection(&sink),
                "test_tbl",
                "picomq-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError on missing Location, got {result:?}",
        );
    }

    #[tokio::test]
    async fn redirect_with_relative_location_is_permanent_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(307).insert_header("Location", "be_endpoint"))
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .send_stream_load(
                opened_connection(&sink),
                "test_tbl",
                "picomq-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError on relative Location, got {result:?}",
        );
    }

    #[tokio::test]
    async fn transient_failure_is_retried_then_succeeds() {
        use wiremock::matchers::{body_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(3);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());
        let expected_label = build_label("picomq", "test_tbl", "orders", 0, 0, 0);
        let expected_body = serde_json::json!([{"k": 1}]);

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .and(header("label", expected_label.as_str()))
            .and(body_json(expected_body.clone()))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .and(header("label", expected_label.as_str()))
            .and(body_json(expected_body))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"Status": "Success", "NumberLoadedRows": 1})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .consume(&topic_meta(), messages_meta(), vec![json_msg(0)])
            .await;

        assert!(
            result.is_ok(),
            "expected consume() to succeed after one retry, got {result:?}",
        );
    }

    #[test]
    fn empty_success_body_is_retried_under_same_label() {
        use wiremock::matchers::{body_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let runtime = tokio::runtime::Runtime::new().expect("test runtime should build");
        runtime.block_on(async {
            let server = MockServer::start().await;
            let mut cfg = make_config();
            cfg.fe_url = server.uri();
            cfg.max_retries = Some(3);
            cfg.retry_delay = Some("1ms".into());
            cfg.max_retry_delay = Some("5ms".into());

            let label = "picomq-test-label";
            let body = serde_json::json!([{"a": 1}]);
            Mock::given(method("PUT"))
                .and(path("/api/test_db/test_tbl/_stream_load"))
                .and(header("label", label))
                .and(body_json(body.clone()))
                .respond_with(ResponseTemplate::new(200))
                .up_to_n_times(1)
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("PUT"))
                .and(path("/api/test_db/test_tbl/_stream_load"))
                .and(header("label", label))
                .and(body_json(body))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "Status": "Label Already Exists",
                    "ExistingJobStatus": "FINISHED",
                    "Message": "job finished",
                })))
                .expect(1)
                .mount(&server)
                .await;

            let mut sink = DorisSink::new(1, cfg);
            sink.open().await.expect("open should succeed");
            let result = sink
                .load_batch("test_tbl", label, Bytes::from_static(b"[{\"a\":1}]"))
                .await;

            assert!(
                matches!(&result, Ok(response) if response.existing_job_status.as_deref() == Some("FINISHED")),
                "expected the retry to confirm the first attempt, got {result:?}",
            );
        });
    }

    #[test]
    fn nonempty_malformed_success_body_is_not_retried() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let runtime = tokio::runtime::Runtime::new().expect("test runtime should build");
        runtime.block_on(async {
            let server = MockServer::start().await;
            let mut cfg = make_config();
            cfg.fe_url = server.uri();
            cfg.max_retries = Some(3);
            cfg.retry_delay = Some("1ms".into());
            cfg.max_retry_delay = Some("5ms".into());

            Mock::given(method("PUT"))
                .and(path("/api/test_db/test_tbl/_stream_load"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_string("<html>proxy error</html>"),
                )
                .expect(1)
                .mount(&server)
                .await;

            let mut sink = DorisSink::new(1, cfg);
            sink.open().await.expect("open should succeed");
            let result = sink
                .load_batch(
                    "test_tbl",
                    "picomq-test-label",
                    Bytes::from_static(b"[{\"a\":1}]"),
                )
                .await;

            assert!(
                matches!(&result, Err(Error::PermanentHttpError(_))),
                "expected non-empty malformed response to stay permanent, got {result:?}",
            );
        });
    }

    #[tokio::test]
    async fn running_duplicate_is_retried_until_finished() {
        use wiremock::matchers::{body_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(3);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());

        let label = "picomq-test-label";
        let body = serde_json::json!([{"a": 1}]);
        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .and(header("label", label))
            .and(body_json(body.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Label Already Exists",
                "ExistingJobStatus": "RUNNING",
                "Message": "job is still running",
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .and(header("label", label))
            .and(body_json(body))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Label Already Exists",
                "ExistingJobStatus": "FINISHED",
                "Message": "job finished",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .load_batch("test_tbl", label, Bytes::from_static(b"[{\"a\":1}]"))
            .await;

        assert!(
            matches!(&result, Ok(response) if response.existing_job_status.as_deref() == Some("FINISHED")),
            "expected FINISHED duplicate after one retry, got {result:?}",
        );
    }

    #[tokio::test]
    async fn publish_timeout_is_not_retried() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(3);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Publish Timeout",
                "Message": "transaction committed; publish is delayed",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .load_batch(
                "test_tbl",
                "picomq-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Ok(response) if response.status == "Publish Timeout"),
            "expected Publish Timeout to be accepted without a retry, got {result:?}",
        );
    }

    #[tokio::test]
    async fn transient_failure_exhausts_retries_and_returns_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(2);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(503))
            .expect(2)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .load_batch(
                "test_tbl",
                "picomq-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Err(Error::CannotStoreData(_))),
            "expected CannotStoreData after exhausting retries, got {result:?}",
        );
    }

    #[tokio::test]
    async fn permanent_failure_is_not_retried() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(5);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(400))
            .expect(1)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .load_batch(
                "test_tbl",
                "picomq-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError with no retry, got {result:?}",
        );
    }

    fn owned(json: &str) -> OwnedValue {
        let mut bytes = json.as_bytes().to_vec();
        simd_json::to_owned_value(&mut bytes).expect("valid JSON")
    }

    #[test]
    fn format_deserializes_from_lowercase() {
        assert_eq!(
            serde_json::from_str::<Format>("\"json\"").unwrap(),
            Format::Json
        );
        assert_eq!(
            serde_json::from_str::<Format>("\"csv\"").unwrap(),
            Format::Csv
        );
        assert!(serde_json::from_str::<Format>("\"xml\"").is_err());
        assert_eq!(Format::default(), Format::Json);
    }

    #[test]
    fn csv_column_names_takes_leading_bare_names() {
        assert_eq!(csv_column_names("id, name, count"), ["id", "name", "count"]);
        assert_eq!(
            csv_column_names("id, name, calc = count + 1"),
            ["id", "name"]
        );
        assert_eq!(csv_column_names(" a ,b, c "), ["a", "b", "c"]);
        assert!(csv_column_names("").is_empty());
        assert!(csv_column_names("calc = x").is_empty());
    }

    #[test]
    fn build_csv_body_encodes_scalars_nulls_and_missing() {
        let row = owned(r#"{"i":-5,"u":10,"f":2.5,"b":false,"nul":null,"empty":""}"#);
        let columns = ["i", "u", "f", "b", "nul", "missing", "empty"].map(String::from);
        let body = build_csv_body(&[&row], &columns).unwrap();
        assert_eq!(*body.last().unwrap(), CSV_LINE_DELIMITER);
        let fields: Vec<&[u8]> = body[..body.len() - 1]
            .split(|&byte| byte == CSV_COLUMN_SEPARATOR)
            .collect();
        assert_eq!(
            fields,
            vec![
                &b"-5"[..],
                &b"10"[..],
                &b"2.5"[..],
                &b"false"[..],
                &b"\\N"[..],
                &b"\\N"[..],
                &b"\"\""[..],
            ],
        );
    }

    #[test]
    fn build_csv_body_prefix_escapes_enclose_and_escape_bytes() {
        let row = owned(r#"{"v":"a\"b\\c"}"#);
        let columns = ["v"].map(String::from);
        let body = build_csv_body(&[&row], &columns).unwrap();
        let expected: &[u8] = &[
            b'"',
            b'a',
            b'\\',
            b'"',
            b'b',
            b'\\',
            b'\\',
            b'c',
            b'"',
            CSV_LINE_DELIMITER,
        ];
        assert_eq!(body, expected);
    }

    #[test]
    fn build_csv_body_keeps_embedded_separator_and_newline_inside_enclosure() {
        let row = owned("{\"v\":\"a\\u0001b\\u0002c\\nd\"}");
        let columns = ["v"].map(String::from);
        let body = build_csv_body(&[&row], &columns).unwrap();
        let expected: &[u8] = &[
            b'"',
            b'a',
            0x01,
            b'b',
            0x02,
            b'c',
            b'\n',
            b'd',
            b'"',
            CSV_LINE_DELIMITER,
        ];
        assert_eq!(body, expected);
    }

    #[test]
    fn build_csv_body_stringifies_nested_values() {
        let row = owned(r#"{"o":{"k":1}}"#);
        let columns = ["o"].map(String::from);
        let body = build_csv_body(&[&row], &columns).unwrap();
        let expected: &[u8] = &[
            b'"',
            b'{',
            b'\\',
            b'"',
            b'k',
            b'\\',
            b'"',
            b':',
            b'1',
            b'}',
            b'"',
            CSV_LINE_DELIMITER,
        ];
        assert_eq!(body, expected);
    }

    #[test]
    fn build_csv_body_rejects_non_object_row() {
        let row = owned("[1, 2, 3]");
        let columns = ["v"].map(String::from);
        assert!(matches!(
            build_csv_body(&[&row], &columns),
            Err(Error::InvalidRecordValue(_))
        ));
    }

    #[tokio::test]
    async fn open_rejects_csv_format_without_columns() {
        let mut cfg = make_config();
        cfg.output_format = Some(Format::Csv);
        cfg.columns = None;
        let mut sink = DorisSink::new(1, cfg);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[tokio::test]
    async fn open_resolves_csv_columns_dropping_derived() {
        let mut cfg = make_config();
        cfg.output_format = Some(Format::Csv);
        cfg.columns = Some("id, name, calc = id + 1".into());
        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let connected = sink.connected.as_ref().unwrap();
        assert_eq!(connected.format, Format::Csv);
        assert_eq!(connected.csv_columns, ["id", "name"]);
    }
}
