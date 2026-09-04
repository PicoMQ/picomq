mod common;
mod row;
mod v2;
mod v3;

use crate::common::{
    is_timestamp_after, query_has_cursor_placeholder, query_has_cursor_placeholder_flux,
    query_has_offset_placeholder, strip_block_comments,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use common::{
    InfluxDbSourceConfig, PayloadFormat, PersistedState, V2State, V3State, validate_cursor,
    validate_cursor_field,
};
use picomq_connector_sdk::retry::{
    CircuitBreaker, ConnectivityConfig, build_retry_client, check_connectivity_with_retry,
    parse_duration,
};
use picomq_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use secrecy::{ExposeSecret, SecretBox};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

source_connector!(InfluxDbSource);

const CONNECTOR_NAME: &str = "InfluxDB source";
const MAX_STATE_SERIALIZE_FAILURES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_POLL_INTERVAL: &str = "5s";
const DEFAULT_TIMEOUT: &str = "10s";
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "60s";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
const DEFAULT_CIRCUIT_COOL_DOWN: &str = "30s";

#[derive(Debug)]
enum VersionState {
    V2(Mutex<V2State>),
    V3(Mutex<V3State>),
}

#[derive(Debug)]
pub struct InfluxDbSource {
    id: u32,
    config: InfluxDbSourceConfig,
    client: Option<ClientWithMiddleware>,
    version_state: VersionState,
    payload_format: PayloadFormat,
    poll_interval: Duration,
    retry_delay: Duration,
    circuit_breaker: Arc<CircuitBreaker>,
    auth_header: Option<SecretBox<String>>,
    state_restore_error: Option<String>,
    state_serialize_failures: AtomicU32,
    pending: Mutex<Option<PersistedState>>,
}

impl InfluxDbSource {
    pub fn new(id: u32, config: InfluxDbSourceConfig, state: Option<ConnectorState>) -> Self {
        let retry_delay = parse_duration(config.retry_delay(), DEFAULT_RETRY_DELAY);
        let poll_interval = parse_duration(config.poll_interval(), DEFAULT_POLL_INTERVAL);
        let payload_format = PayloadFormat::from_config(config.payload_format());

        let cb_threshold = config.circuit_breaker_threshold();
        let cb_cool_down = parse_duration(
            config.circuit_breaker_cool_down(),
            DEFAULT_CIRCUIT_COOL_DOWN,
        );
        let circuit_breaker = Arc::new(CircuitBreaker::new(cb_threshold, cb_cool_down));

        let (version_state, state_restore_error) = match &config {
            InfluxDbSourceConfig::V2(_) => {
                let (s, err) = restore_v2_state(id, state);
                (VersionState::V2(Mutex::new(s)), err)
            }
            InfluxDbSourceConfig::V3(_) => {
                let (s, err) = restore_v3_state(id, state);
                (VersionState::V3(Mutex::new(s)), err)
            }
        };

        InfluxDbSource {
            id,
            config,
            client: None,
            version_state,
            payload_format,
            poll_interval,
            retry_delay,
            circuit_breaker,
            auth_header: None,
            state_restore_error,
            state_serialize_failures: AtomicU32::new(0),
            pending: Mutex::new(None),
        }
    }

    fn get_client(&self) -> Result<&ClientWithMiddleware, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Connection("InfluxDB client not initialized".to_string()))
    }

    async fn serialize_state(&self, state: &PersistedState) -> Option<ConnectorState> {
        let persisted = ConnectorState::serialize(state, CONNECTOR_NAME, self.id);
        match persisted {
            Some(_) => {
                self.state_serialize_failures.store(0, Ordering::Relaxed);
            }
            None => {
                let failures = self
                    .state_serialize_failures
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                if failures >= MAX_STATE_SERIALIZE_FAILURES {
                    self.circuit_breaker.record_failure().await;
                } else {
                    warn!(
                        "{CONNECTOR_NAME} ID: {} - state serialization failed \
                         ({failures}/{MAX_STATE_SERIALIZE_FAILURES}); \
                         cursor will not be persisted. Messages were emitted; \
                         restart may cause re-delivery.",
                        self.id
                    );
                }
            }
        }
        persisted
    }

    async fn stage_batch(
        &self,
        candidate: PersistedState,
        schema: Schema,
        messages: Vec<ProducedMessage>,
    ) -> Result<ProducedMessages, Error> {
        if messages.is_empty() {
            self.apply_state(candidate).await?;
            return Ok(ProducedMessages {
                schema,
                messages,
                state: None,
            });
        }

        let persisted = self.serialize_state(&candidate).await;
        *self.pending.lock().await = Some(candidate);
        Ok(ProducedMessages {
            schema,
            messages,
            state: persisted,
        })
    }

    async fn apply_state(&self, candidate: PersistedState) -> Result<(), Error> {
        match (&self.version_state, candidate) {
            (VersionState::V2(state), PersistedState::V2(candidate)) => {
                *state.lock().await = candidate;
                Ok(())
            }
            (VersionState::V3(state), PersistedState::V3(candidate)) => {
                *state.lock().await = candidate;
                Ok(())
            }
            _ => Err(Error::InvalidState),
        }
    }
}

fn restore_v2_state(id: u32, state: Option<ConnectorState>) -> (V2State, Option<String>) {
    let Some(cs) = state else {
        return (V2State::default(), None);
    };
    let bytes = cs.0;
    match ConnectorState(bytes.clone()).deserialize::<PersistedState>(CONNECTOR_NAME, id) {
        Some(PersistedState::V2(s)) => {
            if let Some(ref ts) = s.last_timestamp
                && let Err(e) = validate_cursor(ts)
            {
                let cause = format!(
                    "persisted V2 cursor {ts:?} failed validation: {e}. \
                         Clear or migrate the connector state to recover."
                );
                error!("{CONNECTOR_NAME} ID {id}: {cause}");
                return (V2State::default(), Some(cause));
            }

            info!(
                "{CONNECTOR_NAME} ID {id}: restored V2 state, \
                 last_timestamp={:?}, processed_rows={}",
                s.last_timestamp, s.processed_rows
            );
            (s, None)
        }
        Some(PersistedState::V3(_)) => {
            let cause = "persisted state is V3 but connector is configured as V2. \
                 Refusing to start to prevent cursor reset. \
                 Clear or migrate the connector state to proceed.";

            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (V2State::default(), Some(cause.to_string()))
        }
        None => {
            if let Some(s) = ConnectorState(bytes).deserialize::<V2State>(CONNECTOR_NAME, id) {
                if let Some(ref ts) = s.last_timestamp
                    && let Err(e) = validate_cursor(ts)
                {
                    let cause = format!(
                        "legacy V2 cursor {ts:?} failed validation: {e}. \
                             Clear or migrate the connector state to recover."
                    );
                    error!("{CONNECTOR_NAME} ID {id}: {cause}");
                    return (V2State::default(), Some(cause));
                }
                info!(
                    "{CONNECTOR_NAME} ID {id}: restored V2 state from legacy format, \
                     last_timestamp={:?}, processed_rows={}",
                    s.last_timestamp, s.processed_rows
                );
                return (s, None);
            }
            let cause = "persisted state exists but could not be deserialized. \
                         Refusing to start to prevent silent cursor reset."
                .to_string();
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (V2State::default(), Some(cause))
        }
    }
}

fn restore_v3_state(id: u32, state: Option<ConnectorState>) -> (V3State, Option<String>) {
    let Some(cs) = state else {
        return (V3State::default(), None);
    };
    match cs.deserialize::<PersistedState>(CONNECTOR_NAME, id) {
        Some(PersistedState::V3(s)) => {
            if let Some(ref ts) = s.last_timestamp
                && let Err(e) = validate_cursor(ts)
            {
                let cause = format!(
                    "persisted V3 cursor {ts:?} failed validation: {e}. \
                         Clear or migrate the connector state to recover."
                );
                error!("{CONNECTOR_NAME} ID {id}: {cause}");
                return (V3State::default(), Some(cause));
            }
            if let Some(ref sc) = s.stuck_cursor
                && let Err(e) = validate_cursor(sc)
            {
                let cause = format!(
                    "persisted V3 stuck_cursor {sc:?} failed validation: {e}. \
                         Clear or migrate the connector state to recover."
                );
                error!("{CONNECTOR_NAME} ID {id}: {cause}");
                return (V3State::default(), Some(cause));
            }

            info!(
                "{CONNECTOR_NAME} ID {id}: restored V3 state, \
                 last_timestamp={:?}, processed_rows={}",
                s.last_timestamp, s.processed_rows
            );
            (s, None)
        }
        Some(PersistedState::V2(_)) => {
            let cause = "persisted state is V2 but connector is configured as V3. \
                 Refusing to start to prevent cursor reset. \
                 Clear or migrate the connector state to proceed.";
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (V3State::default(), Some(cause.to_string()))
        }
        None => {
            let cause = "persisted state exists but could not be deserialized. \
                         Refusing to start to prevent silent cursor reset."
                .to_string();
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (V3State::default(), Some(cause))
        }
    }
}

#[async_trait]
impl Source for InfluxDbSource {
    async fn open(&mut self) -> Result<(), Error> {
        if let Some(ref cause) = self.state_restore_error {
            return Err(Error::InitError(format!("state restore failed: {cause}")));
        }

        let ver = self.config.version_label();
        info!(
            "Opening {CONNECTOR_NAME} with ID: {} (version={ver})",
            self.id
        );

        validate_cursor_field(self.config.cursor_field(), self.config.version_label())?;
        if let Some(offset) = self.config.initial_offset() {
            validate_cursor(offset)?;
        }
        if let Some(fmt) = self.config.payload_format() {
            PayloadFormat::validate(fmt)?;
        }

        match &self.config {
            InfluxDbSourceConfig::V2(c) if c.org.trim().is_empty() => {
                return Err(Error::InvalidConfigValue(
                    "V2 source config requires a non-empty 'org'".into(),
                ));
            }
            InfluxDbSourceConfig::V3(c) if c.db.trim().is_empty() => {
                return Err(Error::InvalidConfigValue(
                    "V3 source config requires a non-empty 'db'".into(),
                ));
            }
            _ => {}
        }

        let has_cursor = match &self.config {
            InfluxDbSourceConfig::V2(c) => query_has_cursor_placeholder_flux(&c.query),
            InfluxDbSourceConfig::V3(c) => query_has_cursor_placeholder(&c.query),
        };
        if !has_cursor {
            return Err(Error::InvalidConfigValue(
                "query must contain the '$cursor' placeholder, without it the connector \
                 cannot advance and will re-deliver the same rows on every poll."
                    .into(),
            ));
        }

        if let InfluxDbSourceConfig::V3(cfg) = &self.config
            && let Some(cap) = cfg.stuck_batch_cap_factor
            && cap > v3::MAX_STUCK_CAP_FACTOR
        {
            return Err(Error::InvalidConfigValue(format!(
                "stuck_batch_cap_factor {cap} exceeds maximum of {}; \
                 reduce it to avoid querying up to {cap}×batch_size rows per poll.",
                v3::MAX_STUCK_CAP_FACTOR
            )));
        }

        if let InfluxDbSourceConfig::V3(cfg) = &self.config
            && cfg.stuck_batch_cap_factor == Some(1)
        {
            return Err(Error::InvalidConfigValue(
                "stuck_batch_cap_factor 1 is invalid, cap equals base_batch so \
                 the connector trips the circuit breaker on the first stuck poll \
                 with no inflation at all; use 0 to disable stuck detection or >= 2."
                    .into(),
            ));
        }

        if let InfluxDbSourceConfig::V3(cfg) = &self.config {
            let cap = cfg
                .stuck_batch_cap_factor
                .unwrap_or(v3::DEFAULT_STUCK_CAP_FACTOR);
            if cap > 0 && !query_has_offset_placeholder(&cfg.query) {
                return Err(Error::InvalidConfigValue(
                    "V3 source query must contain the '$offset' placeholder when \
                     stuck_batch_cap_factor > 0 (the default). Add 'OFFSET $offset' \
                     to your query to prevent duplicate delivery during stuck-batch \
                     inflation. For simple queries add it at the top level: \
                     \"WHERE time > '$cursor' ORDER BY time LIMIT $limit OFFSET $offset\". \
                     For queries with subqueries or CTEs, $offset must appear on the \
                     innermost SELECT that filters by time."
                        .into(),
                ));
            }
            if cap > 0 && !query_has_order_by(&cfg.query) {
                return Err(Error::InvalidConfigValue(
                    "V3 source query must contain an ORDER BY clause when \
                     stuck_batch_cap_factor > 0 (the default). DataFusion does not \
                     guarantee stable row ordering for tied timestamps without ORDER BY; \
                     OFFSET-based dedup relies on consistent ordering. Add \
                     'ORDER BY <cursor_field>' to your query, for example: \
                     \"SELECT * FROM <measurement> WHERE time > '$cursor' ORDER BY time LIMIT $limit OFFSET $offset\"."
                        .into(),
                ));
            }
            if cap > 0 && query_has_desc_order(&cfg.query) {
                return Err(Error::InvalidConfigValue(
                    "V3 source query must use ascending ORDER BY on the cursor column. \
                     ORDER BY ... DESC causes silent data loss: after advancing the cursor \
                     to the highest timestamp in a batch, all older rows in that batch are \
                     below the new cursor and are never re-fetched. \
                     Use 'ORDER BY time' (ascending) instead of 'ORDER BY time DESC'."
                        .into(),
                ));
            }
        }

        if let InfluxDbSourceConfig::V2(cfg) = &self.config
            && query_has_inclusive_cursor(&cfg.query, "//")
            && !query_has_sort_call(&cfg.query)
        {
            return Err(Error::InvalidConfigValue(format!(
                "{CONNECTOR_NAME} ID: {}: V2 query uses '>=' (inclusive cursor) but does \
                 not contain `|> sort(columns: [\"_time\"])`. Skip-N dedup is \
                 order-dependent; without sorting, InfluxDB may return rows in storage \
                 order and the wrong rows will be silently skipped. \
                 Add `|> sort(columns: [\"_time\"])` before `|> limit(...)` in your query.",
                self.id
            )));
        }

        if let InfluxDbSourceConfig::V3(cfg) = &self.config
            && query_has_inclusive_cursor(&cfg.query, "--")
        {
            return Err(Error::InvalidConfigValue(
                "V3 source query uses '>= $cursor' (inclusive). V3 uses strict \
                 '> $cursor' semantics, an inclusive cursor causes rows at the \
                 cursor timestamp to be re-delivered on the next poll. \
                 Change '>=' to '>' in your query."
                    .into(),
            ));
        }

        let timeout = parse_duration(self.config.timeout(), DEFAULT_TIMEOUT);
        let raw_client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create HTTP client: {e}")))?;

        let health_url = Url::parse(&format!("{}/health", self.config.base_url()))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;

        check_connectivity_with_retry(
            &raw_client,
            health_url,
            CONNECTOR_NAME,
            self.id,
            &ConnectivityConfig {
                max_open_retries: self.config.max_open_retries(),
                open_retry_max_delay: parse_duration(
                    self.config.open_retry_max_delay(),
                    DEFAULT_OPEN_RETRY_MAX_DELAY,
                ),
                retry_delay: self.retry_delay,
            },
        )
        .await?;

        let query_retry_max_delay =
            parse_duration(self.config.retry_max_delay(), DEFAULT_RETRY_MAX_DELAY);
        self.client = Some(build_retry_client(
            raw_client,
            self.config.max_retries(),
            self.retry_delay,
            query_retry_max_delay,
            "InfluxDB",
        ));

        let token = self.config.token_secret().expose_secret();
        self.auth_header = Some(SecretBox::new(Box::new(match &self.config {
            InfluxDbSourceConfig::V2(_) => format!("Token {token}"),
            InfluxDbSourceConfig::V3(_) => format!("Bearer {token}"),
        })));

        info!(
            "{CONNECTOR_NAME} ID: {} opened successfully (version={ver})",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let level = if self.config.verbose_logging() {
            tracing::Level::INFO
        } else {
            tracing::Level::DEBUG
        };
        macro_rules! poll_event {
            ($($tt:tt)+) => {
                if level == tracing::Level::INFO {
                    tracing::info!($($tt)+)
                } else {
                    tracing::debug!($($tt)+)
                }
            };
        }
        if self.circuit_breaker.is_open().await {
            warn!(
                "{CONNECTOR_NAME} ID: {}, circuit breaker is OPEN. Skipping poll.",
                self.id
            );
            tokio::time::sleep(self.poll_interval).await;
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: vec![],
                state: None,
            });
        }
        tokio::time::sleep(self.poll_interval).await;

        let client = self.get_client()?;
        let auth = self
            .auth_header
            .as_ref()
            .map(|s| s.expose_secret().as_str())
            .ok_or_else(|| {
                Error::Connection("auth_header not initialised -- was open() called?".to_string())
            })?;
        match &self.version_state {
            VersionState::V2(state_mu) => {
                let InfluxDbSourceConfig::V2(cfg) = &self.config else {
                    return Err(Error::InvalidState);
                };

                let state_snap = state_mu.lock().await.clone();
                match v2::poll(
                    client,
                    cfg,
                    auth,
                    &state_snap,
                    self.payload_format,
                    self.config.include_metadata(),
                )
                .await
                {
                    Ok(result) => {
                        self.circuit_breaker.record_success();
                        let messages = result.messages;
                        let mut candidate = state_snap;
                        candidate.processed_rows += messages.len() as u64;
                        apply_v2_cursor_advance(
                            &mut candidate,
                            result.max_cursor,
                            result.rows_at_max_cursor,
                            result.skipped,
                        );

                        poll_event!(
                            skipped = result.skipped,
                            "{CONNECTOR_NAME} ID: {} produced {} messages (V2). \
                             Total: {}. Cursor: {:?}",
                            self.id,
                            messages.len(),
                            candidate.processed_rows,
                            candidate.last_timestamp.as_deref()
                        );

                        self.stage_batch(PersistedState::V2(candidate), result.schema, messages)
                            .await
                    }
                    Err(e) => self.handle_poll_error(e).await,
                }
            }

            VersionState::V3(state_mu) => {
                let InfluxDbSourceConfig::V3(cfg) = &self.config else {
                    return Err(Error::InvalidState);
                };

                let state_snap = state_mu.lock().await.clone();
                match v3::poll(
                    client,
                    cfg,
                    auth,
                    &state_snap,
                    self.payload_format,
                    self.config.include_metadata(),
                )
                .await
                {
                    Ok(result) => {
                        if result.trip_circuit_breaker {
                            self.circuit_breaker.record_failure().await;
                        } else if !result.is_stuck {
                            self.circuit_breaker.record_success();
                        }

                        let candidate = result.new_state;
                        let messages = result.messages;
                        poll_event!(
                            "{CONNECTOR_NAME} ID: {} produced {} messages (V3). \
                             Total: {}. Cursor: {:?}",
                            self.id,
                            messages.len(),
                            candidate.processed_rows,
                            candidate.last_timestamp.as_deref()
                        );

                        self.stage_batch(PersistedState::V3(candidate), result.schema, messages)
                            .await
                    }
                    Err(e) => self.handle_poll_error(e).await,
                }
            }
        }
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let candidate = self.pending.lock().await.take();
        if result == SourceBatchResult::Ack
            && let Some(candidate) = candidate
        {
            self.apply_state(candidate).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.client = None;
        let processed = match &self.version_state {
            VersionState::V2(mu) => mu.lock().await.processed_rows,
            VersionState::V3(mu) => mu.lock().await.processed_rows,
        };
        info!(
            "{CONNECTOR_NAME} ID: {} closed. Total rows processed: {processed}",
            self.id
        );
        Ok(())
    }
}

impl InfluxDbSource {
    async fn handle_poll_error(&self, e: Error) -> Result<ProducedMessages, Error> {
        self.circuit_breaker.record_failure().await;
        error!("{CONNECTOR_NAME} ID: {} poll failed: {e}", self.id);
        Err(e)
    }
}

fn query_has_order_by(query: &str) -> bool {
    let q = strip_block_comments(query);
    q.lines().any(|line| {
        let code = match line.find("--") {
            Some(pos) => &line[..pos],
            None => line,
        };
        code.to_ascii_uppercase().contains("ORDER BY")
    })
}

fn query_has_desc_order(query: &str) -> bool {
    let q = strip_block_comments(query);
    q.lines().any(|line| {
        let code = match line.find("--") {
            Some(pos) => &line[..pos],
            None => line,
        };
        let upper = code.to_ascii_uppercase();
        if let Some(ob_pos) = upper.find("ORDER BY") {
            let after = &upper[ob_pos + "ORDER BY".len()..];
            after
                .split(|c: char| !c.is_ascii_alphabetic())
                .any(|token| token == "DESC")
        } else {
            false
        }
    })
}

fn query_has_sort_call(query: &str) -> bool {
    let q = strip_block_comments(query);
    q.lines().any(|line| {
        let code = match line.find("//") {
            Some(pos) => &line[..pos],
            None => line,
        };
        let mut search = code;
        while let Some(pos) = search.find("sort(") {
            let preceded_by_word_char = search[..pos]
                .chars()
                .last()
                .is_some_and(|c| c.is_alphanumeric() || c == '_');
            if !preceded_by_word_char {
                return true;
            }
            search = &search[pos + 5..];
        }
        false
    })
}

fn query_has_inclusive_cursor(query: &str, comment_char: &str) -> bool {
    let q = strip_block_comments(query);
    q.lines().any(|line| {
        let code = match line.find(comment_char) {
            Some(pos) => &line[..pos],
            None => line,
        };
        let mut rest = code;
        while let Some(pos) = rest.find(">=") {
            let after = rest[pos + 2..].trim_start_matches([' ', '\t']);
            let inner = after
                .strip_prefix('\'')
                .or_else(|| after.strip_prefix('"'))
                .unwrap_or(after);
            if inner.starts_with("$cursor")
                && !inner["$cursor".len()..]
                    .starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
            {
                return true;
            }
            rest = &rest[pos + 2..];
        }
        false
    })
}

fn apply_v2_cursor_advance(
    state: &mut V2State,
    max_cursor: Option<String>,
    rows_at_max_cursor: u64,
    skipped: u64,
) {
    if let Some(ref new_cursor) = max_cursor {
        let should_advance = match state.last_timestamp.as_deref() {
            None => true,
            Some(old) => match old.parse::<DateTime<Utc>>() {
                Ok(dt) => is_timestamp_after(new_cursor, dt),
                Err(e) => {
                    error!(
                        "V2 source: persisted cursor {old:?} failed RFC 3339 parse ({e}); \
                         advancing to server cursor {new_cursor:?} to recover."
                    );
                    true
                }
            },
        };
        if should_advance {
            state.last_timestamp = Some(new_cursor.clone());
            state.cursor_row_count = rows_at_max_cursor;
        } else {
            state.cursor_row_count += rows_at_max_cursor;
        }
    } else if skipped > 0 {
        state.cursor_row_count = skipped;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::http::StatusCode;
    use axum::routing::post;
    use common::{V2SourceConfig, V3SourceConfig};
    use secrecy::SecretString;
    use std::sync::atomic::AtomicUsize;

    const T2: &str = "2024-01-01T00:00:02Z";

    async fn start_server(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://127.0.0.1:{port}")
    }

    fn rows_then_empty(path: &str, rows: &'static str, rows_responses: usize) -> Router {
        let calls = Arc::new(AtomicUsize::new(0));
        Router::new().route(
            path,
            post(move || {
                let calls = calls.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    let body = if call < rows_responses { rows } else { "" };
                    (StatusCode::OK, body)
                }
            }),
        )
    }

    fn connected_source(config: InfluxDbSourceConfig) -> InfluxDbSource {
        let mut source = InfluxDbSource::new(1, config, None);
        let raw = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        source.client = Some(build_retry_client(
            raw,
            1,
            Duration::from_millis(1),
            Duration::from_millis(10),
            "test",
        ));
        source.auth_header = Some(SecretBox::new(Box::new("Token test".to_string())));
        source
    }

    fn v2_config_for(url: &str) -> InfluxDbSourceConfig {
        match make_v2_config() {
            InfluxDbSourceConfig::V2(mut config) => {
                config.url = url.to_string();
                config.poll_interval = Some("1ms".to_string());
                config.max_retries = Some(1);
                config.retry_delay = Some("1ms".to_string());
                InfluxDbSourceConfig::V2(config)
            }
            other => other,
        }
    }

    fn v3_config_for(url: &str) -> InfluxDbSourceConfig {
        match make_v3_config() {
            InfluxDbSourceConfig::V3(mut config) => {
                config.url = url.to_string();
                config.poll_interval = Some("1ms".to_string());
                config.max_retries = Some(1);
                config.retry_delay = Some("1ms".to_string());
                InfluxDbSourceConfig::V3(config)
            }
            other => other,
        }
    }

    async fn v2_state(source: &InfluxDbSource) -> V2State {
        match &source.version_state {
            VersionState::V2(state) => state.lock().await.clone(),
            VersionState::V3(_) => panic!("expected V2 version state"),
        }
    }

    async fn v3_state(source: &InfluxDbSource) -> V3State {
        match &source.version_state {
            VersionState::V3(state) => state.lock().await.clone(),
            VersionState::V2(_) => panic!("expected V3 version state"),
        }
    }

    const V2_CSV: &str = "_time,_value\n2024-01-01T00:00:01Z,1\n2024-01-01T00:00:02Z,2\n";
    const V3_JSONL: &str = "{\"time\":\"2024-01-01T00:00:01Z\",\"val\":1}\n{\"time\":\"2024-01-01T00:00:02Z\",\"val\":2}\n";

    #[tokio::test]
    async fn given_v2_nack_when_batch_is_staged_should_discard_cursor_and_reread_same_rows() {
        let base = start_server(rows_then_empty("/api/v2/query", V2_CSV, usize::MAX)).await;
        let source = connected_source(v2_config_for(&base));

        let produced = source.poll().await.unwrap();
        assert_eq!(produced.messages.len(), 2);
        assert!(produced.state.is_some());
        assert!(source.pending.lock().await.is_some());

        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .unwrap();

        let state = v2_state(&source).await;
        assert!(state.last_timestamp.is_none());
        assert_eq!(state.processed_rows, 0);
        assert_eq!(state.cursor_row_count, 0);
        assert!(source.pending.lock().await.is_none());

        let redelivered = source.poll().await.unwrap();
        assert_eq!(redelivered.messages.len(), 2);
        assert_eq!(redelivered.messages[0].key, produced.messages[0].key);
        assert_eq!(redelivered.messages[1].key, produced.messages[1].key);
        assert_eq!(
            redelivered.messages[0].payload,
            produced.messages[0].payload
        );
    }

    #[tokio::test]
    async fn given_v2_ack_when_batch_is_staged_should_apply_cursor() {
        let base = start_server(rows_then_empty("/api/v2/query", V2_CSV, usize::MAX)).await;
        let source = connected_source(v2_config_for(&base));

        let produced = source.poll().await.unwrap();
        assert_eq!(produced.messages.len(), 2);

        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .unwrap();

        let state = v2_state(&source).await;
        assert_eq!(state.last_timestamp.as_deref(), Some(T2));
        assert_eq!(state.processed_rows, 2);
        assert_eq!(state.cursor_row_count, 1);
        assert!(source.pending.lock().await.is_none());

        let persisted = produced
            .state
            .unwrap()
            .deserialize::<PersistedState>(CONNECTOR_NAME, 1)
            .unwrap();
        match persisted {
            PersistedState::V2(persisted) => {
                assert_eq!(persisted.last_timestamp.as_deref(), Some(T2));
                assert_eq!(persisted.processed_rows, 2);
            }
            PersistedState::V3(_) => panic!("expected V2 persisted state"),
        }
    }

    #[tokio::test]
    async fn given_v2_empty_poll_after_nack_should_carry_no_advanced_state() {
        let base = start_server(rows_then_empty("/api/v2/query", V2_CSV, 1)).await;
        let source = connected_source(v2_config_for(&base));

        let produced = source.poll().await.unwrap();
        assert_eq!(produced.messages.len(), 2);
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .unwrap();

        let empty = source.poll().await.unwrap();
        assert!(empty.messages.is_empty());
        assert!(empty.state.is_none());
        assert!(source.pending.lock().await.is_none());

        let state = v2_state(&source).await;
        assert!(state.last_timestamp.is_none());
        assert_eq!(state.processed_rows, 0);
    }

    #[tokio::test]
    async fn given_v3_nack_when_batch_is_staged_should_discard_cursor_and_reread_same_rows() {
        let base = start_server(rows_then_empty("/api/v3/query_sql", V3_JSONL, usize::MAX)).await;
        let source = connected_source(v3_config_for(&base));

        let produced = source.poll().await.unwrap();
        assert_eq!(produced.messages.len(), 2);
        assert!(produced.state.is_some());

        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .unwrap();

        let state = v3_state(&source).await;
        assert!(state.last_timestamp.is_none());
        assert_eq!(state.processed_rows, 0);
        assert!(source.pending.lock().await.is_none());

        let redelivered = source.poll().await.unwrap();
        assert_eq!(redelivered.messages.len(), 2);
        assert_eq!(redelivered.messages[0].key, produced.messages[0].key);
        assert_eq!(redelivered.messages[1].key, produced.messages[1].key);
    }

    #[tokio::test]
    async fn given_v3_ack_when_batch_is_staged_should_apply_cursor() {
        let base = start_server(rows_then_empty("/api/v3/query_sql", V3_JSONL, usize::MAX)).await;
        let source = connected_source(v3_config_for(&base));

        let produced = source.poll().await.unwrap();
        assert_eq!(produced.messages.len(), 2);

        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .unwrap();

        let state = v3_state(&source).await;
        assert_eq!(state.last_timestamp.as_deref(), Some(T2));
        assert_eq!(state.processed_rows, 2);
        assert!(source.pending.lock().await.is_none());
    }

    #[tokio::test]
    async fn given_v3_empty_poll_after_nack_should_carry_no_advanced_state() {
        let base = start_server(rows_then_empty("/api/v3/query_sql", V3_JSONL, 1)).await;
        let source = connected_source(v3_config_for(&base));

        let produced = source.poll().await.unwrap();
        assert_eq!(produced.messages.len(), 2);
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .unwrap();

        let empty = source.poll().await.unwrap();
        assert!(empty.messages.is_empty());
        assert!(empty.state.is_none());
        assert!(source.pending.lock().await.is_none());

        let state = v3_state(&source).await;
        assert!(state.last_timestamp.is_none());
        assert_eq!(state.processed_rows, 0);
    }

    #[tokio::test]
    async fn given_ack_without_staged_batch_should_keep_state_unchanged() {
        let source = InfluxDbSource::new(1, make_v2_config(), None);
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .unwrap();
        let state = v2_state(&source).await;
        assert!(state.last_timestamp.is_none());
        assert_eq!(state.processed_rows, 0);
    }

    fn make_v2_config() -> InfluxDbSourceConfig {
        InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:8086".to_string(),
            org: "test_org".to_string(),
            token: SecretString::from("test_token"),
            query: r#"from(bucket:"b") |> range(start: $cursor) |> limit(n: $limit)"#.to_string(),
            poll_interval: Some("1s".to_string()),
            batch_size: Some(100),
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: Some(true),
            verbose_logging: None,
            max_retries: Some(3),
            retry_delay: Some("100ms".to_string()),
            timeout: Some("5s".to_string()),
            max_open_retries: Some(3),
            open_retry_max_delay: Some("5s".to_string()),
            retry_max_delay: Some("1s".to_string()),
            circuit_breaker_threshold: Some(5),
            circuit_breaker_cool_down: Some("30s".to_string()),
        })
    }

    fn make_v3_config() -> InfluxDbSourceConfig {
        InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:8181".to_string(),
            db: "test_db".to_string(),
            token: SecretString::from("test_token"),
            query: "SELECT time, val FROM tbl WHERE time > '$cursor' ORDER BY time LIMIT $limit OFFSET $offset"
                .to_string(),
            poll_interval: Some("1s".to_string()),
            batch_size: Some(100),
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(3),
            retry_delay: Some("100ms".to_string()),
            timeout: Some("5s".to_string()),
            max_open_retries: Some(3),
            open_retry_max_delay: Some("5s".to_string()),
            retry_max_delay: Some("1s".to_string()),
            circuit_breaker_threshold: Some(5),
            circuit_breaker_cool_down: Some("30s".to_string()),
            stuck_batch_cap_factor: Some(10),
        })
    }

    #[test]
    fn v2_source_new_creates_v2_state() {
        let source = InfluxDbSource::new(1, make_v2_config(), None);
        assert!(matches!(source.version_state, VersionState::V2(_)));
        assert!(source.state_restore_error.is_none());
    }

    #[test]
    fn v3_source_new_creates_v3_state() {
        let source = InfluxDbSource::new(1, make_v3_config(), None);
        assert!(matches!(source.version_state, VersionState::V3(_)));
        assert!(source.state_restore_error.is_none());
    }

    #[tokio::test]
    async fn state_restore_fails_on_version_mismatch() {
        let v2_state = PersistedState::V2(V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            processed_rows: 42,
            cursor_row_count: 0,
        });
        let persisted = ConnectorState::serialize(&v2_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v3_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "V3 connector must refuse V2 persisted state"
        );
    }

    #[tokio::test]
    async fn open_returns_init_error_when_restore_failed() {
        let garbage = ConnectorState(vec![0xFF, 0xFE, 0xFD]);
        let mut source = InfluxDbSource::new(1, make_v2_config(), Some(garbage));
        assert!(source.state_restore_error.is_some());
        let result = source.open().await;
        assert!(
            matches!(result, Err(Error::InitError(_))),
            "open() must fail with InitError on restore failure"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("persisted state exists but could not be deserialized"),
            "error message must include the cause: {msg}"
        );
    }

    #[test]
    fn restore_v2_state_with_corrupt_cursor_marks_restore_error() {
        let bad_state = PersistedState::V2(V2State {
            last_timestamp: Some("not-a-timestamp".to_string()),
            processed_rows: 5,
            cursor_row_count: 0,
        });
        let persisted = ConnectorState::serialize(&bad_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "corrupt cursor in persisted V2 state must set state_restore_error"
        );
        let msg = source.state_restore_error.unwrap();
        assert!(
            msg.contains("not-a-timestamp"),
            "error message must quote the bad cursor value: {msg}"
        );
    }

    #[test]
    fn restore_v3_state_with_corrupt_cursor_marks_restore_error() {
        let bad_state = PersistedState::V3(V3State {
            last_timestamp: Some("not-a-timestamp".to_string()),
            processed_rows: 5,
            effective_batch_size: 500,
            last_timestamp_row_offset: 0,
            stuck_cursor: None,
        });
        let persisted = ConnectorState::serialize(&bad_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v3_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "corrupt cursor in persisted V3 state must set state_restore_error"
        );
        let msg = source.state_restore_error.unwrap();
        assert!(
            msg.contains("not-a-timestamp"),
            "error message must quote the bad cursor value: {msg}"
        );
    }

    #[test]
    fn restore_v2_state_with_valid_cursor_succeeds() {
        let good_state = PersistedState::V2(V2State {
            last_timestamp: Some("2025-01-01T00:00:00Z".to_string()),
            processed_rows: 100,
            cursor_row_count: 2,
        });
        let persisted = ConnectorState::serialize(&good_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_none(),
            "valid cursor must not produce restore error"
        );
    }

    #[test]
    fn restore_v2_state_with_none_cursor_succeeds() {
        let state = PersistedState::V2(V2State {
            last_timestamp: None,
            processed_rows: 0,
            cursor_row_count: 0,
        });
        let persisted = ConnectorState::serialize(&state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(source.state_restore_error.is_none());
    }

    #[tokio::test]
    async fn open_rejects_invalid_initial_offset() {
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            initial_offset: Some("not-a-timestamp".to_string()),
            org: "o".to_string(),
            token: SecretString::from("t"),
            query: "SELECT 1".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "expected InvalidConfigValue for bad initial_offset, got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_rejects_timezone_free_initial_offset() {
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            initial_offset: Some("2024-01-15T10:30:00".to_string()),
            org: "o".to_string(),
            token: SecretString::from("t"),
            query: "SELECT 1".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "initial_offset without timezone must be rejected"
        );
    }

    #[tokio::test]
    async fn open_v2_rejects_empty_org() {
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            org: "".to_string(),
            token: SecretString::from("t"),
            query: "q WHERE time > '$cursor'".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "empty org must be rejected"
        );
    }

    #[tokio::test]
    async fn open_v3_rejects_empty_db() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "empty db must be rejected"
        );
    }

    #[tokio::test]
    async fn open_rejects_query_missing_cursor_placeholder() {
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            org: "myorg".to_string(),
            token: SecretString::from("t"),
            query: "from(bucket:\"b\") |> range(start: -1h) |> limit(n: $limit)".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("$cursor")),
            "query without $cursor must be rejected: {err:?}"
        );
    }

    #[tokio::test]
    async fn open_rejects_query_with_cursor_as_prefix_of_longer_identifier() {
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            org: "myorg".to_string(),
            token: SecretString::from("t"),
            query: "from(bucket:\"b\") |> range(start: $cursor_field) |> limit(n: $limit)"
                .to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("$cursor")),
            "$cursor_field must not satisfy the $cursor placeholder check: {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_rejects_inclusive_cursor_operator() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "mydb".to_string(),
            token: SecretString::from("t"),
            query:
                "SELECT * FROM t WHERE time >= '$cursor' ORDER BY time LIMIT $limit OFFSET $offset"
                    .to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains(">=")),
            "V3 query with >= '$cursor' must be rejected: {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_allows_other_gte_in_query() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "mydb".to_string(),
            token: SecretString::from("t"),
            query:
                "SELECT * FROM t WHERE time > '$cursor' AND val >= 0 ORDER BY time LIMIT $limit OFFSET $offset"
                    .to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains(">=")),
            "query with >= on non-cursor field must not be rejected for >=: {err:?}"
        );
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("ORDER BY")),
            "query with ORDER BY must not be rejected for ORDER BY: {err:?}"
        );
    }

    #[tokio::test]
    async fn poll_returns_empty_when_circuit_is_open() {
        let config = match make_v2_config() {
            InfluxDbSourceConfig::V2(mut c) => {
                c.circuit_breaker_threshold = Some(1);
                c.circuit_breaker_cool_down = Some("60s".to_string());
                c.poll_interval = Some("1ms".to_string());
                InfluxDbSourceConfig::V2(c)
            }
            other => other,
        };
        let source = InfluxDbSource::new(1, config, None);
        source.circuit_breaker.record_failure().await;
        assert!(source.circuit_breaker.is_open().await);

        let result = source.poll().await;
        assert!(result.is_ok());
        assert!(result.unwrap().messages.is_empty());
    }

    #[tokio::test]
    async fn close_clears_client() {
        let mut source = InfluxDbSource::new(1, make_v2_config(), None);
        let result = source.close().await;
        assert!(result.is_ok());
        assert!(source.client.is_none());
    }

    #[test]
    fn apply_v2_cursor_advance_moves_cursor() {
        let mut state = V2State::default();
        apply_v2_cursor_advance(&mut state, Some("2024-01-01T00:00:01Z".to_string()), 3, 0);
        assert_eq!(
            state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:01Z")
        );
        assert_eq!(state.cursor_row_count, 3);
    }

    #[test]
    fn apply_v2_cursor_advance_accumulates_same_cursor() {
        let mut state = V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            cursor_row_count: 3,
            processed_rows: 0,
        };
        apply_v2_cursor_advance(&mut state, Some("2024-01-01T00:00:00Z".to_string()), 2, 0);
        assert_eq!(state.cursor_row_count, 5);
    }

    #[test]
    fn apply_v2_cursor_advance_corrects_inflated_counter() {
        let mut state = V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            cursor_row_count: 10,
            processed_rows: 0,
        };
        apply_v2_cursor_advance(&mut state, None, 0, 3);
        assert_eq!(state.cursor_row_count, 3);
    }

    #[tokio::test]
    async fn open_v3_rejects_query_without_offset_when_stuck_cap_active() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "expected InvalidConfigValue when $offset missing in V3 query, got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_accepts_query_with_offset_placeholder() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query:
                "SELECT * FROM t WHERE time > '$cursor' ORDER BY time LIMIT $limit OFFSET $offset"
                    .to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("$offset")),
            "open() must not reject a query that contains $offset; got {err:?}"
        );
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("ORDER BY")),
            "open() must not reject a query that contains ORDER BY; got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_with_zero_stuck_cap_skips_offset_check() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: Some(0),
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("$offset")),
            "open() must not check $offset when stuck_batch_cap_factor=0; got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_rejects_query_without_order_by_when_stuck_cap_active() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("ORDER BY")),
            "V3 query without ORDER BY must be rejected when stuck detection active: {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_with_zero_stuck_cap_skips_order_by_check() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: Some(0),
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("ORDER BY")),
            "ORDER BY check must be skipped when stuck_batch_cap_factor=0; got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_rejects_stuck_batch_cap_factor_above_max() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query: "SELECT 1".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: Some(v3::MAX_STUCK_CAP_FACTOR + 1),
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "expected InvalidConfigValue for oversized stuck_batch_cap_factor, got {err:?}"
        );
    }

    #[test]
    fn config_accessors_v2() {
        let cfg = make_v2_config();
        assert_eq!(cfg.version_label(), "v2");
        assert_eq!(cfg.cursor_field(), "_time");
        assert_eq!(cfg.batch_size(), 100);
    }

    #[test]
    fn config_accessors_v3() {
        let cfg = make_v3_config();
        assert_eq!(cfg.version_label(), "v3");
        assert_eq!(cfg.cursor_field(), "time");
        assert_eq!(cfg.batch_size(), 100);
    }

    #[test]
    fn sort_call_detected_in_flux_pipeline() {
        assert!(query_has_sort_call(
            r#"from(bucket:"b") |> range(start: -1h) |> sort(columns: ["_time"])"#
        ));
    }

    #[test]
    fn sort_call_detected_without_pipe() {
        assert!(query_has_sort_call("sort(columns: [\"_time\"])"));
    }

    #[test]
    fn sort_call_not_detected_when_absent() {
        assert!(!query_has_sort_call(
            r#"from(bucket:"b") |> range(start: $cursor) |> limit(n: $limit)"#
        ));
    }

    #[test]
    fn sort_call_not_false_positive_on_identifier_prefix() {
        assert!(!query_has_sort_call("mysort(columns: [\"_time\"])"));
        assert!(!query_has_sort_call("do_sort(x)"));
    }

    #[test]
    fn sort_call_detected_at_start_of_string() {
        assert!(query_has_sort_call(
            "sort(columns: [\"_time\"]) |> limit(n: 10)"
        ));
    }

    #[test]
    fn sort_call_not_detected_with_space_before_paren() {
        assert!(!query_has_sort_call("sort (columns: [\"_time\"])"));
    }

    #[test]
    fn sort_call_in_line_comment_is_ignored() {
        assert!(!query_has_sort_call(
            "from(bucket:\"b\") // sort(columns:[\"_time\"]) not real"
        ));
        assert!(query_has_sort_call(
            "sort(columns:[\"_time\"]) // also present"
        ));
    }

    #[test]
    fn sort_call_in_block_comment_is_ignored() {
        assert!(!query_has_sort_call(
            "/* sort(columns: [\"_time\"]) */ from(bucket:\"b\")"
        ));
        assert!(!query_has_sort_call(
            "/*\n sort(columns: [\"_time\"])\n*/ from(bucket:\"b\")"
        ));
        assert!(query_has_sort_call(
            "/* comment */ sort(columns: [\"_time\"])"
        ));
    }

    #[test]
    fn order_by_detected_in_sql_query() {
        assert!(query_has_order_by(
            "SELECT * FROM t WHERE time > '$cursor' ORDER BY time LIMIT $limit"
        ));
        assert!(query_has_order_by("SELECT 1 order by id"));
        assert!(query_has_order_by("SELECT 1 Order By x DESC"));
    }

    #[test]
    fn order_by_not_detected_when_absent() {
        assert!(!query_has_order_by(
            "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset"
        ));
    }

    #[test]
    fn order_by_in_sql_comment_is_ignored() {
        assert!(!query_has_order_by(
            "-- ORDER BY legacy behavior\nSELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset"
        ));
        assert!(query_has_order_by(
            "SELECT * FROM t ORDER BY time -- sort by time\nLIMIT $limit"
        ));
    }

    #[test]
    fn order_by_in_block_comment_is_ignored() {
        assert!(!query_has_order_by(
            "/* ORDER BY example */\nSELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset"
        ));
        assert!(!query_has_order_by(
            "/* this is\n   ORDER BY a comment */\nSELECT * FROM t LIMIT $limit"
        ));
        assert!(query_has_order_by(
            "/* header */\nSELECT * FROM t WHERE time > '$cursor' ORDER BY time LIMIT $limit"
        ));
    }

    #[test]
    fn inclusive_cursor_flux_not_detected_for_unrelated_gte() {
        assert!(!query_has_inclusive_cursor(
            r#"from(bucket:"b") |> range(start: $cursor) |> filter(fn: (r) => r.val >= 10)"#,
            "//"
        ));
        assert!(!query_has_inclusive_cursor(
            "from(bucket:\"b\") |> filter(fn: (r) => r.v >= 5) // inclusive",
            "//"
        ));
    }

    #[test]
    fn inclusive_cursor_flux_detected_bare() {
        assert!(query_has_inclusive_cursor(
            "from(bucket:\"b\") |> filter(fn: (r) => r._time >= $cursor)",
            "//"
        ));
    }

    #[test]
    fn inclusive_cursor_flux_detected_single_quote() {
        assert!(query_has_inclusive_cursor(
            "from(bucket:\"b\") |> filter(fn: (r) => r._time >= '$cursor')",
            "//"
        ));
    }

    #[test]
    fn inclusive_cursor_flux_detected_double_quote() {
        assert!(query_has_inclusive_cursor(
            "from(bucket:\"b\") |> filter(fn: (r) => r._time >= \"$cursor\")",
            "//"
        ));
    }

    #[test]
    fn inclusive_cursor_flux_not_detected_when_only_in_line_comment() {
        assert!(!query_has_inclusive_cursor(
            "from(bucket:\"b\") // r._time >= $cursor\n|> range(start: $cursor)",
            "//"
        ));
        assert!(!query_has_inclusive_cursor(
            "from(bucket:\"b\") // batch_size >= 100\n|> range(start: $cursor)",
            "//"
        ));
    }

    #[test]
    fn inclusive_cursor_flux_not_detected_when_only_in_block_comment() {
        assert!(!query_has_inclusive_cursor(
            "/* use >= '$cursor' for inclusive */ from(bucket:\"b\") |> range(start: $cursor)",
            "//"
        ));
    }

    #[test]
    fn inclusive_cursor_flux_not_detected_for_extended_identifier() {
        assert!(!query_has_inclusive_cursor(
            "from(bucket:\"b\") |> filter(fn: (r) => r._time >= $cursor_start)",
            "//"
        ));
    }

    #[test]
    fn inclusive_cursor_detected_single_quote() {
        assert!(query_has_inclusive_cursor(
            "SELECT * FROM t WHERE time >= '$cursor' LIMIT $limit",
            "--"
        ));
        assert!(query_has_inclusive_cursor(
            "SELECT * FROM t WHERE time >='$cursor' LIMIT $limit",
            "--"
        ));
    }

    #[test]
    fn inclusive_cursor_detected_double_quote() {
        assert!(query_has_inclusive_cursor(
            "SELECT * FROM t WHERE time >= \"$cursor\" LIMIT $limit",
            "--"
        ));
        assert!(query_has_inclusive_cursor(
            "SELECT * FROM t WHERE time >=\"$cursor\" LIMIT $limit",
            "--"
        ));
    }

    #[test]
    fn inclusive_cursor_detected_bare() {
        assert!(query_has_inclusive_cursor(
            "SELECT * FROM t WHERE time >= $cursor LIMIT $limit",
            "--"
        ));
        assert!(query_has_inclusive_cursor(
            "SELECT * FROM t WHERE time >=$cursor LIMIT $limit",
            "--"
        ));
    }

    #[test]
    fn inclusive_cursor_not_detected_for_strict_gt() {
        assert!(!query_has_inclusive_cursor(
            "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit",
            "--"
        ));
    }

    #[test]
    fn inclusive_cursor_not_detected_for_extended_identifier() {
        assert!(!query_has_inclusive_cursor(
            "SELECT * FROM t WHERE time >= '$cursor_field' LIMIT $limit",
            "--"
        ));
        assert!(!query_has_inclusive_cursor(
            "SELECT * FROM t WHERE time >= $cursor_field LIMIT $limit",
            "--"
        ));
    }

    #[test]
    fn inclusive_cursor_not_detected_when_only_in_sql_comment() {
        assert!(!query_has_inclusive_cursor(
            "-- use >= '$cursor' for inclusive\nSELECT * FROM t WHERE time > '$cursor'",
            "--"
        ));
    }

    #[test]
    fn inclusive_cursor_not_detected_when_only_in_block_comment() {
        assert!(!query_has_inclusive_cursor(
            "/* example: time >= '$cursor' */ SELECT * FROM t WHERE time > '$cursor'",
            "--"
        ));
    }

    #[test]
    fn apply_v2_cursor_advance_no_new_cursor_no_skipped_is_noop() {
        let mut state = V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            cursor_row_count: 7,
            processed_rows: 0,
        };
        apply_v2_cursor_advance(&mut state, None, 0, 0);
        assert_eq!(state.cursor_row_count, 7);
        assert_eq!(
            state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:00Z")
        );
    }

    #[test]
    fn restore_v2_state_with_v2_persisted_restores_successfully() {
        let v2_state = PersistedState::V2(V2State {
            last_timestamp: Some("2024-06-01T00:00:00Z".to_string()),
            processed_rows: 99,
            cursor_row_count: 3,
        });
        let persisted = ConnectorState::serialize(&v2_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_none(),
            "V2 state on V2 connector must succeed"
        );
        if let VersionState::V2(mu) = &source.version_state {
            let state = mu.blocking_lock();
            assert_eq!(
                state.last_timestamp.as_deref(),
                Some("2024-06-01T00:00:00Z")
            );
            assert_eq!(state.processed_rows, 99);
        } else {
            panic!("expected V2 version state");
        }
    }

    #[test]
    fn restore_v2_state_with_v3_persisted_marks_restore_failed() {
        let v3_state = PersistedState::V3(V3State {
            last_timestamp: Some("2024-06-01T00:00:00Z".to_string()),
            processed_rows: 10,
            effective_batch_size: 500,
            last_timestamp_row_offset: 0,
            stuck_cursor: None,
        });
        let persisted = ConnectorState::serialize(&v3_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "V3 state on V2 connector must set state_restore_error"
        );
    }

    #[test]
    fn restore_v3_state_with_v3_persisted_restores_successfully() {
        let v3_state = PersistedState::V3(V3State {
            last_timestamp: Some("2024-07-15T12:00:00Z".to_string()),
            processed_rows: 500,
            effective_batch_size: 1000,
            last_timestamp_row_offset: 5,
            stuck_cursor: None,
        });
        let persisted = ConnectorState::serialize(&v3_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v3_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_none(),
            "V3 state on V3 connector must succeed"
        );
        if let VersionState::V3(mu) = &source.version_state {
            let state = mu.blocking_lock();
            assert_eq!(
                state.last_timestamp.as_deref(),
                Some("2024-07-15T12:00:00Z")
            );
            assert_eq!(state.processed_rows, 500);
            assert_eq!(state.effective_batch_size, 1000);
        } else {
            panic!("expected V3 version state");
        }
    }

    #[tokio::test]
    async fn poll_returns_connection_error_when_client_not_initialized() {
        let source = InfluxDbSource::new(1, make_v2_config(), None);
        let result = source.poll().await;
        assert!(
            matches!(result, Err(Error::Connection(_))),
            "expected Connection error when client not initialized, got {result:?}"
        );
    }

    #[test]
    fn restore_v2_state_with_legacy_format_succeeds() {
        let old_v2 = V2State {
            last_timestamp: Some("2024-06-01T00:00:00Z".to_string()),
            processed_rows: 42,
            cursor_row_count: 3,
        };
        let cs = ConnectorState::serialize(&old_v2, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(cs));
        assert!(
            source.state_restore_error.is_none(),
            "legacy V2 state without version tag must restore without error"
        );
        if let VersionState::V2(mu) = &source.version_state {
            let state = mu.blocking_lock();
            assert_eq!(state.processed_rows, 42);
            assert_eq!(state.cursor_row_count, 3);
        } else {
            panic!("expected V2 version state");
        }
    }

    #[test]
    fn restore_v3_state_with_invalid_stuck_cursor_marks_restore_error() {
        let bad_state = PersistedState::V3(V3State {
            last_timestamp: Some("2025-01-01T00:00:00Z".to_string()),
            processed_rows: 5,
            effective_batch_size: 500,
            last_timestamp_row_offset: 3,
            stuck_cursor: Some("not-a-timestamp".to_string()),
        });
        let persisted = ConnectorState::serialize(&bad_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v3_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "corrupt stuck_cursor in persisted V3 state must set state_restore_error"
        );
        let msg = source.state_restore_error.unwrap();
        assert!(
            msg.contains("not-a-timestamp"),
            "error message must quote the bad stuck_cursor value: {msg}"
        );
    }

    #[test]
    fn restore_v3_state_with_garbage_data_marks_restore_error() {
        let garbage = ConnectorState(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let source = InfluxDbSource::new(1, make_v3_config(), Some(garbage));
        assert!(
            source.state_restore_error.is_some(),
            "garbage state for V3 connector must set state_restore_error"
        );
        let msg = source.state_restore_error.unwrap();
        assert!(
            msg.contains("could not be deserialized"),
            "error message must explain the cause: {msg}"
        );
    }

    #[test]
    fn apply_v2_cursor_advance_corrupt_cursor_advances_to_server_cursor() {
        let mut state = V2State {
            last_timestamp: Some("this-is-not-a-timestamp".to_string()),
            cursor_row_count: 3,
            processed_rows: 0,
        };
        apply_v2_cursor_advance(&mut state, Some("2024-06-01T00:00:00Z".to_string()), 1, 0);
        assert_eq!(
            state.last_timestamp.as_deref(),
            Some("2024-06-01T00:00:00Z"),
            "corrupt old cursor: advance to server cursor to recover"
        );
        assert_eq!(
            state.cursor_row_count, 1,
            "cursor_row_count reset to rows_at_max_cursor for new cursor"
        );
    }

    #[tokio::test]
    async fn open_v3_rejects_stuck_cap_factor_of_one() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "mydb".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: Some(1),
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "stuck_batch_cap_factor=1 must be rejected, use 0 to disable or >= 2; got {err:?}"
        );
    }
}
