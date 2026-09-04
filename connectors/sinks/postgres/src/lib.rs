use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use humantime::Duration as HumanDuration;
use picomq_connector_sdk::destination::DestinationTemplate;
use picomq_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

sink_connector!(PostgresSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_MAX_CONNECTIONS: u32 = 10;

#[derive(Debug)]
pub struct PostgresSink {
    pub id: u32,
    pool: Option<Pool<Postgres>>,
    config: PostgresSinkConfig,
    state: Mutex<State>,
    verbose: bool,
    retry_delay: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSinkConfig {
    #[serde(serialize_with = "picomq_connector_sdk::secret::serialize_secret")]
    pub connection_string: SecretString,
    pub target_table: DestinationTemplate,
    pub batch_size: Option<u32>,
    pub max_connections: Option<u32>,
    pub auto_create_table: Option<bool>,
    pub include_metadata: Option<bool>,
    pub include_key: Option<bool>,
    pub payload_format: Option<String>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    #[default]
    Bytea,
    Json,
    Text,
}

impl FromStr for PayloadFormat {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "bytea" => Ok(PayloadFormat::Bytea),
            "json" | "jsonb" => Ok(PayloadFormat::Json),
            "text" => Ok(PayloadFormat::Text),
            other => Err(Error::InvalidConfigValue(format!(
                "unsupported payload_format '{other}', expected bytea, json or text"
            ))),
        }
    }
}

impl PayloadFormat {
    fn sql_type(&self) -> &'static str {
        match self {
            PayloadFormat::Bytea => "BYTEA",
            PayloadFormat::Json => "JSONB",
            PayloadFormat::Text => "TEXT",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Columns {
    metadata: bool,
    key: bool,
    payload: PayloadFormat,
}

impl Columns {
    fn per_row(&self) -> u32 {
        1 + if self.metadata { 4 } else { 0 } + if self.key { 1 } else { 0 }
    }
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    insertion_errors: u64,
    ensured_tables: HashSet<String>,
}

impl PostgresSink {
    pub fn new(id: u32, config: PostgresSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let delay_str = config.retry_delay.as_deref().unwrap_or(DEFAULT_RETRY_DELAY);
        let retry_delay = HumanDuration::from_str(delay_str)
            .map(|duration| duration.into())
            .unwrap_or_else(|_| Duration::from_secs(1));
        PostgresSink {
            id,
            pool: None,
            config,
            state: Mutex::new(State {
                messages_processed: 0,
                insertion_errors: 0,
                ensured_tables: HashSet::new(),
            }),
            verbose,
            retry_delay,
        }
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening PostgreSQL sink connector with ID: {}. Target table: {}",
            self.id, self.config.target_table
        );
        self.columns()?;
        self.connect().await?;
        if self.config.target_table.is_static() {
            let table = self.config.target_table.resolve("")?;
            self.ensure_table_exists(&table).await?;
        }
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.process_messages(topic_metadata, &messages_metadata, &messages)
            .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing PostgreSQL sink connector with ID: {}", self.id);

        if let Some(pool) = self.pool.take() {
            pool.close().await;
            info!(
                "PostgreSQL connection pool closed for sink connector ID: {}",
                self.id
            );
        }

        let state = self.state.lock().await;
        info!(
            "PostgreSQL sink ID: {} processed {} messages with {} errors",
            self.id, state.messages_processed, state.insertion_errors
        );
        Ok(())
    }
}

impl PostgresSink {
    async fn connect(&mut self) -> Result<(), Error> {
        let max_connections = self
            .config
            .max_connections
            .unwrap_or(DEFAULT_MAX_CONNECTIONS);
        let redacted = redact_connection_string(self.config.connection_string.expose_secret());

        info!("Connecting to PostgreSQL with max {max_connections} connections: {redacted}");

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(self.config.connection_string.expose_secret())
            .await
            .map_err(|e| Error::InitError(format!("Failed to connect to PostgreSQL: {e}")))?;

        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| Error::InitError(format!("Database connectivity test failed: {e}")))?;

        self.pool = Some(pool);
        info!("Connected to PostgreSQL database with {max_connections} max connections");
        Ok(())
    }

    async fn ensure_table_exists(&self, table_name: &str) -> Result<(), Error> {
        if !self.config.auto_create_table.unwrap_or(false) {
            return Ok(());
        }
        {
            let state = self.state.lock().await;
            if state.ensured_tables.contains(table_name) {
                return Ok(());
            }
        }

        let pool = self.get_pool()?;
        let columns = self.columns()?;
        let sql = build_create_table_query(table_name, columns)?;

        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(pool)
            .await
            .map_err(|e| Error::InitError(format!("Failed to create table '{table_name}': {e}")))?;

        info!(
            "Ensured table '{table_name}' exists with payload type {}",
            columns.payload.sql_type()
        );
        self.state
            .lock()
            .await
            .ensured_tables
            .insert(table_name.to_owned());
        Ok(())
    }

    async fn process_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let pool = self.get_pool()?;
        let table = self.config.target_table.resolve(&topic_metadata.topic)?;
        self.ensure_table_exists(&table).await?;
        let batch_size = self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE) as usize;

        for batch in messages.chunks(batch_size) {
            if let Err(e) = self
                .insert_batch(batch, &table, topic_metadata, messages_metadata, pool)
                .await
            {
                let mut state = self.state.lock().await;
                state.insertion_errors += batch.len() as u64;
                error!("Failed to insert batch into '{table}': {e}");
                return Err(e);
            }
            let mut state = self.state.lock().await;
            state.messages_processed += batch.len() as u64;
        }

        let msg_count = messages.len();
        if self.verbose {
            info!(
                "PostgreSQL sink ID: {} processed {msg_count} messages to table '{table}'",
                self.id
            );
        } else {
            debug!(
                "PostgreSQL sink ID: {} processed {msg_count} messages to table '{table}'",
                self.id
            );
        }

        Ok(())
    }

    async fn insert_batch(
        &self,
        messages: &[ConsumedMessage],
        table_name: &str,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        pool: &Pool<Postgres>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let columns = self.columns()?;
        let query = build_batch_insert_query(table_name, columns, messages.len())?;

        let max_retries = self.get_max_retries();
        let mut attempts = 0u32;

        loop {
            let result = bind_and_execute_batch(
                pool,
                &query,
                messages,
                topic_metadata,
                messages_metadata,
                columns,
            )
            .await;

            match result {
                Ok(()) => return Ok(()),
                Err((e, is_transient)) => {
                    attempts += 1;
                    if !is_transient || attempts >= max_retries {
                        return Err(Error::CannotStoreData(format!(
                            "Batch insert failed after {attempts} attempts: {e}"
                        )));
                    }
                    warn!(
                        "Transient database error (attempt {attempts}/{max_retries}): {e}. Retrying..."
                    );
                    tokio::time::sleep(self.retry_delay * attempts).await;
                }
            }
        }
    }

    fn get_pool(&self) -> Result<&Pool<Postgres>, Error> {
        self.pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))
    }

    fn columns(&self) -> Result<Columns, Error> {
        let payload = match self.config.payload_format.as_deref() {
            Some(format) => format.parse()?,
            None => PayloadFormat::default(),
        };
        Ok(Columns {
            metadata: self.config.include_metadata.unwrap_or(true),
            key: self.config.include_key.unwrap_or(true),
            payload,
        })
    }

    fn get_max_retries(&self) -> u32 {
        self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }
}

async fn bind_and_execute_batch(
    pool: &Pool<Postgres>,
    query: &str,
    messages: &[ConsumedMessage],
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    columns: Columns,
) -> Result<(), (sqlx::Error, bool)> {
    let mut query_builder = sqlx::query(sqlx::AssertSqlSafe(query));

    for message in messages {
        let payload_bytes = message.payload.clone().try_into_vec().map_err(|e| {
            let err_msg = format!("Failed to convert payload to bytes: {e}");
            (sqlx::Error::Protocol(err_msg), false)
        })?;

        if columns.metadata {
            query_builder = query_builder
                .bind(topic_metadata.topic.clone())
                .bind(messages_metadata.partition)
                .bind(message.offset as i64)
                .bind(millis_to_datetime(message.timestamp));
        }

        if columns.key {
            query_builder = query_builder.bind(message.key.clone());
        }

        query_builder = match columns.payload {
            PayloadFormat::Bytea => query_builder.bind(payload_bytes),
            PayloadFormat::Json => {
                let json_value: serde_json::Value = serde_json::from_slice(&payload_bytes)
                    .map_err(|e| {
                        let err_msg = format!("Failed to parse payload as JSON: {e}");
                        (sqlx::Error::Protocol(err_msg), false)
                    })?;
                query_builder.bind(json_value)
            }
            PayloadFormat::Text => {
                let text_value = String::from_utf8(payload_bytes).map_err(|e| {
                    let err_msg = format!("Failed to parse payload as UTF-8 text: {e}");
                    (sqlx::Error::Protocol(err_msg), false)
                })?;
                query_builder.bind(text_value)
            }
        };
    }

    query_builder.execute(pool).await.map_err(|e| {
        let is_transient = is_transient_error(&e);
        (e, is_transient)
    })?;

    Ok(())
}

fn millis_to_datetime(millis: u64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(i64::try_from(millis).unwrap_or(i64::MAX))
        .unwrap_or_else(Utc::now)
}

fn build_create_table_query(table_name: &str, columns: Columns) -> Result<String, Error> {
    let quoted_table = quote_identifier(table_name)?;
    let mut sql = format!("CREATE TABLE IF NOT EXISTS {quoted_table} (id BIGSERIAL PRIMARY KEY");
    if columns.metadata {
        sql.push_str(", pico_topic TEXT NOT NULL");
        sql.push_str(", pico_partition INTEGER NOT NULL");
        sql.push_str(", pico_offset BIGINT NOT NULL");
        sql.push_str(", pico_timestamp TIMESTAMP WITH TIME ZONE NOT NULL");
    }
    if columns.key {
        sql.push_str(", pico_key BYTEA");
    }
    sql.push_str(&format!(", payload {}", columns.payload.sql_type()));
    sql.push_str(", created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()");
    if columns.metadata {
        sql.push_str(", UNIQUE (pico_topic, pico_partition, pico_offset)");
    }
    sql.push(')');
    Ok(sql)
}

fn build_batch_insert_query(
    table_name: &str,
    columns: Columns,
    row_count: usize,
) -> Result<String, Error> {
    let quoted_table = quote_identifier(table_name)?;
    let mut names: Vec<&str> = Vec::with_capacity(6);
    if columns.metadata {
        names.extend([
            "pico_topic",
            "pico_partition",
            "pico_offset",
            "pico_timestamp",
        ]);
    }
    if columns.key {
        names.push("pico_key");
    }
    names.push("payload");

    let params_per_row = columns.per_row();
    let mut query = format!("INSERT INTO {quoted_table} ({}) VALUES ", names.join(", "));
    let mut value_groups = Vec::with_capacity(row_count);
    for row_idx in 0..row_count {
        let base_param = (row_idx as u32) * params_per_row;
        let placeholders: Vec<String> = (1..=params_per_row)
            .map(|index| format!("${}", base_param + index))
            .collect();
        value_groups.push(format!("({})", placeholders.join(", ")));
    }
    query.push_str(&value_groups.join(", "));
    if columns.metadata {
        query.push_str(" ON CONFLICT (pico_topic, pico_partition, pico_offset) DO NOTHING");
    }
    Ok(query)
}

fn is_transient_error(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Io(_) => true,
        sqlx::Error::PoolTimedOut => true,
        sqlx::Error::PoolClosed => false,
        sqlx::Error::Protocol(_) => false,
        sqlx::Error::Database(db_err) => db_err.code().is_some_and(|code| {
            matches!(
                code.as_ref(),
                "40001" | "40P01" | "57P01" | "57P02" | "57P03" | "08000" | "08003" | "08006"
            )
        }),
        _ => false,
    }
}

fn quote_identifier(name: &str) -> Result<String, Error> {
    if name.is_empty() {
        return Err(Error::InitError("Table name cannot be empty".to_string()));
    }
    if name.contains('\0') {
        return Err(Error::InitError(
            "Table name cannot contain null characters".to_string(),
        ));
    }
    let escaped = name.replace('"', "\"\"");
    Ok(format!("\"{escaped}\""))
}

fn redact_connection_string(conn_str: &str) -> String {
    if let Some(scheme_end) = conn_str.find("://") {
        let scheme = &conn_str[..scheme_end + 3];
        let rest = &conn_str[scheme_end + 3..];
        let preview: String = rest.chars().take(3).collect();
        return format!("{scheme}{preview}***");
    }
    let preview: String = conn_str.chars().take(3).collect();
    format!("{preview}***")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PostgresSinkConfig {
        PostgresSinkConfig {
            connection_string: SecretString::from("postgres://localhost/db"),
            target_table: "messages".parse().unwrap(),
            batch_size: Some(100),
            max_connections: None,
            auto_create_table: None,
            include_metadata: None,
            include_key: None,
            payload_format: None,
            verbose_logging: None,
            max_retries: None,
            retry_delay: None,
        }
    }

    fn columns(metadata: bool, key: bool) -> Columns {
        Columns {
            metadata,
            key,
            payload: PayloadFormat::Bytea,
        }
    }

    #[test]
    fn given_json_format_should_return_json() {
        assert_eq!(
            "json".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::Json
        );
        assert_eq!(
            "jsonb".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::Json
        );
        assert_eq!(
            "JSON".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::Json
        );
    }

    #[test]
    fn given_text_format_should_return_text() {
        assert_eq!(
            "text".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::Text
        );
        assert_eq!(
            "TEXT".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::Text
        );
    }

    #[test]
    fn given_unknown_format_should_fail() {
        assert!("unknown".parse::<PayloadFormat>().is_err());
        assert_eq!(
            "bytea".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::Bytea
        );
    }

    #[test]
    fn given_payload_format_should_return_correct_sql_type() {
        assert_eq!(PayloadFormat::Bytea.sql_type(), "BYTEA");
        assert_eq!(PayloadFormat::Json.sql_type(), "JSONB");
        assert_eq!(PayloadFormat::Text.sql_type(), "TEXT");
    }

    #[test]
    fn given_all_options_enabled_should_build_full_insert_query() {
        let query = build_batch_insert_query("messages", columns(true, true), 1).unwrap();

        assert!(query.starts_with(
            "INSERT INTO \"messages\" (pico_topic, pico_partition, pico_offset, pico_timestamp, pico_key, payload) VALUES ($1, $2, $3, $4, $5, $6)"
        ));
        assert!(
            query.ends_with("ON CONFLICT (pico_topic, pico_partition, pico_offset) DO NOTHING")
        );
        assert_eq!(columns(true, true).per_row(), 6);
    }

    #[test]
    fn given_metadata_disabled_should_build_minimal_insert_query() {
        let query = build_batch_insert_query("messages", columns(false, false), 1).unwrap();

        assert_eq!(query, "INSERT INTO \"messages\" (payload) VALUES ($1)");
        assert_eq!(columns(false, false).per_row(), 1);
    }

    #[test]
    fn given_batch_of_3_rows_should_build_multi_row_insert_query() {
        let query = build_batch_insert_query("messages", columns(true, true), 3).unwrap();

        assert!(query.contains("($1, $2, $3, $4, $5, $6)"));
        assert!(query.contains("($7, $8, $9, $10, $11, $12)"));
        assert!(query.contains("($13, $14, $15, $16, $17, $18)"));
    }

    #[test]
    fn given_metadata_enabled_should_create_table_with_unique_offset() {
        let sql = build_create_table_query("messages", columns(true, false)).unwrap();

        assert!(sql.contains("pico_topic TEXT NOT NULL"));
        assert!(sql.contains("UNIQUE (pico_topic, pico_partition, pico_offset)"));
        assert!(!sql.contains("pico_key"));
    }

    #[test]
    fn given_templated_table_should_resolve_per_topic() {
        let mut config = test_config();
        config.target_table = "events_{topic_segment[-1]}".parse().unwrap();
        assert!(!config.target_table.is_static());
        assert_eq!(
            config.target_table.resolve("orders.user42").unwrap(),
            "events_user42"
        );
    }

    #[test]
    fn given_millis_should_convert_timestamp() {
        let dt = millis_to_datetime(1_767_225_600_000);
        assert_eq!(dt.timestamp(), 1_767_225_600);
    }

    #[test]
    fn given_default_config_should_use_default_retries() {
        let sink = PostgresSink::new(1, test_config());
        assert_eq!(sink.get_max_retries(), DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn given_custom_retries_should_use_custom_value() {
        let mut config = test_config();
        config.max_retries = Some(5);
        let sink = PostgresSink::new(1, config);
        assert_eq!(sink.get_max_retries(), 5);
    }

    #[test]
    fn given_default_config_should_use_default_retry_delay() {
        let sink = PostgresSink::new(1, test_config());
        assert_eq!(sink.retry_delay, Duration::from_secs(1));
    }

    #[test]
    fn given_custom_retry_delay_should_parse_humantime() {
        let mut config = test_config();
        config.retry_delay = Some("500ms".to_string());
        let sink = PostgresSink::new(1, config);
        assert_eq!(sink.retry_delay, Duration::from_millis(500));
    }

    #[test]
    fn given_verbose_logging_enabled_should_set_verbose_flag() {
        let mut config = test_config();
        config.verbose_logging = Some(true);
        let sink = PostgresSink::new(1, config);
        assert!(sink.verbose);
    }

    #[test]
    fn given_connection_string_with_credentials_should_redact() {
        let conn = "postgres://user:password@localhost:5432/db";
        assert_eq!(redact_connection_string(conn), "postgres://use***");
    }

    #[test]
    fn given_connection_string_without_scheme_should_redact() {
        assert_eq!(redact_connection_string("localhost:5432/db"), "loc***");
    }

    #[test]
    fn given_special_chars_in_identifier_should_escape() {
        assert_eq!(
            quote_identifier("table\"name").unwrap(),
            "\"table\"\"name\""
        );
    }

    #[test]
    fn given_empty_identifier_should_fail() {
        assert!(quote_identifier("").is_err());
    }

    #[test]
    fn given_null_char_in_identifier_should_fail() {
        assert!(quote_identifier("table\0name").is_err());
    }

    #[test]
    fn given_identifier_with_sql_injection_should_escape() {
        assert_eq!(
            quote_identifier("messages\"; DROP TABLE users; --").unwrap(),
            "\"messages\"\"; DROP TABLE users; --\""
        );
    }
}
