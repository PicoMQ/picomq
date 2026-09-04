use crate::api::config::HttpConfig;
use crate::error::RuntimeError;
use derive_more::Display;
use figment::Figment;
use figment::providers::{Env, Format, Toml};
use humantime::format_duration;
use rdkafka::ClientConfig;
use reqwest::Url;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use tracing::info;

pub const ENV_PREFIX: &str = "PICOMQ_CONNECTORS_";
pub const ENV_NESTED_SEPARATOR: &str = "__";
const DEFAULT_CONFIG: &str = include_str!("../../config.toml");

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub service_name: String,
    pub logs: TelemetryLogsConfig,
    pub traces: TelemetryTracesConfig,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: "picomq-connectors".to_owned(),
            logs: TelemetryLogsConfig::default(),
            traces: TelemetryTracesConfig::default(),
        }
    }
}

impl Display for TelemetryConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, service_name: {}, logs: {}, traces: {} }}",
            self.enabled, self.service_name, self.logs, self.traces
        )
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct TelemetryLogsConfig {
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

impl Default for TelemetryLogsConfig {
    fn default() -> Self {
        Self {
            transport: TelemetryTransport::Grpc,
            endpoint: "http://localhost:4317".to_owned(),
        }
    }
}

impl Display for TelemetryLogsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ transport: {}, endpoint: {} }}",
            self.transport, self.endpoint
        )
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct TelemetryTracesConfig {
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

impl Default for TelemetryTracesConfig {
    fn default() -> Self {
        Self {
            transport: TelemetryTransport::Grpc,
            endpoint: "http://localhost:4317".to_owned(),
        }
    }
}

impl Display for TelemetryTracesConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ transport: {}, endpoint: {} }}",
            self.transport, self.endpoint
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Display, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TelemetryTransport {
    #[display("grpc")]
    Grpc,
    #[display("http")]
    Http,
}

impl FromStr for TelemetryTransport {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "grpc" => Ok(TelemetryTransport::Grpc),
            "http" => Ok(TelemetryTransport::Http),
            _ => Err(format!("Invalid telemetry transport: {s}")),
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConnectorsRuntimeConfig {
    pub http: HttpConfig,
    pub kafka: KafkaConfig,
    pub connectors: ConnectorsConfig,
    pub state: StateConfig,
    pub telemetry: TelemetryConfig,
    pub logging: LoggingConfig,
}

impl ConnectorsRuntimeConfig {
    pub fn load(path: &str) -> Result<Self, RuntimeError> {
        let mut figment = Figment::from(Toml::string(DEFAULT_CONFIG));
        if Path::new(path).exists() {
            figment = figment.merge(Toml::file(path));
        } else {
            info!("Config file '{path}' not found, using defaults and environment variables.");
        }
        figment
            .merge(Env::prefixed(ENV_PREFIX).split(ENV_NESTED_SEPARATOR))
            .extract()
            .map_err(|error| {
                RuntimeError::InvalidConfiguration(format!(
                    "Failed to load runtime configuration from '{path}': {error}"
                ))
            })
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub format: LogFormat,
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub client_id: String,
    pub security_protocol: KafkaSecurityProtocol,
    pub sasl: KafkaSaslConfig,
    pub tls: KafkaTlsConfig,
    pub properties: BTreeMap<String, String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_owned(),
            client_id: "picomq-connectors".to_owned(),
            security_protocol: KafkaSecurityProtocol::Plaintext,
            sasl: KafkaSaslConfig::default(),
            tls: KafkaTlsConfig::default(),
            properties: BTreeMap::new(),
        }
    }
}

impl KafkaConfig {
    pub fn client_config(&self) -> Result<ClientConfig, RuntimeError> {
        if self.bootstrap_servers.trim().is_empty() {
            return Err(RuntimeError::MissingKafkaBootstrap);
        }
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);
        config.set("client.id", &self.client_id);
        config.set("security.protocol", self.security_protocol.to_string());
        if self.security_protocol.uses_sasl() {
            config.set("sasl.mechanism", &self.sasl.mechanism);
            config.set("sasl.username", &self.sasl.username);
            config.set("sasl.password", self.sasl.password.expose_secret());
        }
        if self.security_protocol.uses_tls() {
            if !self.tls.ca_file.is_empty() {
                config.set("ssl.ca.location", &self.tls.ca_file);
            }
            if !self.tls.cert_file.is_empty() {
                config.set("ssl.certificate.location", &self.tls.cert_file);
            }
            if !self.tls.key_file.is_empty() {
                config.set("ssl.key.location", &self.tls.key_file);
            }
            if !self.tls.verify_hostname {
                config.set("ssl.endpoint.identification.algorithm", "none");
            }
        }
        for (key, value) in &self.properties {
            config.set(key, value);
        }
        Ok(config)
    }
}

impl Display for KafkaConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ bootstrap_servers: {}, client_id: {}, security_protocol: {}, sasl: {}, tls: {}, properties: {:?} }}",
            self.bootstrap_servers,
            self.client_id,
            self.security_protocol,
            self.sasl,
            self.tls,
            self.properties.keys()
        )
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum KafkaSecurityProtocol {
    #[default]
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl KafkaSecurityProtocol {
    pub fn uses_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }

    pub fn uses_tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaSaslConfig {
    pub mechanism: String,
    pub username: String,
    #[serde(serialize_with = "serialize_redacted_secret")]
    pub password: SecretString,
}

impl Default for KafkaSaslConfig {
    fn default() -> Self {
        Self {
            mechanism: "PLAIN".to_owned(),
            username: String::new(),
            password: SecretString::from(String::new()),
        }
    }
}

impl std::fmt::Debug for KafkaSaslConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KafkaSaslConfig")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

impl Display for KafkaSaslConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ mechanism: {}, username: {}, password: {} }}",
            self.mechanism,
            self.username,
            if self.password.expose_secret().is_empty() {
                ""
            } else {
                "****"
            }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaTlsConfig {
    pub ca_file: String,
    pub cert_file: String,
    pub key_file: String,
    pub verify_hostname: bool,
}

impl Default for KafkaTlsConfig {
    fn default() -> Self {
        Self {
            ca_file: String::new(),
            cert_file: String::new(),
            key_file: String::new(),
            verify_hostname: true,
        }
    }
}

impl Display for KafkaTlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ ca_file: {:?}, cert_file: {:?}, key_file: {:?}, verify_hostname: {} }}",
            self.ca_file, self.cert_file, self.key_file, self.verify_hostname
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    pub enabled: bool,
    pub max_attempts: u32,
    #[serde(with = "humantime_serde")]
    pub initial_backoff: Duration,
    #[serde(with = "humantime_serde")]
    pub max_backoff: Duration,
    pub backoff_multiplier: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: 2,
        }
    }
}

impl Display for RetryConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, max_attempts: {}, initial_backoff: {}, max_backoff: {}, backoff_multiplier: {} }}",
            self.enabled,
            self.max_attempts,
            format_duration(self.initial_backoff),
            format_duration(self.max_backoff),
            self.backoff_multiplier
        )
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct LocalConnectorsConfig {
    pub config_dir: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct HttpConnectorsConfig {
    pub base_url: String,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub request_headers: HashMap<String, String>,
    pub url_templates: HashMap<String, String>,
    pub response: ResponseConfig,
    pub retry: RetryConfig,
}

impl Default for HttpConnectorsConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            timeout: Duration::from_secs(10),
            request_headers: HashMap::new(),
            url_templates: HashMap::new(),
            response: ResponseConfig::default(),
            retry: RetryConfig::default(),
        }
    }
}

impl Display for HttpConnectorsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ type: \"http\", base_url: {:?}, request_headers: {:?}, timeout: {}, url_templates: {:?}, response: {:?}, retry: {} }}",
            self.base_url,
            self.request_headers.keys(),
            format_duration(self.timeout),
            self.url_templates,
            self.response,
            self.retry
        )
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ResponseConfig {
    pub data_path: Option<String>,
    pub error_path: Option<String>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "config_type", rename_all = "lowercase")]
pub enum ConnectorsConfig {
    Local(LocalConnectorsConfig),
    Http(HttpConnectorsConfig),
}

impl Default for ConnectorsConfig {
    fn default() -> Self {
        Self::Local(LocalConnectorsConfig::default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StateConfig {
    pub path: String,
    pub storage: StateStorageKind,
    pub http: HttpStateConfig,
}

impl Display for StateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, storage: {}, http: {} }}",
            self.path, self.storage, self.http
        )
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum StateStorageKind {
    #[default]
    File,
    Http,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpStateConfig {
    pub url: String,
    pub load_method: HttpStateMethod,
    pub save_method: HttpStateMethod,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(serialize_with = "serialize_redacted_secret_map")]
    pub request_headers: HashMap<String, SecretString>,
    pub retry: RetryConfig,
}

impl Default for HttpStateConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            load_method: HttpStateMethod::Get,
            save_method: HttpStateMethod::Put,
            timeout: Duration::from_secs(5),
            request_headers: HashMap::new(),
            retry: default_state_retry(),
        }
    }
}

impl Display for HttpStateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ url: {:?}, load_method: {}, save_method: {}, timeout: {}, request_headers: {:?}, retry: {} }}",
            state_url_label(&self.url),
            self.load_method,
            self.save_method,
            format_duration(self.timeout),
            self.request_headers.keys(),
            self.retry
        )
    }
}

impl std::fmt::Debug for HttpStateConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpStateConfig")
            .field("url", &state_url_label(&self.url))
            .field("load_method", &self.load_method)
            .field("save_method", &self.save_method)
            .field("timeout", &self.timeout)
            .field("request_headers", &self.request_headers.keys())
            .field("retry", &self.retry)
            .finish()
    }
}

fn state_url_label(value: &str) -> String {
    let Ok(mut url) = Url::parse(value) else {
        return "<invalid URL>".to_string();
    };
    if !url.username().is_empty() {
        let _ = url.set_username("redacted");
    }
    if url.password().is_some() {
        let _ = url.set_password(Some("redacted"));
    }
    if url.query().is_some() {
        url.set_query(Some("redacted"));
    }
    url.to_string()
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "UPPERCASE", ascii_case_insensitive)]
pub enum HttpStateMethod {
    #[default]
    Get,
    Put,
    Post,
    Patch,
}

fn default_state_retry() -> RetryConfig {
    RetryConfig {
        enabled: true,
        max_attempts: 4,
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(2),
        backoff_multiplier: 2,
    }
}

fn serialize_redacted_secret<S: serde::Serializer>(
    _secret: &SecretString,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str("[REDACTED]")
}

fn serialize_redacted_secret_map<S: serde::Serializer>(
    headers: &HashMap<String, SecretString>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_map(headers.keys().map(|name| (name, "[REDACTED]")))
}

impl Display for ConnectorsRuntimeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ http: {}, kafka: {}, connectors: {}, state: {}, telemetry: {}, logging: {{ format: {} }} }}",
            self.http, self.kafka, self.connectors, self.state, self.telemetry, self.logging.format
        )
    }
}

impl Display for ConnectorsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorsConfig::Local(config) => write!(
                f,
                "{{ type: \"file\", config_dir: {:?} }}",
                config.config_dir
            ),
            ConnectorsConfig::Http(config) => write!(f, "{config}",),
        }
    }
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            path: "local_state".to_owned(),
            storage: StateStorageKind::default(),
            http: HttpStateConfig::default(),
        }
    }
}

#[cfg(test)]
mod log_format_tests {
    use super::*;

    #[test]
    fn given_no_explicit_value_when_defaulted_should_be_text() {
        assert_eq!(LogFormat::default(), LogFormat::Text);
    }

    #[test]
    fn given_text_string_when_parsed_should_return_text_variant() {
        assert_eq!(LogFormat::from_str("text").unwrap(), LogFormat::Text);
        assert_eq!(LogFormat::from_str("TEXT").unwrap(), LogFormat::Text);
    }

    #[test]
    fn given_json_string_when_parsed_should_return_json_variant() {
        assert_eq!(LogFormat::from_str("json").unwrap(), LogFormat::Json);
        assert_eq!(LogFormat::from_str("Json").unwrap(), LogFormat::Json);
    }

    #[test]
    fn given_invalid_string_when_parsed_should_return_err() {
        assert!(LogFormat::from_str("yaml").is_err());
        assert!(LogFormat::from_str("").is_err());
    }

    #[test]
    fn given_log_format_when_displayed_should_match_lowercase_variant_name() {
        assert_eq!(LogFormat::Text.to_string(), "text");
        assert_eq!(LogFormat::Json.to_string(), "json");
    }

    #[test]
    fn given_toml_with_logging_section_when_deserialized_should_use_format() {
        let toml = r#"format = "json""#;
        let parsed: LoggingConfig = toml::from_str(toml).expect("parse logging");
        assert_eq!(parsed.format, LogFormat::Json);
    }

    #[test]
    fn given_toml_without_format_field_when_deserialized_should_default_to_text() {
        let parsed: LoggingConfig = toml::from_str("").expect("parse empty logging");
        assert_eq!(parsed.format, LogFormat::Text);
    }
}

#[cfg(test)]
mod kafka_config_tests {
    use super::*;

    #[test]
    fn given_default_kafka_config_when_built_should_set_bootstrap_and_client_id() {
        let config = KafkaConfig::default().client_config().unwrap();
        assert_eq!(config.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(config.get("client.id"), Some("picomq-connectors"));
        assert_eq!(config.get("security.protocol"), Some("plaintext"));
        assert!(config.get("sasl.username").is_none());
    }

    #[test]
    fn given_empty_bootstrap_when_built_should_fail() {
        let config = KafkaConfig {
            bootstrap_servers: " ".to_owned(),
            ..KafkaConfig::default()
        };
        assert!(matches!(
            config.client_config(),
            Err(RuntimeError::MissingKafkaBootstrap)
        ));
    }

    #[test]
    fn given_sasl_ssl_when_built_should_set_sasl_and_tls_properties() {
        let toml = r#"
            bootstrap_servers = "broker:9093"
            security_protocol = "sasl_ssl"
            [sasl]
            mechanism = "SCRAM-SHA-256"
            username = "alice"
            password = "secret"
            [tls]
            ca_file = "/certs/ca.pem"
            verify_hostname = false
            [properties]
            "message.max.bytes" = "2000000"
        "#;
        let parsed: KafkaConfig = toml::from_str(toml).unwrap();
        let config = parsed.client_config().unwrap();
        assert_eq!(config.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(config.get("sasl.mechanism"), Some("SCRAM-SHA-256"));
        assert_eq!(config.get("sasl.username"), Some("alice"));
        assert_eq!(config.get("sasl.password"), Some("secret"));
        assert_eq!(config.get("ssl.ca.location"), Some("/certs/ca.pem"));
        assert_eq!(
            config.get("ssl.endpoint.identification.algorithm"),
            Some("none")
        );
        assert_eq!(config.get("message.max.bytes"), Some("2000000"));
        let rendered = parsed.to_string();
        assert!(!rendered.contains("secret"), "{rendered}");
        let debug = format!("{parsed:?}");
        assert!(!debug.contains("secret"), "{debug}");
        let serialized = toml::to_string(&parsed).unwrap();
        assert!(!serialized.contains("secret"), "{serialized}");
    }
}

#[cfg(test)]
mod runtime_config_tests {
    use super::*;

    #[test]
    fn given_missing_file_when_loaded_should_use_bundled_defaults() {
        let config = ConnectorsRuntimeConfig::load("/nonexistent/picomq-connectors.toml").unwrap();
        assert_eq!(config.kafka.bootstrap_servers, "localhost:9092");
        assert_eq!(config.http.address, "127.0.0.1:8081");
        assert_eq!(config.state.path, "local_state");
    }
}

#[cfg(test)]
mod state_config_tests {
    use super::*;

    #[test]
    fn given_legacy_path_only_state_section_when_parsed_should_default_to_file_storage() {
        let parsed: StateConfig = toml::from_str(r#"path = "local_state""#).expect("parse state");
        assert_eq!(parsed.path, "local_state");
        assert_eq!(parsed.storage, StateStorageKind::File);
        assert!(parsed.http.url.is_empty());
        assert_eq!(parsed.http.load_method, HttpStateMethod::Get);
        assert_eq!(parsed.http.save_method, HttpStateMethod::Put);
        assert_eq!(parsed.http.timeout, Duration::from_secs(5));
    }

    #[test]
    fn given_http_state_section_when_parsed_should_populate_backend_config() {
        let toml = r#"
            path = "local_state"
            storage = "http"

            [http]
            url = "http://127.0.0.1:8080/connectors/state"
            load_method = "post"
            save_method = "patch"
            timeout = "10s"

            [http.request_headers]
            authorization = "Bearer token"

            [http.retry]
            enabled = true
            max_attempts = 7
            initial_backoff = "100ms"
            max_backoff = "1s"
            backoff_multiplier = 3
        "#;
        let parsed: StateConfig = toml::from_str(toml).expect("parse state");
        assert_eq!(parsed.storage, StateStorageKind::Http);
        assert_eq!(parsed.http.url, "http://127.0.0.1:8080/connectors/state");
        assert_eq!(parsed.http.load_method, HttpStateMethod::Post);
        assert_eq!(parsed.http.save_method, HttpStateMethod::Patch);
        assert_eq!(parsed.http.timeout, Duration::from_secs(10));
        assert!(parsed.http.request_headers.contains_key("authorization"));
        assert_eq!(parsed.http.retry.max_attempts, 7);
        assert_eq!(parsed.http.retry.backoff_multiplier, 3);
    }

    #[test]
    fn given_unknown_storage_kind_when_parsed_should_fail() {
        let result = toml::from_str::<StateConfig>(
            r#"
            path = "local_state"
            storage = "s3"
        "#,
        );
        assert!(result.is_err(), "unknown storage kinds must fail boot");
    }

    #[test]
    fn given_state_config_when_displayed_should_not_render_header_values() {
        let mut config = StateConfig::default();
        config.http.url = "https://user:password@example.com/state?token=query-secret".to_string();
        config
            .http
            .request_headers
            .insert("authorization".to_string(), "Bearer top-secret".into());
        let display_output = config.to_string();
        let debug_output = format!("{config:?}");
        let serialized_output = toml::to_string(&config).expect("serialize state config");
        assert!(!display_output.contains("top-secret"), "{display_output}");
        assert!(!debug_output.contains("top-secret"), "{debug_output}");
        assert!(!display_output.contains("password"), "{display_output}");
        assert!(!debug_output.contains("password"), "{debug_output}");
        assert!(!display_output.contains("query-secret"), "{display_output}");
        assert!(!debug_output.contains("query-secret"), "{debug_output}");
        assert!(
            !serialized_output.contains("top-secret"),
            "{serialized_output}"
        );
        assert!(serialized_output.contains("[REDACTED]"));
    }
}
