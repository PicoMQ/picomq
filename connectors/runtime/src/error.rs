use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Failed to serialize topic metadata")]
    FailedToSerializeTopicMetadata,
    #[error("Failed to serialize messages metadata")]
    FailedToSerializeMessagesMetadata,
    #[error("Failed to serialize raw messages")]
    FailedToSerializeRawMessages,
    #[error("Sink connector with ID: {plugin_id} failed to consume batch, status: {status}")]
    SinkConsumeFailed { plugin_id: u32, status: i32 },
    #[error("Connector SDK error")]
    ConnectorSdkError(#[from] picomq_connector_sdk::Error),
    #[error("Failed to load state for source connector '{connector_key}': {source}")]
    StateLoadFailed {
        connector_key: String,
        source: picomq_connector_sdk::Error,
    },
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Kafka bootstrap servers must not be empty")]
    MissingKafkaBootstrap,
    #[error("Invalid topic route: {0}")]
    InvalidTopicRoute(String),
    #[error("JSON error")]
    JsonError(#[from] serde_json::Error),
    #[error("Sink not found with key: {0}")]
    SinkNotFound(String),
    #[error("Sink config not found with key: {0}, version: {1}")]
    SinkConfigNotFound(String, u64),
    #[error("Source not found with key: {0}")]
    SourceNotFound(String),
    #[error("Source config not found with key: {0}, version: {1}")]
    SourceConfigNotFound(String, u64),
    #[error("Cannot convert configuration")]
    CannotConvertConfiguration,
    #[error("IO operation failed with error: {0:?}")]
    IoError(#[from] std::io::Error),
    #[error("HTTP request failed: {0}")]
    HttpRequestFailed(String),
}

impl RuntimeError {
    pub fn as_code(&self) -> &'static str {
        match self {
            RuntimeError::SinkNotFound(_) => "sink_not_found",
            RuntimeError::SinkConfigNotFound(_, _) => "sink_config_not_found",
            RuntimeError::SourceNotFound(_) => "source_not_found",
            RuntimeError::SourceConfigNotFound(_, _) => "source_config_not_found",
            RuntimeError::MissingKafkaBootstrap => "invalid_configuration",
            RuntimeError::InvalidTopicRoute(_) => "invalid_configuration",
            RuntimeError::InvalidConfiguration(_) => "invalid_configuration",
            RuntimeError::HttpRequestFailed(_) => "http_request_failed",
            RuntimeError::StateLoadFailed { .. } => "state_load_failed",
            RuntimeError::SinkConsumeFailed { .. } => "sink_consume_failed",
            _ => "error",
        }
    }
}
