use crate::configs::connectors::{
    SinkConfig, SourceConfig, TopicConsumerConfig, TopicProducerConfig,
};
use crate::manager::{sink::SinkInfo, source::SourceInfo};
pub use picomq_connector_sdk::api::{SinkInfoResponse, SourceInfoResponse};
use picomq_connector_sdk::transforms::TransformType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkDetailsResponse {
    #[serde(flatten)]
    pub info: SinkInfoResponse,
    pub topics: Vec<TopicConsumerConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkConfigResponse {
    #[serde(flatten)]
    pub config: SinkConfig,
    pub active: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceDetailsResponse {
    #[serde(flatten)]
    pub info: SourceInfoResponse,
    pub topics: Vec<TopicProducerConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceConfigResponse {
    #[serde(flatten)]
    pub config: SourceConfig,
    pub active: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransformResponse {
    pub r#type: TransformType,
    pub config: serde_json::Value,
}

impl From<SinkInfo> for SinkInfoResponse {
    fn from(sink: SinkInfo) -> Self {
        SinkInfoResponse {
            id: sink.id,
            key: sink.key,
            name: sink.name,
            path: sink.path,
            enabled: sink.enabled,
            status: sink.status,
            last_error: sink.last_error,
            plugin_config_format: sink.plugin_config_format.map(|f| f.to_string()),
        }
    }
}

impl From<SourceInfo> for SourceInfoResponse {
    fn from(source: SourceInfo) -> Self {
        SourceInfoResponse {
            id: source.id,
            key: source.key,
            name: source.name,
            path: source.path,
            enabled: source.enabled,
            status: source.status,
            last_error: source.last_error,
            plugin_config_format: source.plugin_config_format.map(|f| f.to_string()),
        }
    }
}
