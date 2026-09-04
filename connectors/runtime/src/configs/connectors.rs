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

pub mod http_provider;
mod local_provider;

use crate::configs::connectors::http_provider::HttpConnectorsConfigProvider;
use crate::configs::connectors::local_provider::LocalConnectorsConfigProvider;
use crate::configs::runtime::ConnectorsConfig as RuntimeConnectorsConfig;
use crate::error::RuntimeError;
use crate::router::TopicRoute;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use picomq_connector_sdk::Schema;
use picomq_connector_sdk::transforms::TransformType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Formatter;
use std::path::PathBuf;
use strum::Display;

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Display,
)]
#[serde(rename_all = "lowercase")]
pub enum ConfigFormat {
    #[strum(to_string = "json")]
    Json,
    #[strum(to_string = "yaml")]
    Yaml,
    #[default]
    #[strum(to_string = "toml")]
    Toml,
    #[strum(to_string = "text")]
    Text,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConnectorConfig {
    Sink(SinkConfig),
    Source(SourceConfig),
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self::Sink(SinkConfig::default())
    }
}

impl ConnectorConfig {
    fn version(&self) -> u64 {
        match self {
            ConnectorConfig::Sink(config) => config.version,
            ConnectorConfig::Source(config) => config.version,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CreateSinkConfig {
    pub enabled: bool,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub topics: Vec<TopicConsumerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub benchmark: bool,
}

impl CreateSinkConfig {
    fn to_sink_config(&self, key: &str, version: u64) -> SinkConfig {
        SinkConfig {
            key: key.to_owned(),
            enabled: self.enabled,
            version,
            name: self.name.clone(),
            path: self.path.clone(),
            transforms: self.transforms.clone(),
            topics: self.topics.clone(),
            plugin_config_format: self.plugin_config_format,
            plugin_config: self.plugin_config.clone(),
            verbose: self.verbose,
            benchmark: self.benchmark,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub key: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub topics: Vec<TopicConsumerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub benchmark: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CreateSourceConfig {
    pub enabled: bool,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub topics: Vec<TopicProducerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub benchmark: bool,
}

impl CreateSourceConfig {
    fn to_source_config(&self, key: &str, version: u64) -> SourceConfig {
        SourceConfig {
            key: key.to_owned(),
            enabled: self.enabled,
            version,
            name: self.name.clone(),
            path: self.path.clone(),
            transforms: self.transforms.clone(),
            topics: self.topics.clone(),
            plugin_config_format: self.plugin_config_format,
            plugin_config: self.plugin_config.clone(),
            verbose: self.verbose,
            benchmark: self.benchmark,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub key: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub topics: Vec<TopicProducerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub benchmark: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransformsConfig {
    #[serde(flatten)]
    pub transforms: HashMap<TransformType, serde_json::Value>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TopicConsumerConfig {
    pub topics: Vec<String>,
    pub pattern: Option<String>,
    pub schema: Schema,
    pub avro_schema_json: Option<String>,
    pub avro_schema_path: Option<PathBuf>,
    pub batch_length: Option<u32>,
    pub poll_interval: Option<String>,
    pub consumer_group: Option<String>,
    pub auto_offset_reset: Option<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TopicProducerConfig {
    pub topic: TopicRoute,
    pub schema: Schema,
    pub avro_schema_json: Option<String>,
    pub avro_schema_path: Option<PathBuf>,
    pub batch_length: Option<u32>,
    pub linger_time: Option<String>,
    pub create_topics: bool,
    pub partitions: Option<i32>,
    pub replication_factor: Option<i32>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfigVersionInfo {
    pub version: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConnectorConfigVersions {
    pub sinks: HashMap<String, ConnectorConfigVersionInfo>,
    pub sources: HashMap<String, ConnectorConfigVersionInfo>,
}

#[async_trait]
pub trait ConnectorsConfigProvider: Send + Sync {
    async fn create_sink_config(
        &self,
        key: &str,
        config: CreateSinkConfig,
    ) -> Result<SinkConfig, RuntimeError>;
    async fn create_source_config(
        &self,
        key: &str,
        config: CreateSourceConfig,
    ) -> Result<SourceConfig, RuntimeError>;
    async fn get_active_configs(&self) -> Result<ConnectorsConfig, RuntimeError>;
    #[allow(dead_code)]
    async fn get_active_configs_versions(&self) -> Result<ConnectorConfigVersions, RuntimeError>;
    async fn set_active_sink_version(&self, key: &str, version: u64) -> Result<(), RuntimeError>;
    async fn set_active_source_version(&self, key: &str, version: u64) -> Result<(), RuntimeError>;
    async fn get_sink_configs(&self, key: &str) -> Result<Vec<SinkConfig>, RuntimeError>;
    async fn get_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SinkConfig>, RuntimeError>;
    async fn get_source_configs(&self, key: &str) -> Result<Vec<SourceConfig>, RuntimeError>;
    async fn get_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SourceConfig>, RuntimeError>;
    async fn delete_sink_config(&self, key: &str, version: Option<u64>)
    -> Result<(), RuntimeError>;
    async fn delete_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<(), RuntimeError>;
}

pub async fn create_connectors_config_provider(
    config: &RuntimeConnectorsConfig,
) -> Result<Box<dyn ConnectorsConfigProvider>, RuntimeError> {
    match config {
        RuntimeConnectorsConfig::Local(config) => {
            let provider = LocalConnectorsConfigProvider::new(&config.config_dir);
            let provider = provider.init().await?;
            Ok(Box::new(provider))
        }
        RuntimeConnectorsConfig::Http(config) => {
            let provider = HttpConnectorsConfigProvider::new(
                &config.base_url,
                config.timeout,
                &config.request_headers,
                &config.url_templates,
                &config.response,
                &config.retry,
            )?;
            Ok(Box::new(provider))
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConnectorsConfig {
    sinks: HashMap<String, SinkConfig>,
    sources: HashMap<String, SourceConfig>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SharedTransformConfig {
    pub enabled: bool,
}

impl std::fmt::Display for ConnectorConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorConfig::Sink(config) => {
                write!(f, "sink {config}")
            }
            ConnectorConfig::Source(config) => {
                write!(f, "source {config}",)
            }
        }
    }
}

impl std::fmt::Display for SinkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, topics: [{}], plugin_config_format: {:?}, verbose: {}, benchmark: {} }}",
            self.enabled,
            self.name,
            self.path,
            self.transforms,
            self.topics
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.plugin_config_format,
            self.verbose,
            self.benchmark,
        )
    }
}

impl std::fmt::Display for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, topics: [{}], plugin_config_format: {:?}, verbose: {}, benchmark: {} }}",
            self.enabled,
            self.name,
            self.path,
            self.transforms,
            self.topics
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.plugin_config_format,
            self.verbose,
            self.benchmark,
        )
    }
}

impl std::fmt::Display for TransformsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let transforms: Vec<String> = self
            .transforms
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect();
        write!(f, "{{ {} }}", transforms.join(", "))
    }
}

impl std::fmt::Display for TopicConsumerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ topics: {}, pattern: {:?}, schema: {:?}, avro_schema_json: {:?}, avro_schema_path: {:?}, batch_length: {:?}, poll_interval: {:?}, consumer_group: {:?}, auto_offset_reset: {:?} }}",
            self.topics
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .join(", "),
            self.pattern,
            self.schema,
            self.avro_schema_json,
            self.avro_schema_path,
            self.batch_length,
            self.poll_interval,
            self.consumer_group,
            self.auto_offset_reset
        )
    }
}

impl std::fmt::Display for TopicProducerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ topic: {}, schema: {:?}, avro_schema_json: {:?}, avro_schema_path: {:?}, batch_length: {:?}, linger_time: {:?}, create_topics: {}, partitions: {:?}, replication_factor: {:?} }}",
            self.topic,
            self.schema,
            self.avro_schema_json,
            self.avro_schema_path,
            self.batch_length,
            self.linger_time,
            self.create_topics,
            self.partitions,
            self.replication_factor
        )
    }
}

impl ConnectorsConfig {
    pub fn new(sinks: HashMap<String, SinkConfig>, sources: HashMap<String, SourceConfig>) -> Self {
        Self { sinks, sources }
    }

    pub fn sinks(&self) -> &HashMap<String, SinkConfig> {
        &self.sinks
    }

    pub fn sources(&self) -> &HashMap<String, SourceConfig> {
        &self.sources
    }
}
