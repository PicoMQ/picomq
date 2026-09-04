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

use std::collections::HashSet;

use async_trait::async_trait;
use picomq_connector_sdk::destination::DestinationTemplate;
use picomq_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use simd_json::prelude::*;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

sink_connector!(QuickwitSink);

#[derive(Debug)]
pub struct QuickwitSink {
    id: u32,
    config: QuickwitSinkConfig,
    client: reqwest::Client,
    index_id: Result<DestinationTemplate, Error>,
    ensured_indices: Mutex<HashSet<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuickwitSinkConfig {
    url: String,
    index: String,
    include_metadata: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexConfig {
    index_id: String,
    #[serde(flatten)]
    rest: serde_yaml_ng::Mapping,
}

impl QuickwitSink {
    pub fn new(id: u32, config: QuickwitSinkConfig) -> Self {
        let index_id = serde_yaml_ng::from_str::<IndexConfig>(&config.index)
            .map_err(|error| Error::InvalidConfigValue(format!("invalid index config: {error}")))
            .and_then(|index_config| index_config.index_id.parse());
        QuickwitSink {
            id,
            config,
            index_id,
            client: reqwest::Client::new(),
            ensured_indices: Mutex::new(HashSet::new()),
        }
    }

    fn index_template(&self) -> Result<&DestinationTemplate, Error> {
        self.index_id
            .as_ref()
            .map_err(|error| Error::InvalidConfigValue(error.to_string()))
    }

    fn resolve_index_id(&self, topic: &str) -> Result<String, Error> {
        self.index_template()?.resolve(topic)
    }

    fn index_config_for(&self, index_id: &str) -> Result<String, Error> {
        let mut index_config = serde_yaml_ng::from_str::<IndexConfig>(&self.config.index)
            .map_err(|error| Error::InvalidConfigValue(format!("invalid index config: {error}")))?;
        index_config.index_id = index_id.to_owned();
        serde_yaml_ng::to_string(&index_config)
            .map_err(|error| Error::InvalidConfigValue(format!("invalid index config: {error}")))
    }

    async fn ensure_index(&self, index_id: &str) -> Result<(), Error> {
        if self.ensured_indices.lock().await.contains(index_id) {
            return Ok(());
        }
        if !self.has_index(index_id).await? {
            self.create_index(index_id).await?;
        }
        self.ensured_indices
            .lock()
            .await
            .insert(index_id.to_owned());
        Ok(())
    }

    async fn has_index(&self, index_id: &str) -> Result<bool, Error> {
        let url = format!("{}/api/v1/indexes/{index_id}", self.config.url);
        let response = self.client.get(&url).send().await.map_err(|error| {
            error!(
                "Failed to send HTTP request to check if index with ID: {index_id} exists. {error}"
            );
            Error::HttpRequestFailed(error.to_string())
        })?;
        let status = response.status();
        if status.is_success() {
            Ok(true)
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Ok(false)
        } else {
            Err(Error::HttpRequestFailed(format!(
                "Unexpected status code: {status}",
            )))
        }
    }

    async fn create_index(&self, index_id: &str) -> Result<(), Error> {
        info!("Creating index: {index_id}");
        let url = format!("{}/api/v1/indexes", self.config.url);
        let response = self
            .client
            .post(&url)
            .header("content-type", "application/yaml")
            .body(self.index_config_for(index_id)?)
            .send()
            .await
            .map_err(|error| {
                error!("Failed to send HTTP request to create index: {index_id}. {error}");
                Error::HttpRequestFailed(error.to_string())
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let reason = response.text().await.unwrap_or_default();
            error!(
                "Received an invalid HTTP response when creating index: {index_id}. Status code: {status}, reason: {reason}"
            );
            return Err(Error::InitError(format!(
                "Failed to create index: {index_id}. {reason}"
            )));
        }

        info!("Created index: {index_id}");
        Ok(())
    }

    pub async fn ingest(&self, index_id: &str, messages: Vec<OwnedValue>) -> Result<(), Error> {
        let url = format!("{}/api/v1/{index_id}/ingest?commit=auto", self.config.url);
        debug!("Ingesting messages for index: {index_id}...");
        let messages_count = messages.len();
        let messages = messages
            .into_iter()
            .filter_map(|record| simd_json::to_string(&record).ok())
            .collect::<Vec<_>>()
            .join("\n");

        let response = self
            .client
            .post(&url)
            .body(messages)
            .send()
            .await
            .map_err(|error| {
                error!(
                    "Failed to send HTTP request to ingest messages for index: {index_id}. {error}"
                );
                Error::HttpRequestFailed(error.to_string())
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            error!(
                "Received an invalid HTTP response when ingesting messages for index: {index_id}. Status code: {status}, reason: {text}"
            );
            return Err(Error::CannotStoreData(format!(
                "Status code: {status}, reason: {text}"
            )));
        }

        debug!("Ingested {messages_count} messages for index: {index_id}");
        Ok(())
    }
}

#[async_trait]
impl Sink for QuickwitSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened Quickwit sink connector with ID: {} for URL: {}",
            self.id, self.config.url
        );
        let template = self.index_template()?;
        if template.is_static() {
            let index_id = template.resolve("")?;
            self.ensure_index(&index_id).await?;
        }
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        debug!(
            "Quickwit sink with ID: {} received: {} messages, format: {}, topic: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.topic
        );
        let index_id = self.resolve_index_id(&topic_metadata.topic)?;
        self.ensure_index(&index_id).await?;
        let include_metadata = self.config.include_metadata.unwrap_or(true);

        let mut json_payloads = Vec::with_capacity(messages.len());
        for message in messages {
            match message.payload {
                Payload::Json(mut value) => {
                    if include_metadata && let Some(object) = value.as_object_mut() {
                        object.insert(
                            "pico_topic".to_string(),
                            OwnedValue::from(topic_metadata.topic.as_str()),
                        );
                        object.insert(
                            "pico_partition".to_string(),
                            OwnedValue::from(messages_metadata.partition),
                        );
                        object.insert("pico_offset".to_string(), OwnedValue::from(message.offset));
                        object.insert(
                            "pico_timestamp".to_string(),
                            OwnedValue::from(message.timestamp),
                        );
                    }
                    json_payloads.push(value);
                }
                _ => {
                    warn!("Unsupported payload format: {}", messages_metadata.schema);
                }
            }
        }

        if json_payloads.is_empty() {
            return Ok(());
        }

        self.ingest(&index_id, json_payloads).await
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Quickwit sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INDEX: &str =
        "version: 0.8\nindex_id: events-{topic_segment[-1]}\ndoc_mapping:\n  mode: dynamic\n";

    fn sink(index: &str) -> QuickwitSink {
        QuickwitSink::new(
            1,
            QuickwitSinkConfig {
                url: "http://localhost:7280".to_string(),
                index: index.to_string(),
                include_metadata: None,
            },
        )
    }

    #[test]
    fn given_templated_index_id_should_resolve_per_topic() {
        let sink = sink(INDEX);
        assert_eq!(
            sink.resolve_index_id("orders.user42").unwrap(),
            "events-user42"
        );
    }

    #[test]
    fn given_resolved_index_should_rewrite_index_id_in_yaml() {
        let sink = sink(INDEX);
        let yaml = sink.index_config_for("events-user42").unwrap();
        let parsed: IndexConfig = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(parsed.index_id, "events-user42");
        assert!(yaml.contains("mode: dynamic"));
    }

    #[test]
    fn given_invalid_index_yaml_should_fail_on_open_not_construction() {
        let sink = sink("not: [valid");
        assert!(sink.index_template().is_err());
    }
}
