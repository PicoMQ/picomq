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
use std::time::Duration;

use async_trait::async_trait;
use base64::prelude::*;
use elasticsearch::{
    BulkParts, Elasticsearch,
    auth::Credentials,
    http::{Url, request::JsonBody, transport::TransportBuilder},
    indices::{IndicesCreateParts, IndicesExistsParts},
};
use picomq_connector_sdk::destination::DestinationTemplate;
use picomq_connector_sdk::{
    ConsumedMessage, Error, Headers, MessagesMetadata, Payload, Sink, TopicMetadata,
    convert::owned_value_to_serde_json, sink_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::json;
use simd_json::{OwnedValue, prelude::*};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

sink_connector!(ElasticsearchSink);

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

#[derive(Debug)]
struct State {
    invocations_count: usize,
    documents_indexed: usize,
    errors_count: usize,
    ensured_indices: HashSet<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticsearchSinkConfig {
    pub url: String,
    pub index: DestinationTemplate,
    pub username: Option<String>,
    #[serde(serialize_with = "picomq_connector_sdk::secret::serialize_optional_secret")]
    pub password: Option<SecretString>,
    pub batch_size: Option<usize>,
    pub timeout_seconds: Option<u64>,
    pub create_index_if_not_exists: Option<bool>,
    pub index_mapping: Option<serde_json::Value>,
    pub include_key: Option<bool>,
}

#[derive(Debug)]
pub struct ElasticsearchSink {
    id: u32,
    config: ElasticsearchSinkConfig,
    client: Option<Elasticsearch>,
    state: Mutex<State>,
}

impl ElasticsearchSink {
    pub fn new(id: u32, config: ElasticsearchSinkConfig) -> Self {
        ElasticsearchSink {
            id,
            config,
            client: None,
            state: Mutex::new(State {
                invocations_count: 0,
                documents_indexed: 0,
                errors_count: 0,
                ensured_indices: HashSet::new(),
            }),
        }
    }

    fn create_client(&self) -> Result<Elasticsearch, Error> {
        let url = Url::parse(&self.config.url)
            .map_err(|error| Error::Connection(format!("Invalid Elasticsearch URL: {error}")))?;

        let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);
        let timeout_seconds = self
            .config
            .timeout_seconds
            .unwrap_or(DEFAULT_TIMEOUT_SECONDS)
            .max(1);
        let mut transport_builder =
            TransportBuilder::new(conn_pool).timeout(Duration::from_secs(timeout_seconds));

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            let credentials =
                Credentials::Basic(username.clone(), password.expose_secret().to_string());
            transport_builder = transport_builder.auth(credentials);
        }

        let transport = transport_builder
            .build()
            .map_err(|e| Error::Connection(format!("Failed to build transport: {e}")))?;

        Ok(Elasticsearch::new(transport))
    }

    fn resolve_index(&self, topic: &str) -> Result<String, Error> {
        Ok(sanitize_index_name(&self.config.index.resolve(topic)?))
    }

    async fn ensure_index_exists(&self, client: &Elasticsearch, index: &str) -> Result<(), Error> {
        if !self.config.create_index_if_not_exists.unwrap_or(true) {
            return Ok(());
        }
        if self.state.lock().await.ensured_indices.contains(index) {
            return Ok(());
        }

        let response = client
            .indices()
            .exists(IndicesExistsParts::Index(&[index]))
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to check index existence: {e}")))?;

        if response.status_code().is_success() {
            debug!("Index '{index}' already exists");
        } else {
            let indices = client.indices();
            let request = indices.create(IndicesCreateParts::Index(index));
            let response = match &self.config.index_mapping {
                Some(mapping) => request.body(mapping.clone()).send().await,
                None => request.send().await,
            }
            .map_err(|e| Error::Connection(format!("Failed to create index: {e}")))?;

            if response.status_code().is_success() {
                info!("Successfully created index '{index}'");
            } else {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                if !error_text.contains("resource_already_exists_exception") {
                    return Err(Error::Connection(format!(
                        "Failed to create index '{index}': {error_text}"
                    )));
                }
            }
        }

        self.state
            .lock()
            .await
            .ensured_indices
            .insert(index.to_owned());
        Ok(())
    }

    async fn bulk_index_documents(
        &self,
        client: &Elasticsearch,
        index: &str,
        documents: Vec<(String, OwnedValue)>,
    ) -> Result<(), Error> {
        if documents.is_empty() {
            return Ok(());
        }

        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(documents.len() * 2);
        for (document_id, doc) in documents {
            body.push(json!({ "index": { "_index": index, "_id": document_id } }).into());
            let doc_json: serde_json::Value = owned_value_to_serde_json(&doc);
            body.push(doc_json.into());
        }

        let response = client
            .bulk(BulkParts::None)
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to execute bulk request: {e}")))?;

        if !response.status_code().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::CannotStoreData(format!(
                "Bulk indexing failed: {error_text}"
            )));
        }

        let response_body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| Error::Connection(format!("Failed to parse bulk response: {e}")))?;

        let items = response_body
            .get("items")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        let mut errors = 0usize;
        let mut first_error = None;
        for item in &items {
            if let Some(error) = item.get("index").and_then(|result| result.get("error")) {
                warn!("Document indexing error: {error}");
                errors += 1;
                if first_error.is_none() {
                    first_error = Some(error.to_string());
                }
            }
        }

        let mut state = self.state.lock().await;
        state.errors_count += errors;
        state.documents_indexed += items.len() - errors;
        drop(state);

        if let Some(error) = first_error {
            return Err(Error::CannotStoreData(format!(
                "{errors} of {} documents failed to index into '{index}': {error}",
                items.len()
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for ElasticsearchSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Elasticsearch sink connector with ID: {} for URL: {}, index: {}",
            self.id, self.config.url, self.config.index
        );

        let client = self.create_client()?;
        if self.config.index.is_static() {
            let index = self.resolve_index("")?;
            self.ensure_index_exists(&client, &index).await?;
        }
        self.client = Some(client);

        info!(
            "Successfully opened Elasticsearch sink connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        state.invocations_count += 1;
        let invocation = state.invocations_count;
        drop(state);

        debug!(
            "Elasticsearch sink with ID: {} received: {} messages, schema: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.topic,
            messages_metadata.partition,
            messages_metadata.current_offset,
            invocation
        );

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Elasticsearch client not initialized".to_string()))?;
        let index = self.resolve_index(&topic_metadata.topic)?;
        self.ensure_index_exists(client, &index).await?;
        let include_key = self.config.include_key.unwrap_or(true);

        let messages_count = messages.len();
        let mut documents = Vec::with_capacity(messages_count);
        for message in messages {
            let mut doc = match message.payload {
                Payload::Json(value) => value,
                Payload::Raw(bytes) => {
                    let mut bytes_copy = bytes.clone();
                    match simd_json::from_slice::<OwnedValue>(&mut bytes_copy) {
                        Ok(value) => value,
                        Err(_) => simd_json::json!({
                            "data": BASE64_STANDARD.encode(&bytes),
                            "data_type": "raw"
                        }),
                    }
                }
                Payload::Text(text) => simd_json::json!({
                    "text": text,
                    "data_type": "text"
                }),
                _ => {
                    warn!("Unsupported payload format: {}", messages_metadata.schema);
                    continue;
                }
            };

            if let Some(obj) = doc.as_object_mut() {
                obj.insert("pico_offset".to_string(), OwnedValue::from(message.offset));
                obj.insert(
                    "pico_topic".to_string(),
                    OwnedValue::from(topic_metadata.topic.as_str()),
                );
                obj.insert(
                    "pico_partition".to_string(),
                    OwnedValue::from(messages_metadata.partition),
                );
                obj.insert(
                    "pico_timestamp".to_string(),
                    OwnedValue::from(message.timestamp),
                );
                if include_key && let Some(key) = &message.key {
                    obj.insert("pico_key".to_string(), OwnedValue::from(render_bytes(key)));
                }
                if let Some(headers) = &message.headers {
                    obj.insert("pico_headers".to_string(), render_headers(headers));
                }
            }

            documents.push((
                document_id(
                    &topic_metadata.topic,
                    messages_metadata.partition,
                    message.offset,
                ),
                doc,
            ));
        }

        if !documents.is_empty() {
            self.bulk_index_documents(client, &index, documents).await?;
            debug!(
                "Successfully indexed {messages_count} documents to Elasticsearch index '{index}'"
            );
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Elasticsearch sink connector with ID: {} is closing. Stats: {} invocations, {} documents indexed, {} errors",
            self.id, state.invocations_count, state.documents_indexed, state.errors_count
        );
        drop(state);

        self.client = None;
        info!(
            "Elasticsearch sink connector with ID: {} is closed.",
            self.id
        );
        Ok(())
    }
}

fn document_id(topic: &str, partition: i32, offset: u64) -> String {
    format!("{topic}:{partition}:{offset}")
}

fn sanitize_index_name(name: &str) -> String {
    let mut sanitized: String = name
        .to_lowercase()
        .chars()
        .map(|character| match character {
            '\\' | '/' | '*' | '?' | '"' | '<' | '>' | '|' | ' ' | ',' | '#' | ':' => '_',
            other => other,
        })
        .collect();
    while sanitized.starts_with(['-', '_', '+']) {
        sanitized.remove(0);
    }
    if sanitized.is_empty() {
        sanitized.push_str("index");
    }
    sanitized
}

fn render_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_owned(),
        Err(_) => BASE64_STANDARD.encode(bytes),
    }
}

fn render_headers(headers: &Headers) -> OwnedValue {
    let mut object = simd_json::owned::Object::with_capacity_and_hasher(
        headers.len(),
        simd_json::owned::Object::default().hasher().clone(),
    );
    for (key, value) in headers {
        object.insert(key.clone(), OwnedValue::from(render_bytes(value)));
    }
    OwnedValue::from(object)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_topic_with_uppercase_and_slashes_should_sanitize_index_name() {
        assert_eq!(sanitize_index_name("Orders/EU:west"), "orders_eu_west");
        assert_eq!(sanitize_index_name("_hidden"), "hidden");
        assert_eq!(sanitize_index_name(""), "index");
    }

    #[test]
    fn given_topic_partition_offset_should_build_deterministic_document_id() {
        assert_eq!(document_id("orders", 0, 42), "orders:0:42");
    }

    #[test]
    fn given_templated_index_should_resolve_per_topic() {
        let config: ElasticsearchSinkConfig = serde_json::from_value(json!({
            "url": "http://localhost:9200",
            "index": "events-{topic_segment[-1]}"
        }))
        .unwrap();
        let sink = ElasticsearchSink::new(1, config);
        assert_eq!(
            sink.resolve_index("orders.User42").unwrap(),
            "events-user42"
        );
    }

    #[test]
    fn given_binary_header_should_render_as_base64() {
        let mut headers = Headers::new();
        headers.insert("trace".to_string(), b"abc".to_vec());
        headers.insert("bin".to_string(), vec![0xff, 0xfe]);
        let rendered = render_headers(&headers);
        assert_eq!(rendered["trace"], "abc");
        assert_eq!(rendered["bin"], BASE64_STANDARD.encode([0xff, 0xfe]));
    }
}
