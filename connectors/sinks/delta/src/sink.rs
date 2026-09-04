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

use std::sync::Arc;

use async_trait::async_trait;
use deltalake::writer::{DeltaWriter, JsonWriter};
use picomq_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
    owned_value_to_serde_json,
};
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::coercions::{coerce, create_coercion_tree};
use crate::storage::build_storage_options;
use crate::{DeltaSink, SinkState};

impl DeltaSink {
    async fn open_table(&self, table_uri: &str) -> Result<SinkState, Error> {
        let table_url = url::Url::parse(table_uri).map_err(|e| {
            error!("Failed to parse table URI '{table_uri}': {e}");
            Error::InitError(format!("Invalid table URI: {e}"))
        })?;

        let storage_options = build_storage_options(&self.config).map_err(|e| {
            error!("Invalid storage configuration: {e}");
            Error::InitError(format!("Invalid storage configuration: {e}"))
        })?;

        let table = deltalake::open_table_with_storage_options(table_url, storage_options)
            .await
            .map_err(|e| {
                error!("Failed to load Delta table '{table_uri}': {e}");
                Error::InitError(format!("Failed to load Delta table: {e}"))
            })?;

        let kernel_schema = table
            .snapshot()
            .map_err(|e| {
                error!("Failed to get table snapshot: {e}");
                Error::InitError(format!("Failed to get table snapshot: {e}"))
            })?
            .schema();
        let coercion_tree = create_coercion_tree(&kernel_schema);

        let writer = JsonWriter::for_table(&table).map_err(|e| {
            error!("Failed to create JsonWriter: {e}");
            Error::InitError(format!("Failed to create JsonWriter: {e}"))
        })?;

        info!(
            "Delta Lake sink connector with ID: {} opened table: {table_uri}",
            self.id
        );

        Ok(SinkState {
            table,
            writer,
            coercion_tree,
        })
    }

    async fn table_state(&self, table_uri: &str) -> Result<Arc<Mutex<SinkState>>, Error> {
        if let Some(state) = self.tables.get(table_uri) {
            return Ok(state.clone());
        }
        let state = Arc::new(Mutex::new(self.open_table(table_uri).await?));
        Ok(self
            .tables
            .entry(table_uri.to_owned())
            .or_insert(state)
            .clone())
    }
}

#[async_trait]
impl Sink for DeltaSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Delta Lake sink connector with ID: {} for table: {}",
            self.id, self.config.table_uri
        );

        build_storage_options(&self.config).map_err(|e| {
            error!("Invalid storage configuration: {e}");
            Error::InitError(format!("Invalid storage configuration: {e}"))
        })?;

        if self.config.table_uri.is_static() {
            let table_uri = self.config.table_uri.to_string();
            self.table_state(&table_uri).await?;
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
            "Delta sink with ID: {} received: {} messages, topic: {}, partition: {}, offset: {}",
            self.id,
            messages.len(),
            topic_metadata.topic,
            messages_metadata.partition,
            messages_metadata.current_offset,
        );

        let mut json_values: Vec<serde_json::Value> = Vec::with_capacity(messages.len());
        for msg in &messages {
            match &msg.payload {
                Payload::Json(simd_value) => {
                    json_values.push(owned_value_to_serde_json(simd_value));
                }
                other => {
                    error!(
                        "Unsupported payload type: {other}. Delta sink only supports JSON payloads."
                    );
                    return Err(Error::InvalidPayloadType);
                }
            }
        }

        if json_values.is_empty() {
            debug!("No JSON values to write");
            return Ok(());
        }

        let table_uri = self.config.table_uri.resolve(&topic_metadata.topic)?;
        let state = self.table_state(&table_uri).await?;
        let mut state = state.lock().await;

        for value in &mut json_values {
            coerce(value, &state.coercion_tree).map_err(Error::InvalidRecordValue)?;
        }

        if let Err(e) = state.writer.write(json_values).await {
            state.writer.reset();
            error!("Failed to write to Delta writer for {table_uri}: {e}");
            return Err(Error::Storage(format!(
                "Failed to write to Delta writer: {e}"
            )));
        }

        let SinkState { table, writer, .. } = &mut *state;
        let version = match writer.flush_and_commit(table).await {
            Ok(v) => v,
            Err(e) => {
                writer.reset();
                error!("Failed to flush and commit to Delta table {table_uri}: {e}");
                return Err(Error::Storage(format!("Failed to flush and commit: {e}")));
            }
        };

        debug!(
            "Delta sink with ID: {} committed version {} to {table_uri}",
            self.id, version
        );

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let mut first_error = None;
        for (table_uri, state) in std::mem::take(&mut self.tables) {
            let mut state = state.lock().await;
            let SinkState { table, writer, .. } = &mut *state;
            if let Err(e) = writer.flush_and_commit(table).await {
                error!(
                    "Delta sink with ID: {} failed to flush {table_uri} on close: {e}",
                    self.id
                );
                first_error.get_or_insert(Error::Storage(format!("Failed to flush on close: {e}")));
            }
        }
        info!("Delta Lake sink connector with ID: {} is closed.", self.id);
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
