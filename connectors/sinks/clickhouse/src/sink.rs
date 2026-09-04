use std::sync::Arc;

use async_trait::async_trait;
use picomq_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata};
use tracing::{debug, error, info, warn};

use crate::body::{build_json_body, build_row_binary_body, build_string_body};
use crate::client::{ClickHouseClient, jittered_backoff};
use crate::schema::Column;
use crate::{ClickHouseSink, InsertFormat};

#[async_trait]
impl Sink for ClickHouseSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening ClickHouse sink connector ID: {} -> {}/{} (format: {:?})",
            self.id, self.config.url, self.config.table, self.insert_format,
        );

        let client = ClickHouseClient::new(
            self.config.url.clone(),
            self.database().to_owned(),
            self.insert_format
                .clickhouse_format_name(self.string_format)
                .to_owned(),
            self.username(),
            self.password(),
            self.timeout(),
        )?;

        let max_retries = self.max_retries();
        let retry_delay = self.retry_delay;

        let mut attempts = 0u32;
        loop {
            match client.ping().await {
                Ok(()) => break,
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_retries {
                        error!("Ping failed after {attempts} attempt(s): {e}");
                        return Err(e);
                    }
                    let backoff = jittered_backoff(retry_delay, attempts);
                    warn!(
                        "Ping failed (attempt {attempts}/{max_retries}): {e}. Retrying in {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
        info!("ClickHouse sink ID: {} ping OK", self.id);
        self.client = Some(client);

        if self.insert_format == InsertFormat::RowBinary && self.config.table.is_static() {
            let table = self.config.table.resolve("")?;
            self.table_schema(&table).await?;
        }

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

        debug!(
            "ClickHouse sink ID: {} received {} messages from topic {} partition {} offset {}",
            self.id,
            messages.len(),
            topic_metadata.topic,
            messages_metadata.partition,
            messages_metadata.current_offset,
        );

        let client = self.get_client()?;
        let table = self.config.table.resolve(&topic_metadata.topic)?;
        let format_name = self
            .insert_format
            .clickhouse_format_name(self.string_format);

        let body = match self.insert_format {
            InsertFormat::JsonEachRow => build_json_body(&messages)?,
            InsertFormat::RowBinary => {
                let schema = self.table_schema(&table).await?;
                build_row_binary_body(&messages, &schema)?
            }
            InsertFormat::StringPassthrough => build_string_body(&messages, self.string_format)?,
        };

        let deduplication_token = deduplication_token(
            &topic_metadata.topic,
            messages_metadata.partition,
            &messages,
        );
        client
            .insert(
                &table,
                &deduplication_token,
                body,
                self.max_retries(),
                self.retry_delay,
            )
            .await?;

        let count = messages.len() as u64;
        let mut state = self.state.lock().await;
        state.messages_processed += count;

        if self.verbose() {
            info!(
                "ClickHouse sink ID: {} inserted {} messages into '{table}' FORMAT {format_name}",
                self.id, count
            );
        } else {
            debug!(
                "ClickHouse sink ID: {} inserted {} messages into '{table}' FORMAT {format_name}",
                self.id, count
            );
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        info!(
            "ClickHouse sink ID: {} closed. Processed {} messages.",
            self.id, state.messages_processed,
        );
        state.table_schemas.clear();
        self.client = None;
        Ok(())
    }
}

impl ClickHouseSink {
    async fn table_schema(&self, table: &str) -> Result<Arc<[Column]>, Error> {
        {
            let state = self.state.lock().await;
            if let Some(schema) = state.table_schemas.get(table) {
                return Ok(Arc::clone(schema));
            }
        }

        let client = self.get_client()?;
        let max_retries = self.max_retries();
        let mut attempts = 0u32;
        let schema = loop {
            match client.fetch_schema(table).await {
                Ok(schema) => break schema,
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_retries {
                        error!(
                            "fetch_schema for '{table}' failed after {attempts} attempt(s): {e}"
                        );
                        return Err(e);
                    }
                    let backoff = jittered_backoff(self.retry_delay, attempts);
                    warn!(
                        "fetch_schema for '{table}' failed (attempt {attempts}/{max_retries}): {e}. Retrying in {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        };
        info!(
            "ClickHouse sink ID: {} loaded schema ({} columns) for table '{table}'",
            self.id,
            schema.len(),
        );

        let schema: Arc<[Column]> = Arc::from(schema);
        self.state
            .lock()
            .await
            .table_schemas
            .insert(table.to_owned(), Arc::clone(&schema));
        Ok(schema)
    }
}

fn deduplication_token(topic: &str, partition: i32, messages: &[ConsumedMessage]) -> String {
    let first = messages.first().map(|message| message.offset).unwrap_or(0);
    let last = messages.last().map(|message| message.offset).unwrap_or(0);
    format!("{topic}:{partition}:{first}-{last}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use picomq_connector_sdk::Payload;

    fn message(offset: u64) -> ConsumedMessage {
        ConsumedMessage {
            offset,
            timestamp: 0,
            key: None,
            headers: None,
            payload: Payload::Raw(Vec::new()),
        }
    }

    #[test]
    fn given_batch_should_build_deterministic_deduplication_token() {
        let messages = vec![message(10), message(11), message(12)];
        assert_eq!(
            deduplication_token("orders", 3, &messages),
            "orders:3:10-12"
        );
        assert_eq!(
            deduplication_token("orders", 3, &messages),
            deduplication_token("orders", 3, &messages)
        );
    }

    #[test]
    fn given_different_partitions_should_build_distinct_tokens() {
        let messages = vec![message(0)];
        assert_ne!(
            deduplication_token("orders", 0, &messages),
            deduplication_token("orders", 1, &messages)
        );
    }
}
