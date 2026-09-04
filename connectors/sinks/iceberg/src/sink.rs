use async_trait::async_trait;
use iceberg::Catalog;
use picomq_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata};
use tracing::{debug, error, info};

use crate::{
    IcebergSink,
    catalog::init_catalog,
    router::{dynamic_router::DynamicRouter, static_router::StaticRouter},
};

#[async_trait]
impl Sink for IcebergSink {
    async fn open(&mut self) -> Result<(), Error> {
        match (
            &self.config.store_access_key_id,
            &self.config.store_secret_access_key,
        ) {
            (Some(store_access_key_id), Some(store_secret_access_key)) => {
                let redacted_store_key = store_access_key_id.chars().take(3).collect::<String>();
                let redacted_store_secret =
                    store_secret_access_key.chars().take(3).collect::<String>();
                info!(
                    "Opened Iceberg sink connector with ID: {} for URL: {}, store access key ID: {redacted_store_key}***  store secret: {redacted_store_secret}***",
                    self.id, self.config.uri
                );
            }
            (None, None) => {
                info!(
                    "Opened Iceberg sink connector with ID: {} for URL: {}. No explicit credentials provided, falling back to default credential provider chain",
                    self.id, self.config.uri
                );
            }
            _ => {
                return Err(Error::InvalidConfigValue(
                    "Partially configured credentials. You must provide both store_access_key_id and store_secret_access_key, or omit both.".to_owned(),
                ));
            }
        }

        info!(
            "Configuring Iceberg catalog with the following config:\n-region: {}\n-url: {}\n-store class: {}\n-catalog type: {}\n",
            self.config.store_region,
            self.config.store_url,
            self.config.store_class,
            self.config.catalog_type
        );

        let catalog: Box<dyn Catalog> = init_catalog(&self.config).await?;

        if self.config.dynamic_routing {
            self.router = Some(Box::new(DynamicRouter::new(
                catalog,
                self.config.dynamic_route_field.clone(),
            )))
        } else {
            self.router = Some(Box::new(
                StaticRouter::new(catalog, &self.config.tables).await?,
            ));
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
            "Iceberg sink with ID: {} received: {} messages, format: {}",
            self.id,
            messages.len(),
            messages_metadata.schema
        );

        match &self.router {
            Some(router) => {
                router
                    .route_data(topic_metadata, messages_metadata, messages)
                    .await?
            }
            None => {
                error!("Iceberg connector has no router configured");
                return Err(Error::InvalidConfig);
            }
        };

        debug!("Finished successfully");

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Iceberg sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
