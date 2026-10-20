use bytes::Bytes;
use picomq_protocol::config::{ConfigRequest, ConfigUpdate, TopicConfig};
use picomq_protocol::schemas::{DeleteSchemaRequest, GetSchemaRequest, PutSchemaRequest, Schema};
use picomq_protocol::{WireError, WireRequest};
use reqwest::Response;

use crate::error::Result;
use crate::transport::{Transport, invalid_response};

impl Transport {
    pub(crate) async fn config(&self, name: &str) -> Result<Option<TopicConfig>> {
        let response = self
            .control(ConfigRequest { stream: name }.encode())
            .await?;
        if response.status() == 404 {
            return Ok(None);
        }
        TopicConfig::decode(&response.bytes().await?)
            .map(Some)
            .map_err(invalid_response)
    }

    pub(crate) async fn update_config(&self, update: &ConfigUpdate) -> Result<TopicConfig> {
        let response = self.control(update.encode()).await?;
        TopicConfig::decode(&response.bytes().await?).map_err(invalid_response)
    }

    pub(crate) async fn put_schema(
        &self,
        name: &str,
        content_type: Option<&str>,
        body: Bytes,
    ) -> Result<()> {
        let request = PutSchemaRequest {
            name,
            content_type,
            body,
        };
        self.control(request.encode()).await.map(drop)
    }

    pub(crate) async fn get_schema(&self, name: &str) -> Result<Option<Schema>> {
        let response = self.control(GetSchemaRequest { name }.encode()).await?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        Ok(Schema::decode(status, &headers, response.bytes().await?))
    }

    pub(crate) async fn delete_schema(&self, name: &str) -> Result<bool> {
        let response = self.control(DeleteSchemaRequest { name }.encode()).await?;
        Ok(response.status() == 204)
    }

    async fn control(&self, wire: WireRequest) -> Result<Response> {
        self.call(wire, |status, _, body| WireError::control(status, body))
            .await
    }
}
