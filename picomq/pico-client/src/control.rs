use picomq_protocol::config::{ConfigRequest, ConfigUpdate, TopicConfig};
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

    async fn control(&self, wire: WireRequest) -> Result<Response> {
        self.call(wire, |status, _, body| WireError::control(status, body))
            .await
    }
}
