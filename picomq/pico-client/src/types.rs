use async_trait::async_trait;
use bytes::Bytes;
use picomq_protocol::config::{ConfigUpdate, TopicConfig};
use picomq_protocol::schemas::Schema;

use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    #[default]
    Pico,
    Ds,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pico => "pico",
            Self::Ds => "ds",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub name: String,
    pub content_type: Option<String>,
    pub partitions: u32,
    pub start: Option<String>,
    pub next: Option<String>,
    pub closed: bool,
    pub ttl_seconds: Option<u64>,
    pub expires_at: Option<String>,
    pub retention_ms: Option<u64>,
    pub retention_bytes: Option<u64>,
    pub schema: Option<String>,
    pub kafka_topic: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CreateOptions {
    pub content_type: Option<String>,
    pub partitions: Option<u32>,
    pub ttl_seconds: Option<u64>,
    pub expires_at: Option<String>,
    pub retention_ms: Option<u64>,
    pub retention_bytes: Option<u64>,
    pub schema: Option<String>,
    pub schema_validate: bool,
    pub closed: bool,
}

impl CreateOptions {
    pub fn new(content_type: impl Into<String>) -> Self {
        Self {
            content_type: Some(content_type.into()),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendAck {
    pub start: String,
    pub next: String,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub position: String,
    pub timestamp: Option<i64>,
    /// Kafka-shaped: an optional key and ordered headers with byte values.
    /// DS reads carry neither.
    pub key: Option<Bytes>,
    pub headers: Vec<(String, Bytes)>,
    pub body: Bytes,
}

#[derive(Debug, Clone)]
pub struct ReadPage {
    pub records: Vec<Record>,
    pub next: String,
    pub up_to_date: bool,
    pub closed: bool,
}

#[derive(Debug, Clone)]
pub struct StreamListing {
    pub streams: Vec<StreamInfo>,
    pub next: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadLimits {
    pub count: u64,
    pub bytes: u64,
}

impl ReadLimits {
    pub fn server_default() -> Self {
        Self::default()
    }

    pub fn bytes(bytes: u64) -> Self {
        Self { count: 0, bytes }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Live {
    Off,
    LongPoll,
}

#[async_trait]
pub trait StreamApi: Send + Sync {
    fn protocol(&self) -> Protocol;

    fn beginning(&self) -> String;

    fn now(&self) -> Result<String>;

    async fn create(&self, name: &str, options: &CreateOptions) -> Result<bool>;

    async fn head(&self, name: &str, partition: Option<u32>) -> Result<Option<StreamInfo>>;

    async fn append(
        &self,
        name: &str,
        partition: Option<u32>,
        records: &[Bytes],
        content_type: &str,
    ) -> Result<AppendAck>;

    async fn read(
        &self,
        name: &str,
        partition: Option<u32>,
        from: &str,
        live: Live,
        limits: ReadLimits,
    ) -> Result<ReadPage>;

    async fn list(&self, prefix: &str, limit: u64, cursor: Option<&str>) -> Result<StreamListing>;

    async fn close(&self, name: &str) -> Result<String>;

    async fn delete(&self, name: &str) -> Result<bool>;

    async fn config(&self, name: &str) -> Result<Option<TopicConfig>>;

    async fn update_config(&self, update: &ConfigUpdate) -> Result<TopicConfig>;

    async fn put_schema(&self, name: &str, content_type: Option<&str>, body: Bytes) -> Result<()>;

    async fn get_schema(&self, name: &str) -> Result<Option<Schema>>;

    async fn delete_schema(&self, name: &str) -> Result<bool>;
}
