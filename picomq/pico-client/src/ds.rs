use async_trait::async_trait;
use bytes::Bytes;
use picomq_protocol::WireRequest;
use picomq_protocol::config::{ConfigUpdate, TopicConfig};
use picomq_protocol::ds::{
    AppendRequest, AppendResponse, CloseRequest, CreateRequest, CreateResponse, DEFAULT_CT,
    DeleteRequest, DeleteResponse, HeadRequest, HeadResponse, LIVE_LONG_POLL, OFFSET_BEGINNING,
    OFFSET_NOW, ReadRequest, ReadResponse, decode_error,
};
use picomq_protocol::schemas::Schema;
use reqwest::Response;

use crate::ClientConfig;
use crate::error::{ClientError, Result};
use crate::retry::RetryPolicy;
use crate::transport::Transport;
use crate::types::{
    AppendAck, CreateOptions, Live, Protocol, ReadLimits, ReadPage, Record, StreamApi, StreamInfo,
    StreamListing,
};

pub struct DsClient {
    transport: Transport,
    retry: RetryPolicy,
}

impl DsClient {
    pub fn new(endpoint: &str) -> Result<Self> {
        Self::with_config(endpoint, &ClientConfig::default())
    }

    pub fn with_config(endpoint: &str, config: &ClientConfig) -> Result<Self> {
        Ok(Self {
            transport: Transport::new(endpoint, crate::http_client(config)?),
            retry: config.retry.clone(),
        })
    }

    async fn call(&self, wire: WireRequest) -> Result<Response> {
        self.transport.call(wire, decode_error).await
    }

    async fn head_once(&self, name: &str, partition: Option<u32>) -> Result<Option<StreamInfo>> {
        let request = HeadRequest {
            stream: name,
            partition,
        };
        let response = self.call(request.encode()).await?;
        let Some(head) = HeadResponse::decode(response.status().as_u16(), response.headers())
        else {
            return Ok(None);
        };
        Ok(Some(StreamInfo {
            name: name.to_owned(),
            content_type: head.content_type,
            partitions: head.partitions,
            start: head
                .next_offset
                .is_some()
                .then(|| OFFSET_BEGINNING.to_owned()),
            next: head.next_offset,
            closed: head.closed,
            ttl_seconds: head.ttl_seconds,
            expires_at: head.expires_at,
            retention_ms: head.retention_ms,
            retention_bytes: head.retention_bytes,
            schema: head.schema,
            kafka_topic: None,
        }))
    }

    async fn read_once(
        &self,
        name: &str,
        partition: Option<u32>,
        from: &str,
        live: Live,
        _limits: ReadLimits,
    ) -> Result<ReadPage> {
        let mut request = ReadRequest::new(name, from);
        request.partition = partition;
        request.live = (live == Live::LongPoll).then_some(LIVE_LONG_POLL);
        let response = self.call(request.encode()).await?;

        let read = ReadResponse::decode(response.status().as_u16(), response.headers());
        let next = read.next_offset.unwrap_or_else(|| from.to_owned());
        let body = response.bytes().await?;

        let records = if read.no_content || body.is_empty() {
            Vec::new()
        } else {
            vec![Record {
                position: next.clone(),
                timestamp: None,
                key: None,
                headers: Vec::new(),
                body,
            }]
        };
        Ok(ReadPage {
            up_to_date: read.up_to_date || read.no_content,
            records,
            next,
            closed: read.closed,
        })
    }
}

#[async_trait]
impl StreamApi for DsClient {
    fn protocol(&self) -> Protocol {
        Protocol::Ds
    }

    fn beginning(&self) -> String {
        OFFSET_BEGINNING.to_owned()
    }

    fn now(&self) -> Result<String> {
        Ok(OFFSET_NOW.to_owned())
    }

    async fn create(&self, name: &str, options: &CreateOptions) -> Result<bool> {
        let content_type = options.content_type.as_deref().unwrap_or(DEFAULT_CT);
        let request = CreateRequest {
            ttl_seconds: options.ttl_seconds,
            expires_at: options.expires_at.as_deref(),
            closed: options.closed,
            schema: options.schema.as_deref(),
            schema_validate: options.schema_validate,
            partitions: options.partitions,
            retention_ms: options.retention_ms,
            retention_bytes: options.retention_bytes,
            ..CreateRequest::new(name, content_type)
        };
        let response = self.call(request.encode()).await?;
        Ok(CreateResponse::decode(response.status().as_u16(), response.headers()).created)
    }

    async fn head(&self, name: &str, partition: Option<u32>) -> Result<Option<StreamInfo>> {
        self.retry.run(|| self.head_once(name, partition)).await
    }

    async fn append(
        &self,
        name: &str,
        partition: Option<u32>,
        records: &[Bytes],
        content_type: &str,
    ) -> Result<AppendAck> {
        let [body] = records else {
            return Err(ClientError::unsupported(format!(
                "the Durable Streams protocol appends one message per request, got {}",
                records.len()
            )));
        };
        let mut request = AppendRequest::new(name, content_type, body.clone());
        request.partition = partition;
        let response = self.call(request.encode()).await?;
        let next = AppendResponse::decode(response.headers())
            .next_offset
            .unwrap_or_default();
        Ok(AppendAck {
            start: next.clone(),
            next,
            timestamp: None,
        })
    }

    async fn read(
        &self,
        name: &str,
        partition: Option<u32>,
        from: &str,
        live: Live,
        limits: ReadLimits,
    ) -> Result<ReadPage> {
        self.retry
            .run(|| self.read_once(name, partition, from, live, limits))
            .await
    }

    async fn list(&self, _prefix: &str, _limit: u64) -> Result<StreamListing> {
        Err(ClientError::unsupported(
            "the Durable Streams protocol has no stream listing; use --protocol pico",
        ))
    }

    async fn close(&self, name: &str) -> Result<String> {
        let response = self.call(CloseRequest { stream: name }.encode()).await?;
        Ok(AppendResponse::decode(response.headers())
            .next_offset
            .unwrap_or_default())
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        let response = self.call(DeleteRequest { stream: name }.encode()).await?;
        Ok(DeleteResponse::decode(response.status().as_u16()).found)
    }
    async fn config(&self, name: &str) -> Result<Option<TopicConfig>> {
        self.retry.run(|| self.transport.config(name)).await
    }

    async fn update_config(&self, update: &ConfigUpdate) -> Result<TopicConfig> {
        self.transport.update_config(update).await
    }

    async fn put_schema(&self, name: &str, content_type: Option<&str>, body: Bytes) -> Result<()> {
        self.transport.put_schema(name, content_type, body).await
    }

    async fn get_schema(&self, name: &str) -> Result<Option<Schema>> {
        self.retry.run(|| self.transport.get_schema(name)).await
    }

    async fn delete_schema(&self, name: &str) -> Result<bool> {
        self.transport.delete_schema(name).await
    }
}
