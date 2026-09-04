use async_trait::async_trait;
use bytes::Bytes;
use picomq_protocol::WireRequest;
use picomq_protocol::ds::{
    AppendRequest, AppendResponse, CloseRequest, CreateRequest, CreateResponse, DeleteRequest,
    DeleteResponse, HeadRequest, HeadResponse, LIVE_LONG_POLL, OFFSET_BEGINNING, OFFSET_NOW,
    ReadRequest, ReadResponse, decode_error,
};
use reqwest::Response;

use crate::error::{ClientError, Result};
use crate::pico::{build, default_http, send};
use crate::retry::RetryPolicy;
use crate::types::{
    AppendAck, Live, Protocol, ReadLimits, ReadPage, Record, StreamApi, StreamInfo, StreamListing,
};

pub struct DsClient {
    http: reqwest::Client,
    base_url: String,
    retry: RetryPolicy,
}

impl DsClient {
    pub fn new(endpoint: &str) -> Result<Self> {
        Ok(Self::with_http(
            endpoint,
            default_http()?,
            RetryPolicy::none(),
        ))
    }

    pub fn with_http(endpoint: &str, http: reqwest::Client, retry: RetryPolicy) -> Self {
        Self {
            http,
            base_url: endpoint.trim_end_matches('/').to_owned(),
            retry,
        }
    }

    async fn call(&self, wire: WireRequest) -> Result<Response> {
        let ok = wire.ok;
        let response = send(&self.http, build(&self.http, &self.base_url, wire)).await?;
        expect(response, ok).await
    }

    async fn head_once(&self, name: &str) -> Result<Option<StreamInfo>> {
        let response = self.call(HeadRequest { stream: name }.encode()).await?;
        let Some(head) = HeadResponse::decode(response.status().as_u16(), response.headers())
        else {
            return Ok(None);
        };
        Ok(Some(StreamInfo {
            name: name.to_owned(),
            content_type: head.content_type,
            start: OFFSET_BEGINNING.to_owned(),
            next: head.next_offset.unwrap_or_default(),
            closed: head.closed,
            ttl_seconds: head.ttl_seconds,
            expires_at: head.expires_at,
        }))
    }

    async fn read_once(
        &self,
        name: &str,
        from: &str,
        live: Live,
        _limits: ReadLimits,
    ) -> Result<ReadPage> {
        let mut request = ReadRequest::new(name, from);
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

    async fn create(
        &self,
        name: &str,
        content_type: &str,
        ttl_seconds: Option<u64>,
    ) -> Result<bool> {
        let mut request = CreateRequest::new(name, content_type);
        request.ttl_seconds = ttl_seconds;
        let response = self.call(request.encode()).await?;
        Ok(CreateResponse::decode(response.status().as_u16(), response.headers()).created)
    }

    async fn head(&self, name: &str) -> Result<Option<StreamInfo>> {
        self.retry.run(|| self.head_once(name)).await
    }

    async fn append(&self, name: &str, records: &[Bytes], content_type: &str) -> Result<AppendAck> {
        let [body] = records else {
            return Err(ClientError::unsupported(format!(
                "the Durable Streams protocol appends one message per request, got {}",
                records.len()
            )));
        };
        let request = AppendRequest::new(name, content_type, body.clone());
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
        from: &str,
        live: Live,
        limits: ReadLimits,
    ) -> Result<ReadPage> {
        self.retry
            .run(|| self.read_once(name, from, live, limits))
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
}

async fn expect(response: Response, expected: &[u16]) -> Result<Response> {
    let status = response.status().as_u16();
    if expected.contains(&status) {
        return Ok(response);
    }
    let headers = response.headers().clone();
    let body = response.text().await.unwrap_or_default();
    Err(decode_error(status, &headers, &body).into())
}
