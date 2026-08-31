use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use picomq_protocol::envelope::RecordEnvelope;
use picomq_protocol::pico::{
    decode_error, AppendRequest, AppendResponse, CloseRequest, CloseResponse, CreateRequest,
    CreateResponse, DeleteRequest, DeleteResponse, HeadRequest, HeadResponse, ListRequest, Listing,
    ReadRequest, ReadResponse, TrimRequest, TrimResponse, LIVE_LONG_POLL, SEQ_BEGINNING,
};
use picomq_protocol::WireRequest;
use reqwest::Response;

use crate::error::{ClientError, ErrorKind, Result};
use crate::retry::RetryPolicy;
use crate::types::{
    AppendAck, Live, Protocol, ReadLimits, ReadPage, Record, StreamApi, StreamInfo, StreamListing,
};

pub use picomq_protocol::pico::Producer as ProducerRef;

#[derive(Debug, Clone)]
pub struct ProducerAck {
    pub applied: bool,
    pub duplicate: bool,
    pub ack: AppendAck,
}

#[derive(Clone)]
pub struct PicoClient {
    http: reqwest::Client,
    base_url: String,
    retry: RetryPolicy,
}

impl PicoClient {
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

    pub async fn append_as(
        &self,
        name: &str,
        records: &[Bytes],
        producer: &ProducerRef<'_>,
    ) -> Result<ProducerAck> {
        let envelopes = envelopes(records);
        let mut request = AppendRequest::new(name, &envelopes);
        request.producer = Some(*producer);
        let response = self.call(request.encode()).await?;
        let ack = AppendResponse::decode(response.headers());
        let next = seq_string(ack.next_seq);
        let applied = ack.start_seq.is_some();
        Ok(ProducerAck {
            applied,
            duplicate: !applied && !records.is_empty(),
            ack: AppendAck {
                start: ack
                    .start_seq
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| next.clone()),
                next,
                timestamp: ack.timestamp,
            },
        })
    }

    pub async fn trim(&self, name: &str, seq: u64) -> Result<String> {
        let response = self
            .call(TrimRequest { stream: name, seq }.encode())
            .await?;
        Ok(seq_string(
            TrimResponse::decode(response.headers()).start_seq,
        ))
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
            start: seq_string(head.start_seq),
            next: seq_string(head.next_seq),
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
        limits: ReadLimits,
    ) -> Result<ReadPage> {
        let mut request = ReadRequest::new(name, from);
        request.count = limits.count;
        request.bytes = limits.bytes;
        request.live = (live == Live::LongPoll).then_some(LIVE_LONG_POLL);
        let response = self.call(request.encode()).await?;

        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await?;
        let read = ReadResponse::decode(status, &headers, &body).map_err(invalid_response)?;

        Ok(ReadPage {
            up_to_date: read.up_to_date || (read.no_content && live == Live::LongPoll),
            records: read
                .records
                .into_iter()
                .map(|record| Record {
                    position: record.seq.to_string(),
                    timestamp: Some(record.envelope.timestamp),
                    headers: record.envelope.headers,
                    body: record.envelope.body,
                })
                .collect(),
            next: read
                .next_seq
                .map(|v| v.to_string())
                .unwrap_or_else(|| from.to_owned()),
            closed: read.closed,
        })
    }

    async fn list_once(&self, prefix: &str, limit: u64) -> Result<StreamListing> {
        let request = ListRequest {
            prefix,
            limit,
            start_after: None,
        };
        let response = self.call(request.encode()).await?;
        let body = response.bytes().await?;
        let listing = Listing::decode(&body).map_err(invalid_response)?;

        Ok(StreamListing {
            streams: listing
                .streams
                .into_iter()
                .map(|entry| StreamInfo {
                    name: entry.name,
                    content_type: entry.content_type,
                    start: entry.start_seq.to_string(),
                    next: entry.next_seq.to_string(),
                    closed: entry.closed,
                    ttl_seconds: entry.ttl_seconds,
                    expires_at: entry.expires_at,
                })
                .collect(),
            has_more: listing.has_more,
        })
    }
}

#[async_trait]
impl StreamApi for PicoClient {
    fn protocol(&self) -> Protocol {
        Protocol::Pico
    }

    fn beginning(&self) -> String {
        SEQ_BEGINNING.to_owned()
    }

    fn now(&self) -> Result<String> {
        Err(ClientError::unsupported(
            "the Pico protocol has no `now` token; read from the stream's next seq",
        ))
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

    async fn append(
        &self,
        name: &str,
        records: &[Bytes],
        _content_type: &str,
    ) -> Result<AppendAck> {
        let envelopes = envelopes(records);
        let response = self
            .call(AppendRequest::new(name, &envelopes).encode())
            .await?;
        let ack = AppendResponse::decode(response.headers());
        let next = seq_string(ack.next_seq);
        Ok(AppendAck {
            start: ack
                .start_seq
                .map(|v| v.to_string())
                .unwrap_or_else(|| next.clone()),
            next,
            timestamp: ack.timestamp,
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

    async fn list(&self, prefix: &str, limit: u64) -> Result<StreamListing> {
        self.retry.run(|| self.list_once(prefix, limit)).await
    }

    async fn close(&self, name: &str) -> Result<String> {
        let response = self.call(CloseRequest { stream: name }.encode()).await?;
        Ok(seq_string(
            CloseResponse::decode(response.headers()).next_seq,
        ))
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        let response = self.call(DeleteRequest { stream: name }.encode()).await?;
        Ok(DeleteResponse::decode(response.status().as_u16()).found)
    }
}

fn envelopes(records: &[Bytes]) -> Vec<RecordEnvelope> {
    records
        .iter()
        .map(|body| RecordEnvelope::new(0, BTreeMap::new(), body.clone()))
        .collect()
}

fn seq_string(seq: Option<u64>) -> String {
    seq.map(|v| v.to_string())
        .unwrap_or_else(|| SEQ_BEGINNING.to_owned())
}

fn invalid_response(e: picomq_protocol::CodecError) -> ClientError {
    ClientError::new(0, ErrorKind::Other, "invalid_response").with_message(Some(e.to_string()))
}

pub(crate) fn default_http() -> Result<reqwest::Client> {
    crate::http_client(&crate::ClientConfig::default())
}

pub(crate) fn build(
    http: &reqwest::Client,
    base_url: &str,
    wire: WireRequest,
) -> reqwest::RequestBuilder {
    let mut builder = http.request(wire.method, format!("{base_url}{}", wire.path_and_query));
    for (name, value) in wire.headers {
        builder = builder.header(name, value);
    }
    if !wire.body.is_empty() {
        builder = builder.body(wire.body);
    }
    builder
}

const MAX_REDIRECT_HOPS: usize = 5;

// Re-issues the request at the Location so every header, including
// Authorization, rides each redirect hop.
pub(crate) async fn send(
    http: &reqwest::Client,
    builder: reqwest::RequestBuilder,
) -> Result<Response> {
    let mut request = builder.build()?;
    for _ in 0..MAX_REDIRECT_HOPS {
        let base = request.url().clone();
        let retry = request.try_clone();
        let response = http.execute(request).await?;
        let status = response.status().as_u16();
        if status != 307 && status != 308 {
            return Ok(response);
        }
        let Some(target) = header(&response, "location").and_then(|loc| base.join(&loc).ok())
        else {
            return Err(ClientError::new(
                status,
                ErrorKind::Other,
                "redirect_without_location",
            ));
        };
        let mut next = retry.expect("bodies are Bytes, always clonable");
        *next.url_mut() = target;
        request = next;
    }
    Err(ClientError::new(0, ErrorKind::Other, "too_many_redirects"))
}

pub(crate) fn header(response: &Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

pub(crate) async fn expect(response: Response, expected: &[u16]) -> Result<Response> {
    let status = response.status().as_u16();
    if expected.contains(&status) {
        return Ok(response);
    }
    let headers = response.headers().clone();
    let body = response.text().await.unwrap_or_default();
    Err(decode_error(status, &headers, &body).into())
}
