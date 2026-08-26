//! The Durable Streams protocol client.
//!
//! Left out: idempotent producer sessions, streaming reader iterators and
//! the SSE parser (see the crate docs). A read here is one chunk, which is
//! exactly what such iterators would loop over.

use async_trait::async_trait;
use bytes::Bytes;
use pico_protocol::ds::{
    H_PRODUCER_EPOCH, H_PRODUCER_EXPECTED_SEQ, H_PRODUCER_RECEIVED_SEQ, H_STREAM_CLOSED,
    H_STREAM_NEXT_OFFSET, H_STREAM_TTL, H_STREAM_UP_TO_DATE,
};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, Response, StatusCode};

use crate::error::{ClientError, ErrorKind, Result};
use crate::pico::{default_http, header, send, truthy, urlencode};
use crate::retry::RetryPolicy;
use crate::types::{
    AppendAck, Live, Protocol, ReadLimits, ReadPage, Record, StreamApi, StreamInfo, StreamListing,
};

const OFFSET_BEGINNING: &str = "-1";
const OFFSET_NOW: &str = "now";

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

    fn url(&self, name: &str, query: &str) -> String {
        let path = if name.starts_with('/') {
            name.to_owned()
        } else {
            format!("/{name}")
        };
        format!("{}{path}{query}", self.base_url)
    }

    async fn head_once(&self, name: &str) -> Result<Option<StreamInfo>> {
        let request = self.http.request(Method::HEAD, self.url(name, ""));
        let response = send(&self.http, request).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = expect(response, &[200]).await?;
        let next = header(&response, H_STREAM_NEXT_OFFSET).unwrap_or_default();
        Ok(Some(StreamInfo {
            name: name.to_owned(),
            content_type: header(&response, CONTENT_TYPE.as_str()),
            // The protocol reports no start offset. The beginning token is
            // always a valid read position.
            start: OFFSET_BEGINNING.to_owned(),
            next,
            closed: truthy(&response, H_STREAM_CLOSED),
            ttl_seconds: header(&response, H_STREAM_TTL).and_then(|v| v.parse().ok()),
            expires_at: header(&response, "Stream-Expires-At"),
        }))
    }

    /// `limits` has no wire representation. The DS read request is only
    /// `(offset, live, cursor)`, and how much a chunk carries is the server's
    /// `max_chunk_size` to decide.
    async fn read_once(
        &self,
        name: &str,
        from: &str,
        live: Live,
        _limits: ReadLimits,
    ) -> Result<ReadPage> {
        let mut query = format!("?offset={}", urlencode(from));
        if live == Live::LongPoll {
            query.push_str("&live=long-poll");
        }
        let response = send(&self.http, self.http.get(self.url(name, &query))).await?;
        let response = expect(response, &[200, 204]).await?;

        let next = header(&response, H_STREAM_NEXT_OFFSET).unwrap_or_else(|| from.to_owned());
        let up_to_date = truthy(&response, H_STREAM_UP_TO_DATE);
        let closed = truthy(&response, H_STREAM_CLOSED);
        let empty = response.status() == StatusCode::NO_CONTENT;
        let body = response.bytes().await?;

        // A chunk is the unit the protocol returns: bodies arrive
        // concatenated, with no per-record framing to split them on.
        let records = if empty || body.is_empty() {
            Vec::new()
        } else {
            vec![Record {
                position: next.clone(),
                timestamp: None,
                headers: Default::default(),
                body,
            }]
        };
        Ok(ReadPage {
            up_to_date: up_to_date || empty,
            records,
            next,
            closed,
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
        let mut request = self
            .http
            .put(self.url(name, ""))
            .header(CONTENT_TYPE, content_type);
        if let Some(ttl) = ttl_seconds {
            request = request.header(H_STREAM_TTL, ttl.to_string());
        }
        let response = expect(send(&self.http, request).await?, &[200, 201]).await?;
        Ok(response.status() == StatusCode::CREATED)
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
        let request = self
            .http
            .post(self.url(name, ""))
            .header(CONTENT_TYPE, content_type)
            .body(body.clone());
        let response = send(&self.http, request).await?;
        let response = expect(response, &[200, 204]).await?;
        let next = header(&response, H_STREAM_NEXT_OFFSET).unwrap_or_default();
        Ok(AppendAck {
            // The protocol reports only where the stream now ends.
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
        let request = self
            .http
            .post(self.url(name, ""))
            .header(H_STREAM_CLOSED, "true");
        let response = send(&self.http, request).await?;
        let response = expect(response, &[200, 204]).await?;
        Ok(header(&response, H_STREAM_NEXT_OFFSET).unwrap_or_default())
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        let response = send(&self.http, self.http.delete(self.url(name, ""))).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        expect(response, &[204]).await?;
        Ok(true)
    }
}

async fn expect(response: Response, expected: &[u16]) -> Result<Response> {
    let status = response.status().as_u16();
    if expected.contains(&status) {
        return Ok(response);
    }

    let closed = truthy(&response, H_STREAM_CLOSED);
    let epoch = header(&response, H_PRODUCER_EPOCH);
    let expected_seq = header(&response, H_PRODUCER_EXPECTED_SEQ);
    let received_seq = header(&response, H_PRODUCER_RECEIVED_SEQ);
    let next = header(&response, H_STREAM_NEXT_OFFSET);
    let body = response.text().await.unwrap_or_default();

    let (kind, code) = match status {
        400 => (ErrorKind::BadRequest, "bad_request"),
        401 => (ErrorKind::Unauthenticated, "unauthenticated"),
        // DS has no error codes. A fencing 403 carries Producer-Epoch, an
        // auth 403 never does.
        403 if epoch.is_some() => (ErrorKind::StaleEpoch, "stale_epoch"),
        403 => (ErrorKind::PermissionDenied, "permission_denied"),
        404 => (ErrorKind::NotFound, "not_found"),
        409 if closed => (ErrorKind::Closed, "closed"),
        409 if expected_seq.is_some() || received_seq.is_some() => {
            (ErrorKind::Conflict, "sequence_conflict")
        }
        409 => (ErrorKind::Conflict, "conflict"),
        410 => (ErrorKind::OffsetGone, "offset_gone"),
        _ => (ErrorKind::Other, "request_failed"),
    };
    let mut message = if body.is_empty() { None } else { Some(body) };
    if kind == ErrorKind::StaleEpoch {
        if let Some(epoch) = epoch {
            message = Some(format!(
                "{} (current epoch {epoch})",
                message.unwrap_or_else(|| "stale producer epoch".to_owned())
            ));
        }
    }
    if let (Some(expected_seq), Some(received_seq)) = (&expected_seq, &received_seq) {
        message = Some(format!(
            "{} (expected seq {expected_seq}, received {received_seq})",
            message.unwrap_or_else(|| "producer sequence gap".to_owned())
        ));
    }

    Err(ClientError::new(status, kind, code)
        .with_message(message)
        .with_next(next))
}
