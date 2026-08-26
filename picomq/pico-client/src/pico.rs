//! The Pico protocol client.
//!
//! Read-shaped calls retry, appends do not. Producer sessions live in
//! `producer.rs`. There are no future-returning variants (an async client
//! makes them redundant).

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use pico_protocol::envelope::{decode_batch_read, encode_batch_append, RecordEnvelope};
use pico_protocol::pico::{
    CT_BATCH_BINARY, H_CLOSED, H_EXPIRES_AT, H_NEXT_SEQ, H_PRODUCER_EPOCH, H_PRODUCER_ID,
    H_PRODUCER_SEQ, H_START_SEQ, H_TIMESTAMP, H_TRIM_SEQ, H_TTL, H_UP_TO_DATE,
};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, Response, StatusCode};

use crate::error::{ClientError, ErrorKind, Result};
use crate::retry::RetryPolicy;
use crate::types::{
    AppendAck, Live, Protocol, ReadLimits, ReadPage, Record, StreamApi, StreamInfo, StreamListing,
};

/// Who is appending, and where in its own sequence this append sits.
#[derive(Debug, Clone, Copy)]
pub struct ProducerRef<'a> {
    pub id: &'a str,
    pub epoch: u64,
    pub seq: u64,
}

/// The outcome of an identified append.
#[derive(Debug, Clone)]
pub struct ProducerAck {
    /// False when the append changed nothing: a duplicate, or a close-only
    /// request.
    pub applied: bool,
    /// The server had already applied this sequence. The records are in the
    /// stream exactly once.
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

    /// Append as an identified producer, so the server can order and
    /// de-duplicate the request.
    ///
    /// The server requires `seq == last_seq + 1` for the producer, which makes
    /// the sequence do double duty: a request that arrives early is rejected
    /// with [`ErrorKind::SequenceGap`] instead of landing out of order, and a
    /// re-sent request is recognized as a duplicate and applied once. Both are
    /// visible in the returned [`ProducerAck`].
    ///
    /// Sent as the `Pico-Producer-Id`/`-Epoch`/`-Seq` request headers.
    pub async fn append_as(
        &self,
        name: &str,
        records: &[Bytes],
        producer: &ProducerRef<'_>,
    ) -> Result<ProducerAck> {
        let envelopes: Vec<RecordEnvelope> = records
            .iter()
            .map(|body| RecordEnvelope::new(0, BTreeMap::new(), body.clone()))
            .collect();
        let request = self
            .http
            .post(self.url(name, ""))
            .header(CONTENT_TYPE, CT_BATCH_BINARY)
            .header(H_PRODUCER_ID, producer.id)
            .header(H_PRODUCER_EPOCH, producer.epoch.to_string())
            .header(H_PRODUCER_SEQ, producer.seq.to_string())
            .body(encode_batch_append(&envelopes));
        let response = send(&self.http, request).await?;
        let response = expect(response, &[200]).await?;
        let next = header(&response, H_NEXT_SEQ).unwrap_or_else(|| "0".to_owned());
        // A duplicate is answered with 200 and "nothing applied": the records
        // are already in the stream, so there is no new start to report.
        let start = header(&response, H_START_SEQ);
        let applied = start.is_some();
        Ok(ProducerAck {
            applied,
            duplicate: !applied && !records.is_empty(),
            ack: AppendAck {
                start: start.unwrap_or_else(|| next.clone()),
                next,
                timestamp: header(&response, H_TIMESTAMP).and_then(|v| v.parse().ok()),
            },
        })
    }

    pub async fn trim(&self, name: &str, seq: u64) -> Result<String> {
        let request = self
            .http
            .post(self.url(name, ""))
            .header(H_TRIM_SEQ, seq.to_string());
        let response = send(&self.http, request).await?;
        let response = expect(response, &[200]).await?;
        Ok(header(&response, H_START_SEQ).unwrap_or_else(|| "0".to_owned()))
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
        Ok(Some(StreamInfo {
            name: name.to_owned(),
            content_type: header(&response, CONTENT_TYPE.as_str()),
            start: header(&response, H_START_SEQ).unwrap_or_else(|| "0".to_owned()),
            next: header(&response, H_NEXT_SEQ).unwrap_or_else(|| "0".to_owned()),
            closed: truthy(&response, H_CLOSED),
            ttl_seconds: header(&response, H_TTL).and_then(|v| v.parse().ok()),
            expires_at: header(&response, H_EXPIRES_AT),
        }))
    }

    async fn read_once(
        &self,
        name: &str,
        from: &str,
        live: Live,
        limits: ReadLimits,
    ) -> Result<ReadPage> {
        let mut query = format!("?format=binary&seq={from}");
        if limits.count > 0 {
            query.push_str(&format!("&count={}", limits.count));
        }
        if limits.bytes > 0 {
            query.push_str(&format!("&bytes={}", limits.bytes));
        }
        if live == Live::LongPoll {
            query.push_str("&live=long-poll");
        }
        let response = send(&self.http, self.http.get(self.url(name, &query))).await?;
        let response = expect(
            response,
            if live == Live::Off {
                &[200]
            } else {
                &[200, 204]
            },
        )
        .await?;

        let next = header(&response, H_NEXT_SEQ).unwrap_or_else(|| from.to_owned());
        let up_to_date = truthy(&response, H_UP_TO_DATE);
        let closed = truthy(&response, H_CLOSED);
        let empty = response.status() == StatusCode::NO_CONTENT;
        let body = response.bytes().await?;

        let records = if empty || body.is_empty() {
            Vec::new()
        } else {
            decode_batch_read(&body)
                .map_err(|e| {
                    ClientError::new(0, ErrorKind::Other, "invalid_response")
                        .with_message(Some(e.to_string()))
                })?
                .into_iter()
                .map(|record| Record {
                    position: record.seq.to_string(),
                    timestamp: Some(record.envelope.timestamp),
                    headers: record.envelope.headers,
                    body: record.envelope.body,
                })
                .collect()
        };
        Ok(ReadPage {
            up_to_date: up_to_date || (empty && live == Live::LongPoll),
            records,
            next,
            closed,
        })
    }

    async fn list_once(&self, prefix: &str, limit: u64) -> Result<StreamListing> {
        let mut query = format!("?prefix={}", urlencode(prefix));
        if limit > 0 {
            query.push_str(&format!("&limit={limit}"));
        }
        let response = send(&self.http, self.http.get(self.url("/", &query))).await?;
        let response = expect(response, &[200]).await?;
        let body: serde_json::Value = response.json().await?;

        let streams = body["streams"]
            .as_array()
            .map(|entries| entries.iter().map(stream_info).collect())
            .unwrap_or_default();
        Ok(StreamListing {
            streams,
            has_more: body["has_more"].as_bool().unwrap_or(false),
        })
    }
}

#[async_trait]
impl StreamApi for PicoClient {
    fn protocol(&self) -> Protocol {
        Protocol::Pico
    }

    fn beginning(&self) -> String {
        "0".to_owned()
    }

    /// The tail is wherever the stream is now, so callers `head` first.
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
        let mut request = self
            .http
            .put(self.url(name, ""))
            .header(CONTENT_TYPE, content_type);
        if let Some(ttl) = ttl_seconds {
            request = request.header(H_TTL, ttl.to_string());
        }
        let response = expect(send(&self.http, request).await?, &[200, 201]).await?;
        Ok(response.status() == StatusCode::CREATED)
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
        let envelopes: Vec<RecordEnvelope> = records
            .iter()
            .map(|body| RecordEnvelope::new(0, BTreeMap::new(), body.clone()))
            .collect();
        let request = self
            .http
            .post(self.url(name, ""))
            .header(CONTENT_TYPE, CT_BATCH_BINARY)
            .body(encode_batch_append(&envelopes));
        let response = send(&self.http, request).await?;
        let response = expect(response, &[200]).await?;
        let next = header(&response, H_NEXT_SEQ).unwrap_or_else(|| "0".to_owned());
        Ok(AppendAck {
            start: header(&response, H_START_SEQ).unwrap_or_else(|| next.clone()),
            next,
            timestamp: header(&response, H_TIMESTAMP).and_then(|v| v.parse().ok()),
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
        let request = self.http.post(self.url(name, "")).header(H_CLOSED, "true");
        let response = send(&self.http, request).await?;
        let response = expect(response, &[200]).await?;
        Ok(header(&response, H_NEXT_SEQ).unwrap_or_else(|| "0".to_owned()))
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

fn stream_info(node: &serde_json::Value) -> StreamInfo {
    StreamInfo {
        name: node["name"].as_str().unwrap_or_default().to_owned(),
        content_type: node["content_type"].as_str().map(str::to_owned),
        start: node["start_seq"].as_u64().unwrap_or(0).to_string(),
        next: node["next_seq"].as_u64().unwrap_or(0).to_string(),
        closed: node["closed"].as_bool().unwrap_or(false),
        ttl_seconds: node["ttl"].as_u64(),
        expires_at: node["expires_at"].as_str().map(str::to_owned),
    }
}

pub(crate) fn default_http() -> Result<reqwest::Client> {
    crate::http_client(&crate::ClientConfig::default())
}

const MAX_REDIRECT_HOPS: usize = 5;

/// Send, following ownership redirects (307/308) by re-issuing the request at
/// the Location. The clone keeps every header, so the credential rides each
/// hop, which reqwest's own redirect handling would strip across origins.
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

pub(crate) fn truthy(response: &Response, name: &str) -> bool {
    header(response, name).is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

pub(crate) fn urlencode(value: &str) -> String {
    // Stream names and prefixes are paths. Only the characters that would
    // break a query string need escaping.
    value
        .chars()
        .map(|c| match c {
            '&' => "%26".to_owned(),
            '=' => "%3D".to_owned(),
            '?' => "%3F".to_owned(),
            '#' => "%23".to_owned(),
            ' ' => "%20".to_owned(),
            '+' => "%2B".to_owned(),
            other => other.to_string(),
        })
        .collect()
}

pub(crate) async fn expect(response: Response, expected: &[u16]) -> Result<Response> {
    let status = response.status().as_u16();
    if expected.contains(&status) {
        return Ok(response);
    }

    let closed = truthy(&response, H_CLOSED);
    let body = response.text().await.unwrap_or_default();
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    let code = parsed["error"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("http_{status}"));
    let message = parsed["message"]
        .as_str()
        .map(str::to_owned)
        .or(Some(body).filter(|b| !b.is_empty() && parsed.is_null()));
    let next = parsed["next_seq"].as_u64().map(|v| v.to_string());

    Err(ClientError::new(status, kind(status, &code, closed), code)
        .with_message(message)
        .with_next(next))
}

fn kind(status: u16, code: &str, closed: bool) -> ErrorKind {
    match status {
        400 => ErrorKind::BadRequest,
        401 => ErrorKind::Unauthenticated,
        // The auth gate says `permission_denied`, producer fencing says
        // `fenced`. Same status, different problem.
        403 if code == "permission_denied" => ErrorKind::PermissionDenied,
        403 => ErrorKind::StaleEpoch,
        404 => ErrorKind::NotFound,
        409 if closed || code == "closed" => ErrorKind::Closed,
        409 | 412 => ErrorKind::Conflict,
        410 => ErrorKind::OffsetGone,
        _ => ErrorKind::Other,
    }
}
