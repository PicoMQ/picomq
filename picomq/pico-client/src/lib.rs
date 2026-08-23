//! HTTP clients for both protocols behind `StreamApi`, plus the batching producer.

pub mod ds;
pub mod error;
pub mod pico;
pub mod producer;
pub mod retry;
pub mod types;

pub use ds::DsClient;
pub use error::{ClientError, ErrorKind, Result};
pub use pico::PicoClient;
pub use retry::RetryPolicy;
pub use types::{
    AppendAck, Live, Protocol, ReadLimits, ReadPage, Record, StreamApi, StreamInfo, StreamListing,
};

/// How the transport under a client behaves.
#[derive(Debug, Clone, Default)]
pub struct ClientConfig {
    /// Speak HTTP/2 over cleartext (h2c) instead of HTTP/1.1.
    ///
    /// HTTP/1.1 carries one request per connection at a time, so a
    /// caller's concurrency is capped by its connection count and a burst of
    /// requests turns into a burst of sockets. HTTP/2 multiplexes them onto one
    /// connection, which is what makes deep append pipelines practical. It is
    /// opt-in because a plain HTTP/2 request has no fallback: the peer must
    /// speak it, and an HTTP/1.1-only server or proxy will simply fail.
    pub http2: bool,
    pub retry: RetryPolicy,
    /// Bearer token (wire form) sent on every request, including each
    /// redirect hop.
    pub token: Option<String>,
}

/// Open a client for `protocol` against `endpoint`, HTTP/1.1 and no retries.
pub fn connect(protocol: Protocol, endpoint: &str) -> Result<Box<dyn StreamApi>> {
    connect_with(protocol, endpoint, &ClientConfig::default())
}

/// Open a client for `protocol` against `endpoint` with `config`.
pub fn connect_with(
    protocol: Protocol,
    endpoint: &str,
    config: &ClientConfig,
) -> Result<Box<dyn StreamApi>> {
    let http = http_client(config)?;
    Ok(match protocol {
        Protocol::Pico => Box::new(PicoClient::with_http(endpoint, http, config.retry.clone())),
        Protocol::Ds => Box::new(DsClient::with_http(endpoint, http, config.retry.clone())),
    })
}

/// Automatic redirects are off: reqwest strips the Authorization header when
/// a 307 to the owning node crosses origins. The clients follow redirects
/// themselves (see `pico::send`), keeping the credential on every hop.
pub fn http_client(config: &ClientConfig) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(65))
        .redirect(reqwest::redirect::Policy::none());
    if config.http2 {
        // Cleartext HTTP/2 has no ALPN to negotiate with, so the client has to
        // assume the server speaks it.
        builder = builder.http2_prior_knowledge();
    }
    if let Some(token) = &config.token {
        let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| ClientError::transport("token is not a valid header value"))?;
        value.set_sensitive(true);
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::AUTHORIZATION, value);
        builder = builder.default_headers(headers);
    }
    builder.build().map_err(ClientError::from)
}
