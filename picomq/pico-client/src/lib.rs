//! PicoMQ client: the Pico and Durable Streams protocols over HTTP.

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

#[derive(Debug, Clone, Default)]
pub struct ClientConfig {
    pub http2: bool,
    pub retry: RetryPolicy,
    pub token: Option<String>,
}

pub fn connect(protocol: Protocol, endpoint: &str) -> Result<Box<dyn StreamApi>> {
    connect_with(protocol, endpoint, &ClientConfig::default())
}

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

pub fn http_client(config: &ClientConfig) -> Result<reqwest::Client> {
    // Redirects stay off: reqwest strips Authorization on cross-origin 307s,
    // so the clients follow ownership redirects themselves (see `pico::send`).
    let mut builder = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(65))
        .redirect(reqwest::redirect::Policy::none());
    if config.http2 {
        builder = builder.http2_prior_knowledge();
    }
    if let Some(token) = &config.token {
        let mut value = reqwest::header::HeaderValue::from_str(&picomq_protocol::bearer(token))
            .map_err(|_| ClientError::transport("token is not a valid header value"))?;
        value.set_sensitive(true);
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::AUTHORIZATION, value);
        builder = builder.default_headers(headers);
    }
    builder.build().map_err(ClientError::from)
}
