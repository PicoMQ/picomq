use std::sync::Arc;

use picomq_protocol::{WireError, WireRequest, check_path};
use reqwest::Response;
use reqwest::header::HeaderMap;

use crate::error::{ClientError, ErrorKind, Result};
use crate::routes::Routes;

pub(crate) type Decode = fn(u16, &HeaderMap, &str) -> WireError;

#[derive(Clone)]
pub(crate) struct Transport {
    http: reqwest::Client,
    base_url: String,
    routes: Arc<Routes>,
}

impl Transport {
    pub(crate) fn new(endpoint: &str, http: reqwest::Client) -> Self {
        Self {
            http,
            base_url: endpoint.trim_end_matches('/').to_owned(),
            routes: Arc::new(Routes::new(endpoint)),
        }
    }

    pub(crate) async fn call(&self, wire: WireRequest, decode: Decode) -> Result<Response> {
        check_path(&wire.path_and_query).map_err(|error| {
            ClientError::new(0, ErrorKind::BadRequest, "bad_request")
                .with_message(Some(error.message))
        })?;

        let ok = wire.ok;
        let response = send(
            &self.http,
            &self.routes,
            build(&self.http, &self.base_url, wire),
        )
        .await?;
        expect(response, ok, decode).await
    }
}

pub(crate) fn invalid_response(e: picomq_protocol::CodecError) -> ClientError {
    ClientError::new(0, ErrorKind::Other, "invalid_response").with_message(Some(e.to_string()))
}

pub(crate) fn default_http() -> Result<reqwest::Client> {
    crate::http_client(&crate::ClientConfig::default())
}

fn build(http: &reqwest::Client, base_url: &str, wire: WireRequest) -> reqwest::RequestBuilder {
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
async fn send(
    http: &reqwest::Client,
    routes: &Routes,
    builder: reqwest::RequestBuilder,
) -> Result<Response> {
    let mut request = builder.build()?;
    let key = routes.key(request.url());
    if let Some(key) = &key {
        routes.resolve(key, request.url_mut());
    }
    let deleting = request.method() == reqwest::Method::DELETE;
    for _ in 0..MAX_REDIRECT_HOPS {
        let base = request.url().clone();
        let retry = request.try_clone();
        let response = match http.execute(request).await {
            Ok(response) => response,
            Err(error) => {
                if let Some(key) = &key {
                    routes.forget(key);
                }
                return Err(error.into());
            }
        };
        let status = response.status().as_u16();
        if status != 307 && status != 308 {
            if deleting
                && response.status().is_success()
                && let Some(key) = &key
            {
                routes.forget(key);
            }
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
        if let Some(key) = &key {
            routes.learn(key, &target);
        }
        let mut next = retry.expect("bodies are Bytes, always clonable");
        *next.url_mut() = target;
        request = next;
    }
    Err(ClientError::new(0, ErrorKind::Other, "too_many_redirects"))
}

fn header(response: &Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

async fn expect(response: Response, expected: &[u16], decode: Decode) -> Result<Response> {
    let status = response.status().as_u16();
    if expected.contains(&status) {
        return Ok(response);
    }
    let headers = response.headers().clone();
    let body = response.text().await.unwrap_or_default();
    Err(decode(status, &headers, &body).into())
}
