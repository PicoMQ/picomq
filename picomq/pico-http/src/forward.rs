use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use picomq_server::PicoNode;
use serde_json::Value;

use crate::admin::PROTOCOL_NAME;
use crate::common::error_response;

pub const FORWARDED_BY: &str = "pico-forwarded-by";
pub const NODE_ID: &str = "pico-node-id";

pub(crate) const TIMEOUT: Duration = Duration::from_secs(10);

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_RESPONSE: usize = 4 << 20;

#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    #[error("the CA certificate is not PEM")]
    Certificate,
    #[error("the forwarding client cannot be built: {0}")]
    Client(String),
}

#[derive(Debug, Clone)]
pub struct Forward {
    client: reqwest::Client,
}

impl Forward {
    pub fn new(ca_certificate: Option<&[u8]>) -> Result<Self, ForwardError> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none());

        if let Some(pem) = ca_certificate {
            let roots = reqwest::Certificate::from_pem_bundle(pem)
                .ok()
                .filter(|roots| !roots.is_empty())
                .ok_or(ForwardError::Certificate)?;
            for root in roots {
                builder = builder.add_root_certificate(root);
            }
        }

        let client = builder
            .build()
            .map_err(|error| ForwardError::Client(error.to_string()))?;
        Ok(Self { client })
    }

    pub(crate) async fn to(&self, node: &PicoNode, target: i32, request: Request) -> Response {
        if request.headers().contains_key(FORWARDED_BY) {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("node {target} is not reachable through this node, retry"),
            );
        }

        let address = {
            let view = node.views().load();
            let Some(row) = view.state.nodes().get(&target) else {
                return error_response(
                    StatusCode::NOT_FOUND,
                    format!("node {target} is not registered"),
                );
            };
            match row.protocol_addresses.get(PROTOCOL_NAME) {
                Some(address) => address.clone(),
                None => {
                    return error_response(
                        StatusCode::BAD_GATEWAY,
                        format!("node {target} has no admin address"),
                    );
                }
            }
        };

        let (parts, body) = request.into_parts();
        let path = parts.uri.path_and_query().map_or("/", |path| path.as_str());

        let mut outbound = self
            .client
            .request(
                parts.method,
                format!("{}{path}", address.trim_end_matches('/')),
            )
            .timeout(TIMEOUT)
            .header(FORWARDED_BY, node.config().node_id.to_string())
            .body(reqwest::Body::wrap_stream(body.into_data_stream()));
        for name in [header::AUTHORIZATION, header::CONTENT_TYPE, header::ACCEPT] {
            if let Some(value) = parts.headers.get(&name) {
                outbound = outbound.header(name, value.clone());
            }
        }

        match outbound.send().await {
            Ok(response) => response_of(response),
            Err(error) if error.is_timeout() => error_response(
                StatusCode::GATEWAY_TIMEOUT,
                format!("node {target} did not answer in time"),
            ),
            Err(_) => error_response(
                StatusCode::BAD_GATEWAY,
                format!("node {target} could not be reached at {address}"),
            ),
        }
    }

    pub(crate) async fn post_json(
        &self,
        node: &PicoNode,
        target: i32,
        path: &str,
        headers: &HeaderMap,
        body: &Value,
    ) -> Option<Value> {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(credential) = headers.get(header::AUTHORIZATION) {
            request = request.header(header::AUTHORIZATION, credential);
        }
        let request = request.body(Body::from(body.to_string())).ok()?;

        let response = self.to(node, target, request).await;
        if response.status() != StatusCode::OK {
            return None;
        }
        let body = axum::body::to_bytes(response.into_body(), MAX_RESPONSE)
            .await
            .ok()?;
        serde_json::from_slice(&body).ok()
    }
}

pub(crate) fn served_by(node: &PicoNode, mut response: Response) -> Response {
    if let Ok(value) = HeaderValue::from_str(&node.config().node_id.to_string()) {
        response.headers_mut().insert(NODE_ID, value);
    }
    response
}

fn response_of(response: reqwest::Response) -> Response {
    let status = response.status();
    let headers = end_to_end(response.headers());

    let mut relayed = Body::from_stream(response.bytes_stream()).into_response();
    *relayed.status_mut() = status;
    *relayed.headers_mut() = headers;
    relayed
}

fn end_to_end(headers: &HeaderMap) -> HeaderMap {
    headers
        .iter()
        .filter(|(name, _)| {
            ![
                header::CONNECTION,
                header::TRANSFER_ENCODING,
                header::CONTENT_LENGTH,
            ]
            .contains(*name)
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}
