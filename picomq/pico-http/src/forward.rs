use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use picomq_server::PicoNode;
use serde_json::json;

use crate::admin::PROTOCOL_NAME;

pub const FORWARDED_BY: &str = "pico-forwarded-by";
pub const NODE_ID: &str = "pico-node-id";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

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

    pub(crate) async fn to(
        &self,
        node: &PicoNode,
        target: i32,
        request: Request,
        timeout: Duration,
    ) -> Response {
        if request.headers().contains_key(FORWARDED_BY) {
            return refused(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("node {target} is not reachable through this node, retry"),
            );
        }

        let address = {
            let view = node.views().load();
            let Some(row) = view.state.nodes().get(&target) else {
                return refused(
                    StatusCode::NOT_FOUND,
                    format!("node {target} is not registered"),
                );
            };
            match row.protocol_addresses.get(PROTOCOL_NAME) {
                Some(address) => address.clone(),
                None => {
                    return refused(
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
            .timeout(timeout)
            .header(FORWARDED_BY, node.config().node_id.to_string())
            .body(reqwest::Body::wrap_stream(body.into_data_stream()));
        for name in [header::AUTHORIZATION, header::CONTENT_TYPE, header::ACCEPT] {
            if let Some(value) = parts.headers.get(&name) {
                outbound = outbound.header(name, value.clone());
            }
        }

        match outbound.send().await {
            Ok(response) => relay(response),
            Err(error) if error.is_timeout() => refused(
                StatusCode::GATEWAY_TIMEOUT,
                format!("node {target} did not answer in time"),
            ),
            Err(_) => refused(
                StatusCode::BAD_GATEWAY,
                format!("node {target} could not be reached at {address}"),
            ),
        }
    }
}

pub(crate) fn served_by(node: &PicoNode, mut response: Response) -> Response {
    if let Ok(value) = HeaderValue::from_str(&node.config().node_id.to_string()) {
        response.headers_mut().insert(NODE_ID, value);
    }
    response
}

fn relay(response: reqwest::Response) -> Response {
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

fn refused(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}
