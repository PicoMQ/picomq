//! Ownership routing shared by both protocols: serve locally or 307 to the
//! owner of the topic's log.
//!
//! OPTIONS and the list root `/` are local, a request for a topic that does
//! not exist yet is local (PUT creates it here), everything else redirects
//! when the owner is a different node with a known address. Routing failures
//! degrade to 503 with an `X-Error` header rather than guessing.

use axum::http::{HeaderValue, Method, StatusCode, Uri, header};
use axum::response::Response;

use picomq_server::ownership::OwnershipService;

use crate::http::{bad_request, percent_decode};
use picomq_server::{ErrorKind, Log, ServiceError, Topics, partition_log};

use crate::partition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RoutingMode {
    #[default]
    Redirect,
    LocalAlways,
}

pub async fn route(
    ownership: &dyn OwnershipService,
    topics: &Topics,
    mode: RoutingMode,
    method: &Method,
    uri: &Uri,
    name: &str,
    requested: Option<u32>,
) -> Option<Response> {
    if is_root(uri) || *method == Method::OPTIONS {
        return None;
    }

    let topic = match topics.get(name).await {
        Ok(Some(topic)) => topic,
        Ok(None) => return None,
        Err(error) if error.kind == ErrorKind::InvalidName => return None,
        Err(error) => return Some(routing_error(&error.message)),
    };
    let Ok(log) =
        partition::choose(&topic, requested).and_then(|index| partition_log(&topic, index))
    else {
        return None;
    };

    owner_redirect(ownership, mode, uri, &log).await
}

pub async fn owner_redirect(
    ownership: &dyn OwnershipService,
    mode: RoutingMode,
    uri: &Uri,
    log: &Log,
) -> Option<Response> {
    if mode == RoutingMode::LocalAlways {
        return None;
    }
    let owner = match ownership.owner_of(log).await {
        Ok(owner) => owner,
        Err(e) => return Some(routing_error(&e.message)),
    };
    let local = ownership.local_node();
    let address = match owner.owner_advertised_address {
        _ if owner.local => return None,
        None => return None,
        Some(address) if address == local.advertised_address => return None,
        Some(address) => address,
    };

    let mut location = address.trim_end_matches('/').to_owned();
    location.push_str(uri.path());
    if let Some(query) = uri.query()
        && !query.is_empty()
    {
        location.push('?');
        location.push_str(query);
    }

    let Ok(location) = HeaderValue::from_str(&location) else {
        return Some(routing_error("invalid owner address"));
    };
    Some(
        Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header(header::LOCATION, location)
            .header(header::CACHE_CONTROL, "no-store")
            .body(axum::body::Body::empty())
            .expect("static response"),
    )
}

pub fn is_root(uri: &Uri) -> bool {
    matches!(uri.path(), "" | "/")
}

pub fn stream_name(uri: &Uri) -> Result<String, ServiceError> {
    if is_root(uri) {
        return Ok("/".to_owned());
    }
    percent_decode(uri.path()).ok_or_else(|| bad_request("stream name is not valid text"))
}

fn routing_error(message: &str) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(header::CACHE_CONTROL, "no-store");
    if let Ok(value) = HeaderValue::from_str(message) {
        builder = builder.header("X-Error", value);
    } else {
        builder = builder.header("X-Error", "routing failed");
    }
    builder
        .body(axum::body::Body::empty())
        .expect("static response")
}
