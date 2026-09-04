//! Ownership routing shared by both protocols: serve locally or 307 to the
//! stream's owner.
//!
//! One copy for both protocols: PUT and OPTIONS are
//! always local (create places the stream, redirects start once an owner is
//! opened elsewhere), the list root `/` is local, everything else redirects
//! when the owner is a different node with a known address. Routing failures
//! degrade to 503 with an `X-Error` header rather than guessing.

use axum::http::{HeaderValue, Method, StatusCode, Uri, header};
use axum::response::Response;

use picomq_server::ownership::OwnershipService;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RoutingMode {
    #[default]
    Redirect,
    LocalAlways,
}

/// Decide whether this request is served here. `Some(response)` short-circuits
/// (redirect or routing error).`None` means handle locally. Ownership is by
/// the stored `name`. The redirect location is from the original URI.
pub async fn route(
    ownership: &dyn OwnershipService,
    mode: RoutingMode,
    method: &Method,
    uri: &Uri,
    name: &str,
) -> Option<Response> {
    if mode == RoutingMode::LocalAlways
        || stream_name(uri) == "/"
        || *method == Method::PUT
        || *method == Method::OPTIONS
    {
        return None;
    }

    let owner = match ownership.owner_of(name).await {
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

pub fn stream_name(uri: &Uri) -> String {
    let path = uri.path();
    if path.is_empty() {
        "/".to_owned()
    } else {
        path.to_owned()
    }
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
