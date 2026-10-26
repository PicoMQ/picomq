use std::time::Instant;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use picomq_auth::{Audience, AuthError};
use picomq_metrics::{Method, Protocol, Reason};

pub(crate) async fn observe(
    State(audience): State<Audience>,
    request: Request,
    next: Next,
) -> Response {
    let method = method(request.method());
    let started = Instant::now();

    let response = next.run(request).await;
    picomq_metrics::http(
        protocol(audience),
        method,
        response.status().as_u16(),
        started.elapsed(),
    );
    response
}

pub(crate) fn refused(audience: Audience, err: &AuthError) {
    if let Some(reason) = reason(err) {
        picomq_metrics::auth_failure(protocol(audience), reason);
    }
}

fn protocol(audience: Audience) -> Protocol {
    match audience {
        Audience::Pico => Protocol::Pico,
        Audience::DurableStreams => Protocol::DurableStreams,
        Audience::Admin => Protocol::Admin,
        Audience::Kafka => Protocol::Kafka,
    }
}

fn method(method: &axum::http::Method) -> Method {
    match *method {
        axum::http::Method::GET => Method::Get,
        axum::http::Method::HEAD => Method::Head,
        axum::http::Method::PUT => Method::Put,
        axum::http::Method::POST => Method::Post,
        axum::http::Method::PATCH => Method::Patch,
        axum::http::Method::DELETE => Method::Delete,
        axum::http::Method::OPTIONS => Method::Options,
        _ => Method::Other,
    }
}

fn reason(err: &AuthError) -> Option<Reason> {
    match err {
        AuthError::Unauthenticated => Some(Reason::Unauthenticated),
        AuthError::Malformed => Some(Reason::Malformed),
        AuthError::Expired => Some(Reason::Expired),
        AuthError::WrongAudience => Some(Reason::WrongAudience),
        AuthError::Denied => Some(Reason::Denied),
        AuthError::NarrowingRejected => Some(Reason::NarrowingRejected),
        AuthError::Store(_) => None,
    }
}
