//! Protocol-listener auth gate: classify, authenticate, authorize.
//!
//! Rejections use existing Pico JSON / DS plain-text builders. DS gets no new
//! headers. `403` here never sets `Producer-Epoch`.

use axum::http::{HeaderMap, Method, Uri, header};
use axum::response::Response;
use bytes::Bytes;
use picomq_auth::{Audience, AuthError, AuthPrincipal, Authorizer, Operation};
use picomq_protocol::ds::H_STREAM_CLOSED;
use picomq_protocol::pico::{H_CLOSED, H_TRIM_SEQ};

use crate::ds;
use crate::http::query_param;
use crate::http::{header_str, parse_strict_u64_header, set_header, truthy};
use crate::pico;
use crate::route::{is_root, stream_name};
use picomq_server::ServiceError;

/// Authorized request: principal plus the stored stream name.
#[derive(Debug)]
pub struct Permit {
    pub principal: AuthPrincipal,
    pub stream_name: String,
}

/// Runs on the buffered request, so it sees what the handler will do.
/// `Ok(None)`: auth off, or OPTIONS.
pub async fn gate(
    authorizer: Option<&Authorizer>,
    audience: Audience,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Option<Permit>, Box<Response>> {
    let Some(ops) = classify(audience, method, uri, headers, body) else {
        return Err(Box::new(method_not_allowed(audience)));
    };
    let Some(authorizer) = authorizer.filter(|_| !ops.is_empty()) else {
        return Ok(None);
    };
    let caller = authenticate(authorizer, audience, headers).await?;
    let client_name =
        auth_resource(audience, uri).map_err(|error| Box::new(service_error(audience, error)))?;
    let stored = caller.resolve(authorizer, &client_name)?;
    for op in ops {
        caller.authorize(authorizer, op, Some(&stored))?;
    }
    Ok(Some(Permit {
        principal: caller.principal,
        stream_name: stored,
    }))
}

pub(crate) struct Caller {
    pub(crate) principal: AuthPrincipal,
    audience: Audience,
    anonymous: bool,
}

impl Caller {
    // Anonymous scope denials are 401, not 403: the remedy is a credential.
    fn demote(&self, err: AuthError) -> Box<Response> {
        let err = match err {
            AuthError::Store(_) => err,
            _ if self.anonymous => AuthError::Unauthenticated,
            _ => err,
        };
        Box::new(reject(self.audience, err))
    }

    pub(crate) fn resolve(
        &self,
        authorizer: &Authorizer,
        client_name: &str,
    ) -> Result<String, Box<Response>> {
        authorizer
            .resolve_stream_name(&self.principal, client_name)
            .map_err(|err| self.demote(err))
    }

    pub(crate) fn authorize(
        &self,
        authorizer: &Authorizer,
        op: Operation,
        resource: Option<&str>,
    ) -> Result<(), Box<Response>> {
        authorizer
            .authorize(&self.principal, op, resource)
            .map_err(|err| self.demote(err))
    }
}

pub(crate) async fn authenticate(
    authorizer: &Authorizer,
    audience: Audience,
    headers: &HeaderMap,
) -> Result<Caller, Box<Response>> {
    let now_ms = picomq_common::now_ms();
    let (principal, anonymous) = match header_str(headers, header::AUTHORIZATION.as_str()) {
        Some(credential) => (
            authorizer
                .authenticate(credential, audience, now_ms)
                .await
                .map_err(|err| Box::new(reject(audience, err)))?,
            false,
        ),
        None => (
            authorizer
                .authenticate_anonymous(audience, now_ms)
                .await
                .map_err(|err| Box::new(reject(audience, err)))?,
            true,
        ),
    };
    Ok(Caller {
        principal,
        audience,
        anonymous,
    })
}

/// The operations a request performs. `None`: a method the protocol does not
/// serve. OPTIONS performs none.
fn classify(
    audience: Audience,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Option<Vec<Operation>> {
    match audience {
        Audience::Pico => classify_pico(method, uri, headers, body),
        Audience::DurableStreams => classify_ds(method, headers, body),
        Audience::Admin | Audience::Kafka => Some(Vec::new()),
    }
}

fn classify_pico(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Option<Vec<Operation>> {
    let closed = truthy(headers, H_CLOSED);
    Some(match *method {
        Method::OPTIONS => Vec::new(),
        Method::PUT if closed => vec![Operation::Create, Operation::Close],
        Method::PUT => vec![Operation::Create],
        Method::POST if trims(headers) => vec![Operation::Trim],
        Method::POST => write_ops(closed, body.is_empty()),
        Method::DELETE => vec![Operation::Delete],
        Method::HEAD => vec![Operation::Head],
        Method::GET if is_root(uri) => vec![Operation::List],
        Method::GET => vec![Operation::Read],
        _ => return None,
    })
}

fn classify_ds(method: &Method, headers: &HeaderMap, body: &Bytes) -> Option<Vec<Operation>> {
    let closed = truthy(headers, H_STREAM_CLOSED);
    Some(match *method {
        Method::OPTIONS => Vec::new(),
        Method::PUT => {
            let mut ops = vec![Operation::Create];
            if !body.is_empty() {
                ops.push(Operation::Append);
            }
            if closed {
                ops.push(Operation::Close);
            }
            ops
        }
        Method::POST => write_ops(closed, body.is_empty()),
        Method::DELETE => vec![Operation::Delete],
        Method::HEAD => vec![Operation::Head],
        Method::GET => vec![Operation::Read],
        _ => return None,
    })
}

/// A trim exactly as the POST handler parses it. A malformed value is
/// rejected by the handler before anything happens.
fn trims(headers: &HeaderMap) -> bool {
    !matches!(
        parse_strict_u64_header(headers, H_TRIM_SEQ, "invalid Pico-Trim-Seq"),
        Ok(None)
    )
}

fn write_ops(close: bool, empty: bool) -> Vec<Operation> {
    match (close, empty) {
        (true, true) => vec![Operation::Close],
        (true, false) => vec![Operation::Append, Operation::Close],
        (false, _) => vec![Operation::Append],
    }
}

fn method_not_allowed(audience: Audience) -> Response {
    match audience {
        Audience::DurableStreams => ds::fail(405, "method not allowed"),
        Audience::Pico | Audience::Admin | Audience::Kafka => {
            pico::error(405, "method_not_allowed", "method not allowed", None)
        }
    }
}

/// The name to authorize. A Pico list is checked against its prefix filter.
fn auth_resource(audience: Audience, uri: &Uri) -> Result<String, ServiceError> {
    if audience == Audience::Pico && is_root(uri) {
        let prefix = query_param(uri, picomq_protocol::pico::Q_PREFIX)?;
        return Ok(prefix
            .filter(|p| !p.is_empty())
            .unwrap_or_else(|| "/".to_owned()));
    }
    stream_name(uri)
}

fn service_error(audience: Audience, error: ServiceError) -> Response {
    match audience {
        Audience::DurableStreams => ds::service_error_response(error),
        Audience::Pico | Audience::Admin | Audience::Kafka => pico::service_error_response(error),
    }
}

fn reject(audience: Audience, err: AuthError) -> Response {
    if let AuthError::Store(detail) = &err {
        tracing::warn!(%detail, "auth store failure");
    }
    match audience {
        Audience::DurableStreams => ds_reject(err),
        Audience::Pico | Audience::Admin | Audience::Kafka => pico_reject(err),
    }
}

fn pico_reject(err: AuthError) -> Response {
    let (status, code, message) = status_code(&err);
    let mut response = pico::error(status, code, message, None);
    if status == 401 {
        set_header(&mut response, header::WWW_AUTHENTICATE.as_str(), "Bearer");
    }
    response
}

fn ds_reject(err: AuthError) -> Response {
    let (status, _, message) = status_code(&err);
    let mut response = ds::fail(status, message);
    if status == 401 {
        set_header(&mut response, header::WWW_AUTHENTICATE.as_str(), "Bearer");
    }
    response
}

/// Store failures get a generic message: backend detail stays in server logs.
pub(crate) fn status_code(err: &AuthError) -> (u16, &'static str, &'static str) {
    use picomq_protocol::pico::{E_PERMISSION_DENIED, E_UNAUTHENTICATED};
    match err {
        AuthError::Unauthenticated | AuthError::Malformed => {
            (401, E_UNAUTHENTICATED, "unauthenticated")
        }
        AuthError::Expired => (401, E_UNAUTHENTICATED, "token expired"),
        AuthError::WrongAudience => (401, E_UNAUTHENTICATED, "wrong audience"),
        AuthError::Denied | AuthError::NarrowingRejected => {
            (403, E_PERMISSION_DENIED, "permission denied")
        }
        AuthError::Store(_) => (500, "internal", "auth unavailable"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use picomq_auth::{
        AccessToken, MemoryTokenStore, OperationGroups, ReadWrite, ResourceSet, Scope, TokenRecord,
        TokenStore,
    };
    use serde_json::Value;
    use std::sync::Arc;

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        headers
    }

    #[test]
    fn pico_classifies_what_the_handler_does() {
        let uri = Uri::from_static("/orders");
        let root = Uri::from_static("/");
        let none = Bytes::new();
        let body = Bytes::from_static(b"data");

        assert_eq!(
            classify_pico(
                &Method::POST,
                &uri,
                &headers_with(&[(H_TRIM_SEQ, "1")]),
                &none
            ),
            Some(vec![Operation::Trim])
        );
        assert_eq!(
            classify_pico(
                &Method::POST,
                &uri,
                &headers_with(&[(H_TRIM_SEQ, "")]),
                &body
            ),
            Some(vec![Operation::Append]),
            "an empty trim header is an append, as the handler reads it"
        );
        assert_eq!(
            classify_pico(
                &Method::POST,
                &uri,
                &headers_with(&[(H_CLOSED, "true")]),
                &none
            ),
            Some(vec![Operation::Close])
        );
        assert_eq!(
            classify_pico(
                &Method::POST,
                &uri,
                &headers_with(&[(H_CLOSED, "true")]),
                &body
            ),
            Some(vec![Operation::Append, Operation::Close])
        );
        assert_eq!(
            classify_pico(
                &Method::PUT,
                &uri,
                &headers_with(&[(H_CLOSED, "true")]),
                &none
            ),
            Some(vec![Operation::Create, Operation::Close])
        );
        assert_eq!(
            classify_pico(&Method::GET, &root, &HeaderMap::new(), &none),
            Some(vec![Operation::List])
        );
        assert_eq!(
            classify_pico(&Method::GET, &uri, &HeaderMap::new(), &none),
            Some(vec![Operation::Read])
        );
        assert_eq!(
            classify_pico(&Method::OPTIONS, &uri, &HeaderMap::new(), &none),
            Some(Vec::new())
        );
        assert!(classify_pico(&Method::PATCH, &uri, &HeaderMap::new(), &none).is_none());
    }

    #[test]
    fn ds_classifies_what_the_handler_does() {
        let none = Bytes::new();
        let body = Bytes::from_static(b"data");

        assert_eq!(
            classify_ds(
                &Method::PUT,
                &headers_with(&[(H_STREAM_CLOSED, "true")]),
                &body
            ),
            Some(vec![Operation::Create, Operation::Append, Operation::Close])
        );
        assert_eq!(
            classify_ds(
                &Method::POST,
                &headers_with(&[(H_STREAM_CLOSED, "true")]),
                &none
            ),
            Some(vec![Operation::Close])
        );
        assert_eq!(
            classify_ds(&Method::POST, &headers_with(&[(H_TRIM_SEQ, "1")]), &body),
            Some(vec![Operation::Append])
        );
        assert_eq!(
            classify_ds(&Method::GET, &HeaderMap::new(), &none),
            Some(vec![Operation::Read])
        );
        assert!(classify_ds(&Method::PATCH, &HeaderMap::new(), &none).is_none());
    }

    #[tokio::test]
    async fn an_unserved_method_is_refused_before_routing() {
        let response = gate(
            None,
            Audience::Pico,
            &Method::PATCH,
            &Uri::from_static("/secret"),
            &HeaderMap::new(),
            &Bytes::new(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.status(), 405);
    }

    async fn authorizer(scope: Scope) -> (Authorizer, String) {
        let store = Arc::new(MemoryTokenStore::new());
        let (token, verifier) = AccessToken::issue("svc/reader").unwrap();
        store
            .put_if_absent(TokenRecord {
                id: token.id.clone(),
                verifier,
                scope,
                created_at_ms: 1,
                issued_by: String::new(),
            })
            .await
            .unwrap();
        (Authorizer::new(store), token.render())
    }

    fn read_scope() -> Scope {
        Scope {
            streams: ResourceSet::prefix("/acct/"),
            groups: OperationGroups {
                stream: ReadWrite::read_only(),
                ..OperationGroups::default()
            },
            audiences: [Audience::Pico, Audience::DurableStreams].into(),
            ..Scope::default()
        }
    }

    #[tokio::test]
    async fn gate_off_and_options_skip() {
        let uri = Uri::from_static("/acct/orders");
        assert!(
            gate(
                None,
                Audience::Pico,
                &Method::GET,
                &uri,
                &HeaderMap::new(),
                &Bytes::new()
            )
            .await
            .unwrap()
            .is_none()
        );
        let (auth, wire) = authorizer(read_scope()).await;
        let headers = headers_with(&[("authorization", &format!("Bearer {wire}"))]);
        assert!(
            gate(
                Some(&auth),
                Audience::Pico,
                &Method::OPTIONS,
                &uri,
                &headers,
                &Bytes::new()
            )
            .await
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn missing_bearer_is_401_without_producer_epoch() {
        let (auth, _) = authorizer(read_scope()).await;
        let err = gate(
            Some(&auth),
            Audience::DurableStreams,
            &Method::GET,
            &Uri::from_static("/acct/orders"),
            &HeaderMap::new(),
            &Bytes::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), 401);
        assert!(err.headers().get("Producer-Epoch").is_none());
        assert_eq!(
            err.headers()
                .get(header::WWW_AUTHENTICATE)
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer"
        );
    }

    #[tokio::test]
    async fn append_without_write_is_403() {
        let (auth, wire) = authorizer(read_scope()).await;
        let headers = headers_with(&[("authorization", &format!("Bearer {wire}"))]);
        let err = gate(
            Some(&auth),
            Audience::Pico,
            &Method::POST,
            &Uri::from_static("/acct/orders"),
            &headers,
            &Bytes::from_static(b"data"),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), 403);
        let body = axum::body::to_bytes(err.into_body(), 1024).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "permission_denied");
    }

    #[tokio::test]
    async fn read_on_allowed_prefix_passes() {
        let (auth, wire) = authorizer(read_scope()).await;
        let headers = headers_with(&[("authorization", &format!("Bearer {wire}"))]);
        let permit = match gate(
            Some(&auth),
            Audience::Pico,
            &Method::GET,
            &Uri::from_static("/acct/orders"),
            &headers,
            &Bytes::new(),
        )
        .await
        {
            Ok(Some(permit)) => permit,
            Ok(None) => panic!("gate skipped"),
            Err(_) => panic!("gate rejected"),
        };
        assert_eq!(permit.stream_name, "/acct/orders");
        assert_eq!(permit.principal.id, "svc/reader");
    }
}
