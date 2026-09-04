//! Protocol-listener auth gate: classify, authenticate, authorize.
//!
//! Rejections use existing Pico JSON / DS plain-text builders. DS gets no new
//! headers. `403` here never sets `Producer-Epoch`.

use axum::http::{HeaderMap, Method, Uri, header};
use axum::response::Response;
use picomq_auth::{Audience, AuthError, AuthPrincipal, Authorizer, Operation};
use picomq_protocol::ds::H_STREAM_CLOSED;
use picomq_protocol::pico::{H_CLOSED, H_TRIM_SEQ};

use crate::ds;
use crate::http::{header_str, set_header, truthy};
use crate::pico;
use crate::route::stream_name;

/// Authorized request: principal plus the stored stream name.
#[derive(Debug)]
pub struct Permit {
    pub principal: AuthPrincipal,
    pub stream_name: String,
}

/// `Ok(None)`: auth off, or OPTIONS (no credential). `Ok(Some)`: allowed.
pub async fn gate(
    authorizer: Option<&Authorizer>,
    audience: Audience,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<Option<Permit>, Box<Response>> {
    let Some(ops) = classify(audience, method, uri, headers) else {
        return Ok(None);
    };
    let Some(authorizer) = authorizer else {
        return Ok(None);
    };
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
    // Anonymous scope denials are 401, not 403: the remedy is a credential.
    let demote = |err: AuthError| match err {
        AuthError::Store(_) => err,
        _ if anonymous => AuthError::Unauthenticated,
        _ => err,
    };
    let client_name = auth_resource(audience, uri);
    let stored = authorizer
        .resolve_stream_name(&principal, &client_name)
        .map_err(|err| Box::new(reject(audience, demote(err))))?;
    for op in ops {
        authorizer
            .authorize(&principal, op, Some(&stored))
            .map_err(|err| Box::new(reject(audience, demote(err))))?;
    }
    Ok(Some(Permit {
        principal,
        stream_name: stored,
    }))
}

fn classify(
    audience: Audience,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Option<Vec<Operation>> {
    match audience {
        Audience::Pico => classify_pico(method, uri, headers),
        Audience::DurableStreams => classify_ds(method, uri, headers),
        Audience::Admin => None,
    }
}

fn classify_pico(method: &Method, uri: &Uri, headers: &HeaderMap) -> Option<Vec<Operation>> {
    Some(match *method {
        Method::PUT => vec![Operation::Create],
        Method::POST if header_str(headers, H_TRIM_SEQ).is_some() => {
            vec![Operation::Trim]
        }
        Method::POST => write_ops(truthy(headers, H_CLOSED), empty_body(headers)),
        Method::DELETE => vec![Operation::Delete],
        Method::HEAD => vec![Operation::Head],
        Method::GET if stream_name(uri) == "/" => vec![Operation::List],
        Method::GET => vec![Operation::Read],
        _ => return None,
    })
}

fn classify_ds(method: &Method, _uri: &Uri, headers: &HeaderMap) -> Option<Vec<Operation>> {
    Some(match *method {
        Method::PUT => vec![Operation::Create],
        Method::POST => write_ops(truthy(headers, H_STREAM_CLOSED), empty_body(headers)),
        Method::DELETE => vec![Operation::Delete],
        Method::HEAD => vec![Operation::Head],
        Method::GET => vec![Operation::Read],
        _ => return None,
    })
}

fn empty_body(headers: &HeaderMap) -> bool {
    header_str(headers, header::CONTENT_LENGTH.as_str()) == Some("0")
}

fn write_ops(close: bool, empty: bool) -> Vec<Operation> {
    match (close, empty) {
        (true, true) => vec![Operation::Close],
        (true, false) => vec![Operation::Append, Operation::Close],
        (false, _) => vec![Operation::Append],
    }
}

/// The name to authorize. A Pico list is checked against its prefix filter.
fn auth_resource(audience: Audience, uri: &Uri) -> String {
    if audience == Audience::Pico && stream_name(uri) == "/" {
        crate::http::query_param(uri, picomq_protocol::pico::Q_PREFIX)
            .filter(|p| !p.is_empty())
            .unwrap_or_else(|| stream_name(uri))
    } else {
        stream_name(uri)
    }
}

fn reject(audience: Audience, err: AuthError) -> Response {
    if let AuthError::Store(detail) = &err {
        tracing::warn!(%detail, "auth store failure");
    }
    match audience {
        Audience::DurableStreams => ds_reject(err),
        Audience::Pico | Audience::Admin => pico_reject(err),
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
    fn pico_classifies_trim_list_and_close() {
        let uri = Uri::from_static("/orders");
        let root = Uri::from_static("/");
        assert_eq!(
            classify_pico(&Method::POST, &uri, &headers_with(&[(H_TRIM_SEQ, "1")])),
            Some(vec![Operation::Trim])
        );
        assert_eq!(
            classify_pico(
                &Method::POST,
                &uri,
                &headers_with(&[(H_CLOSED, "true"), ("content-length", "0")])
            ),
            Some(vec![Operation::Close])
        );
        assert_eq!(
            classify_pico(
                &Method::POST,
                &uri,
                &headers_with(&[(H_CLOSED, "true"), ("content-length", "4")])
            ),
            Some(vec![Operation::Append, Operation::Close])
        );
        assert_eq!(
            classify_pico(&Method::GET, &root, &HeaderMap::new()),
            Some(vec![Operation::List])
        );
        assert_eq!(
            classify_pico(&Method::GET, &uri, &HeaderMap::new()),
            Some(vec![Operation::Read])
        );
        assert!(classify_pico(&Method::OPTIONS, &uri, &HeaderMap::new()).is_none());
    }

    #[test]
    fn ds_has_no_trim_or_list() {
        let uri = Uri::from_static("/");
        assert_eq!(
            classify_ds(
                &Method::POST,
                &uri,
                &headers_with(&[(H_STREAM_CLOSED, "true"), ("content-length", "0")])
            ),
            Some(vec![Operation::Close])
        );
        assert_eq!(
            classify_ds(&Method::POST, &uri, &headers_with(&[(H_TRIM_SEQ, "1")])),
            Some(vec![Operation::Append])
        );
        assert_eq!(
            classify_ds(&Method::GET, &uri, &HeaderMap::new()),
            Some(vec![Operation::Read])
        );
        assert!(classify_ds(&Method::OPTIONS, &uri, &HeaderMap::new()).is_none());
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
            gate(None, Audience::Pico, &Method::GET, &uri, &HeaderMap::new())
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
                &headers
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
