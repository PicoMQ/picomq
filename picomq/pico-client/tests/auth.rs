//! Client credentials against an auth-required server: the bearer header on
//! every request, the new error kinds, 403 disambiguation, and redirect hops
//! that keep the credential.

use std::net::SocketAddr;

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use picomq_auth::{
    AccessToken, Audience, OperationGroups, ReadWrite, ResourceSet, Scope, TokenRecord, TokenStore,
};
use picomq_client::{ClientConfig, ErrorKind, PicoClient, RetryPolicy, StreamApi};
use picomq_http::HttpProtocol as ServeProtocol;
use picomq_runtime::{AuthMode, MetaBackend, PicoServer, ServerConfig};

async fn secured_server(dir: &std::path::Path) -> (PicoServer, String, AccessToken) {
    let (root, _) = AccessToken::issue("ops/root").unwrap();
    let server = picomq_runtime::start(ServerConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        admin_addr: None,
        http_protocol: ServeProtocol::Pico,
        kafka: None,
        meta_backend: MetaBackend::parse("sqlite::memory:").unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!("2@file://{}", dir.join("wal").display())),
        auth_mode: AuthMode::Required,
        bootstrap_token: Some(root.render()),
        ..Default::default()
    })
    .await
    .unwrap();
    let endpoint = format!("http://{}", server.local_addr());
    (server, endpoint, root)
}

fn with_token(endpoint: &str, token: Option<String>) -> PicoClient {
    let config = ClientConfig {
        token,
        ..Default::default()
    };
    PicoClient::with_http(
        endpoint,
        picomq_client::http_client(&config).unwrap(),
        RetryPolicy::none(),
    )
}

#[tokio::test]
async fn error_kinds_distinguish_auth_from_fencing() {
    let dir = tempfile::tempdir().unwrap();
    let (server, endpoint, root) = secured_server(dir.path()).await;

    let anonymous = with_token(&endpoint, None);
    let err = anonymous
        .create("/orders", "text/plain", None)
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unauthenticated);
    assert_eq!(err.status, 401);

    let (reader, verifier) = AccessToken::issue("svc/reader").unwrap();
    server
        .node()
        .tokens()
        .store()
        .put_if_absent(TokenRecord {
            id: reader.id.clone(),
            verifier,
            scope: Scope {
                streams: ResourceSet::prefix(""),
                groups: OperationGroups {
                    stream: ReadWrite::read_only(),
                    ..OperationGroups::default()
                },
                audiences: [Audience::Pico].into(),
                ..Scope::default()
            },
            created_at_ms: 1,
            issued_by: String::new(),
        })
        .await
        .unwrap();

    let read_only = with_token(&endpoint, Some(reader.render()));
    let err = read_only
        .create("/orders", "text/plain", None)
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::PermissionDenied);
    assert_eq!(err.status, 403);

    let rooted = with_token(&endpoint, Some(root.render()));
    assert!(rooted.create("/orders", "text/plain", None).await.unwrap());

    // A fencing 403 through the same client keeps its own kind.
    let producer = picomq_client::pico::ProducerRef {
        id: "p1",
        epoch: 2,
        seq: 0,
    };
    rooted
        .append_as("/orders", &[Bytes::from_static(b"a")], &producer)
        .await
        .unwrap();
    let stale = picomq_client::pico::ProducerRef {
        id: "p1",
        epoch: 1,
        seq: 1,
    };
    let err = rooted
        .append_as("/orders", &[Bytes::from_static(b"b")], &stale)
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::StaleEpoch);
    assert_eq!(err.status, 403);

    server.shutdown().await;
}

async fn spawn(router: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    addr
}

/// The hop changes origin (a different port), which is exactly where
/// reqwest's own redirect handling would drop the Authorization header.
#[tokio::test]
async fn redirects_keep_the_credential_per_hop() {
    let owner = spawn(Router::new().route(
        "/orders",
        any(|headers: HeaderMap| async move {
            match headers.get("authorization") {
                Some(value) if value == "Bearer tok" => {
                    (StatusCode::OK, [("Pico-Next-Seq", "7")]).into_response()
                }
                _ => StatusCode::UNAUTHORIZED.into_response(),
            }
        }),
    ))
    .await;
    let entry = spawn(Router::new().route(
        "/orders",
        any(move || async move {
            (
                StatusCode::TEMPORARY_REDIRECT,
                [("location", format!("http://{owner}/orders"))],
            )
                .into_response()
        }),
    ))
    .await;

    let client = with_token(&format!("http://{entry}"), Some("tok".to_owned()));
    let info = client.head("/orders").await.unwrap().unwrap();
    assert_eq!(info.next, "7", "served by the owner behind the redirect");
}

#[tokio::test]
async fn redirect_loops_fail_instead_of_spinning() {
    let looper = spawn(Router::new().route(
        "/orders",
        any(|| async {
            (StatusCode::TEMPORARY_REDIRECT, [("location", "/orders")]).into_response()
        }),
    ))
    .await;

    let client = with_token(&format!("http://{looper}"), None);
    let err = client.head("/orders").await.unwrap_err();
    assert_eq!(err.code, "too_many_redirects");
}
