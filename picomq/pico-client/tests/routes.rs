//! Requests go straight to an owner learned from an ownership redirect, and
//! fall back to the configured endpoint once that owner is gone.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::http::{Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::any;
use picomq_client::{PicoClient, RetryPolicy, StreamApi};

async fn spawn(router: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    addr
}

fn unused_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[tokio::test]
async fn learned_owners_skip_the_redirect_until_they_are_gone() {
    let owner = spawn(Router::new().route(
        "/orders",
        any(|method: Method| async move {
            if method == Method::DELETE {
                StatusCode::NO_CONTENT.into_response()
            } else {
                (StatusCode::OK, [("Pico-Next-Seq", "7")]).into_response()
            }
        }),
    ))
    .await;

    let redirects = Arc::new(AtomicUsize::new(0));
    let target = Arc::new(Mutex::new(owner));
    let entry = spawn(Router::new().route(
        "/orders",
        any({
            let (redirects, target) = (redirects.clone(), target.clone());
            move || async move {
                redirects.fetch_add(1, Ordering::SeqCst);
                let to = *target.lock().unwrap();
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [("location", format!("http://{to}/orders"))],
                )
                    .into_response()
            }
        }),
    ))
    .await;

    let client = PicoClient::with_http(
        &format!("http://{entry}"),
        picomq_client::http_client(&Default::default()).unwrap(),
        RetryPolicy::none(),
    );

    for _ in 0..3 {
        assert!(client.head("/orders", None).await.unwrap().is_some());
    }
    assert_eq!(
        redirects.load(Ordering::SeqCst),
        1,
        "one redirect, then direct"
    );

    assert!(client.delete("/orders").await.unwrap());
    client.head("/orders", None).await.unwrap();
    assert_eq!(
        redirects.load(Ordering::SeqCst),
        2,
        "a delete forgets the owner"
    );

    *target.lock().unwrap() = unused_addr();
    let fresh = PicoClient::with_http(
        &format!("http://{entry}"),
        picomq_client::http_client(&Default::default()).unwrap(),
        RetryPolicy::none(),
    );
    assert!(fresh.head("/orders", None).await.is_err());
    *target.lock().unwrap() = owner;
    assert!(fresh.head("/orders", None).await.unwrap().is_some());
    assert_eq!(
        redirects.load(Ordering::SeqCst),
        4,
        "an unreachable owner is forgotten"
    );
}
