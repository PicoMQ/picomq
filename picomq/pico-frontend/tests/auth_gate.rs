//! End-to-end auth gate over real sockets: the gate rejects before routing,
//! and the two protocols reject in their own vocabularies.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use pico_auth::{
    AccessToken, Audience, OperationGroups, ReadWrite, ResourceSet, Scope, TokenRecord, TokenStore,
};
use pico_frontend::{serve, Protocol, RoutingMode, ServeOptions};
use pico_server::PicoNode;

fn full_stream_scope(prefix: &str, auto_prefix: bool) -> Scope {
    Scope {
        streams: ResourceSet::prefix(prefix),
        groups: OperationGroups {
            stream: ReadWrite::all(),
            ..OperationGroups::default()
        },
        audiences: [Audience::Pico, Audience::DurableStreams].into(),
        auto_prefix_streams: auto_prefix,
        ..Scope::default()
    }
}

/// A node with one seeded token, served with the gate on. Returns the wire token.
async fn gated_server(
    protocol: Protocol,
    scope: Scope,
) -> (pico_frontend::RunningServer, String, Arc<PicoNode>) {
    let node = common::start_node().await;
    let (token, verifier) = AccessToken::issue("it/tester").unwrap();
    node.tokens()
        .store()
        .put_if_absent(TokenRecord {
            id: token.id.clone(),
            verifier,
            scope,
            created_at_ms: 1,
            issued_by: String::new(),
        })
        .await
        .unwrap();

    let server = serve(
        node.clone(),
        ServeOptions {
            protocol,
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            admin_addr: None,
            routing_mode: RoutingMode::LocalAlways,
            authorizer: Some(node.authorizer()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    (server, token.render(), node)
}

#[tokio::test]
async fn pico_gate_enforces_bearer_and_scope() {
    let (server, wire, _node) =
        gated_server(Protocol::Pico, full_stream_scope("/it/", false)).await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    // No credential: 401 JSON with the challenge header.
    let response = client.get(format!("{base}/it/x")).send().await.unwrap();
    assert_eq!(response.status(), 401);
    assert_eq!(
        response
            .headers()
            .get("www-authenticate")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer"
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "unauthenticated");

    // OPTIONS passes without a credential (CORS preflight).
    let response = client
        .request(reqwest::Method::OPTIONS, format!("{base}/it/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);

    // In-scope create and read succeed.
    let response = client
        .put(format!("{base}/it/orders"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let response = client
        .get(format!("{base}/it/orders"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // Out-of-scope stream: 403 before any stream state is touched.
    let response = client
        .get(format!("{base}/other/x"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "permission_denied");

    server.shutdown().await;
}

#[tokio::test]
async fn auto_prefix_resolves_stores_and_strips() {
    let (server, wire, node) =
        gated_server(Protocol::Pico, full_stream_scope("/acct/", true)).await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    // The client-relative name lands under the token prefix. The location
    // echoes the client path.
    let response = client
        .put(format!("{base}/orders"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    assert_eq!(
        response
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap(),
        "/orders"
    );
    assert!(node.service().head("/acct/orders").await.unwrap().is_some());
    assert!(node.service().head("/orders").await.unwrap().is_none());

    let response = client
        .get(format!("{base}/orders"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // List is scoped to the prefix and echoes stripped names.
    let response = client
        .get(format!("{base}/"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let names: Vec<&str> = body["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["orders"]);

    server.shutdown().await;
}

/// The gate answers before any long poll or SSE stream starts: a rejected
/// streaming read returns immediately as a plain error, never a held
/// connection or an event stream.
#[tokio::test]
async fn streaming_reads_refused_at_the_gate() {
    let (server, wire, _node) = gated_server(Protocol::Ds, full_stream_scope("/it/", false)).await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let sse = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.get(format!("{base}/it/x?offset=-1&live=sse")).send(),
    )
    .await
    .expect("refusal is immediate")
    .unwrap();
    assert_eq!(sse.status(), 401);
    assert!(sse.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/plain"));

    let out_of_scope = client
        .get(format!("{base}/other/x?offset=-1&live=sse"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(out_of_scope.status(), 403);
    server.shutdown().await;

    let (server, _, _node) = gated_server(Protocol::Pico, full_stream_scope("/it/", false)).await;
    let base = format!("http://{}", server.local_addr());
    let long_poll = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client
            .get(format!("{base}/it/x?seq=0&live=long-poll"))
            .send(),
    )
    .await
    .expect("refusal is immediate")
    .unwrap();
    assert_eq!(long_poll.status(), 401);
    server.shutdown().await;
}

/// Two auto-prefixed tenants on one node cannot see or name each other's
/// streams: every client path resolves inside the caller's own prefix.
#[tokio::test]
async fn auto_prefix_isolates_tenants() {
    let (server, tenant_a, node) =
        gated_server(Protocol::Pico, full_stream_scope("/a/", true)).await;
    let (token_b, verifier_b) = AccessToken::issue("it/tenant-b").unwrap();
    node.tokens()
        .store()
        .put_if_absent(TokenRecord {
            id: token_b.id.clone(),
            verifier: verifier_b,
            scope: full_stream_scope("/b/", true),
            created_at_ms: 1,
            issued_by: String::new(),
        })
        .await
        .unwrap();
    let tenant_b = token_b.render();
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .put(format!("{base}/orders"))
            .bearer_auth(&tenant_a)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert!(node.service().head("/a/orders").await.unwrap().is_some());

    // The same client name is a different stream for the other tenant.
    assert_eq!(
        client
            .get(format!("{base}/orders"))
            .bearer_auth(&tenant_b)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    // Naming the other tenant's stored path does not escape the prefix
    // either: it resolves to /b/a/orders.
    assert_eq!(
        client
            .get(format!("{base}/a/orders"))
            .bearer_auth(&tenant_b)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let listing: serde_json::Value = client
        .get(format!("{base}/"))
        .bearer_auth(&tenant_b)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listing["streams"].as_array().unwrap().len(), 0);

    server.shutdown().await;
}

/// The reserved `anonymous` grant opens exactly its scope to uncredentialed
/// callers, out-of-scope requests stay 401, and revoking it closes the door.
#[tokio::test]
async fn anonymous_grant_scopes_uncredentialed_access() {
    let (server, wire, node) = gated_server(Protocol::Pico, full_stream_scope("/", false)).await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let (_, verifier) = AccessToken::issue(pico_auth::ANONYMOUS_TOKEN_ID).unwrap();
    let record = TokenRecord {
        id: pico_auth::ANONYMOUS_TOKEN_ID.into(),
        verifier,
        scope: Scope {
            streams: ResourceSet::prefix("/public/"),
            groups: OperationGroups {
                stream: pico_auth::ReadWrite::read_only(),
                ..OperationGroups::default()
            },
            audiences: [Audience::Pico].into(),
            ..Scope::default()
        },
        created_at_ms: 1,
        issued_by: String::new(),
    };
    node.tokens()
        .store()
        .put_if_absent(record.clone())
        .await
        .unwrap();

    assert_eq!(
        client
            .put(format!("{base}/public/feed"))
            .bearer_auth(&wire)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );

    let anonymous = |path: &str| client.get(format!("{base}{path}")).send();
    assert_eq!(anonymous("/public/feed").await.unwrap().status(), 200);
    assert_eq!(anonymous("/private/x").await.unwrap().status(), 401);
    let write = client
        .post(format!("{base}/public/feed"))
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(write.status(), 401, "writes stay credentialed");

    node.tokens()
        .store()
        .delete_if(pico_auth::ANONYMOUS_TOKEN_ID, &record.verifier)
        .await
        .unwrap();
    assert_eq!(anonymous("/public/feed").await.unwrap().status(), 401);

    server.shutdown().await;
}

#[tokio::test]
async fn pico_fencing_403_stays_distinct_from_auth_403() {
    let (server, wire, _node) =
        gated_server(Protocol::Pico, full_stream_scope("/it/", false)).await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .put(format!("{base}/it/f"))
            .bearer_auth(&wire)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let producer = |epoch: &'static str| {
        client
            .post(format!("{base}/it/f"))
            .bearer_auth(&wire)
            .header("Content-Type", "text/plain")
            .header("Pico-Producer-Id", "p1")
            .header("Pico-Producer-Epoch", epoch)
            .header("Pico-Producer-Seq", "0")
            .body("x")
    };
    assert_eq!(producer("2").send().await.unwrap().status(), 200);

    let fenced = producer("1").send().await.unwrap();
    assert_eq!(fenced.status(), 403);
    assert_eq!(fenced.headers()["Pico-Producer-Epoch"], "2");
    let body: serde_json::Value = fenced.json().await.unwrap();
    assert_eq!(body["error"], "fenced");

    server.shutdown().await;
}

#[tokio::test]
async fn ds_fencing_403_keeps_producer_epoch_under_auth() {
    let (server, wire, _node) = gated_server(Protocol::Ds, full_stream_scope("/it/", false)).await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .put(format!("{base}/it/f"))
            .bearer_auth(&wire)
            .header("Content-Type", "text/plain")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let producer = |epoch: &'static str| {
        client
            .post(format!("{base}/it/f"))
            .bearer_auth(&wire)
            .header("Content-Type", "text/plain")
            .header("Producer-Id", "p1")
            .header("Producer-Epoch", epoch)
            .header("Producer-Seq", "0")
            .body("x")
    };
    assert_eq!(producer("2").send().await.unwrap().status(), 200);

    let fenced = producer("1").send().await.unwrap();
    assert_eq!(fenced.status(), 403);
    assert_eq!(fenced.headers()["Producer-Epoch"], "2");

    server.shutdown().await;
}

#[tokio::test]
async fn ds_gate_rejects_in_plain_text_without_new_vocabulary() {
    let (server, wire, _node) = gated_server(Protocol::Ds, full_stream_scope("/it/", false)).await;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let response = client.get(format!("{base}/it/x")).send().await.unwrap();
    assert_eq!(response.status(), 401);
    assert!(response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/plain"));
    assert!(response.headers().get("producer-epoch").is_none());

    let response = client
        .put(format!("{base}/it/orders"))
        .bearer_auth(&wire)
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);

    let response = client
        .get(format!("{base}/other/x"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    assert!(response.headers().get("producer-epoch").is_none());

    server.shutdown().await;
}
