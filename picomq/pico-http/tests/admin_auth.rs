//! Gated admin plane over a real socket: probes and assets open, `/admin`
//! bearer-gated, and the token list, issue, and revoke lifecycle.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use picomq_auth::{AccessToken, Scope, TokenRecord, TokenStore};
use picomq_http::{HttpProtocol, RoutingMode, RunningServer, ServeOptions, serve};
use picomq_server::PicoNode;
use serde_json::{Value, json};

async fn admin_server() -> (RunningServer, String, Arc<PicoNode>) {
    let node = common::start_node().await;
    let (token, verifier) = AccessToken::issue("ops/root").unwrap();
    node.tokens()
        .store()
        .put_if_absent(TokenRecord {
            id: token.id.clone(),
            verifier,
            scope: Scope::root(),
            created_at_ms: 1,
            issued_by: String::new(),
        })
        .await
        .unwrap();
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = serve(
        node.clone(),
        ServeOptions {
            protocol: HttpProtocol::Pico,
            addr: loopback,
            admin_addr: Some(loopback),
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
async fn probes_and_assets_open_admin_routes_gated() {
    let (server, wire, _node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("{admin}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(format!("{admin}/ready"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client.get(&admin).send().await.unwrap().status(),
        200,
        "dashboard shell stays open"
    );

    let denied = client
        .get(format!("{admin}/admin/cluster"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 401);
    assert_eq!(
        denied
            .headers()
            .get("www-authenticate")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer"
    );

    let allowed = client
        .get(format!("{admin}/admin/cluster"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), 200);

    let preflight = client
        .request(reqwest::Method::OPTIONS, format!("{admin}/admin/cluster"))
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 204);
    assert_eq!(
        preflight
            .headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        "*"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn token_lifecycle_issue_list_revoke() {
    let (server, wire, _node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let client = reqwest::Client::new();

    let issued = client
        .post(format!("{admin}/admin/tokens"))
        .bearer_auth(&wire)
        .json(&json!({
            "id": "svc/reader",
            "scope": {
                "streams": [{ "prefix": "/acct/" }],
                "groups": { "stream": { "read": true } },
                "audiences": ["pico"],
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(issued.status(), 201);
    let body: Value = issued.json().await.unwrap();
    assert_eq!(body["id"], "svc/reader");
    let child = body["token"].as_str().unwrap().to_owned();
    assert!(!child.is_empty());

    // Same id again: conflict, no silent replace.
    let duplicate = client
        .post(format!("{admin}/admin/tokens"))
        .bearer_auth(&wire)
        .json(&json!({
            "id": "svc/reader",
            "scope": { "groups": { "stream": { "read": true } }, "streams": [{ "prefix": "" }], "audiences": ["pico"] },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate.status(), 409);

    // The read-only child cannot issue tokens.
    let widen = client
        .post(format!("{admin}/admin/tokens"))
        .bearer_auth(&child)
        .json(&json!({
            "id": "svc/other",
            "scope": { "groups": { "stream": { "read": true } }, "streams": [{ "prefix": "" }], "audiences": ["pico"] },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(widen.status(), 401, "child lacks the admin audience");

    let listing: Value = client
        .get(format!("{admin}/admin/tokens"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listing["count"], 2);
    let ids: Vec<&str> = listing["tokens"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["ops/root", "svc/reader"]);
    assert!(
        listing["tokens"][1].get("token").is_none()
            && listing["tokens"][1].get("verifier").is_none(),
        "secrets never listed"
    );

    let revoked = client
        .delete(format!("{admin}/admin/tokens/svc/reader"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), 204);
    let again = client
        .delete(format!("{admin}/admin/tokens/svc/reader"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 404);

    let listing: Value = client
        .get(format!("{admin}/admin/tokens"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listing["count"], 1);

    server.shutdown().await;
}

/// A revoke through the control plane takes effect on the data plane at
/// once: the conditional delete is applied through the metadata log before
/// the admin call returns, and the authorizer cache never serves a record
/// newer state has removed.
#[tokio::test]
async fn revocation_propagates_to_the_gate() {
    let (server, wire, _node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let issued: Value = client
        .post(format!("{admin}/admin/tokens"))
        .bearer_auth(&wire)
        .json(&json!({
            "id": "svc/doomed",
            "scope": {
                "streams": [{ "prefix": "/" }],
                "groups": { "stream": { "read": true, "write": true } },
                "audiences": ["pico"],
            },
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let doomed = issued["token"].as_str().unwrap().to_owned();

    assert_eq!(
        client
            .put(format!("{base}/live"))
            .bearer_auth(&doomed)
            .send()
            .await
            .unwrap()
            .status(),
        201,
        "the child token works before revocation"
    );

    assert_eq!(
        client
            .delete(format!("{admin}/admin/tokens/svc/doomed"))
            .bearer_auth(&wire)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );

    let rejected = client
        .get(format!("{base}/live"))
        .bearer_auth(&doomed)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 401, "revocation is immediate");

    server.shutdown().await;
}

#[tokio::test]
async fn anonymous_grant_cannot_carry_the_admin_audience() {
    let (server, wire, _node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let response = reqwest::Client::new()
        .post(format!("{admin}/admin/tokens"))
        .bearer_auth(&wire)
        .json(&json!({
            "id": "anonymous",
            "scope": {
                "streams": [{ "prefix": "/public/" }],
                "groups": { "stream": { "read": true }, "admin": { "read": true } },
                "audiences": ["pico", "admin"],
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    server.shutdown().await;
}

#[tokio::test]
async fn issuance_rejects_widening_and_dead_scopes() {
    let (server, wire, _node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let client = reqwest::Client::new();

    // An issuer narrowed to /acct/ cannot mint a wider child.
    let issued = client
        .post(format!("{admin}/admin/tokens"))
        .bearer_auth(&wire)
        .json(&json!({
            "id": "ops/acct",
            "scope": {
                "streams": [{ "prefix": "/acct/" }],
                "tokens": [{ "prefix": "svc/" }],
                "groups": { "stream": { "read": true, "write": true }, "tokens": { "read": true, "write": true } },
                "audiences": ["pico", "admin"],
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(issued.status(), 201);
    let body: Value = issued.json().await.unwrap();
    let issuer = body["token"].as_str().unwrap().to_owned();

    let widened = client
        .post(format!("{admin}/admin/tokens"))
        .bearer_auth(&issuer)
        .json(&json!({
            "id": "svc/wide",
            "scope": {
                "streams": [{ "prefix": "/" }],
                "groups": { "stream": { "read": true } },
                "audiences": ["pico"],
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(widened.status(), 403);

    let dead = client
        .post(format!("{admin}/admin/tokens"))
        .bearer_auth(&wire)
        .json(&json!({
            "id": "svc/dead",
            "scope": { "streams": [{ "prefix": "/x/" }], "audiences": ["pico"] },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(dead.status(), 400, "no ops means a dead credential");

    server.shutdown().await;
}
