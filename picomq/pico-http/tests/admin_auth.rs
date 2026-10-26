//! Gated admin plane over a real socket: probes and assets open, `/admin`
//! bearer-gated, and the token list, issue, and revoke lifecycle.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use picomq_auth::{AccessToken, Audience, Operation, ResourceSet, Scope, TokenRecord, TokenStore};
use picomq_http::{HttpProtocol, RoutingMode, RunningServer, ServeOptions, serve};
use picomq_listener::Listener;
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
        Listener::bind(picomq_metrics::Listener::Stream, loopback, 1024, None).unwrap(),
        Some(Listener::bind(picomq_metrics::Listener::Admin, loopback, 1024, None).unwrap()),
        ServeOptions {
            protocol: HttpProtocol::Pico,
            routing_mode: RoutingMode::LocalAlways,
            authorizer: Some(node.authorizer()),
            ..Default::default()
        },
    )
    .await;
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
                "access": { "stream": { "read": true } },
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
            "scope": { "access": { "stream": { "read": true } }, "streams": [{ "prefix": "" }], "audiences": ["pico"] },
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
            "scope": { "access": { "stream": { "read": true } }, "streams": [{ "prefix": "" }], "audiences": ["pico"] },
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
    let ids: Vec<&str> = listing["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["ops/root", "svc/reader"]);
    assert_eq!(listing["next"], Value::Null);
    assert!(
        listing["items"][1].get("token").is_none() && listing["items"][1].get("verifier").is_none(),
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
    assert_eq!(listing["items"].as_array().unwrap().len(), 1);

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
                "access": { "stream": { "read": true, "write": true } },
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
                "access": { "stream": { "read": true }, "admin": { "read": true } },
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
                "access": { "stream": { "read": true, "write": true }, "tokens": { "read": true, "write": true } },
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
                "access": { "stream": { "read": true } },
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

async fn seed(node: &PicoNode, id: &str, scope: Scope) -> String {
    let (token, verifier) = AccessToken::issue(id).unwrap();
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
    token.render()
}

async fn every_id(client: &reqwest::Client, url: &str, wire: &str) -> (Vec<String>, usize) {
    let mut ids = Vec::new();
    let mut pages = 0;
    let mut cursor: Option<String> = None;

    loop {
        let separator = if url.contains('?') { '&' } else { '?' };
        let page_url = match &cursor {
            Some(cursor) => format!("{url}{separator}cursor={cursor}"),
            None => url.to_owned(),
        };
        let page: Value = client
            .get(page_url)
            .bearer_auth(wire)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        pages += 1;
        ids.extend(
            page["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["id"].as_str().unwrap().to_owned()),
        );

        match page["next"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => return (ids, pages),
        }
    }
}

#[tokio::test]
async fn the_token_list_pages_through_every_token_once() {
    let (server, wire, node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let client = reqwest::Client::new();
    let mut expected = vec!["ops/root".to_owned()];
    for n in 0..250 {
        let id = format!("svc/{n:03}");
        seed(&node, &id, Scope::default()).await;
        expected.push(id);
    }

    let (ids, pages) = every_id(&client, &format!("{admin}/admin/tokens?limit=100"), &wire).await;
    assert_eq!(ids, expected);
    assert_eq!(pages, 3);

    let (prefixed, _) = every_id(
        &client,
        &format!("{admin}/admin/tokens?prefix=svc/1&limit=7"),
        &wire,
    )
    .await;
    assert_eq!(
        prefixed,
        (100..200)
            .map(|n| format!("svc/{n:03}"))
            .collect::<Vec<_>>()
    );

    server.shutdown().await;
}

#[tokio::test]
async fn a_narrow_token_gets_short_pages_that_still_finish() {
    let (server, _, node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let client = reqwest::Client::new();
    for n in 0..500 {
        seed(&node, &format!("svc/{n:03}"), Scope::default()).await;
    }
    let narrow = seed(
        &node,
        "z/lister",
        Scope {
            tokens: ResourceSet::prefix("z/"),
            ops: [Operation::ListTokens].into(),
            audiences: [Audience::Admin].into(),
            ..Scope::default()
        },
    )
    .await;

    let first: Value = client
        .get(format!("{admin}/admin/tokens?limit=5"))
        .bearer_auth(&narrow)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        first["items"],
        json!([]),
        "the budget ends the scan before z/"
    );
    assert!(first["next"].is_string());

    let (ids, pages) = every_id(&client, &format!("{admin}/admin/tokens?limit=5"), &narrow).await;
    assert_eq!(ids, ["z/lister"]);
    assert!(pages > 1);

    server.shutdown().await;
}

#[tokio::test]
async fn a_cursor_only_resumes_its_own_query() {
    let (server, wire, node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let client = reqwest::Client::new();
    for n in 0..5 {
        seed(&node, &format!("svc/{n}"), Scope::default()).await;
    }

    let first: Value = client
        .get(format!("{admin}/admin/tokens?prefix=svc/&limit=2"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let cursor = first["next"].as_str().unwrap();

    let elsewhere = client
        .get(format!(
            "{admin}/admin/tokens?prefix=ops/&limit=2&cursor={cursor}"
        ))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(elsewhere.status(), 400);
    let tampered = client
        .get(format!("{admin}/admin/tokens?cursor=garbage"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(tampered.status(), 400);
    let unbounded = client
        .get(format!("{admin}/admin/tokens?limit=5000"))
        .bearer_auth(&wire)
        .send()
        .await
        .unwrap();
    assert_eq!(unbounded.status(), 400);

    server.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn token_pages_stay_bounded_at_the_scale_target() {
    let (server, wire, node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let client = reqwest::Client::new();
    for n in 0..100_000 {
        seed(&node, &format!("svc/{n:06}"), Scope::default()).await;
    }
    let narrow = seed(
        &node,
        "z/lister",
        Scope {
            tokens: ResourceSet::prefix("z/"),
            ops: [Operation::ListTokens].into(),
            audiences: [Audience::Admin].into(),
            ..Scope::default()
        },
    )
    .await;

    for (wire, limit, expected) in [(&wire, 1_000, 100_002), (&narrow, 100, 1)] {
        let mut cursor: Option<String> = None;
        let mut seen = 0;
        let mut slowest = std::time::Duration::ZERO;

        loop {
            let url = match &cursor {
                Some(cursor) => format!("{admin}/admin/tokens?limit={limit}&cursor={cursor}"),
                None => format!("{admin}/admin/tokens?limit={limit}"),
            };
            let started = std::time::Instant::now();
            let page: Value = client
                .get(url)
                .bearer_auth(wire)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            slowest = slowest.max(started.elapsed());

            let items = page["items"].as_array().unwrap().len();
            assert!(items <= limit);
            seen += items;
            match page["next"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => break,
            }
        }

        println!("limit {limit}: {seen} tokens, slowest page {slowest:?}");
        assert_eq!(seen, expected);
        assert!(slowest < std::time::Duration::from_millis(250));
    }

    server.shutdown().await;
}
