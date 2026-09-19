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

/// Lifecycle operations use the admin audience and the existing stream
/// permissions together. General admin-write permission alone is insufficient.
#[tokio::test]
async fn stream_lifecycle_requires_admin_audience_operation_and_resource() {
    let (server, root, node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();

    let admin_only = issue_scoped_token(
        &client,
        &admin,
        &root,
        "lifecycle/admin",
        json!({
            "audiences": ["admin"],
            "ops": ["create", "delete"],
            "streams": [{ "prefix": "/managed/" }],
        }),
    )
    .await;
    let pico_only = issue_scoped_token(
        &client,
        &admin,
        &root,
        "lifecycle/pico",
        json!({
            "audiences": ["pico"],
            "ops": ["create", "delete"],
            "streams": [{ "prefix": "/managed/" }],
        }),
    )
    .await;
    let no_stream_write = issue_scoped_token(
        &client,
        &admin,
        &root,
        "lifecycle/general-admin",
        json!({
            "audiences": ["admin"],
            "groups": { "admin": { "read": true, "write": true } },
            "streams": [{ "prefix": "/managed/" }],
        }),
    )
    .await;
    let outside = issue_scoped_token(
        &client,
        &admin,
        &root,
        "lifecycle/other-streams",
        json!({
            "audiences": ["admin"],
            "ops": ["create", "delete"],
            "streams": [{ "prefix": "/other/" }],
        }),
    )
    .await;
    let create_only = issue_scoped_token(
        &client,
        &admin,
        &root,
        "lifecycle/create-only",
        json!({
            "audiences": ["admin"],
            "ops": ["create"],
            "streams": [{ "prefix": "/managed/" }],
        }),
    )
    .await;
    let delete_only = issue_scoped_token(
        &client,
        &admin,
        &root,
        "lifecycle/delete-only",
        json!({
            "audiences": ["admin"],
            "ops": ["delete"],
            "streams": [{ "prefix": "/managed/" }],
        }),
    )
    .await;
    let url = format!("{admin}/admin/streams/managed/orders");

    for (token, status) in [
        (None, 401),
        (Some(&pico_only), 401),
        (Some(&no_stream_write), 403),
        (Some(&outside), 403),
        (Some(&delete_only), 403),
    ] {
        let mut request = client.put(&url);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), status);
        assert!(
            node.service()
                .head("/managed/orders")
                .await
                .unwrap()
                .is_none()
        );
    }

    let created = client
        .put(&url)
        .bearer_auth(&admin_only)
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201, "pico audience is not required");
    assert!(
        node.service()
            .head("/managed/orders")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        client
            .put(format!("{base}/managed/protocol"))
            .bearer_auth(&admin_only)
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "admin token still cannot write through the protocol listener"
    );

    for (token, status) in [
        (None, 401),
        (Some(&pico_only), 401),
        (Some(&no_stream_write), 403),
        (Some(&outside), 403),
        (Some(&create_only), 403),
    ] {
        let mut request = client.delete(&url);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        assert_eq!(request.send().await.unwrap().status(), status);
        assert!(
            node.service()
                .head("/managed/orders")
                .await
                .unwrap()
                .is_some()
        );
    }
    assert_eq!(
        client
            .delete(&url)
            .bearer_auth(&admin_only)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert!(
        node.service()
            .head("/managed/orders")
            .await
            .unwrap()
            .is_none()
    );
    server.shutdown().await;
}

#[tokio::test]
async fn stream_lifecycle_uses_absolute_names_even_with_auto_prefix() {
    let (server, root, node) = admin_server().await;
    let admin = format!("http://{}", server.admin_addr().unwrap());
    let client = reqwest::Client::new();
    let prefixed = issue_scoped_token(
        &client,
        &admin,
        &root,
        "lifecycle/tenant",
        json!({
            "audiences": ["admin"],
            "ops": ["create", "delete"],
            "streams": [{ "prefix": "/tenant/" }],
            "autoPrefixStreams": true,
        }),
    )
    .await;

    // Admin paths decode once, like the existing inspection endpoint. A
    // native stream with a literal %2F in its name must escape that percent.
    // Administrative paths are absolute, even for a token that rewrites
    // protocol paths. A relative-looking path must not silently select a
    // different stored stream than the existing admin inspection API.
    let relative = format!("{admin}/admin/streams/orders%252Ftoday");
    for method in [reqwest::Method::PUT, reqwest::Method::DELETE] {
        assert_eq!(
            client
                .request(method, &relative)
                .bearer_auth(&prefixed)
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
    }
    let url = format!("{admin}/admin/streams/tenant/orders%252Ftoday");
    let created = client
        .put(&url)
        .bearer_auth(&prefixed)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    assert_eq!(
        created.headers()["Location"],
        "/admin/streams/tenant/orders%252Ftoday"
    );
    assert!(
        node.service()
            .head("/tenant/orders%2Ftoday")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        node.service()
            .head("/orders%2Ftoday")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        node.service()
            .head("/tenant/orders/today")
            .await
            .unwrap()
            .is_none()
    );

    let exact = issue_scoped_token(
        &client,
        &admin,
        &root,
        "lifecycle/exact-escaped",
        json!({
            "audiences": ["admin"],
            "ops": ["create", "delete"],
            "streams": [{ "exact": "/tenant/orders%2Ftoday" }],
        }),
    )
    .await;
    let other_name = format!("{admin}/admin/streams/tenant/orders%2Ftoday");
    for method in [reqwest::Method::PUT, reqwest::Method::DELETE] {
        assert_eq!(
            client
                .request(method, &other_name)
                .bearer_auth(&exact)
                .send()
                .await
                .unwrap()
                .status(),
            403,
            "authorization uses the same decoded name as the lifecycle service"
        );
    }
    assert_eq!(
        client
            .delete(&url)
            .bearer_auth(&prefixed)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert!(
        node.service()
            .head("/tenant/orders%2Ftoday")
            .await
            .unwrap()
            .is_none()
    );
    server.shutdown().await;
}

async fn issue_scoped_token(
    client: &reqwest::Client,
    admin: &str,
    root: &str,
    id: &str,
    scope: Value,
) -> String {
    let response = client
        .post(format!("{admin}/admin/tokens"))
        .bearer_auth(root)
        .json(&json!({ "id": id, "scope": scope }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let body: Value = response.json().await.unwrap();
    body["token"].as_str().unwrap().to_owned()
}
