//! `pico admin tokens` against a secured server: issue from the root token,
//! use the issued token, narrowing, listing, and revocation.

mod common;

use picomq_auth::AccessToken;

fn tokens(server: &common::Server, token: &str, args: &[&str]) -> common::Run {
    let all: Vec<&str> = [
        &[
            "admin",
            "--admin-endpoint",
            &server.admin_url,
            "--token",
            token,
            "tokens",
        ][..],
        args,
    ]
    .concat();
    common::pico_raw(&common::no_config(), &all, None)
}

fn stream(server: &common::Server, token: &str, args: &[&str]) -> common::Run {
    let all: Vec<&str> = [
        &["--endpoint", &server.base_url, "--token", token][..],
        args,
    ]
    .concat();
    common::pico_raw(&common::no_config(), &all, None)
}

#[tokio::test]
async fn tokens_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let (root, _) = AccessToken::issue("ops/root").unwrap();
    let token_file = dir.path().join("bootstrap-token");
    std::fs::write(&token_file, root.render()).unwrap();
    let server = common::start_with(
        dir.path(),
        "pico",
        &[
            "--auth",
            "required",
            "--auth-bootstrap-token-file",
            token_file.to_str().unwrap(),
        ],
    );
    common::await_ready(&reqwest::Client::new(), &server.admin_url).await;
    let root = root.render();

    // An operator token that may manage `svc/` tokens and every stream.
    let operator = tokens(
        &server,
        &root,
        &[
            "issue",
            "ops/operator",
            "--stream-prefix",
            "/",
            "--token-prefix",
            "svc/",
            "--grant",
            "stream:rw",
            "--grant",
            "tokens:rw",
            "--grant",
            "admin:read",
            "--audience",
            "pico",
            "--audience",
            "admin",
        ],
    )
    .ok();
    assert!(
        operator.stderr.contains("issued id=ops/operator"),
        "{}",
        operator.stderr
    );
    // The secret alone is on stdout, so `$(pico admin tokens issue ...)` works.
    let operator = operator.stdout.trim().to_owned();
    assert!(AccessToken::parse(&operator).is_ok(), "{operator}");

    // An app token, issued by the operator and confined to its prefix.
    let app = tokens(
        &server,
        &operator,
        &[
            "issue",
            "svc/orders",
            "--stream-prefix",
            "/orders/",
            "--grant",
            "stream:rw",
            "--audience",
            "pico",
            "--expires-in",
            "1d",
        ],
    )
    .ok()
    .stdout
    .trim()
    .to_owned();
    stream(&server, &app, &["create", "/orders/eu"]).ok();
    let outside = stream(&server, &app, &["create", "/billing/eu"]);
    assert_eq!(outside.code, 1, "{}", outside.stderr);
    assert!(outside.stderr.contains("403"), "{}", outside.stderr);

    // The operator cannot mint outside `svc/` or widen past its own scope.
    let wrong_id = tokens(
        &server,
        &operator,
        &[
            "issue",
            "ops/other",
            "--stream-prefix",
            "/",
            "--grant",
            "stream:read",
            "--audience",
            "pico",
        ],
    );
    assert_eq!(wrong_id.code, 1, "{}", wrong_id.stderr);
    assert!(wrong_id.stderr.contains("403"), "{}", wrong_id.stderr);
    let wider = tokens(
        &server,
        &operator,
        &[
            "issue",
            "svc/wide",
            "--stream-prefix",
            "/",
            "--grant",
            "admin:write",
            "--audience",
            "admin",
        ],
    );
    assert_eq!(wider.code, 1, "{}", wider.stderr);
    assert!(
        wider.stderr.contains("scope exceeds issuer"),
        "{}",
        wider.stderr
    );

    // The raw JSON form is accepted as well.
    tokens(
        &server,
        &operator,
        &[
            "issue",
            "svc/reader",
            "--scope",
            r#"{"streams":[{"exact":"/orders/eu"}],"ops":["read"],"audiences":["pico"]}"#,
        ],
    )
    .ok();

    // The operator lists only the ids it may manage.
    let listed = tokens(&server, &operator, &["list"]).ok();
    assert!(listed.stdout.contains("id=svc/orders"), "{}", listed.stdout);
    assert!(
        listed
            .stdout
            .contains("streams=prefix:/orders/ tokens=- groups=stream:rw"),
        "{}",
        listed.stdout
    );
    assert!(
        listed.stdout.contains("issuedBy=ops/operator"),
        "{}",
        listed.stdout
    );
    assert!(
        listed
            .stdout
            .contains("id=svc/reader streams=exact:/orders/eu"),
        "{}",
        listed.stdout
    );
    assert!(listed.stdout.contains("ops=read"), "{}", listed.stdout);
    assert!(!listed.stdout.contains("ops/root"), "{}", listed.stdout);
    let parsed: serde_json::Value = serde_json::from_str(
        &common::pico_raw(
            &common::no_config(),
            &[
                "admin",
                "--admin-endpoint",
                &server.admin_url,
                "--token",
                &operator,
                "--json",
                "tokens",
                "list",
            ],
            None,
        )
        .ok()
        .stdout,
    )
    .unwrap();
    assert_eq!(parsed["count"], 2, "{parsed}");

    // Ids may hold characters that need encoding in the revoke path.
    tokens(
        &server,
        &operator,
        &[
            "issue",
            "svc/odd id?#",
            "--stream",
            "/orders/eu",
            "--op",
            "head",
            "--audience",
            "pico",
        ],
    )
    .ok();
    tokens(&server, &operator, &["revoke", "svc/odd id?#"]).ok();

    // Revocation takes effect on the next request. Ids keep their slash.
    let revoked = tokens(&server, &operator, &["revoke", "svc/orders"]).ok();
    assert!(
        revoked.stderr.contains("revoked id=svc/orders"),
        "{}",
        revoked.stderr
    );
    let after = stream(&server, &app, &["create", "/orders/us"]);
    assert_eq!(after.code, 1, "{}", after.stderr);
    assert!(after.stderr.contains("401"), "{}", after.stderr);
    let missing = tokens(&server, &operator, &["revoke", "svc/orders"]);
    assert_eq!(missing.code, 1, "{}", missing.stderr);
    assert!(missing.stderr.contains("404"), "{}", missing.stderr);

    // Local validation happens before anything is sent.
    let no_audience = tokens(
        &server,
        &root,
        &["issue", "svc/x", "--grant", "stream:read"],
    );
    assert_eq!(no_audience.code, 1);
    assert!(
        no_audience.stderr.contains("--audience"),
        "{}",
        no_audience.stderr
    );
    let bad_op = tokens(
        &server,
        &root,
        &["issue", "svc/x", "--op", "fly", "--audience", "pico"],
    );
    assert_eq!(bad_op.code, 2);
    assert!(
        bad_op.stderr.contains("possible values") && bad_op.stderr.contains("issue_token"),
        "{}",
        bad_op.stderr
    );
}
