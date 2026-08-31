//! `pico auth` against a secured server: login over stdin, status
//! verification, stored credentials on stream and admin commands, and logout.

mod common;

use picomq_auth::AccessToken;

#[tokio::test]
async fn auth_flow_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
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

    // No stored credential yet.
    let status = common::pico_raw(&config, &["auth", "status"], None);
    assert_eq!(status.code, 1);
    assert!(status.stdout.contains("token=none"), "{}", status.stdout);

    // Login over stdin, then status verifies against the live server.
    common::pico_raw(&config, &["auth", "login"], Some(&root.render())).ok();
    let status = common::pico_raw(
        &config,
        &["--endpoint", &server.base_url, "auth", "status"],
        None,
    )
    .ok();
    assert!(
        status.stdout.contains("token=ops/root"),
        "{}",
        status.stdout
    );
    assert!(
        status.stdout.contains("status=authenticated"),
        "{}",
        status.stdout
    );

    // Stream commands pick up the stored credential.
    common::pico_raw(
        &config,
        &["--endpoint", &server.base_url, "create", "/cli/orders"],
        None,
    )
    .ok();
    common::pico_raw(
        &config,
        &["--endpoint", &server.base_url, "append", "/cli/orders"],
        Some("one\n"),
    )
    .ok();
    let read = common::pico_raw(
        &config,
        &["--endpoint", &server.base_url, "read", "/cli/orders"],
        None,
    )
    .ok();
    assert!(read.stdout.contains("one"), "{}", read.stdout);

    // The admin plane takes the same stored credential.
    let cluster = common::pico_raw(
        &config,
        &["admin", "--admin-endpoint", &server.admin_url, "cluster"],
        None,
    )
    .ok();
    assert!(cluster.stdout.contains("cluster="), "{}", cluster.stdout);

    // After logout the same command is refused by the server.
    common::pico_raw(&config, &["auth", "logout"], None).ok();
    let denied = common::pico_raw(
        &config,
        &["--endpoint", &server.base_url, "create", "/cli/again"],
        None,
    );
    assert_eq!(denied.code, 1);
    assert!(
        denied.stderr.contains("unauthenticated"),
        "{}",
        denied.stderr
    );
}
