mod common;

use common::{await_ready, no_config, pico, pico_raw, start_with};
use picomq_listener::testing::Authority;

fn certificate(dir: &std::path::Path) -> std::path::PathBuf {
    let authority = Authority::new().unwrap();
    authority
        .issue(&["localhost", "127.0.0.1"])
        .unwrap()
        .write(dir)
        .unwrap();
    std::fs::write(dir.join("ca.pem"), authority.root_pem()).unwrap();

    dir.join("ca.pem")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cli_reaches_a_tls_node_with_its_ca() {
    let dir = tempfile::tempdir().unwrap();
    let ca = certificate(dir.path());
    let (certificate, key) = (
        dir.path().join("certificate.pem"),
        dir.path().join("key.pem"),
    );
    let server = start_with(
        dir.path(),
        "pico",
        &[
            "--tls-certificate",
            certificate.to_str().unwrap(),
            "--tls-key",
            key.to_str().unwrap(),
        ],
    );
    assert!(server.base_url.starts_with("https://"));
    assert!(server.admin_url.starts_with("https://"));

    let http = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(&ca).unwrap()).unwrap())
        .build()
        .unwrap();
    await_ready(&http, &server.admin_url).await;

    let ca = ca.to_str().unwrap();
    let refused = pico(&server, "pico", &["create", "/streams/tls"], None);
    assert_ne!(refused.code, 0, "an unknown CA was trusted");

    pico(
        &server,
        "pico",
        &[
            "--ca-certificate",
            ca,
            "create",
            "/streams/tls",
            "--content-type",
            "text/plain",
        ],
        None,
    )
    .ok();
    pico(
        &server,
        "pico",
        &[
            "--ca-certificate",
            ca,
            "append",
            "/streams/tls",
            "--batch",
            "1",
        ],
        Some("over tls\n"),
    )
    .ok();
    let read = pico(
        &server,
        "pico",
        &["--ca-certificate", ca, "read", "/streams/tls"],
        None,
    )
    .ok();
    assert!(read.stdout.contains("over tls"), "stdout: {}", read.stdout);

    let cluster = pico_raw(
        &no_config(),
        &[
            "--ca-certificate",
            ca,
            "admin",
            "--admin-endpoint",
            &server.admin_url,
            "cluster",
        ],
        None,
    )
    .ok();
    assert!(
        cluster.stdout.contains("cluster="),
        "stdout: {}",
        cluster.stdout
    );

    server.terminate();
}
