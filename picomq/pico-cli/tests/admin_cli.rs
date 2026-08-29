//! The admin commands, driven as an operator would against a real server.

mod common;

use common::{await_ready, no_config, pico, pico_raw, start};

fn admin(server: &common::Server, args: &[&str]) -> common::Run {
    let all: Vec<&str> = [&["admin", "--admin-endpoint", &server.admin_url][..], args].concat();
    pico_raw(&no_config(), &all, None)
}

#[tokio::test]
async fn admin_commands() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), "pico");
    await_ready(&reqwest::Client::new(), &server.admin_url).await;

    pico(
        &server,
        "pico",
        &["create", "/streams/adm", "--content-type", "text/plain"],
        None,
    )
    .ok();
    pico(&server, "pico", &["append", "/streams/adm"], Some("one\n")).ok();

    let cluster = admin(&server, &["cluster"]).ok();
    assert!(cluster.stdout.contains("node=1"), "{}", cluster.stdout);
    assert!(cluster.stdout.contains("streams=2"), "{}", cluster.stdout);

    let as_json = admin(&server, &["--json", "cluster"]).ok();
    let parsed: serde_json::Value = serde_json::from_str(&as_json.stdout).unwrap();
    assert_eq!(parsed["nodeId"], 1);

    let nodes = admin(&server, &["nodes"]).ok();
    assert!(nodes.stdout.contains("node=1"), "{}", nodes.stdout);
    assert!(nodes.stdout.contains("(this node)"), "{}", nodes.stdout);

    let stream = admin(&server, &["stream", "/streams/adm"]).ok();
    assert!(
        stream.stdout.contains("name=/streams/adm"),
        "{}",
        stream.stdout
    );
    assert!(stream.stdout.contains("state=opened"), "{}", stream.stdout);

    let missing = admin(&server, &["stream", "/absent"]);
    assert_eq!(missing.code, 1, "{}", missing.stderr);
    assert!(missing.stderr.contains("404"), "{}", missing.stderr);

    let slots = admin(&server, &["set-slots", "1", "--slots", "5"]).ok();
    assert!(slots.stderr.contains("slots=5"), "{}", slots.stderr);

    // The target node is not registered, so the proposal is rejected.
    let rejected = admin(&server, &["transfer", "/streams/adm", "--to-node", "9"]);
    assert_eq!(rejected.code, 1, "{}", rejected.stderr);
}
