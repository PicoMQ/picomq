//! The stream commands, driven as a user would: one binary, both protocols.
//!
//! Records to stdout, acknowledgements to stderr, `head` on a missing stream
//! exiting 1. Asserted once per protocol against a real server.

mod common;

use common::{await_ready, pico, start};

/// Every command that means the same thing on both wires.
async fn lifecycle(protocol: &str) {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), protocol);
    await_ready(&reqwest::Client::new(), &server.admin_url).await;

    let missing = pico(&server, protocol, &["head", "/streams/nope"], None);
    assert_eq!(missing.code, 1, "head on a missing stream exits 1");
    assert!(missing.stderr.contains("not found"), "{}", missing.stderr);

    let created = pico(
        &server,
        protocol,
        &["create", "/streams/cli", "--content-type", "text/plain"],
        None,
    )
    .ok();
    assert!(
        created.stderr.contains("created=true"),
        "{}",
        created.stderr
    );

    let appended = pico(
        &server,
        protocol,
        &["append", "/streams/cli", "--content-type", "text/plain"],
        Some("alpha\n"),
    )
    .ok();
    assert!(appended.stderr.contains("next="), "{}", appended.stderr);

    let head = pico(&server, protocol, &["head", "/streams/cli"], None).ok();
    assert!(head.stdout.contains("closed=false"), "{}", head.stdout);
    assert!(head.stdout.contains("text/plain"), "{}", head.stdout);

    let read = pico(&server, protocol, &["read", "/streams/cli"], None).ok();
    assert!(
        read.stdout.contains("alpha"),
        "records go to stdout: {:?}",
        read.stdout
    );

    // No `--follow`: the tail returns as soon as it is caught up.
    let tail = pico(&server, protocol, &["tail", "/streams/cli"], None).ok();
    assert!(tail.stdout.is_empty(), "{}", tail.stdout);

    let closed = pico(&server, protocol, &["close", "/streams/cli"], None).ok();
    assert!(closed.stderr.contains("closed next="), "{}", closed.stderr);

    let rejected = pico(
        &server,
        protocol,
        &["append", "/streams/cli", "--content-type", "text/plain"],
        Some("after-close\n"),
    );
    assert_eq!(rejected.code, 1, "{}", rejected.stderr);
    assert!(
        rejected.stderr.contains("closed"),
        "the error names the cause: {}",
        rejected.stderr
    );

    let deleted = pico(&server, protocol, &["delete", "/streams/cli"], None).ok();
    assert!(
        deleted.stderr.contains("deleted=true"),
        "{}",
        deleted.stderr
    );
}

#[tokio::test]
async fn pico_protocol_lifecycle() {
    lifecycle("pico").await;
}

#[tokio::test]
async fn ds_protocol_lifecycle() {
    lifecycle("ds").await;
}

async fn partition_flag(protocol: &str) {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), protocol);
    await_ready(&reqwest::Client::new(), &server.admin_url).await;

    pico(
        &server,
        protocol,
        &["create", "/streams/single", "--content-type", "text/plain"],
        None,
    )
    .ok();

    let head = pico(&server, protocol, &["head", "/streams/single"], None).ok();
    assert!(head.stdout.starts_with("partitions=1 "), "{}", head.stdout);

    let appended = pico(
        &server,
        protocol,
        &[
            "append",
            "/streams/single",
            "--partition",
            "0",
            "--content-type",
            "text/plain",
        ],
        Some("alpha\n"),
    )
    .ok();
    assert!(appended.stderr.contains("next="), "{}", appended.stderr);

    let head = pico(
        &server,
        protocol,
        &["head", "/streams/single", "--partition", "0"],
        None,
    )
    .ok();
    assert!(head.stdout.starts_with("partitions=1 "), "{}", head.stdout);
    assert!(!head.stdout.contains("next=-"), "{}", head.stdout);

    let read = pico(
        &server,
        protocol,
        &["read", "/streams/single", "--partition", "0"],
        None,
    )
    .ok();
    assert!(read.stdout.contains("alpha"), "{}", read.stdout);

    let head = pico(
        &server,
        protocol,
        &["head", "/streams/single", "--partition", "1"],
        None,
    );
    assert_eq!(head.code, 1, "{}", head.stderr);
    assert!(head.stderr.contains("not found"), "{}", head.stderr);

    let appended = pico(
        &server,
        protocol,
        &[
            "append",
            "/streams/single",
            "--partition",
            "1",
            "--content-type",
            "text/plain",
        ],
        Some("beta\n"),
    );
    assert_eq!(appended.code, 1, "{}", appended.stderr);
    assert!(appended.stderr.contains("not_found"), "{}", appended.stderr);

    let read = pico(
        &server,
        protocol,
        &["read", "/streams/single", "--partition", "1"],
        None,
    );
    assert_eq!(read.code, 1, "{}", read.stderr);
    assert!(read.stderr.contains("not_found"), "{}", read.stderr);
}

#[tokio::test]
async fn pico_protocol_partition_flag() {
    partition_flag("pico").await;
}

#[tokio::test]
async fn ds_protocol_partition_flag() {
    partition_flag("ds").await;
}

/// Topic settings set on create, described, and altered, the same on both
/// wires.
async fn settings(protocol: &str) {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), protocol);
    await_ready(&reqwest::Client::new(), &server.admin_url).await;

    pico(
        &server,
        protocol,
        &[
            "create",
            "/streams/settings",
            "--partitions",
            "2",
            "--retention-ms",
            "60000",
        ],
        None,
    )
    .ok();
    let described = pico(&server, protocol, &["describe", "/streams/settings"], None).ok();
    for line in ["partitions=2", "retention.ms=60000", "retention.bytes=-"] {
        assert!(described.stdout.contains(line), "{}", described.stdout);
    }

    let altered = pico(
        &server,
        protocol,
        &[
            "alter",
            "/streams/settings",
            "--partitions",
            "3",
            "--retention-ms",
            "none",
            "--retention-bytes",
            "4096",
        ],
        None,
    )
    .ok();
    for line in ["partitions=3", "retention.ms=-", "retention.bytes=4096"] {
        assert!(altered.stdout.contains(line), "{}", altered.stdout);
    }

    let shrink = pico(
        &server,
        protocol,
        &["alter", "/streams/settings", "--partitions", "1"],
        None,
    );
    assert_eq!(shrink.code, 1, "{}", shrink.stderr);
    assert!(shrink.stderr.contains("bad_request"), "{}", shrink.stderr);
}

#[tokio::test]
async fn pico_protocol_settings() {
    settings("pico").await;
}

#[tokio::test]
async fn ds_protocol_settings() {
    settings("ds").await;
}

/// Batching and listing are Pico protocol features. The CLI says so instead of
/// pretending.
#[tokio::test]
async fn protocol_specific_commands() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), "pico");
    await_ready(&reqwest::Client::new(), &server.admin_url).await;

    pico(&server, "pico", &["create", "/streams/batched"], None).ok();
    let appended = pico(
        &server,
        "pico",
        &["append", "/streams/batched", "--batch", "3"],
        Some("one\ntwo\nthree\n"),
    )
    .ok();
    assert_eq!(
        appended.stderr.lines().count(),
        1,
        "three records, one request: {}",
        appended.stderr
    );
    assert!(
        appended.stderr.contains("start=0 next=3"),
        "{}",
        appended.stderr
    );

    let read = pico(&server, "pico", &["read", "/streams/batched"], None).ok();
    assert_eq!(read.stdout, "0\tone\n1\ttwo\n2\tthree\n");

    let listed = pico(&server, "pico", &["ls", "--prefix", "/streams/"], None).ok();
    assert!(
        listed.stdout.contains("/streams/batched partitions=1 "),
        "{}",
        listed.stdout
    );

    // Trim only drops records the engine has already committed, so the new
    // start follows durability rather than the request.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let trimmed = loop {
        let run = pico(
            &server,
            "pico",
            &["trim", "/streams/batched", "--seq", "2"],
            None,
        )
        .ok();
        if run.stderr.contains("start=2") || std::time::Instant::now() > deadline {
            break run;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };
    assert!(trimmed.stderr.contains("start=2"), "{}", trimmed.stderr);

    // The same server, addressed with the wrong protocol's commands.
    let listed_ds = pico(&server, "ds", &["ls"], None);
    assert_eq!(listed_ds.code, 1);
    assert!(
        listed_ds.stderr.contains("no stream listing"),
        "{}",
        listed_ds.stderr
    );

    let batched_ds = pico(
        &server,
        "ds",
        &["append", "/streams/batched", "--batch", "2"],
        Some("a\nb\n"),
    );
    assert_eq!(batched_ds.code, 1);
    assert!(
        batched_ds.stderr.contains("one message per request"),
        "{}",
        batched_ds.stderr
    );
}
