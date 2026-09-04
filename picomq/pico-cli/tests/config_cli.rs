//! Saved endpoints: editing them, and their effect on other commands.

mod common;

use common::{Run, pico_raw};
use tempfile::TempDir;

fn run(dir: &TempDir, args: &[&str]) -> Run {
    pico_raw(&dir.path().join("config.toml"), args, None)
}

#[test]
fn a_profile_is_written_read_and_removed() {
    let dir = TempDir::new().unwrap();

    run(
        &dir,
        &[
            "--endpoint",
            "https://pico.example:4437",
            "--protocol",
            "ds",
            "--http2",
            "config",
            "set",
            "prod",
        ],
    )
    .ok();
    run(
        &dir,
        &["--endpoint", "http://127.0.0.1:1", "config", "set", "dev"],
    )
    .ok();

    // The first profile saved becomes the default.
    let listed = run(&dir, &["config", "ls"]).ok().stdout;
    assert!(listed.contains("prod (default)"), "{listed}");
    assert!(listed.contains("ds"), "{listed}");
    assert!(listed.contains("http2"), "{listed}");
    assert!(listed.contains("dev\thttp://127.0.0.1:1"), "{listed}");

    run(&dir, &["config", "use", "dev"]).ok();
    let got = run(&dir, &["config", "get"]).ok().stdout;
    assert!(got.starts_with("dev\thttp://127.0.0.1:1"), "{got}");

    run(&dir, &["config", "rm", "dev"]).ok();
    assert_eq!(run(&dir, &["config", "get", "dev"]).code, 1);
    // Removing the default leaves no default rather than picking one.
    assert_eq!(run(&dir, &["config", "get"]).code, 1);
}

#[test]
fn set_needs_something_to_save() {
    let dir = TempDir::new().unwrap();
    let run = run(&dir, &["config", "set", "empty"]);
    assert_eq!(run.code, 1);
    assert!(run.stderr.contains("nothing to save"), "{}", run.stderr);
}

#[test]
fn a_missing_profile_is_an_error_not_a_default() {
    let dir = TempDir::new().unwrap();
    let run = run(&dir, &["--profile", "nope", "ls"]);
    assert_eq!(run.code, 1);
    assert!(run.stderr.contains("no profile named"), "{}", run.stderr);
}

/// The point of the file: a command with no connection flags reaches the
/// profile's endpoint. `ls` against a port nothing serves fails to connect,
/// which is proof enough that the address came from the profile. A wrong
/// address would name a different port.
#[test]
fn a_profile_supplies_the_endpoint() {
    let dir = TempDir::new().unwrap();
    let port = common::free_port();
    run(
        &dir,
        &[
            "--endpoint",
            &format!("http://127.0.0.1:{port}"),
            "config",
            "set",
            "local",
        ],
    )
    .ok();

    let failed = run(&dir, &["ls"]);
    assert_eq!(failed.code, 1);
    assert!(
        failed.stderr.contains(&port.to_string()),
        "{}",
        failed.stderr
    );
}

/// Precedence: an explicit flag beats the profile it would otherwise inherit.
#[test]
fn a_flag_overrides_the_profile() {
    let dir = TempDir::new().unwrap();
    let profile_port = common::free_port();
    let flag_port = common::free_port();
    run(
        &dir,
        &[
            "--endpoint",
            &format!("http://127.0.0.1:{profile_port}"),
            "config",
            "set",
            "local",
        ],
    )
    .ok();

    let failed = run(
        &dir,
        &["--endpoint", &format!("http://127.0.0.1:{flag_port}"), "ls"],
    );
    assert_eq!(failed.code, 1);
    assert!(
        failed.stderr.contains(&flag_port.to_string()),
        "{}",
        failed.stderr
    );
    assert!(
        !failed.stderr.contains(&profile_port.to_string()),
        "{}",
        failed.stderr
    );
}
