//! A `pico serve` process for the CLI tests to talk to.
//!
//! Each test binary includes this module and uses part of it, so unused
//! helpers here are expected rather than dead.
#![allow(dead_code)]

use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A port nothing is listening on. Racy in principle. The window is one
/// process spawn and the alternative (ephemeral bind inside the child) gives
/// the test no address to talk to.
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Kills the child on drop so a failed assertion cannot leak a server.
pub struct Server {
    child: Child,
    pub base_url: String,
    pub admin_url: String,
    pub kafka_addr: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn start(dir: &std::path::Path, protocol: &str) -> Server {
    start_with(dir, protocol, &[])
}

pub fn start_with(dir: &std::path::Path, protocol: &str, extra: &[&str]) -> Server {
    let http = free_port();
    let admin = free_port();
    let kafka = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_pico"))
        .args([
            "serve",
            "--protocol",
            protocol,
            "--listen",
            &format!("127.0.0.1:{http}"),
            "--admin-listen",
            &format!("127.0.0.1:{admin}"),
            "--kafka-listen",
            &format!("127.0.0.1:{kafka}"),
            "--meta-url",
            &format!("sqlite:{}", dir.join("meta.db").display()),
            "--storage",
            &format!("1@file://{}", dir.join("objects").display()),
            "--wal",
            &format!("2@file://{}", dir.join("wal").display()),
            // Keep an idle tail from holding the test for the default poll.
            "--long-poll-timeout-sec",
            "1",
            // Commit low-volume writes promptly so trims can advance.
            "--wal-upload-interval-ms",
            "200",
        ])
        .args(extra)
        .spawn()
        .unwrap();
    Server {
        child,
        base_url: format!("http://127.0.0.1:{http}"),
        admin_url: format!("http://127.0.0.1:{admin}"),
        kafka_addr: format!("127.0.0.1:{kafka}"),
    }
}

pub async fn await_ready(client: &reqwest::Client, admin_url: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = client.get(format!("{admin_url}/ready")).send().await {
            if response.status() == 200 {
                return;
            }
        }
        assert!(Instant::now() < deadline, "server never became ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// What one `pico` invocation produced.
pub struct Run {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl Run {
    pub fn ok(self) -> Self {
        assert_eq!(self.code, 0, "stderr: {}", self.stderr);
        self
    }
}

/// Run the built binary against `server`, optionally feeding it stdin.
pub fn pico(server: &Server, protocol: &str, args: &[&str], stdin: Option<&str>) -> Run {
    let all: Vec<&str> = [
        &[
            "--endpoint",
            server.base_url.as_str(),
            "--protocol",
            protocol,
        ][..],
        args,
    ]
    .concat();
    pico_raw(&no_config(), &all, stdin)
}

/// A config path that does not exist, so tests never read (or are steered by)
/// the developer's own `~/.config/pico/config.toml`.
pub fn no_config() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("absent-config.toml")
}

/// Run the built binary with an explicit config file and nothing implied.
/// The keyring is skipped so tests never touch the developer's keychain.
pub fn pico_raw(config: &std::path::Path, args: &[&str], stdin: Option<&str>) -> Run {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pico"))
        .env("PICO_CONFIG", config)
        .env("PICO_NO_KEYRING", "1")
        .args(args)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }

    let output = child.wait_with_output().unwrap();
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}
