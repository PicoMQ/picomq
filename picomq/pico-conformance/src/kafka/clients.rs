use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::kafka::docker::{docker, remove};

const KAFKA: &str = "apache/kafka:4.3.1";
const KCAT: &str = "pico-conformance-kcat";
const OLD_KCAT: &str = "edenhill/kcat:1.7.1";
const HOST: &str = "--add-host=host.docker.internal:host-gateway";
const TIMEOUT: Duration = Duration::from_secs(60);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);

pub struct Clients {
    network: String,
    container: String,
}

pub struct Output {
    pub ok: bool,
    pub stdout: String,
}

impl Clients {
    pub async fn start(network: &str) -> anyhow::Result<Self> {
        let clients = Self {
            network: network.to_owned(),
            container: format!("{network}-clients"),
        };

        let context = concat!(env!("CARGO_MANIFEST_DIR"), "/clients/kafka");
        docker(&["build", "-q", "-t", KCAT, context]).await?;

        docker(&[
            "run",
            "-d",
            "--name",
            &clients.container,
            "--network",
            network,
            HOST,
            "--entrypoint",
            "sleep",
            KAFKA,
            "infinity",
        ])
        .await?;

        Ok(clients)
    }

    pub async fn wait_for(&self, broker: &str) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;

        while tokio::time::Instant::now() < deadline {
            let probe = self
                .java(
                    "kafka-broker-api-versions",
                    &["--bootstrap-server", broker],
                    None,
                )
                .await;

            if probe.ok {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        anyhow::bail!("broker {broker} never became ready")
    }

    pub async fn java(&self, tool: &str, args: &[&str], input: Option<&str>) -> Output {
        let path = format!("/opt/kafka/bin/{tool}.sh");

        let mut command = vec![
            "exec",
            "-i",
            "-e",
            "KAFKA_HEAP_OPTS=-Xmx256m",
            &self.container,
            &path,
        ];
        command.extend_from_slice(args);

        run(&command, input).await
    }

    pub async fn kcat(&self, args: &[&str], input: Option<&str>) -> Output {
        self.kcat_image(KCAT, args, input).await
    }

    pub async fn old_kcat(&self, args: &[&str], input: Option<&str>) -> Output {
        self.kcat_image(OLD_KCAT, args, input).await
    }

    async fn kcat_image(&self, image: &str, args: &[&str], input: Option<&str>) -> Output {
        let mut command = vec!["run", "--rm", "-i", "--network", &self.network, HOST, image];
        command.extend_from_slice(args);

        run(&command, input).await
    }
}

impl Drop for Clients {
    fn drop(&mut self) {
        remove(&["rm", "-f", &self.container]);
    }
}

async fn run(args: &[&str], input: Option<&str>) -> Output {
    let failed = Output {
        ok: false,
        stdout: String::new(),
    };

    let Ok(mut child) = Command::new("docker")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
    else {
        return failed;
    };

    if let (Some(mut stdin), Some(input)) = (child.stdin.take(), input) {
        let _ = stdin.write_all(input.as_bytes()).await;
    }

    match tokio::time::timeout(TIMEOUT, child.wait_with_output()).await {
        Ok(Ok(output)) => Output {
            ok: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        },
        _ => failed,
    }
}
