use std::time::Duration;

use crate::kafka::docker::{docker, docker_unchecked, output};
use crate::kafka::reference::IMAGE;

pub const KCAT: &str = "pico-conformance-kcat";
pub const OLD_KCAT: &str = "edenhill/kcat:1.7.1";
const ADD_HOST: &str = "--add-host=host.docker.internal:host-gateway";
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
            ADD_HOST,
            "--entrypoint",
            "sleep",
            IMAGE,
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

    pub async fn kcat(&self, image: &str, args: &[&str], input: Option<&str>) -> Output {
        let mut command = vec![
            "run",
            "--rm",
            "-i",
            "--network",
            &self.network,
            ADD_HOST,
            image,
        ];
        command.extend_from_slice(args);

        run(&command, input).await
    }
}

impl Drop for Clients {
    fn drop(&mut self) {
        docker_unchecked(&["rm", "-f", &self.container]);
    }
}

async fn run(args: &[&str], input: Option<&str>) -> Output {
    match tokio::time::timeout(TIMEOUT, output(args, input)).await {
        Ok(Ok(output)) => Output {
            ok: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        },
        _ => Output {
            ok: false,
            stdout: String::new(),
        },
    }
}
