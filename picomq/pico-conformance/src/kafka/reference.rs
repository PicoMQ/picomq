use crate::kafka::docker::{docker, docker_unchecked};

pub const IMAGE: &str = "apache/kafka:4.3.1";

pub struct Reference {
    network: String,
    container: String,
    bootstrap: String,
    host_address: String,
}

impl Reference {
    pub async fn start() -> anyhow::Result<Self> {
        let network = format!("pico-conformance-{}", std::process::id());
        let mut reference = Self {
            container: format!("{network}-reference"),
            bootstrap: format!("{network}-reference:9092"),
            host_address: String::new(),
            network,
        };

        docker(&["network", "create", &reference.network]).await?;

        let advertised = format!(
            "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{}",
            reference.bootstrap
        );

        let mut args = vec![
            "run",
            "-d",
            "--name",
            &reference.container,
            "--network",
            &reference.network,
            "-p",
            "127.0.0.1::9092",
        ];
        for env in [
            "KAFKA_NODE_ID=1",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
            &advertised,
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "KAFKA_HEAP_OPTS=-Xmx512m -Xms512m",
        ] {
            args.extend(["-e", env]);
        }

        args.push(IMAGE);
        docker(&args).await?;

        let port = docker(&["port", &reference.container, "9092/tcp"]).await?;
        reference.host_address = port.lines().next().unwrap_or_default().trim().to_owned();

        anyhow::ensure!(
            !reference.host_address.is_empty(),
            "docker port printed no address for {}",
            reference.container
        );

        Ok(reference)
    }

    pub fn network(&self) -> &str {
        &self.network
    }

    pub fn bootstrap(&self) -> &str {
        &self.bootstrap
    }

    pub fn host_address(&self) -> &str {
        &self.host_address
    }
}

impl Drop for Reference {
    fn drop(&mut self) {
        docker_unchecked(&["rm", "-f", &self.container]);

        let filter = format!("network={}", self.network);
        for id in docker_unchecked(&["ps", "-aq", "--filter", &filter]).split_whitespace() {
            docker_unchecked(&["rm", "-f", id]);
        }

        docker_unchecked(&["network", "rm", &self.network]);
    }
}
