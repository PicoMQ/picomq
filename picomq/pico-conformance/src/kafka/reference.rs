use std::path::PathBuf;

use tempfile::TempDir;

use crate::certificates::Certificates;
use crate::kafka::clients::Clients;
use crate::kafka::docker::{docker, docker_unchecked};
use crate::kafka::sasl::{Credentials, READER_GROUPS};

pub const IMAGE: &str = "apache/kafka:4.3.1";

const JAAS: &str = "/etc/kafka/secrets/jaas.conf";
const CERTIFICATES: &str = "/etc/kafka/certs";

pub fn host() -> String {
    format!("{}-reference", network())
}

fn network() -> String {
    format!("pico-conformance-{}", std::process::id())
}

pub struct Reference {
    network: String,
    container: String,
    bootstrap: String,
    host_address: String,
    sasl_bootstrap: String,
    sasl_host_address: String,
    secure_bootstrap: String,
    secure_host_address: String,
    root: Credentials,
    reader: Credentials,
    writer: Credentials,
    _secrets: TempDir,
}

impl Reference {
    pub async fn start(certificates: &Certificates) -> anyhow::Result<Self> {
        let network = network();
        let root = Credentials {
            username: "picomq".to_owned(),
            password: "picomq-secret".to_owned(),
        };
        let reader = Credentials {
            username: "reader".to_owned(),
            password: "reader-secret".to_owned(),
        };
        let writer = Credentials {
            username: "writer".to_owned(),
            password: "writer-secret".to_owned(),
        };
        let secrets = tempfile::tempdir()?;
        let jaas = write_jaas(&secrets, &[&root, &reader, &writer])?;

        let mut reference = Self {
            container: host(),
            bootstrap: format!("{}:9092", host()),
            host_address: String::new(),
            sasl_bootstrap: format!("{}:9094", host()),
            sasl_host_address: String::new(),
            secure_bootstrap: format!("{}:9095", host()),
            secure_host_address: String::new(),
            root,
            reader,
            writer,
            _secrets: secrets,
            network,
        };

        docker(&["network", "create", &reference.network]).await?;

        let advertised = format!(
            "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{},SASL://{},SECURE://{}",
            reference.bootstrap, reference.sasl_bootstrap, reference.secure_bootstrap
        );
        let super_users = format!(
            "KAFKA_SUPER_USERS=User:ANONYMOUS;User:{}",
            reference.root.username
        );
        let jaas_mount = format!("{}:{JAAS}:ro", jaas.display());
        let certificates_mount = format!("{}:{CERTIFICATES}:ro", certificates.dir().display());
        let keystore =
            format!("KAFKA_LISTENER_NAME_SECURE_SSL_KEYSTORE_LOCATION={CERTIFICATES}/keystore.pem");
        let jaas_opts = format!("KAFKA_OPTS=-Djava.security.auth.login.config={JAAS}");

        let mut args = vec![
            "run",
            "-d",
            "--name",
            &reference.container,
            "--network",
            &reference.network,
            "-p",
            "127.0.0.1::9092",
            "-p",
            "127.0.0.1::9094",
            "-p",
            "127.0.0.1::9095",
            "-v",
            &jaas_mount,
            "-v",
            &certificates_mount,
        ];
        for env in [
            "KAFKA_NODE_ID=1",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,SASL://:9094,SECURE://:9095",
            &advertised,
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT,SECURE:SASL_SSL",
            "KAFKA_LISTENER_NAME_SECURE_SSL_KEYSTORE_TYPE=PEM",
            &keystore,
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "KAFKA_SASL_ENABLED_MECHANISMS=PLAIN",
            "KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
            &super_users,
            &jaas_opts,
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

        reference.host_address = reference.published("9092/tcp").await?;
        reference.sasl_host_address = reference.published("9094/tcp").await?;
        reference.secure_host_address = reference.published("9095/tcp").await?;

        Ok(reference)
    }

    pub async fn grant(&self, clients: &Clients) -> anyhow::Result<()> {
        for (identity, operation) in [(&self.reader, "Read"), (&self.writer, "Write")] {
            let principal = format!("User:{}", identity.username);

            let output = clients
                .java(
                    "kafka-acls",
                    &[
                        "--bootstrap-server",
                        &self.bootstrap,
                        "--add",
                        "--allow-principal",
                        &principal,
                        "--operation",
                        operation,
                        "--topic",
                        "*",
                    ],
                    None,
                )
                .await;

            anyhow::ensure!(
                output.ok,
                "granting {operation} to {principal} failed:\n{}",
                output.stdout
            );
        }

        let principal = format!("User:{}", self.reader.username);
        let output = clients
            .java(
                "kafka-acls",
                &[
                    "--bootstrap-server",
                    &self.bootstrap,
                    "--add",
                    "--allow-principal",
                    &principal,
                    "--operation",
                    "Read",
                    "--group",
                    READER_GROUPS,
                    "--resource-pattern-type",
                    "prefixed",
                ],
                None,
            )
            .await;
        anyhow::ensure!(
            output.ok,
            "granting group Read to {principal} failed:\n{}",
            output.stdout
        );

        Ok(())
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

    pub fn sasl_bootstrap(&self) -> &str {
        &self.sasl_bootstrap
    }

    pub fn sasl_host_address(&self) -> &str {
        &self.sasl_host_address
    }

    pub fn secure_bootstrap(&self) -> &str {
        &self.secure_bootstrap
    }

    pub fn secure_host_address(&self) -> &str {
        &self.secure_host_address
    }

    pub fn root(&self) -> &Credentials {
        &self.root
    }

    pub fn reader(&self) -> &Credentials {
        &self.reader
    }

    pub fn writer(&self) -> &Credentials {
        &self.writer
    }

    async fn published(&self, port: &str) -> anyhow::Result<String> {
        let output = docker(&["port", &self.container, port]).await?;
        let address = output.lines().next().unwrap_or_default().trim().to_owned();

        if address.is_empty() {
            let status = docker_unchecked(&[
                "inspect",
                "-f",
                "{{.State.Status}} exit {{.State.ExitCode}}",
                &self.container,
            ]);
            let logs = docker_unchecked(&["logs", "--tail", "20", &self.container]);
            anyhow::bail!(
                "{} has no published {port} ({}):\n{logs}",
                self.container,
                status.trim()
            );
        }

        Ok(address)
    }
}

fn write_jaas(secrets: &TempDir, users: &[&Credentials]) -> anyhow::Result<PathBuf> {
    let users = users
        .iter()
        .map(|user| format!("  user_{}=\"{}\"", user.username, user.password))
        .collect::<Vec<_>>()
        .join("\n");
    let config = format!(
        "KafkaServer {{\n  org.apache.kafka.common.security.plain.PlainLoginModule required\n{users};\n}};\n"
    );

    let path = secrets.path().join("jaas.conf");
    std::fs::write(&path, config)?;

    Ok(path)
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
