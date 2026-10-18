use crate::kafka::Broker;
use crate::kafka::clients::Clients;
use crate::kafka::{raw, versions};

#[derive(Debug, Clone, Copy)]
pub enum Scenario {
    ApiVersions,
    Metadata,
    UnknownTopic,
    ProduceConsume,
    JavaProduceConsume,
    KeyedPartitions,
    GroupConsume,
    ListOffsets,
    MaxTimestamp,
    DescribeTopic,
    DescribeConfigs,
    CreatePartitions,
    OldLibrdkafka,
    FutureApiVersions,
    UnsupportedVersion,
    ControllerApi,
    ProduceV0,
    ProduceV2,
    CorruptBatch,
}

#[derive(Debug, Clone, Copy)]
pub enum Compare {
    Equal,
    Compatible,
}

const RECORDS: &str = "k1:v1\nk2:v2\nk3:v3\n";
const KEYED: &str = "a:1\nb:2\nc:3\nd:4\ne:5\nf:6\n";

impl Scenario {
    pub const ALL: [Scenario; 19] = [
        Scenario::ApiVersions,
        Scenario::Metadata,
        Scenario::UnknownTopic,
        Scenario::ProduceConsume,
        Scenario::JavaProduceConsume,
        Scenario::KeyedPartitions,
        Scenario::GroupConsume,
        Scenario::ListOffsets,
        Scenario::MaxTimestamp,
        Scenario::DescribeTopic,
        Scenario::DescribeConfigs,
        Scenario::CreatePartitions,
        Scenario::OldLibrdkafka,
        Scenario::FutureApiVersions,
        Scenario::UnsupportedVersion,
        Scenario::ControllerApi,
        Scenario::ProduceV0,
        Scenario::ProduceV2,
        Scenario::CorruptBatch,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Scenario::ApiVersions => "api-versions",
            Scenario::Metadata => "metadata",
            Scenario::UnknownTopic => "unknown-topic",
            Scenario::ProduceConsume => "produce-consume",
            Scenario::JavaProduceConsume => "java-produce-consume",
            Scenario::KeyedPartitions => "keyed-partitions",
            Scenario::GroupConsume => "group-consume",
            Scenario::ListOffsets => "list-offsets",
            Scenario::MaxTimestamp => "max-timestamp",
            Scenario::DescribeTopic => "describe-topic",
            Scenario::DescribeConfigs => "describe-configs",
            Scenario::CreatePartitions => "create-partitions",
            Scenario::OldLibrdkafka => "old-librdkafka",
            Scenario::FutureApiVersions => "future-api-versions",
            Scenario::UnsupportedVersion => "unsupported-version",
            Scenario::ControllerApi => "controller-api",
            Scenario::ProduceV0 => "produce-v0",
            Scenario::ProduceV2 => "produce-v2",
            Scenario::CorruptBatch => "corrupt-batch",
        }
    }

    pub fn compare(self) -> Compare {
        match self {
            Scenario::ApiVersions => Compare::Compatible,
            _ => Compare::Equal,
        }
    }

    pub async fn run(self, clients: &Clients, broker: &Broker<'_>, topic: &str) -> Vec<String> {
        let host = broker.host_address;
        let broker = broker.bootstrap;

        let lines = match self {
            Scenario::ApiVersions => api_versions(clients, broker).await,
            Scenario::Metadata => {
                let mut lines = vec![create(clients, broker, topic, 1).await];
                lines.extend(metadata(clients, broker, topic).await);
                lines
            }
            Scenario::UnknownTopic => unknown_topic(clients, broker, topic).await,
            Scenario::ProduceConsume => vec![
                create(clients, broker, topic, 1).await,
                produce(clients, broker, topic, RECORDS).await,
            ]
            .into_iter()
            .chain(consume(clients, broker, topic, "%p %o %k %s\\n").await)
            .collect(),
            Scenario::JavaProduceConsume => java_produce_consume(clients, broker, topic).await,
            Scenario::KeyedPartitions => vec![
                create(clients, broker, topic, 3).await,
                produce(clients, broker, topic, KEYED).await,
            ]
            .into_iter()
            .chain(sorted(consume(clients, broker, topic, "%p %k %s\\n").await))
            .collect(),
            Scenario::GroupConsume => group_consume(clients, broker, topic).await,
            Scenario::ListOffsets => {
                list_offsets(clients, broker, topic, &["-1", "-2", "0", "4102444800000"]).await
            }
            Scenario::MaxTimestamp => list_offsets(clients, broker, topic, &["-3"]).await,
            Scenario::DescribeTopic => describe_topic(clients, broker, topic).await,
            Scenario::DescribeConfigs => describe_configs(clients, broker, topic).await,
            Scenario::CreatePartitions => create_partitions(clients, broker, topic).await,
            Scenario::OldLibrdkafka => old_librdkafka(clients, broker, topic).await,
            Scenario::FutureApiVersions => vec![raw::future_api_versions(host).await],
            Scenario::UnsupportedVersion => vec![raw::unsupported_version(host).await],
            Scenario::ControllerApi => vec![raw::controller_api(host).await],
            Scenario::ProduceV0 => vec![
                create(clients, broker, topic, 1).await,
                raw::legacy_produce(host, topic, 0).await,
            ],
            Scenario::ProduceV2 => vec![
                create(clients, broker, topic, 1).await,
                raw::legacy_produce(host, topic, 2).await,
            ],
            Scenario::CorruptBatch => vec![
                create(clients, broker, topic, 1).await,
                raw::corrupt_produce(host, topic).await,
            ],
        };

        lines
            .into_iter()
            .map(|line| line.replace(topic, "{topic}").trim_end().to_owned())
            .collect()
    }
}

impl Compare {
    pub fn mismatch(self, reference: &[String], actual: &[String]) -> Option<String> {
        match self {
            Compare::Equal => (reference != actual).then(|| {
                format!(
                    "reference:\n{}\npicomq:\n{}",
                    indent(reference),
                    indent(actual)
                )
            }),
            Compare::Compatible => {
                let mismatches = versions::mismatches(reference, actual);
                (!mismatches.is_empty()).then(|| mismatches.join("\n"))
            }
        }
    }
}

fn indent(lines: &[String]) -> String {
    lines
        .iter()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn api_versions(clients: &Clients, broker: &str) -> Vec<String> {
    let output = clients
        .java(
            "kafka-broker-api-versions",
            &["--bootstrap-server", broker],
            None,
        )
        .await;
    versions::parse(&output.stdout)
}

async fn create(clients: &Clients, broker: &str, topic: &str, partitions: u32) -> String {
    let partitions = partitions.to_string();

    let output = clients
        .java(
            "kafka-topics",
            &[
                "--bootstrap-server",
                broker,
                "--create",
                "--topic",
                topic,
                "--partitions",
                &partitions,
                "--replication-factor",
                "1",
            ],
            None,
        )
        .await;
    status("create", output.ok)
}

async fn produce(clients: &Clients, broker: &str, topic: &str, records: &str) -> String {
    let output = clients
        .kcat(&["-b", broker, "-P", "-t", topic, "-K:"], Some(records))
        .await;
    status("produce", output.ok)
}

async fn consume(clients: &Clients, broker: &str, topic: &str, format: &str) -> Vec<String> {
    let output = clients
        .kcat(
            &[
                "-b",
                broker,
                "-C",
                "-t",
                topic,
                "-o",
                "beginning",
                "-e",
                "-f",
                format,
            ],
            None,
        )
        .await;
    lines(&output.stdout)
}

async fn metadata(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let output = clients.kcat(&["-b", broker, "-L", "-t", topic], None).await;

    normalize_metadata(&output.stdout)
}

async fn unknown_topic(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let output = clients
        .kcat(
            &[
                "-b",
                broker,
                "-L",
                "-t",
                topic,
                "-X",
                "allow.auto.create.topics=false",
            ],
            None,
        )
        .await;
    normalize_metadata(&output.stdout)
}

async fn old_librdkafka(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = vec![create(clients, broker, topic, 1).await];

    let produced = clients
        .old_kcat(&["-b", broker, "-P", "-t", topic, "-K:"], Some(RECORDS))
        .await;
    lines.push(status("produce", produced.ok));

    let consumed = clients
        .old_kcat(
            &[
                "-b",
                broker,
                "-C",
                "-t",
                topic,
                "-o",
                "beginning",
                "-e",
                "-f",
                "%p %o %k %s\\n",
            ],
            None,
        )
        .await;
    lines.extend(self::lines(&consumed.stdout));

    let listed = clients
        .old_kcat(&["-b", broker, "-L", "-t", topic], None)
        .await;
    lines.extend(normalize_metadata(&listed.stdout));

    lines
}

fn normalize_metadata(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("topic ") || line.starts_with("partition "))
        .map(|line| {
            line.split(", ")
                .map(normalize)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect()
}

fn normalize(piece: &str) -> String {
    if let Some(leader) = piece.strip_prefix("leader ") {
        return format!("leader {}", if leader == "-1" { "none" } else { "set" });
    }

    for label in ["replicas: ", "isrs: "] {
        if let Some(nodes) = piece.strip_prefix(label) {
            return format!("{label}{}", nodes.split(',').count());
        }
    }

    piece.to_owned()
}

async fn java_produce_consume(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = vec![create(clients, broker, topic, 1).await];

    let sent = clients
        .java(
            "kafka-console-producer",
            &["--bootstrap-server", broker, "--topic", topic],
            Some("v1\nv2\nv3\n"),
        )
        .await;
    lines.push(status("produce", sent.ok));

    let received = clients
        .java(
            "kafka-console-consumer",
            &[
                "--bootstrap-server",
                broker,
                "--topic",
                topic,
                "--from-beginning",
                "--max-messages",
                "3",
                "--timeout-ms",
                "30000",
            ],
            None,
        )
        .await;
    lines.extend(self::lines(&received.stdout));

    lines
}

async fn group_consume(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let group = format!("{topic}-group");

    let mut lines = vec![
        create(clients, broker, topic, 1).await,
        produce(clients, broker, topic, RECORDS).await,
    ];

    let received = clients
        .java(
            "kafka-console-consumer",
            &[
                "--bootstrap-server",
                broker,
                "--topic",
                topic,
                "--group",
                &group,
                "--from-beginning",
                "--max-messages",
                "3",
                "--timeout-ms",
                "30000",
            ],
            None,
        )
        .await;
    lines.extend(self::lines(&received.stdout));

    let described = clients
        .java(
            "kafka-consumer-groups",
            &[
                "--bootstrap-server",
                broker,
                "--describe",
                "--group",
                &group,
            ],
            None,
        )
        .await;
    lines.extend(described.stdout.lines().filter_map(|row| {
        let columns: Vec<&str> = row.split_whitespace().collect();
        (columns.first() == Some(&group.as_str()) && columns.len() >= 6)
            .then(|| columns[1..6].join(" "))
    }));

    lines
}

async fn list_offsets(clients: &Clients, broker: &str, topic: &str, times: &[&str]) -> Vec<String> {
    let mut lines = vec![create(clients, broker, topic, 1).await];

    for record in RECORDS.lines() {
        lines.push(produce(clients, broker, topic, &format!("{record}\n")).await);
    }

    for time in times {
        let output = clients
            .java(
                "kafka-get-offsets",
                &[
                    "--bootstrap-server",
                    broker,
                    "--topic",
                    topic,
                    "--time",
                    time,
                ],
                None,
            )
            .await;
        lines.push(format!("{time} {}", output.stdout.trim()));
    }

    lines
}

async fn describe_topic(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = vec![create(clients, broker, topic, 1).await];

    let output = clients
        .java(
            "kafka-topics",
            &["--bootstrap-server", broker, "--describe", "--topic", topic],
            None,
        )
        .await;
    lines.push(status("describe", output.ok));

    lines.extend(output.stdout.lines().map(|line| {
        line.split('\t')
            .map(str::trim)
            .filter(|field| {
                [
                    "Topic:",
                    "PartitionCount:",
                    "ReplicationFactor:",
                    "Partition:",
                ]
                .iter()
                .any(|label| field.starts_with(label))
            })
            .collect::<Vec<_>>()
            .join(" ")
    }));

    lines
}

async fn describe_configs(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = vec![create(clients, broker, topic, 1).await];

    let output = clients
        .java(
            "kafka-configs",
            &[
                "--bootstrap-server",
                broker,
                "--describe",
                "--entity-type",
                "topics",
                "--entity-name",
                topic,
            ],
            None,
        )
        .await;
    lines.push(status("describe", output.ok));

    lines.extend(self::lines(&output.stdout));

    lines
}

async fn create_partitions(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = vec![create(clients, broker, topic, 1).await];

    let output = clients
        .java(
            "kafka-topics",
            &[
                "--bootstrap-server",
                broker,
                "--alter",
                "--topic",
                topic,
                "--partitions",
                "3",
            ],
            None,
        )
        .await;
    lines.push(status("alter", output.ok));

    lines.extend(metadata(clients, broker, topic).await);

    lines
}

fn status(step: &str, ok: bool) -> String {
    format!("{step} {}", if ok { "ok" } else { "failed" })
}

fn lines(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect()
}

fn sorted(mut lines: Vec<String>) -> Vec<String> {
    lines.sort();

    lines
}
