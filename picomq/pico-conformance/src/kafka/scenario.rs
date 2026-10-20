use crate::kafka::Broker;
use crate::kafka::clients::{Clients, KCAT, OLD_KCAT};
use crate::kafka::rebalance;
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
    DeleteRecords,
    DeleteGroups,
    DeleteOffsets,
    ClusterId,
    GroupLeave,
    GroupRejoin,
    GroupMissing,
}

#[derive(Debug, Clone, Copy)]
pub enum Compare {
    Equal,
    Compatible,
}

const RECORDS: &str = "k1:v1\nk2:v2\nk3:v3\n";
const KEYED: &str = "a:1\nb:2\nc:3\nd:4\ne:5\nf:6\n";

impl Scenario {
    pub const ALL: &[Scenario] = &[
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
        Scenario::DeleteRecords,
        Scenario::DeleteGroups,
        Scenario::DeleteOffsets,
        Scenario::ClusterId,
        Scenario::GroupLeave,
        Scenario::GroupRejoin,
        Scenario::GroupMissing,
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
            Scenario::DeleteRecords => "delete-records",
            Scenario::DeleteGroups => "delete-groups",
            Scenario::DeleteOffsets => "delete-offsets",
            Scenario::ClusterId => "cluster-id",
            Scenario::GroupLeave => "group-leave-during-rebalance",
            Scenario::GroupRejoin => "group-rejoin-while-pending",
            Scenario::GroupMissing => "group-missing-member",
        }
    }

    pub fn compare(self) -> Compare {
        match self {
            Scenario::ApiVersions => Compare::Compatible,
            _ => Compare::Equal,
        }
    }

    pub async fn run(self, clients: &Clients, broker: &Broker<'_>, topic: &str) -> Vec<String> {
        let host_address = broker.host_address;
        let broker = broker.bootstrap;

        let lines = match self {
            Scenario::ApiVersions => api_versions(clients, broker).await,
            Scenario::Metadata => metadata(clients, broker, topic).await,
            Scenario::UnknownTopic => unknown_topic(clients, broker, topic).await,
            Scenario::ProduceConsume => produce_consume(clients, KCAT, broker, topic).await,
            Scenario::JavaProduceConsume => java_produce_consume(clients, broker, topic).await,
            Scenario::KeyedPartitions => keyed_partitions(clients, broker, topic).await,
            Scenario::GroupConsume => group_consume(clients, broker, topic).await,
            Scenario::ListOffsets => {
                list_offsets(clients, broker, topic, &["-1", "-2", "0", "4102444800000"]).await
            }
            Scenario::MaxTimestamp => list_offsets(clients, broker, topic, &["-3"]).await,
            Scenario::DescribeTopic => describe_topic(clients, broker, topic).await,
            Scenario::DescribeConfigs => describe_configs(clients, broker, topic).await,
            Scenario::CreatePartitions => create_partitions(clients, broker, topic).await,
            Scenario::OldLibrdkafka => old_librdkafka(clients, broker, topic).await,
            Scenario::FutureApiVersions => vec![raw::future_api_versions(host_address).await],
            Scenario::UnsupportedVersion => vec![raw::unsupported_version(host_address).await],
            Scenario::ControllerApi => vec![raw::controller_api(host_address).await],
            Scenario::ProduceV0 => legacy_produce(clients, broker, host_address, topic, 0).await,
            Scenario::ProduceV2 => legacy_produce(clients, broker, host_address, topic, 2).await,
            Scenario::CorruptBatch => corrupt_batch(clients, broker, host_address, topic).await,
            Scenario::DeleteRecords => delete_records(clients, broker, topic).await,
            Scenario::DeleteGroups => delete_groups(clients, broker, topic).await,
            Scenario::DeleteOffsets => delete_offsets(clients, broker, topic).await,
            Scenario::ClusterId => cluster_id(clients, broker).await,
            Scenario::GroupLeave => rebalance::leave(host_address, topic).await,
            Scenario::GroupRejoin => rebalance::rejoin(host_address, topic).await,
            Scenario::GroupMissing => rebalance::missing(host_address, topic).await,
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

async fn produce(
    clients: &Clients,
    kcat: &str,
    broker: &str,
    topic: &str,
    records: &str,
) -> String {
    let output = clients
        .kcat(
            kcat,
            &["-b", broker, "-P", "-t", topic, "-K:"],
            Some(records),
        )
        .await;

    status("produce", output.ok)
}

async fn consume(
    clients: &Clients,
    kcat: &str,
    broker: &str,
    topic: &str,
    format: &str,
) -> Vec<String> {
    let output = clients
        .kcat(
            kcat,
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

async fn list_metadata(clients: &Clients, kcat: &str, broker: &str, topic: &str) -> Vec<String> {
    let output = clients
        .kcat(kcat, &["-b", broker, "-L", "-t", topic], None)
        .await;

    normalize_metadata(&output.stdout)
}

async fn metadata(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = vec![create(clients, broker, topic, 1).await];

    lines.extend(list_metadata(clients, KCAT, broker, topic).await);

    lines
}

async fn unknown_topic(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let output = clients
        .kcat(
            KCAT,
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

async fn produce_consume(clients: &Clients, kcat: &str, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = vec![
        create(clients, broker, topic, 1).await,
        produce(clients, kcat, broker, topic, RECORDS).await,
    ];

    lines.extend(consume(clients, kcat, broker, topic, "%p %o %k %s\\n").await);

    lines
}

async fn keyed_partitions(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = vec![
        create(clients, broker, topic, 3).await,
        produce(clients, KCAT, broker, topic, KEYED).await,
    ];

    let mut received = consume(clients, KCAT, broker, topic, "%p %k %s\\n").await;
    received.sort();
    lines.extend(received);

    lines
}

async fn old_librdkafka(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = produce_consume(clients, OLD_KCAT, broker, topic).await;

    lines.extend(list_metadata(clients, OLD_KCAT, broker, topic).await);

    lines
}

async fn legacy_produce(
    clients: &Clients,
    broker: &str,
    host_address: &str,
    topic: &str,
    version: i16,
) -> Vec<String> {
    vec![
        create(clients, broker, topic, 1).await,
        raw::legacy_produce(host_address, topic, version).await,
    ]
}

async fn corrupt_batch(
    clients: &Clients,
    broker: &str,
    host_address: &str,
    topic: &str,
) -> Vec<String> {
    vec![
        create(clients, broker, topic, 1).await,
        raw::corrupt_produce(host_address, topic).await,
    ]
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
    let mut lines = consume_as_group(clients, broker, topic, &group).await;

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
        lines.push(produce(clients, KCAT, broker, topic, &format!("{record}\n")).await);
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

    lines.extend(list_metadata(clients, KCAT, broker, topic).await);

    lines
}

async fn consume_as_group(
    clients: &Clients,
    broker: &str,
    topic: &str,
    group: &str,
) -> Vec<String> {
    let mut lines = vec![
        create(clients, broker, topic, 1).await,
        produce(clients, KCAT, broker, topic, RECORDS).await,
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
                group,
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

async fn delete_records(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let mut lines = vec![
        create(clients, broker, topic, 1).await,
        produce(clients, KCAT, broker, topic, RECORDS).await,
    ];

    let offsets =
        format!(r#"{{"partitions":[{{"topic":"{topic}","partition":0,"offset":2}}],"version":1}}"#);
    let deleted = clients
        .java(
            "kafka-delete-records",
            &[
                "--bootstrap-server",
                broker,
                "--offset-json-file",
                "/dev/stdin",
            ],
            Some(&offsets),
        )
        .await;
    lines.push(status("delete", deleted.ok));
    lines.extend(self::lines(&deleted.stdout));

    lines.extend(consume(clients, KCAT, broker, topic, "%o %k %s\\n").await);

    lines
}

async fn delete_groups(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let group = format!("{topic}-group");
    let mut lines = consume_as_group(clients, broker, topic, &group).await;

    let deleted = clients
        .java(
            "kafka-consumer-groups",
            &["--bootstrap-server", broker, "--delete", "--group", &group],
            None,
        )
        .await;
    lines.push(status("delete", deleted.ok));
    lines.extend(self::lines(&deleted.stdout));

    let again = clients
        .java(
            "kafka-consumer-groups",
            &["--bootstrap-server", broker, "--delete", "--group", &group],
            None,
        )
        .await;
    lines.push(status("delete again", again.ok));

    lines
}

async fn delete_offsets(clients: &Clients, broker: &str, topic: &str) -> Vec<String> {
    let group = format!("{topic}-group");
    let mut lines = consume_as_group(clients, broker, topic, &group).await;

    let deleted = clients
        .java(
            "kafka-consumer-groups",
            &[
                "--bootstrap-server",
                broker,
                "--delete-offsets",
                "--group",
                &group,
                "--topic",
                topic,
            ],
            None,
        )
        .await;
    lines.push(status("delete offsets", deleted.ok));
    lines.extend(
        deleted
            .stdout
            .lines()
            .map(|row| row.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|row| !row.is_empty()),
    );

    lines
}

async fn cluster_id(clients: &Clients, broker: &str) -> Vec<String> {
    let output = clients
        .java(
            "kafka-cluster",
            &["cluster-id", "--bootstrap-server", broker],
            None,
        )
        .await;

    vec![
        status("cluster-id", output.ok),
        status(
            "id present",
            output
                .stdout
                .trim()
                .strip_prefix("Cluster ID: ")
                .is_some_and(|id| !id.is_empty()),
        ),
    ]
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
