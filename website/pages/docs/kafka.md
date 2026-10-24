# Kafka protocol

Every node serves the Kafka wire protocol on `--kafka-listen`, next to its HTTP listeners. Standard Kafka clients, the Java client, librdkafka and everything built on it, connect with a bootstrap address. There is no PicoMQ client for Kafka because none is needed.

```bash
pico serve \
    --kafka-listen 0.0.0.0:9092 --kafka-advertise node1.internal:9092 \
    --auth required --auth-bootstrap-token-file /run/secrets/pico-root \
    --meta-url postgres://user:pass@pg:5432/picomq \
    --storage=-2@s3://bucket?region=us-east-1
```

Topics are shared across protocols. A topic written over HTTP is readable by Kafka clients, and a topic a Kafka client created is readable over HTTP.

```bash
# Any Kafka tooling works against the listener.
kcat -b 127.0.0.1:9092 -t orders -P   # produce
kcat -b 127.0.0.1:9092 -t orders -C   # consume

# The same records over HTTP.
curl http://127.0.0.1:4437/orders
```

## Topics

A Kafka topic is a PicoMQ topic, with the same partitions. The HTTP name `/orders/eu` is the Kafka topic `orders.eu`, and a topic whose name has characters Kafka does not allow is reachable only over HTTP, see [Topics and streams](/docs/design/streams#names). A name starting with `_` is refused with `INVALID_TOPIC_EXCEPTION`, so tools that create `_schemas` or `_confluent-*` topics by default need another name.

- **Partitions.** `CreateTopics` takes any partition count, and `CreatePartitions` adds more later. Explicit replica assignments place each partition on the named node.
- **Replication.** Each partition has one replica, its owning node, and durability comes from object storage. The replication factor must be `1` or `-1`.
- **Leaders.** Metadata names each partition's owner as its leader. A request sent to another node answers `NOT_LEADER_OR_FOLLOWER`, and the client refreshes its metadata.
- **Offsets.** Kafka offsets are the partition's record offsets, so `earliest` is the trim watermark and `latest` is the high watermark.
- **Auto creation.** A metadata request creates a missing topic with one partition when the client asks for it.

Produce and fetch are a copy of the stored [RecordBatch v2](/docs/design/protocols#records) bytes. The broker patches each produced batch's base offset, the one rewrite a Kafka broker also performs and one the batch CRC excludes. Everything below the frontend, durability, ownership, transfers, and failover, behaves as the [design docs](/docs/design/overview) describe.

## Supported APIs

| API | Versions | Notes |
| --- | --- | --- |
| `ApiVersions` | 0 to 4 | |
| `Metadata` | 0 to 12 | |
| `Produce` | 3 to 10 | `acks` 0, 1, and all behave alike: an acknowledged record is durable in the WAL |
| `Fetch` | 4 to 16 | Long polling with `fetch.min.bytes` and `fetch.max.wait.ms` |
| `ListOffsets` | 1 to 7 | Earliest, latest, by timestamp, and max timestamp |
| `OffsetForLeaderEpoch` | 2 to 4 | The leader epoch is the partition's stream epoch |
| `InitProducerId` | 0 to 4 | Idempotent producers with epoch fencing and sequence checks |
| `CreateTopics` | 2 to 7 | |
| `DeleteTopics` | 1 to 6 | |
| `CreatePartitions` | 0 to 3 | Partitions only grow |
| `DeleteRecords` | 0 to 2 | Trims a partition. Waits for buffered records to reach data objects, up to the request timeout |
| `DescribeConfigs` | 1 to 4 | See [Configs](#configs) |
| `IncrementalAlterConfigs` | 0 to 1 | See [Configs](#configs) |
| `DescribeCluster` | 0 to 2 | |
| `FindCoordinator` | 0 to 3 | Group coordinators only |
| `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup` | 0 to 7, 0 to 5, 0 to 4, 0 to 5 | The classic group protocol, see [Consumer groups](/docs/design/groups) |
| `OffsetCommit`, `OffsetFetch` | 2 to 8, 1 to 7 | |
| `DescribeGroups`, `ListGroups` | 0 to 5 | `ListGroups` answers per broker, as in Kafka |
| `DeleteGroups` | 0 to 2 | Only groups without members |
| `OffsetDelete` | 0 | Not for topics the group is subscribed to |
| `SaslHandshake`, `SaslAuthenticate` | 0 to 1, 0 to 2 | Advertised with `--auth required`, see [Authentication](#authentication) |

Clients negotiate through `ApiVersions` and stay inside the advertised ranges.

Not supported: transactions and control batches, which are rejected, and the KIP-848 consumer protocol.

## Configs

| Config | Value | Changeable |
| --- | --- | --- |
| `cleanup.policy` | `delete` | No |
| `retention.ms` | The topic's limit, `-1` when unlimited | Yes |
| `retention.bytes` | The topic's limit, `-1` when unlimited | Yes |
| `pico.schema` | The bound [schema](/docs/schemas) | Yes |
| `pico.schema.validate` | Whether produce is validated against it | Yes |

Both retention configs can be set on `CreateTopics` too, and `-1` removes a limit. Retention goes by when PicoMQ wrote the data, not by record timestamps, so it matches Kafka's `LogAppendTime` topics, see [Retention](/docs/design/streams#retention). The `pico.schema` configs appear only when a schema is bound, and can also be set on `CreateTopics`. Changing any other config fails with `INVALID_CONFIG`.

PicoMQ does not compact topics, so `CreateTopics` refuses `cleanup.policy=compact` with `INVALID_CONFIG` instead of creating a topic that would never compact. This rules out Kafka Connect and Kafka Streams state stores, which need compacted topics. Broker resources describe no configs.

## Errors

Kafka error codes for each failure are listed in [Protocols](/docs/design/protocols#errors). A node that is stopping, or that cannot reach the metadata store, answers `NOT_LEADER_OR_FOLLOWER`, so clients refresh their metadata and retry on a live node.

## Authentication

With `--auth required` the listener authenticates every connection with SASL PLAIN, and the credential is the same token the HTTP listeners take as a bearer. The token model, issuing, and revocation are covered in [Authentication](/docs/operations/auth).

| Client setting | Value |
| --- | --- |
| `security.protocol` | `SASL_PLAINTEXT` |
| `sasl.mechanism` | `PLAIN` |
| `sasl.username` | The token id, for example `svc/ingest` |
| `sasl.password` | The token in wire form, `BASE64URL(id).BASE64URL(secret)` |

The token needs the `kafka` audience. A token that has `pico` and `kafka` works over both protocols with one scope, and a token with only `kafka` is a Kafka-only credential.

```bash
kcat -b node1.internal:9092 -t orders -P \
    -X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN \
    -X sasl.username=svc/ingest -X sasl.password="$TOKEN"
```

```properties
# Java clients and the kafka-*.sh tools, through --command-config
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="svc/ingest" password="<token>";
```

Until a connection authenticates, the listener holds it to the same limits a Kafka broker does. Breaking any of them closes the connection.

| Before authentication | Limit |
| --- | --- |
| Requests | `ApiVersions`, `SaslHandshake`, and `SaslAuthenticate` only |
| Frame size | 512 KiB, as Kafka's `sasl.server.max.receive.size` |
| Idle time | 10 minutes, as Kafka's `connections.max.idle.ms` |
| Credential | A wrong one answers `SASL_AUTHENTICATION_FAILED` |

### What the scope allows

Each request needs an operation from the token's scope, on the topic it names. A topic is matched under its HTTP name, so a scope with the prefix `/orders/` covers the Kafka topics `orders.eu` and `orders.us`.

| Request | Needs |
| --- | --- |
| `Produce` | `append` |
| `Fetch`, `OffsetCommit`, `OffsetFetch`, `OffsetDelete` | `read` |
| `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup` | `read`, and for a consumer's `JoinGroup` also `read` on every subscribed topic |
| `Metadata`, `ListOffsets`, `OffsetForLeaderEpoch`, `DescribeConfigs` | `head`, which any stream right on the topic grants |
| `CreateTopics`, `CreatePartitions`, `IncrementalAlterConfigs`, auto creation | `create` |
| `DeleteTopics` | `delete` |
| `DeleteRecords` | `trim` |
| `InitProducerId` | `append` on some topic |
| `DescribeGroups`, `ListGroups` | `stream_inspect` |
| `DeleteGroups` | `delete_group` |

A refused topic answers `TOPIC_AUTHORIZATION_FAILED`, a refused group `GROUP_AUTHORIZATION_FAILED`, and a refused `InitProducerId` `CLUSTER_AUTHORIZATION_FAILED`, so clients surface the same errors they would from a Kafka broker with ACLs. A `Metadata` request for all topics lists only the topics the token may describe, and `ListGroups` lists nothing without `stream_inspect`. A token that stops being valid closes the connection on its next request, see [Revocation](/docs/design/auth#revocation).

### Differences from Kafka

- Groups are not resources of their own, so rights come from the topics. A consumer joins only with `read` on what it subscribes to, but any token with `read` can heartbeat in or leave any group, and commit offsets for any group on the topics it reads, where Kafka would also check `Read` on the group.
- `DescribeConfigs` needs `head`, where Kafka needs a separate `DescribeConfigs` right. Configs hold retention and schema settings, nothing secret.
- SASL PLAIN is the only mechanism and `SASL_PLAINTEXT` the only security protocol. SCRAM and a TLS listener are listed under [Deliberately excluded](/docs/design/auth#deliberately-excluded).
- Tokens with `autoPrefixStreams` are refused at authentication rather than half applied, since Kafka names cannot be rewritten per tenant.

### Auth off

With `--auth off` the listener takes any client, the SASL APIs are not advertised, and the node binds the listener to loopback unless `--insecure-allow-remote` opts out for a network that is guarded some other way.
