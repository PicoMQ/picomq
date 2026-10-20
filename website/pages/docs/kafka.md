# Kafka protocol

Every node serves the Kafka wire protocol on `--kafka-listen`, next to its HTTP listeners. Standard Kafka clients, the Java client, librdkafka and everything built on it, connect with a bootstrap address. There is no PicoMQ client for Kafka because none is needed.

```bash
pico serve \
    --kafka-listen 0.0.0.0:9092 --kafka-advertise node1.internal:9092 \
    --insecure-allow-remote \
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

A Kafka topic is a PicoMQ topic, with the same partitions. The HTTP name `/orders/eu` is the Kafka topic `orders.eu`, and a topic whose name has characters Kafka does not allow is reachable only over HTTP, see [Topics and streams](/docs/design/streams#names).

- **Partitions.** `CreateTopics` takes any partition count, and `CreatePartitions` adds more later. Explicit replica assignments place each partition on the named node.
- **Replication.** Each partition has one replica, its owning node, and durability comes from object storage. The replication factor must be `1` or `-1`.
- **Leaders.** Metadata names each partition's owner as its leader. A request sent to another node answers `NOT_LEADER_OR_FOLLOWER`, and the client refreshes its metadata.
- **Offsets.** Kafka offsets are the partition's record offsets, so `earliest` is the trim watermark and `latest` is the high watermark.
- **Auto creation.** A metadata request creates a missing topic with one partition when the client asks for it.

Produce and fetch are a copy of the stored [RecordBatch v2](/docs/design/protocols#records) bytes. The broker patches each produced batch's base offset, the one rewrite a Kafka broker also performs and one the batch CRC excludes. Everything below the frontend, durability, ownership, transfers, and failover, behaves as the [design docs](/docs/design/overview) describe.

## Supported APIs

| API | Versions | Notes |
| --- | --- | --- |
| `ApiVersions` | 0–4 | |
| `Metadata` | 0–12 | |
| `Produce` | 3–10 | `acks` 0, 1, and all behave alike: an acknowledged record is durable in the WAL |
| `Fetch` | 4–16 | Long polling with `fetch.min.bytes` and `fetch.max.wait.ms` |
| `ListOffsets` | 1–7 | Earliest, latest, by timestamp, and max timestamp |
| `OffsetForLeaderEpoch` | 2–4 | The leader epoch is the partition's stream epoch |
| `InitProducerId` | 0–4 | Idempotent producers with epoch fencing and sequence checks |
| `CreateTopics` | 2–7 | |
| `DeleteTopics` | 1–6 | |
| `CreatePartitions` | 0–3 | Partitions only grow |
| `DeleteRecords` | 0–2 | Trims a partition. Waits for buffered records to reach data objects, up to the request timeout |
| `DescribeConfigs` | 1–4 | See [Configs](#configs) |
| `IncrementalAlterConfigs` | 0–1 | See [Configs](#configs) |
| `DescribeCluster` | 0–2 | |
| `FindCoordinator` | 0–3 | Group coordinators only |
| `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup` | 0–7, 0–5, 0–4, 0–5 | The classic group protocol, see [Consumer groups](/docs/design/groups) |
| `OffsetCommit`, `OffsetFetch` | 2–8, 1–7 | |
| `DescribeGroups`, `ListGroups` | 0–5 | `ListGroups` answers per broker, as in Kafka |
| `DeleteGroups` | 0–2 | Only groups without members |
| `OffsetDelete` | 0 | Not for topics the group is subscribed to |

Clients negotiate through `ApiVersions` and stay inside the advertised ranges.

Not supported: transactions and control batches, which are rejected, the KIP-848 consumer protocol, and SASL, see [Exposure](#exposure).

## Configs

| Config | Value | Changeable |
| --- | --- | --- |
| `cleanup.policy` | `delete` | No |
| `retention.ms` | `-1` | No |
| `retention.bytes` | `-1` | No |
| `pico.schema` | The bound [schema](/docs/schemas) | Yes |
| `pico.schema.validate` | Whether produce is validated against it | Yes |

`retention.ms` is `-1` because PicoMQ does not delete records by age. A topic's TTL expires the whole topic once nothing appends to it or reads it for that long, and `DeleteRecords` trims explicitly. The `pico.schema` configs appear only when a schema is bound, and can also be set on `CreateTopics`. Changing any other config fails with `INVALID_CONFIG`.

PicoMQ does not compact topics, so `CreateTopics` refuses `cleanup.policy=compact` with `INVALID_CONFIG` instead of creating a topic that would never compact. This rules out Kafka Connect and Kafka Streams state stores, which need compacted topics. Broker resources describe no configs.

## Errors

Kafka error codes for each failure are listed in [Protocols](/docs/design/protocols#errors). A node that is stopping, or that cannot reach the metadata store, answers `NOT_LEADER_OR_FOLLOWER`, so clients refresh their metadata and retry on a live node.

## Exposure

The Kafka listener has no authentication of its own, so binding it beyond loopback always requires `--insecure-allow-remote`, even with `--auth required`, which gates only the HTTP listeners. Treat it as an in-network service behind your own boundary.
