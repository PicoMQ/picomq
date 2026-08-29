# Kafka protocol

A node started with `--protocol kafka` serves the Kafka wire protocol. Standard Kafka clients, the Java client, librdkafka and everything built on it, connect with nothing but a bootstrap address. There is no PicoMQ client for Kafka because none is needed. That is the point of the compatibility surface.

```bash
pico serve --protocol kafka \
    --kafka-listen 0.0.0.0:9092 --kafka-advertise node1.internal:9092 \
    --meta-url postgres://user:pass@pg:5432/picomq \
    --storage=-2@s3://bucket?region=us-east-1
```

The admin API and dashboard keep working in Kafka mode. The HTTP stream routes do not: a node serves one client protocol, and in Kafka mode that protocol is Kafka.

```bash
# Any Kafka tooling works against the listener.
kcat -b 127.0.0.1:9092 -t orders -P   # produce
kcat -b 127.0.0.1:9092 -t orders -C   # consume
```

## Topics are streams

A topic maps to the stream `/{topic}` with a single partition. Topic names cannot contain `/`, so a topic can never collide with the reserved `/_sys/` subtree, and Kafka-created streams appear in the admin API like any other. Kafka offsets are the stream's record offsets, so `earliest` is the trim watermark and `latest` is the high watermark.

Record batches are stored verbatim. The broker patches each batch's base-offset field to the assigned offset, the one rewrite a real Kafka broker also performs and one the batch CRC deliberately excludes, and hands the same bytes back on fetch with no per-record decode. Produce and fetch are zero-copy translations over the stream service, and everything below the frontend (durability, ownership, transfers, garbage collection) behaves exactly as the [design docs](/docs/design/overview) describe.

## What is supported

| Area | Support |
| --- | --- |
| Produce | `acks` 0, 1, and all behave identically: an acknowledged record is durable on object storage. Idempotent producers get exact broker semantics with epoch fencing, sequence checking, and duplicate replay at the original offset. |
| Fetch | Long-polling with `fetch.min.bytes` and `fetch.max.wait.ms`, served from the same event-driven waiters as HTTP long polls. |
| Topics | `CreateTopics` and `DeleteTopics` for single-partition topics, plus metadata auto-creation when the client enables it. Requests for more than one partition are rejected. |
| Offsets | `ListOffsets` for earliest, latest, and by timestamp. |
| Catalog | The [catalog changelog](/docs/design/catalog) is the read-only internal topic `__catalog`. |
| Consumer groups | The classic group protocol: `FindCoordinator`, join/sync/heartbeat/leave, offset commit and fetch, describe and list. Rebalances, generations, and fencing follow Kafka's state machine. |
| Not supported | Transactions and control batches (rejected as invalid), multiple partitions per topic, the KIP-848 consumer protocol, and SASL (see the note on exposure below). |

Clients negotiate through `ApiVersions` and stay inside the advertised ranges, so a well-behaved client uses only what the broker implements.

## Consumer groups

Group coordination is served by whichever node owns the group's internal stream, `/_sys/groups/{group}`. Only committed offsets are durable: commits append delta records, with a periodic snapshot and trim so replay after a coordinator move stays bounded. Membership, generations, and assignments are ephemeral and rebuilt from client rejoins, exactly as Kafka brokers behave. A coordinator move looks to clients like an ordinary rebalance.

## Exposure

The Kafka listener carries no authentication of its own, so binding it beyond loopback always requires the explicit `--insecure-allow-remote` opt-out, even with `--auth required`, which gates only the HTTP listeners. Treat it as an in-network service behind your own boundary. Token auth on the admin listener is unaffected.
