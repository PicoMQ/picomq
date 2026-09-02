# Kafka protocol

A node serves the Kafka wire protocol on `--kafka-listen` unless `--no-kafka` is set. Standard Kafka clients, the Java client, librdkafka and everything built on it, connect with a bootstrap address. There is no PicoMQ client for Kafka because none is needed.

```bash
pico serve \
    --kafka-listen 0.0.0.0:9092 --kafka-advertise node1.internal:9092 \
    --insecure-allow-remote \
    --meta-url postgres://user:pass@pg:5432/picomq \
    --storage=-2@s3://bucket?region=us-east-1
```

The HTTP stream listener and the admin API keep serving next to it. A stream written over HTTP is a topic to Kafka clients, and a topic a Kafka client created is a stream to HTTP clients.

```bash
# Any Kafka tooling works against the listener.
kcat -b 127.0.0.1:9092 -t orders -P   # produce
kcat -b 127.0.0.1:9092 -t orders -C   # consume

# The same records over HTTP.
curl http://127.0.0.1:4437/orders
```

## Topics are streams

A topic is a stream with a single partition. A stream created over HTTP as `/orders/eu` is the topic `orders.eu` when that name is legal and free. `CreateTopics` from a Kafka client creates the stream of the same name. Topic names cannot contain `/`, and reserved streams under `/_sys`, `/_schemas` and `/_streams` have no topic. Kafka-created streams appear in the admin API like any other.

Kafka offsets are the stream's record offsets, so `earliest` is the trim watermark and `latest` is the high watermark.

Produce and fetch are a copy of the stored [RecordBatch v2](/docs/design/protocols) bytes. The broker patches each produced batch's base offset, the one rewrite a Kafka broker also performs and one the batch CRC excludes. HTTP appends land in the same format. Everything below the frontend (durability, ownership, transfers, garbage collection) behaves exactly as the [design docs](/docs/design/overview) describe.

## What is supported

| Area | Support |
| --- | --- |
| Produce | `acks` 0, 1, and all behave identically: an acknowledged record is durable on object storage. Idempotent producers get exact broker semantics with epoch fencing, sequence checking, and duplicate replay at the original offset. |
| Fetch | Long-polling with `fetch.min.bytes` and `fetch.max.wait.ms`, served from the same event-driven waiters as HTTP long polls. |
| Topics | `CreateTopics` and `DeleteTopics` for single-partition topics, plus metadata auto-creation when the client enables it. Requests for more than one partition are rejected. The `pico.schema` config binds a [schema](/docs/schemas), and `pico.schema.validate=true` validates produce against it. |
| Offsets | `ListOffsets` for earliest, latest, and by timestamp. |
| Consumer groups | The classic group protocol: `FindCoordinator`, join/sync/heartbeat/leave, offset commit and fetch, describe and list. Rebalances, generations, and fencing follow Kafka's state machine. |
| Not supported | Transactions and control batches (rejected as invalid), multiple partitions per topic, the KIP-848 consumer protocol, and SASL (see the note on exposure below). |

Clients negotiate through `ApiVersions` and stay inside the advertised ranges, so a well-behaved client uses only what the broker implements.

## Consumer groups

Group coordination is served by whichever node owns the group's internal stream, `/_sys/groups/{group}`. Only committed offsets are durable: commits append delta records, with a periodic snapshot and trim so replay after a coordinator move stays bounded. Membership, generations, and assignments are ephemeral and rebuilt from client rejoins, exactly as Kafka brokers behave. A coordinator move looks to clients like an ordinary rebalance.

## Exposure

The Kafka listener carries no authentication of its own, so binding it beyond loopback always requires the explicit `--insecure-allow-remote` opt-out, even with `--auth required`, which gates only the HTTP listeners. Treat it as an in-network service behind your own boundary. Token auth on the admin listener is unaffected.
