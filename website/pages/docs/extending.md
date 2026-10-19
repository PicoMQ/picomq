# Protocol facades

Reference for `Topics`, the internal API every protocol is built on. Pico, Durable Streams and Kafka are facades over it: parse the wire, call `Topics`, encode the result. Partitions, logs, the engine, the metadata log and the stored record format are not a facade's concern.

<div class="pico-diagram">
<svg viewBox="0 0 720 250" width="720" role="img" aria-label="Three facades, Pico, Durable Streams and Kafka, plus a slot for yours, each call Topics, which owns names, partitions, producers, offsets and RecordBatch v2 encoding, and writes through the s3stream engine.">
  <defs>
    <marker id="arrf" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="20" width="150" height="46" class="box"/>
  <text x="95" y="40" text-anchor="middle" class="label">Pico</text>
  <text x="95" y="56" text-anchor="middle" class="sub">HTTP</text>
  <rect x="20" y="76" width="150" height="46" class="box"/>
  <text x="95" y="96" text-anchor="middle" class="label">Durable Streams</text>
  <text x="95" y="112" text-anchor="middle" class="sub">HTTP</text>
  <rect x="20" y="132" width="150" height="46" class="box"/>
  <text x="95" y="152" text-anchor="middle" class="label">Kafka</text>
  <text x="95" y="168" text-anchor="middle" class="sub">TCP</text>
  <rect x="20" y="188" width="150" height="46" fill="none" class="edge-soft" stroke-dasharray="4 4"/>
  <text x="95" y="208" text-anchor="middle" class="label">yours</text>
  <text x="95" y="224" text-anchor="middle" class="sub">parse, encode, errors</text>
  <rect x="270" y="76" width="200" height="102" class="box"/>
  <text x="370" y="112" text-anchor="middle" class="label">Topics</text>
  <text x="370" y="132" text-anchor="middle" class="sub">names, partitions, producers</text>
  <text x="370" y="148" text-anchor="middle" class="sub">offsets, RecordBatch v2</text>
  <rect x="540" y="76" width="160" height="102" class="box-accent"/>
  <text x="620" y="112" text-anchor="middle" class="label">s3stream</text>
  <text x="620" y="132" text-anchor="middle" class="sub">WAL, objects</text>
  <text x="620" y="148" text-anchor="middle" class="sub">caches</text>
  <path d="M170 43 L262 110" class="edge" marker-end="url(#arrf)"/>
  <path d="M170 99 L262 120" class="edge" marker-end="url(#arrf)"/>
  <path d="M170 155 L262 134" class="edge" marker-end="url(#arrf)"/>
  <path d="M170 211 L262 144" class="edge-soft" marker-end="url(#arrf)"/>
  <path d="M470 127 L532 127" class="edge" marker-end="url(#arrf)"/>
</svg>
</div>

## Conventions

A facade takes what it needs from `PicoNode` at startup: `node.topics()`, `node.ownership()`, and `node.groups()` for consumer groups. `picomq_kafka::BrokerContext::new(node)` and `spawn_kafka` in `picomq-runtime/src/lib.rs` are the reference wiring.

- **Topics** are named by path. Any spelling of a name works, see [Topics and streams](/docs/design/streams#names).
- **Partitions** are addressed by index. Calls that take a name and a partition resolve the topic themselves. Calls that take a `&Topic` are for a facade that already loaded it, as Kafka does once per request.
- **Records** are `LogRecord { timestamp_ms, key, value, headers }`, a Kafka record.
- **Positions** are `OffsetToken`: `parse()` treats `None`, `-1` and negatives as the beginning, `value()` renders the padded string HTTP returns, `record_offset()` is the `u64` Kafka wants.
- **Errors** are `ServiceError`. A facade maps its `kind`, see [Errors](#errors).

## Topics

```rust
create(CreateCommand) -> CreateResult
get(name) -> Option<Topic>
list(prefix, start_after, limit) -> TopicList
update(UpdateTopicCommand) -> Topic
grow(name, partitions, owners) -> Topic
close(name) -> CloseResult
delete(name) -> bool
```

`create` is idempotent: `created` is `false` when the topic existed, and an existing topic with different settings is `Conflict`.

| `CreateCommand` field | Meaning |
| --- | --- |
| `name`, `content_type` | Required. `CreateCommand::new(name, ct)` sets defaults for the rest |
| `partitions` | Defaults to `1` |
| `owners` | A node for each partition. Empty lets placement choose |
| `ttl_seconds`, `expires_at_ms` | Expiry. Setting both is `BadRequest` |
| `closed` | Create already sealed |
| `initial_records` | Appended to partition `0` in the same call |
| `schema_name`, `schema_validate` | Bind a [schema](/docs/schemas) and whether appends are checked against it |

`update` changes the schema bind. Its fields are `Option`, so `None` leaves a field alone and `Some(None)` clears the schema. `grow` adds partitions. `close` seals the topic. `list` pages by name and says through `has_more` whether to expose a continuation. Kafka also uses `get_kafka(name)` and `kafka_topics()`.

## Append

```rust
append(name, partition, AppendCommand) -> AppendResult
```

Records in. The service assigns offsets, stamps a monotonic `LogAppendTime`, validates against the bound schema, encodes one RecordBatch v2, and returns once it is durable.

| `AppendCommand` field | Meaning |
| --- | --- |
| `records` | Empty together with `close_after` seals without writing |
| `content_type` | Checked against the topic's. A mismatch is `Conflict`. `None` skips the check |
| `match_seq` | Conditional append. `MatchFailed` unless the tail is exactly this offset |
| `producer` | Idempotent producer: string id, epoch, seq. A repeated seq is acknowledged without writing, a stale epoch is `Fenced`, a gap is `SequenceGap` |
| `stream_seq` | Durable Streams' client sequence. Lexically compared, must advance, else `Conflict`. Other facades leave it `None` |
| `close_after` | Seal once durable |

```rust
submit_batch(&topic, partition, payload) -> SubmittedBatchAppend
finish_batch(SubmittedBatchAppend) -> AppendBatchResult
```

RecordBatch v2 bytes in, as a Kafka client produces them. `submit_batch` checks the CRC, rejects transactional and control batches, applies the batch's producer state, patches the base offset, and hands the bytes to the engine. `finish_batch` waits for durability. Kafka produce submits every partition in a request before finishing any of them. The result is `base_offset`, `log_start_offset`, and `duplicate`, in which case `base_offset` is the original.

Both paths write the same format, and either read call reads both.

## Read

```rust
read(name, partition, from, max_bytes, max_records) -> ReadResult
read_batches(&topic, partition, from, max_bytes) -> BatchReadResult
```

`read` decodes records from `from` to the durable tail, with `0` meaning no cap. The result has `records`, the topic's `content_type`, `next_offset` to resume from, `up_to_date` when the tail was reached, and `closed`. `read_batches` returns the stored batches as bytes, for protocols that forward them verbatim, with the watermarks.

```rust
wait_appended(name, partition, from, timeout) -> bool
wait_partition(&topic, partition, from, timeout) -> bool
```

Block until the tail passes `from`, the topic is sealed, or the timeout expires. `true` means there is something to read. Long poll, SSE, and Kafka `fetch.max.wait.ms` sit on these. A node that is stopping ends every wait with `Unavailable`, so the facade answers and the client reconnects elsewhere.

`offsets(&topic, partition)` and `watermarks(&topic, partition)` return a partition's range, and `record_at_or_after` and `record_with_max_timestamp` serve offset lookups by time.

## Trim

```rust
trim(name, partition, new_start_offset) -> u64
delete_records(&topic, partition, offset, timeout) -> u64
```

`trim` advances the start offset and returns it. `delete_records` first waits, up to `timeout`, for the records below `offset` to reach data objects, as Kafka `DeleteRecords` expects.

## Ownership

```rust
ownership.owner_of(&Log) -> Owner
partition_log(&topic, index) -> Log
```

`Owner.local` says whether this node serves the partition. When it does not, `owner_advertised_address` is where the client goes, and `owner_node_id` names the node. HTTP answers `307`, Kafka answers `NOT_LEADER_OR_FOLLOWER`. Creates skip the check. `pico-http/src/route.rs` and `leader_partition` in `pico-kafka` are the two existing translations. The rules are in [Ownership and routing](/docs/design/ownership).

## Errors

`ServiceError` is a `kind` plus structured companions: `next_offset` on `Closed`, `Conflict` and `MatchFailed`, `producer_epoch` on `Fenced`, `expected_seq` and `received_seq` on `SequenceGap`. Map the kind, never parse `message`. The mapping each existing facade uses is in [Protocols](/docs/design/protocols#errors), and a new facade should follow the same retryable split.

## Adding a facade

A crate beside `pico-http` and `pico-kafka` that depends on `picomq-server` and nothing below it.

1. Handlers parse the request, check ownership, call `Topics`, and encode the result.
2. One function maps `ErrorKind` to the wire.
3. In `picomq-runtime`, a flag, a bound socket, and a spawned listener, with the advertised address registered in `NodeConfig::protocol_addresses`.
4. Rows in `pico-runtime/tests/cross_protocol.rs` in both directions.

If a handler needs a call `Topics` does not have, add it there. Anything only one protocol could want stays in the facade. [Open an issue](/docs/contribute) before starting.
