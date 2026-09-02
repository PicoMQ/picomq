# Protocol facades

Reference for `S3StreamService`, the internal API every protocol is built on. Pico, Durable Streams and Kafka are facades over it: parse the wire, call the service, encode the result. The engine, the metadata log and the stored record format are not a facade's concern.

<div class="pico-diagram">
<svg viewBox="0 0 720 250" width="720" role="img" aria-label="Three facades, Pico, Durable Streams and Kafka, plus a slot for yours, each call the one stream service, which owns the registry, producers, offsets and RecordBatch v2 encoding, and writes through the s3stream engine.">
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
  <text x="370" y="112" text-anchor="middle" class="label">S3StreamService</text>
  <text x="370" y="132" text-anchor="middle" class="sub">registry, aliases, producers</text>
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

A facade takes `node.service()` and `node.ownership()` from `PicoNode` at startup. `spawn_kafka` in `picomq-runtime/src/lib.rs` is the reference wiring.

Stream names are paths. A record is `LogRecord { timestamp_ms, key, value, headers }`, a Kafka record. Positions are `OffsetToken`: `parse()` treats `None`, `-1` and negatives as the beginning, `value()` renders the padded string HTTP returns, `record_offset()` is the `u64` Kafka wants.

Every call returns `Result<_, ServiceError>`. The error's `kind` is what a facade maps, see [Errors](#errors).

## Create

```rust
create(CreateCommand) -> CreateResult
```

Idempotent and always served locally. `created` is `false` when the stream existed. An existing stream with a different configuration is `Conflict`.

| Field | Meaning |
| --- | --- |
| `name`, `content_type` | Required. `CreateCommand::new(name, ct)` sets defaults for the rest. |
| `ttl_seconds`, `expires_at_ms` | Retention. Setting both is `BadRequest`. |
| `closed` | Create already sealed. |
| `initial_records` | Appended in the same call. |
| `kafka_topic` | Topic alias. Derived from the name (`/a/b` to `a.b`) when unset and that name is legal and free. |
| `schema_name`, `schema_validate` | Bind a [schema](/docs/schemas) and whether appends are checked against it. |
| `external_id` | A 16-byte id the facade owns, such as a Kafka topic id. Resolved later with `lookup_by_external_id`. |
| `internal` | Allows the reserved `/_sys`, `/_schemas` and `/_streams` prefixes. Never set from client input. |

## Append

```rust
append(AppendCommand) -> AppendResult
```

Records in. The service assigns offsets, stamps a per-stream monotonic `LogAppendTime`, validates against the bound schema, encodes one RecordBatch v2 and returns once it is durable.

| Field | Meaning |
| --- | --- |
| `records` | Empty together with `close_after` closes without writing. |
| `content_type` | Checked against the stream's. A mismatch is `Conflict`. `None` skips the check. |
| `match_seq` | Conditional append. `MatchFailed` unless the tail is exactly this offset. |
| `producer` | Idempotent producer: string id, epoch, seq. A repeated seq is acknowledged without writing, a stale epoch is `Fenced`, a gap is `SequenceGap`. |
| `stream_seq` | Durable Streams' client sequence. Lexically compared, must advance, else `Conflict`. Other facades leave it `None`. |
| `close_after` | Seal once durable. |

The result carries `next_offset`, `timestamp_ms`, `closed`, the echoed producer epoch and seq, and `applied`, which is `false` for a close-only call.

```rust
append_batch(AppendBatchCommand { name, payload }) -> AppendBatchResult
```

RecordBatch v2 bytes in, as a Kafka client produces them. The service checks the CRC, rejects transactional and control batches, applies the batch's numeric producer state for idempotence, patches the base offset and stores the bytes unchanged with their client timestamps. The result is `base_offset`, `log_start_offset` and `duplicate`, in which case `base_offset` is the original.

For pipelining, `submit_batch_append` does the validation and hands the batch to the engine, then `finish_batch_append` awaits durability and publishes the tail. Kafka produce submits every partition in a request before finishing any of them.

Both paths write the same format. A batch from either reads back through either read call.

## Read

```rust
read(name, from: OffsetToken, max_bytes, max_records) -> ReadResult
```

Decoded records from `from` to the durable tail. `0` for either cap means unbounded. `from` beyond the tail is `BadRequest`. The result has `records`, the stream's `content_type`, `next_offset` to resume from, `up_to_date` when the tail was reached, and `closed`. `concatenated_values()` joins the values for a raw body.

```rust
read_batches(name, from: u64, max_bytes) -> BatchReadResult
```

The stored batches as bytes, each with its offset range and count, plus `next_offset`, `high_watermark` and `log_start_offset`. For protocols that forward batches verbatim. `watermarks(name)` returns just the two offsets.

```rust
wait_appended(name, from: OffsetToken, timeout) -> bool
```

Blocks until the tail passes `from`, the stream closes, or the timeout expires. `true` means there is something to read. This is what long poll, SSE and Kafka `fetch.max.wait.ms` sit on. The facade owns the client-visible timeout.

## Inspect

```rust
head(name) -> Option<StreamMeta>
describe(name) -> Option<StreamMeta>
```

Same result, different cost. `head` opens the stream and reports the live tail, for a request about one stream. `describe` reads the committed view without opening, for listing and cluster description. `StreamMeta` has the name, content type, retention, start and next offsets, closed flag, external id, schema and topic alias.

```rust
list(prefix, start_after, limit) -> StreamList
```

Paged by name. `has_more` says whether to expose a continuation.

```rust
stream_config(name) -> Option<StreamConfig>
update_stream(UpdateStreamCommand) -> StreamConfig
```

Schema binding, validation flag and topic alias. The update fields are `Option<Option<T>>`, so `Some(None)` clears and `None` leaves alone.

## Close, delete, trim

```rust
close(name) -> CloseResult
delete(name) -> bool
trim(name, new_start_offset) -> u64
```

Close seals the stream and returns `next_offset`. Delete returns whether anything existed. Trim advances the start offset and returns it.

## Names

```rust
lookup_by_topic(topic) -> Option<String>
lookup_by_external_id(id: [u8; 16]) -> Option<String>
lookup_stream_id(name) -> Option<u64>
list_topics() -> Vec<(String, String)>
```

One topic alias per stream, indexed both ways. `list_topics` reads the committed view and does no I/O. `picomq_server::alias` has the name rules: `is_valid_topic`, `derive_topic`, `stream_name_for_topic`. Resolve to the stream name once at the top of a handler and use that from there on.

## Ownership

```rust
ownership.owner_of(name) -> Owner
```

`local` says whether this node serves the stream. When it does not, `owner_advertised_address` is where to send the client. HTTP answers `307`, Kafka answers `NOT_LEADER_OR_FOLLOWER`. Create skips the check. `pico-http/src/route.rs` and `ensure_local_leader` in `pico-kafka` are the two existing translations.

## Errors

`ServiceError` is a `kind` plus structured companions: `next_offset` on `Closed`, `Conflict` and `MatchFailed`, `producer_epoch` on `Fenced`, `expected_seq` and `received_seq` on `SequenceGap`. Map the kind. Never parse `message`.

| `ErrorKind` | Pico | Durable Streams | Kafka |
| --- | --- | --- | --- |
| `NotFound` | `404` | `404` | `UNKNOWN_TOPIC_OR_PARTITION` |
| `Conflict` | `409` | `409` | `TOPIC_ALREADY_EXISTS` |
| `Closed` | `409` | `409` | `POLICY_VIOLATION` |
| `BadRequest` | `400` | `400` | `INVALID_REQUEST` |
| `CorruptBatch` | `400` | `400` | `CORRUPT_MESSAGE` |
| `SchemaViolation` | `400` | `400` | `INVALID_RECORD` |
| `Fenced` | `403` | `403` | `INVALID_PRODUCER_EPOCH` |
| `SequenceGap` | `409` | `409` | `OUT_OF_ORDER_SEQUENCE_NUMBER` |
| `MatchFailed` | `412` | `400` | not reachable |
| `Durability` | `500` | `500` | `KAFKA_STORAGE_ERROR` |

## Schemas

```rust
put_schema(name, format, bytes)
get_schema(name) -> Option<(SchemaFormat, Bytes)>
delete_schema(name) -> bool
validation_schema_of(name) -> Option<String>
```

Only needed if the facade exposes schema management. Appends validate on their own. `schema_registry()` is `None` when the registry is off.

## Adding a facade

A crate beside `pico-http` and `pico-kafka` that depends on `picomq-server` and nothing below it. Handlers parse, resolve the stream name, check ownership, call the service, encode. One function maps `ErrorKind` to the wire. In `picomq-runtime`, a flag, a bound socket and a spawned listener, with the advertised address registered in `NodeConfig::protocol_addresses`. Rows in `pico-runtime/tests/cross_protocol.rs` in both directions.

If a handler needs a service method that does not exist, add it to `S3StreamService`. Anything only one protocol could want stays in the facade. [Open an issue](/docs/contribute) before starting.
