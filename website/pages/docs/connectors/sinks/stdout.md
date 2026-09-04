# stdout sink

Writes one log line per batch to the runtime log, and optionally one line per record. It stores nothing, so it is the sink to wire up first when checking that a topic pattern matches, that a schema decodes, or that a transform produces the expected shape.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_stdout_sink` |
| Ships in | The `pico-connectors` image |
| Destination | The runtime log, at `info` |
| Creates destination | Nothing to create |
| On replay | Lines repeat |
| Payload | Any schema. Printed in debug form |

<div class="pico-diagram">
<svg viewBox="0 30 720 140" width="720" role="img" aria-label="Records from a topic are batched by the runtime and handed to the stdout sink, which writes one info log line per batch and, when print_payload is on, one line per record.">
  <defs>
    <marker id="arrstdout" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">user-eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 100</text>
  <rect x="210" y="80" width="150" height="56" class="box"/>
  <text x="285" y="104" text-anchor="middle" class="label">consume</text>
  <text x="285" y="122" text-anchor="middle" class="sub">count invocation</text>
  <rect x="420" y="80" width="280" height="56" class="box-accent"/>
  <text x="560" y="104" text-anchor="middle" class="label">runtime log</text>
  <text x="560" y="122" text-anchor="middle" class="sub">one info line per batch</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrstdout)"/>
  <path d="M360 108 L412 108" class="edge" marker-end="url(#arrstdout)"/>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "debug_stdout"
enabled = true
version = 0
name = "Debug to stdout"
path = "libpicomq_connector_stdout_sink"

[[topics]]
pattern = "user-.*"
schema = "json"
batch_length = 100
poll_interval = "100ms"

[plugin_config]
print_payload = true
```

There is no secret to override.

## How it works

On `open()` the sink logs its id and the `print_payload` setting. It opens no connection and does no validation beyond parsing the configuration.

For each batch the runtime hands over, the sink does the following.

1. Increments an invocation counter held behind a lock.
2. Logs one `info` line with the sink id, the record count, the schema, the topic, the partition, the batch offset and the invocation number.
3. With `print_payload = true`, logs one `info` line per record with its offset, its key and its payload.
4. Returns success. There is nothing that can fail, so there are no retries.

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `print_payload` | bool | `false` | Log every record in the batch, not only the batch summary |

## What lands in the log

Every batch produces a line of this form.

```text
Stdout sink with ID: 3 received: 100 messages, schema: json, topic: user-eu, partition: 0, offset: 4200, invocation: 42
```

With `print_payload = true`, each record adds an entry. The payload is pretty-printed, so one record spans several lines.

```text
Message offset: 4101, key: Some("user-1"), payload: Json(
    Object(
        {
            "id": Static(I64(7)),
            "name": String("Ada"),
        },
    ),
)
```

| Field | Content |
| --- | --- |
| `offset` | Record offset |
| `key` | Record key decoded as UTF-8, invalid bytes replaced, or `None` |
| `payload` | The decoded payload in Rust debug form, so `Json(...)`, `Text(...)`, `Raw(...)` and so on by schema |

The debug form is for eyes, not parsers. Headers are not printed.

## Replay

The runtime redelivers a batch after a crash between the write and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| Any | The batch line and any record lines are logged again |

The invocation counter restarts at `1` after a restart, so it cannot be used to spot a replay.

## Requirements

- Nothing beyond the runtime. The output goes wherever the runtime's log goes, which is the container's stdout by default.
- Log level `info` or lower for the runtime, or the lines are filtered out.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| No lines at all | The runtime log level is above `info`, or the topic pattern matches nothing |
| Batch lines but no record lines | `print_payload` is `false` or missing |
| `payload: Raw([...])` for a topic that should be JSON | The topic's `schema` is `raw`. Set `schema = "json"` on the `[[topics]]` entry |
| Very large log volume | `print_payload = true` on a busy topic. Turn it off, or narrow the topic pattern |
