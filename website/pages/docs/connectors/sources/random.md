# Random source

Generates JSON records on a timer. It exists to exercise the rest of the pipeline: routing rules, sinks, transforms, and the crash and replay behaviour, without an external system to set up. Every record carries a monotonic `sequence` and a `user_id` drawn from a small pool, which makes it a convenient feed for fan-out demos and a strict check for gaps or duplicates.

| | |
| --- | --- |
| Type | Source |
| Library | `libpicomq_connector_random_source` |
| Ships in | The `pico-connectors` image |
| Modes | One, timed generation |
| Output schema | `json` |
| State | Count of records produced |
| On replay | The same `sequence` numbers are generated again with fresh `id` and `text` |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Every interval the random source generates a batch of records numbered from the committed count, hands it to the runtime, and on ack commits the new count.">
  <defs>
    <marker id="arrrnd" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box"/>
  <text x="85" y="104" text-anchor="middle" class="label">sleep</text>
  <text x="85" y="122" text-anchor="middle" class="sub">interval</text>
  <rect x="210" y="80" width="150" height="56" class="box-accent"/>
  <text x="285" y="104" text-anchor="middle" class="label">generate</text>
  <text x="285" y="122" text-anchor="middle" class="sub">seq n .. n+k</text>
  <rect x="420" y="80" width="130" height="56" class="box"/>
  <text x="485" y="104" text-anchor="middle" class="label">produce</text>
  <text x="485" y="122" text-anchor="middle" class="sub">runtime</text>
  <rect x="610" y="80" width="90" height="56" class="box-accent"/>
  <text x="655" y="104" text-anchor="middle" class="label">ack</text>
  <text x="655" y="122" text-anchor="middle" class="sub">n = n+k</text>
  <path d="M150 108 L202 108" class="edge" marker-end="url(#arrrnd)"/>
  <path d="M360 108 L412 108" class="edge" marker-end="url(#arrrnd)"/>
  <path d="M550 108 L602 108" class="edge" marker-end="url(#arrrnd)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">k is drawn from messages_range each poll</text>
</svg>
</div>

## Quick start

```toml
type = "source"
key = "random"
enabled = true
version = 0
name = "Random source"
path = "libpicomq_connector_random_source"

[[topics]]
topic = { strategy = "field", path = "user_id", template = "{value}" }
schema = "json"
batch_length = 100
linger_time = "5ms"
create_topics = true

[plugin_config]
interval = "1s"
messages_range = [5, 20]
payload_size = 64
user_pool = 4
```

This is the definition the [first connector](/docs/connectors/first-connector) walk-through uses. It fans out into `user-0` through `user-3`.

## How it works

`open()` does nothing beyond logging the settings. Each `poll()` then does the following.

1. Sleeps `interval`.
2. Reads the committed count `n`. If `max_count` is set and `n` has reached it, returns an empty batch and stages nothing.
3. Draws a batch size `k` from `messages_range`, capped at whatever remains under `max_count`.
4. Generates records numbered `n` to `n + k - 1`.
5. Stages `n + k` as the candidate state and returns the batch with that state attached.

On `Ack` the candidate becomes the committed count. On `Nack` it is dropped, and the next poll generates from `n` again.

## Configuration

All keys go under `[plugin_config]`. Every key is optional.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `interval` | duration | `1s` | Sleep before each batch. An unparseable value falls back to `1s` |
| `messages_range` | `[min, max]` | `[10, 50]` | Batch size is drawn uniformly from this range, `max` exclusive |
| `payload_size` | int | `100` | Length of the random `text` field in characters |
| `max_count` | int | none | Stop after this many records. The source then returns empty batches forever |
| `user_pool` | int | `10` | Number of distinct `user_id` values. `sequence % user_pool` picks one. Values below 1 become 1 |
| `key_by_user` | bool | `false` | Set the record key to the `user_id` bytes, for `strategy = "key"` routing |

## Output

```json
{
  "id": "7f1a1c8e-3d4b-4a0e-9f0c-2b6c1d5e8a90",
  "sequence": 1042,
  "user_id": "user-2",
  "title": "Hello",
  "name": "World",
  "text": "kQ2n8ZpL0vX3..."
}
```

| Field | Content |
| --- | --- |
| `id` | Fresh UUID v4 on every generation |
| `sequence` | Position in the stream, starting at 0 and continuing across restarts |
| `user_id` | `user-<sequence mod user_pool>` |
| `title`, `name` | Fixed strings |
| `text` | `payload_size` random alphanumeric characters |

Records have no headers and no timestamp of their own, so the runtime stamps them at produce time. The key is unset unless `key_by_user` is on.

## State

| Stored in the runtime's state store | Stored anywhere else |
| --- | --- |
| The count of records produced | Nothing |

Losing the state file restarts `sequence` at 0. With `max_count` set, that also restarts the countdown.

## Using it as a probe

The `sequence` field is what makes this source useful beyond demos.

- A sink that sees a gap in `sequence` has lost data somewhere, which should never happen.
- A sink that sees a repeated `sequence` has observed a replay, which is expected after a crash and should be absorbed by the sink's idempotency.
- `id` differs between the two copies of a replayed record, so a sink that keys on `id` instead of `topic:partition:offset` will show the duplicate. That is a useful way to check which identity a sink actually uses.

## Requirements

None.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| No records after the first few | `max_count` was reached. Raise it or remove it |
| Every batch is one size | `messages_range` has `max = min + 1` |
| All records land in one topic | `user_pool = 1`, or the routing rule is not reading `user_id` |
| `sequence` restarts at 0 after a restart | The state volume was not mounted, or the connector `key` changed |
