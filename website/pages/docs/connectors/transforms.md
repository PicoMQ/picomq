# Transforms

A transform changes a record between the runtime decoding it and the plugin receiving it. Transforms are part of the runtime, not the plugin, and the same eight are available to every connector.

<div class="pico-diagram">
<svg viewBox="0 30 720 230" width="720" role="img" aria-label="On a sink, transforms sit between decode and the plugin's consume. On a source, they sit between the plugin's poll and the router, so a field a transform adds can be the field a routing rule reads.">
  <defs>
    <marker id="arrtr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="20" y="60" class="label">sink</text>
  <rect x="20" y="72" width="120" height="44" class="box"/>
  <text x="80" y="99" text-anchor="middle" class="label">fetch + decode</text>
  <rect x="220" y="72" width="140" height="44" class="box-accent"/>
  <text x="290" y="99" text-anchor="middle" class="label">transforms</text>
  <rect x="440" y="72" width="120" height="44" class="box"/>
  <text x="500" y="99" text-anchor="middle" class="label">consume</text>
  <path d="M140 94 L212 94" class="edge" marker-end="url(#arrtr)"/>
  <path d="M360 94 L432 94" class="edge" marker-end="url(#arrtr)"/>
  <text x="20" y="170" class="label">source</text>
  <rect x="20" y="182" width="120" height="44" class="box"/>
  <text x="80" y="209" text-anchor="middle" class="label">poll + decode</text>
  <rect x="220" y="182" width="140" height="44" class="box-accent"/>
  <text x="290" y="209" text-anchor="middle" class="label">transforms</text>
  <rect x="440" y="182" width="140" height="44" class="box"/>
  <text x="510" y="209" text-anchor="middle" class="label">route + produce</text>
  <path d="M140 204 L212 204" class="edge" marker-end="url(#arrtr)"/>
  <path d="M360 204 L432 204" class="edge" marker-end="url(#arrtr)"/>
  <text x="290" y="250" text-anchor="middle" class="sub">fields added here can be routed on</text>
</svg>
</div>

Transforms are declared under `[transforms]` in a connector definition, one table per type, each with an `enabled` flag that defaults to on. A definition without a `[transforms]` table passes records through untouched.

```toml
[transforms.unwrap_envelope]
field = "payload"

[transforms.add_fields]
fields = [
  { key = "ingested_at", value = { computed = "date_time" } },
  { key = "source", value = { static = "orders-cdc" } },
]

[transforms.delete_fields]
fields = ["password", "ssn"]
```

::: warning Ordering
Each transform type appears at most once, and the order in which different types are applied is not defined. Combine transforms whose results do not depend on order, or chain two connectors when they do.
:::

## The eight

| Transform | Does | Acts on |
| --- | --- | --- |
| `add_fields` | Inserts fields that are not already present | JSON object |
| `update_fields` | Sets fields, with a condition | JSON object |
| `delete_fields` | Removes named fields | JSON object |
| `filter_fields` | Keeps or drops fields by key and value patterns | JSON object |
| `unwrap_envelope` | Replaces the payload with one of its fields | JSON object |
| `proto_convert` | Protobuf to JSON or back | Payload encoding |
| `flatbuffer_convert` | FlatBuffers to JSON or back | Payload encoding |
| `avro_convert` | Avro to JSON or back | Payload encoding |

The five field transforms operate on the top level of a JSON object. A `raw` or `text` schema, or a JSON payload that is an array or scalar, passes through them unchanged.

## Field values

`add_fields` and `update_fields` take the same value forms.

| Form | Example | Produces |
| --- | --- | --- |
| `static` | `{ static = "orders" }` | Any JSON literal: string, number, boolean, object, array |
| `computed = "date_time"` | | RFC 3339 timestamp |
| `computed = "timestamp_seconds"` | | Current time as an integer |
| `computed = "timestamp_millis"` | | Same, milliseconds |
| `computed = "timestamp_micros"` | | Same, microseconds |
| `computed = "timestamp_nanos"` | | Same, nanoseconds |
| `computed = "uuid_v4"` | | A fresh identifier per record |

`update_fields` adds a `condition` per field.

| `condition` | Effect |
| --- | --- |
| `always` | Overwrite. The default |
| `key_exists` | Only change fields that are present |
| `key_not_exists` | Only add fields that are absent. Same as `add_fields` |

```toml
[transforms.update_fields]
fields = [
  { key = "status", value = { static = "processed" }, condition = "key_exists" },
  { key = "version", value = { static = 2 }, condition = "always" },
]
```

## Filtering fields

`filter_fields` is the general form. `keep_fields` are always retained. Every other field is tested against `patterns`.

- Matched and `include_matching = true`: kept.
- Matched and `include_matching = false`: dropped.
- Unmatched: the opposite in each case.

A pattern has an optional `key_pattern` and an optional `value_pattern`. Both must match when both are given.

| `key_pattern` | Matches when the key |
| --- | --- |
| `exact` | equals the string |
| `starts_with`, `ends_with`, `contains` | has the string in that position |
| `regex` | matches the expression |

| `value_pattern` | Matches when the value |
| --- | --- |
| `equals` | is that JSON value |
| `contains` | is a string containing the substring |
| `regex` | is a string matching the expression |
| `greater_than`, `less_than`, `between` | is a number in that range |
| `is_null`, `is_not_null` | is or is not `null` |
| `is_string`, `is_number`, `is_boolean`, `is_object`, `is_array` | has that type |

```toml
[transforms.filter_fields]
keep_fields = ["id"]
include_matching = false
patterns = [
  { key_pattern = { starts_with = "_" } },
  { key_pattern = { regex = "^tmp_" }, value_pattern = { is_null = true } },
]
```

This keeps `id`, drops every field starting with `_`, and drops any `tmp_*` field whose value is `null`.

## Unwrapping envelopes

`unwrap_envelope` replaces the payload with the value of one field. The usual case is a CDC or webhook envelope whose interesting content is under `payload` or `after`.

<div class="pico-diagram">
<svg viewBox="0 30 720 150" width="720" role="img" aria-label="An envelope object with op, ts and after fields is unwrapped on the after field, and the value of after becomes the whole record.">
  <defs>
    <marker id="arrun" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="50" width="260" height="110" class="box"/>
  <text x="40" y="74" class="label">{</text>
  <text x="60" y="94" class="sub">"op": "u", "ts": 1767225600,</text>
  <text x="60" y="114" class="sub">"after": { "id": 7 }</text>
  <text x="40" y="140" class="label">}</text>
  <rect x="312" y="80" width="136" height="48" class="box-accent"/>
  <text x="380" y="101" text-anchor="middle" class="label">unwrap_envelope</text>
  <text x="380" y="118" text-anchor="middle" class="sub">field = "after"</text>
  <rect x="480" y="76" width="220" height="56" class="box"/>
  <text x="590" y="108" text-anchor="middle" class="sub">{ "id": 7 }</text>
  <path d="M280 104 L304 104" class="edge" marker-end="url(#arrun)"/>
  <path d="M448 104 L472 104" class="edge" marker-end="url(#arrun)"/>
</svg>
</div>

The field's value becomes the whole record, whatever its type. A record without the field passes through unchanged.

## Format conversions

`proto_convert`, `flatbuffer_convert` and `avro_convert` change the encoding of the payload rather than its content. Each has a `source_format` and a `target_format`, one of which is `json` and the other the binary format, plus a way to find the schema.

| Transform | Schema from |
| --- | --- |
| `proto_convert` | `schema_path` to a `.proto` with `include_paths` for imports and `message_type` for the root, or `descriptor_set` bytes, or `schema_registry_url` |
| `flatbuffer_convert` | `schema_path` to an `.fbs`, `include_paths`, `root_table_name` |
| `avro_convert` | `schema_path`, or inline `schema_json` |

All three share the following.

| Option | Effect |
| --- | --- |
| `field_mappings` | A table renaming fields on the way through |
| `conversion_options.strict_mode` | Fail on fields the schema does not know rather than dropping them |
| `conversion_options.pretty_json` | Readable output when converting to JSON |
| `conversion_options.include_metadata` | Add the record's topic and offset to the result |

```toml
[transforms.proto_convert]
source_format = "proto"
target_format = "json"
schema_path = "/etc/picomq-connectors/schemas/user.proto"
message_type = "example.User"

[transforms.proto_convert.field_mappings]
userId = "user_id"
```

A conversion changes the schema of what the plugin sees. A sink declared with `schema = "proto"` and a `proto_convert` to JSON hands its plugin JSON records, so the plugin configuration should agree, `payload_format = "json"` for Postgres for instance.

## Where transforms run

- Inside the runtime, on the batch, between decode and the plugin.
- None of the shipped transforms drops a record or splits one into several. They filter fields, not messages. Record-level filtering belongs in the source query or the sink plugin.
- They hold no state and call out to nothing, which keeps them cheap enough to run on every record.

The transform configuration a connector is running with is visible at `GET /sinks/{key}/transforms` and `GET /sources/{key}/transforms` on the runtime's HTTP API.
