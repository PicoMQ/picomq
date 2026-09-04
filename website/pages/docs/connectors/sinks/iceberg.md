# Iceberg sink

Appends each batch of JSON records to one or more Apache Iceberg tables through a REST catalog. Records are converted to Arrow against the table's current schema, written as Parquet data files on S3, and committed as one fast-append snapshot per batch. Tables are never created. Every table the sink writes to has to exist in the catalog with its schema and partition spec already defined.

Two routing modes are available. Static routing writes every batch to the tables listed in `tables`, which can be templated per topic. Dynamic routing reads a field from each record and treats its value as the `namespace.table` to write to.

| | |
| --- | --- |
| Type | Sink |
| Library | `libpicomq_connector_iceberg_sink` |
| Ships in | Released as a separate `.so` artifact, see [Operations](/docs/operations/connectors#the-image) |
| Destination | Existing Iceberg tables, templated per topic or chosen per record |
| Creates destination | No |
| On replay | Appended data files repeat rows |
| Payload | `json` only |

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="Records from a topic are batched by the runtime, routed to one or more Iceberg tables by template or by a record field, converted to Arrow and written as Parquet data files, one per partition value, and committed to the REST catalog as a fast append.">
  <defs>
    <marker id="arrice" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="130" height="56" class="box-accent"/>
  <text x="85" y="104" text-anchor="middle" class="label">events.eu</text>
  <text x="85" y="122" text-anchor="middle" class="sub">batch of 1000</text>
  <rect x="190" y="80" width="150" height="56" class="box"/>
  <text x="265" y="104" text-anchor="middle" class="label">route</text>
  <text x="265" y="122" text-anchor="middle" class="sub">template or field</text>
  <rect x="380" y="80" width="150" height="56" class="box"/>
  <text x="455" y="104" text-anchor="middle" class="label">Parquet files</text>
  <text x="455" y="122" text-anchor="middle" class="sub">one per partition</text>
  <rect x="570" y="80" width="140" height="56" class="box-accent"/>
  <text x="640" y="104" text-anchor="middle" class="label">fast append</text>
  <text x="640" y="122" text-anchor="middle" class="sub">REST commit</text>
  <path d="M150 108 L182 108" class="edge" marker-end="url(#arrice)"/>
  <path d="M340 108 L372 108" class="edge" marker-end="url(#arrice)"/>
  <path d="M530 108 L562 108" class="edge" marker-end="url(#arrice)"/>
  <text x="360" y="160" text-anchor="middle" class="sub">uncommitted files are deleted when a write fails</text>
</svg>
</div>

## Quick start

```toml
type = "sink"
key = "events_iceberg"
enabled = true
version = 0
name = "Events to Iceberg"
path = "libpicomq_connector_iceberg_sink"

[[topics]]
pattern = "events\\..*"
schema = "json"
batch_length = 1000
poll_interval = "100ms"

[plugin_config]
tables = ["analytics.events_{topic_segment[-1]}"]
catalog_type = "rest"
uri = "http://iceberg-rest:8181"
warehouse = "s3://lake/warehouse"
store_class = "s3"
store_url = "http://minio:9000"
store_region = "us-east-1"
store_access_key_id = "minio"
store_secret_access_key = "minio123"
store_path_style_access = true
```

Keep the credentials out of the file with environment overrides.

```bash
PICOMQ_CONNECTORS_SINK_EVENTS_ICEBERG_PLUGIN_CONFIG_STORE_ACCESS_KEY_ID=minio
PICOMQ_CONNECTORS_SINK_EVENTS_ICEBERG_PLUGIN_CONFIG_STORE_SECRET_ACCESS_KEY=secret
```

## How it works

On `open()` the sink checks that `store_access_key_id` and `store_secret_access_key` are either both set or both absent, builds the S3 file IO from `store_url`, `store_region` and `store_path_style_access`, and loads a REST catalog from `uri` and `warehouse`. It then builds the router for the configured mode. In static mode every literal entry in `tables` is loaded from the catalog at this point. An entry that is not of the form `namespace.table` is logged and dropped, and an entry the catalog does not know is logged and skipped. If nothing usable is left, `open()` fails.

For each batch the runtime hands over, the sink does the following.

1. Resolves the target tables. Static mode resolves every template in `tables` against the topic and loads any table it has not cached yet. Dynamic mode groups the records by the value of `dynamic_route_field`.
2. Keeps the JSON payloads. A record with any other payload type is logged and dropped from the batch. A batch with no JSON payload at all fails with `Invalid payload type`.
3. Converts the records to Arrow using the table's current Iceberg schema. Fields the table does not have are ignored, and a value that does not fit its column fails the batch.
4. Writes Parquet data files under the table location. A partitioned table gets one writer per partition value in the batch, an unpartitioned table gets one writer.
5. Commits the data files to the catalog as a fast-append snapshot. In static mode this is repeated for every resolved table, so a batch routed to two tables lands in both.
6. Returns an error to the runtime if any step fails. In static mode the table is also evicted from the cache so the next attempt reloads its metadata.

The sink has no retry loop of its own. A failed batch is returned to the runtime, which retries it with the offset unmoved. When the Parquet write fails, the data files written so far are deleted from the store before the error is returned. When the catalog rejects the commit, the files stay in the store as orphans.

### Static routing

`tables` is a list of [destination templates](/docs/connectors/routing#destination-templates-on-sinks). Each resolves to `namespace.table`, where the namespace can itself be dotted, so `lake.raw.events` is a namespace `lake.raw` with a table `events`. A resolved name with no dot fails the batch with `Resolved table '...' has no namespace`, and one the catalog does not know fails it with `Cannot store data: Table '...' does not exist in the catalog`.

Every template in the list is applied to every batch. Two entries mean each record is written twice, once per table.

### Dynamic routing

With `dynamic_routing = true` the sink ignores `tables` and the topic name. For each record it reads the top-level field named by `dynamic_route_field`, converts the value to a string and uses it as the `namespace.table` to write to. Records for the same table are written together in one commit.

::: warning Records without a valid route are dropped
In dynamic mode a record whose route field is missing, whose value is not `namespace.table`, or whose table is not in the catalog is skipped without failing the batch. The sink logs a warning for a malformed name and nothing at all for a missing field or table. The runtime commits the offset and the record is gone.
:::

Dynamic mode looks the table up in the catalog on every batch. Tables are committed one after another, so a failure on the second table after a successful commit on the first returns an error, and the runtime's retry appends the first table's rows again.

## Configuration

All keys go under `[plugin_config]`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `tables` | list of template | required | Tables to write to in static mode, each `namespace.table`. Supports `{topic}` and `{topic_segment[n]}`, see [templating](/docs/connectors/routing#destination-templates-on-sinks). Ignored, but still required, in dynamic mode. `[]` is accepted there |
| `catalog_type` | string | required | Only `rest` |
| `uri` | string | required | Base URL of the REST catalog |
| `warehouse` | string | required | Warehouse location passed to the catalog |
| `dynamic_routing` | bool | `false` | Choose the table per record from `dynamic_route_field` |
| `dynamic_route_field` | string | `""` | Top-level JSON field holding `namespace.table`. Needed when `dynamic_routing` is on |
| `store_class` | string | required | Object store for data files. Only `s3` works. `fs`, `gcs`, `azdls` and `oss` parse but fail `open()` |
| `store_url` | string | required | S3 endpoint, `s3.endpoint` in Iceberg terms |
| `store_region` | string | required | S3 region |
| `store_access_key_id` | string | none | Static credentials. Set together with `store_secret_access_key` or omit both to use the default credential chain. Redacted in the API |
| `store_secret_access_key` | string | none | See above. Redacted in the API |
| `store_path_style_access` | bool | `true` | Path-style S3 addressing, needed by MinIO and most self-hosted stores. Set `false` for virtual-hosted buckets |

Setting only one of the two credential keys fails `open()` with `Partially configured credentials`.

## What lands in the table

Each batch becomes one snapshot on each target table. The snapshot adds Parquet data files under the table's `data/` location, in partition directories when the table is partitioned.

```text
s3://lake/warehouse/analytics/events_eu/data/region=eu/00000-0-3f1c...-1.parquet
s3://lake/warehouse/analytics/events_eu/data/region=us/00000-0-3f1c...-1.parquet
s3://lake/warehouse/analytics/events_eu/metadata/snap-8231...avro
```

| Content | Source |
| --- | --- |
| Columns | The table's current schema. Each record's top-level JSON fields are matched to columns by name |
| Partition values | Computed from the record with the table's partition spec, including transforms such as `day(ts)` or `bucket(16, id)` |
| Rows per file | One rolling writer per partition value, split at the default target file size |
| Metadata | None. No `pico_topic`, `pico_offset` or similar columns are added |

Keys and headers are not stored. Add them to the payload with a [transform](/docs/connectors/transforms) and a matching column if they are needed.

### Table names

The resolved template is split on `.` and used as the catalog identifier verbatim, without quoting or rewriting. A topic named `events-eu` with a template of `analytics.{topic}` looks for a table literally called `events-eu` in namespace `analytics`. Either create the table with that name or use `{topic_segment[-1]}` with dotted topics so only the clean tail is used.

## Replay

The runtime redelivers a batch after a crash between the commit and the offset commit. See [Delivery guarantees](/docs/connectors/delivery).

| Configuration | Result of a replayed batch |
| --- | --- |
| Any | A second snapshot with the same rows. Iceberg has no key to collide on |

Iceberg cannot deduplicate on the way in. Downstream, a `MERGE INTO` keyed on a record id, or a query that deduplicates by that id, hides the repeats. Give records a stable id in the payload before they reach the sink.

## Requirements

- An Iceberg REST catalog reachable from the runtime at `uri`.
- The target tables created in the catalog with their schema and partition spec. The sink does not create or alter tables.
- An S3 or S3-compatible object store holding the warehouse. Credentials need read and write on the table locations, and list for the metadata directory.
- Topics with `schema = "json"`. Every record is a JSON object whose field names match the table's columns.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `Partially configured credentials` at start | Only one of `store_access_key_id` and `store_secret_access_key` is set |
| `Invalid config` at start with no other detail | `store_class` is not `s3`, or static mode found no usable table |
| `Failed to initialize REST catalog` at start | `uri` is wrong, the catalog is down, or `warehouse` is not one it knows |
| `No valid tables found. Can't initiate Iceberg connector` at start | Every entry in `tables` is a literal that is malformed or missing from the catalog. The preceding `Declared table ... doesn't exist in the configured catalog` lines name them |
| `Cannot store data: Table '...' resolved from template ... does not exist in the catalog` | A templated name resolved to a table nobody created. Create it or change the template |
| `Batch of N messages has no JSON payloads, the Iceberg sink requires schema = json` | The topic is not `schema = "json"` |
| `Schema mismatch` or `Invalid record value` on every batch | A field's JSON type does not fit its Iceberg column. Check nested types and required columns |
| `Catalog commit error` | The catalog rejected the snapshot, usually a concurrent writer on the same table. The runtime retries the batch |
| Records vanish in dynamic mode | The route field is missing, malformed, or names a table that is not in the catalog. Nothing is logged for the first and last case |
