# Schemas

A schema is a named resource registered once and shared: any number of streams bind to it by name at create time. Writes to a bound stream are validated against the schema before they are acknowledged. Unbound streams are untouched, and reads always return the stored bytes, so decoding stays with the client.

::: info Note
The schema registry is built into Pico so you do not need a separate external registry. Register a schema and bind it to a stream so clients can discover the payload model.

Broker-side validation (`schemaValidate`) adds CPU cost and reduces write throughput. Prefer validating in producers and consumers. Use the registry as discovery metadata, not as an enforcement path.
:::

Schemas are opt-in. A node started without `--schema-registry` validates nothing.

<div class="pico-diagram">
<svg viewBox="0 30 700 190" width="700" role="img" aria-label="Register a schema, bind it on stream create, validate on every write.">
  <defs>
    <marker id="arrsch" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="70" width="180" height="70" class="box"/>
  <text x="110" y="100" text-anchor="middle" class="label">register</text>
  <text x="110" y="118" text-anchor="middle" class="sub">PUT /_schemas/{name}</text>
  <rect x="260" y="70" width="180" height="70" class="box"/>
  <text x="350" y="100" text-anchor="middle" class="label">bind</text>
  <text x="350" y="118" text-anchor="middle" class="sub">create stream or topic</text>
  <rect x="500" y="70" width="180" height="70" class="box-accent"/>
  <text x="590" y="100" text-anchor="middle" class="label">validate</text>
  <text x="590" y="118" text-anchor="middle" class="sub">every append or produce</text>
  <path d="M200 105 L252 105" class="edge" marker-end="url(#arrsch)"/>
  <path d="M440 105 L492 105" class="edge" marker-end="url(#arrsch)"/>
  <text x="350" y="180" text-anchor="middle" class="sub">one schema, many streams. reads stay opaque bytes</text>
</svg>
</div>

## Enable

`--schema-registry` (`PICO_SCHEMA_REGISTRY`) points at the object store holding the schemas. It takes the same bucket URI form as `--storage`: `{id}@s3://bucket?k=v` with `s3://`, `file://`, and `mem://` backends.

```bash
pico serve --schema-registry 1@s3://schemas?region=us-east-1 \
    --meta-url postgres://user:pass@pg:5432/picomq \
    --storage=-2@s3://data?region=us-east-1
```

A schema named `orders` is the object `orders.proto`, `orders.json`, or `orders.avsc`, and lookup tries the three extensions in that order. The store is plain objects, so schemas can also be placed there directly, with the HTTP API below as the managed write path.

## Register

The schema routes live on the stream listener (`--listen`), in every protocol mode including Kafka. The `/_schemas/` prefix is reserved, so a stream can never collide with them.

| Method and path | What it does |
| --- | --- |
| `PUT /_schemas/{name}` | Store or replace the schema. `204` on success. |
| `GET /_schemas/{name}` | Return the schema with its content type. `404` when absent. |
| `DELETE /_schemas/{name}` | Remove it. `204` on success, `404` when absent. |

The format comes from the request's `Content-Type`, or from an extension on the name.

| Format | Content type | Extension |
| --- | --- | --- |
| JSON Schema | `application/schema+json` | `.json` |
| Avro | `application/avro` | `.avsc` |
| Protobuf | `application/x-protobuf` | `.proto` |

```bash
curl -X PUT http://127.0.0.1:4437/_schemas/person \
  -H 'Content-Type: application/schema+json' \
  -d '{"type":"object","properties":{"value":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}}'
```

A `PUT` rejects a schema that does not parse in its format, and all three routes return `400` on a node running without `--schema-registry`. With `--auth required` they take a bearer token whose scope covers the operation and includes the `admin` audience, exactly like the [admin API](/docs/operations/admin).

## Bind

A stream binds a schema by name as part of its create. The name must exist in the registry or the create fails. Validation is a separate opt-in on the same create, off by default. A bind without it is discovery metadata with no write-path cost.

| Protocol | Bind on create | Validate |
| --- | --- | --- |
| Pico | `Pico-Schema: {name}` on `PUT /{stream}` | `Pico-Schema-Validate: true` |
| Durable Streams | `Stream-Schema: {name}` | `Stream-Schema-Validate: true` |
| Kafka | Topic config `pico.schema={name}` on `CreateTopics` | `pico.schema.validate=true` |

After create, both fields are mutable through the common stream config API on every protocol mode, including Kafka:

| Method and path | What it does |
| --- | --- |
| `GET /_streams/{name}` | Return `{ "schema", "schemaValidate" }`. `404` when the stream is absent. |
| `PATCH /_streams/{name}` | Update either field. Body keys are optional. `"schema": null` clears the bind and turns validation off. |

Kafka topics use the stream `/{topic}`, so `PATCH /_streams/orders` updates topic `orders`. In a cluster these routes follow stream ownership: a request landing on a non-owner node returns a `307` redirect to the owner, like stream reads and writes.

Inspect returns the bound name, in `Pico-Schema` on the Pico protocol and `Stream-Schema` on Durable Streams, so a client can discover which schema to decode with.

## Validation

Every append or produce on a validated stream is checked before the write is acknowledged, and a payload that does not match fails the whole request: `400` on HTTP, `INVALID_RECORD` (error `87`) on Kafka produce. Reads never involve the schema. Fetch and `GET` return the stored bytes whether or not the stream is bound.

A validated stream rejects writes when its schema was deleted from the registry. On a node running without `--schema-registry` the validate flag is ignored and writes pass through. Nodes cache registry reads for 30 seconds.
