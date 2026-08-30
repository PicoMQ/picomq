# HTTP API

Reference for the Pico protocol and the admin endpoints. The Kafka listener is covered in [Kafka protocol](/docs/kafka).

OpenAPI spec: <a href="/openapi.yaml" download>openapi.yaml</a>

## Conventions

A stream name is the request path, so `/logs/api/prod` addresses the stream of the same name. Custom headers use the `Pico-` prefix. Sequences are the record offsets described in [Streams](/docs/design/streams), starting at `0` and stable forever.

A request for a stream owned by another node returns `307` with the owner's address in `Location`. Errors are JSON:

```json
{ "error": "conflict", "message": "producer epoch 3 is stale", "next_seq": 42 }
```

`message` and `next_seq` appear when they help. Unknown streams return `404`, malformed requests `400`, and ownership conflicts `409`.

With auth required, every request sends `Authorization: Bearer <token>`. A missing or invalid credential is `401` with a `WWW-Authenticate: Bearer` challenge, and a valid credential with insufficient scope is `403`. Standard HTTP clients drop `Authorization` on cross-origin redirects and ownership redirects cross origins, so clients must follow `307`s themselves and re-attach the credential, which is what `pico-client` and the CLI do. See [Authorization](/docs/design/auth) for the scope model.

## Create

```
PUT /{stream}
```

Creates the stream. Idempotent: `201` when created, `200` when it already exists with the same content type, `409` when the content type differs. A request body becomes the first records.

| Request header | Meaning |
| --- | --- |
| `Content-Type` | Stored as the stream's content type. Defaults to `application/octet-stream`. |
| `Pico-TTL` | Seconds of retention. Records older than this expire. |
| `Pico-Expires-At` | Absolute expiry time for the stream. |
| `Pico-Closed` | `true` creates the stream already sealed. |
| `Pico-Schema` | Binds a registered [schema](/docs/schemas) by name. |
| `Pico-Schema-Validate` | `true` validates appends against the bound schema. Defaults to `false`. |

## Append

```
POST /{stream}
```

Appends records. The body is a single record in the stream's content type, a JSON batch (`application/vnd.picomq.batch+json`), or a binary batch (`application/vnd.picomq.batch`). Returns `200` with the position of the first appended record.

| Request header | Meaning |
| --- | --- |
| `Pico-Producer-Id`, `Pico-Producer-Epoch`, `Pico-Producer-Seq` | Idempotent producer identity. A repeated seq is acknowledged without writing. |
| `Pico-Match-Seq` | Conditional append: succeeds only if the tail is at this sequence. |
| `Pico-Closed` | `true` seals the stream after this append. |
| `Pico-Trim-Seq` | Turns the request into a trim, dropping records below this sequence. No body. |

| Response header | Meaning |
| --- | --- |
| `Pico-Start-Seq` | Sequence of the first record in this append. |
| `Pico-Next-Seq` | The stream's next sequence after the append. |
| `Pico-Timestamp` | Timestamp assigned to the records. |
| `Pico-Expected-Seq`, `Pico-Received-Seq` | On a producer seq mismatch (`409`), what the server expected next to what it got. |

## Read

```
GET /{stream}?seq=0
```

Reads records from a sequence. Without `live` it returns what exists and stops. `seq=now` starts at the tail, and an SSE reconnect can supply `Last-Event-ID` instead of `seq`.

| Query parameter | Meaning |
| --- | --- |
| `seq` | Start position: a number, or `now`. Defaults to `0`. |
| `count`, `bytes` | Caps on records and bytes returned. |
| `format` | `json` (default), `binary` batch, or `raw` concatenated bodies. |
| `live` | `long-poll` waits for the next record, `sse` holds the response open as an event stream. |

| Response header | Meaning |
| --- | --- |
| `Pico-Next-Seq` | Where to resume. Pass it as the next `seq`. |
| `Pico-Up-To-Date` | `true` when the response reaches the tail. |
| `Pico-Closed` | `true` when the stream is sealed and fully read. |
| `Pico-Cursor` | Opaque resume token for paginated catch-up reads. |

Catch-up responses include an `ETag` and honor `If-None-Match` with `304`, so polling readers that are caught up transfer nothing. A long poll that times out with no data returns `204`. SSE delivers each record as an event with its sequence as the event id, and ends with a control event when the stream closes.

## Inspect

```
HEAD /{stream}
```

Returns the stream's metadata in headers with no body: `Pico-Next-Seq`, `Pico-Start-Seq`, `Pico-TTL`, `Pico-Expires-At`, `Pico-Closed`, and `Pico-Schema` when bound. `404` when the stream does not exist.

## Delete

```
DELETE /{stream}
```

Removes the stream and its records. `204` on success, `404` when it does not exist. The name is immediately reusable.

## List

```
GET /?prefix=/logs/&limit=100
```

Lists streams as JSON. `prefix` filters by name prefix, `limit` caps the page, and `start_after` continues after a name from the previous page.

## Schemas

Schema registration (`/_schemas/{name}`) and stream schema config (`GET`/`PATCH /_streams/{name}`) share this listener and are covered in [Schemas](/docs/schemas).

## Admin API

Admin endpoints, the token control plane (`/admin/tokens`), and the dashboard are covered in [Admin API and dashboard](/docs/operations/admin) and [Authentication](/docs/operations/auth). The OpenAPI spec includes them as well.

## Durable Streams

A listener started with `--protocol ds` speaks the Durable Streams open protocol on its exact wire vocabulary, with `Stream-*` and `Producer-*` headers, raw record bodies, and one record per append. Streams created through either protocol are readable through both. Auth rejections on this listener are plain text `401`/`403` with no headers beyond the spec's own, so producer fencing (`403` with `Producer-Epoch`) stays distinguishable from a scope denial.
