# HTTP API

Reference for the Pico protocol on the stream listener. The Kafka listener is covered in [Kafka protocol](/docs/kafka), and the admin listener in [Admin API and dashboard](/docs/operations/admin).

OpenAPI spec: <a href="/openapi.yaml" download>openapi.yaml</a>

## Conventions

- **Names.** The request path is the topic name, so `/logs/api/prod` addresses that topic. Slashes and dots are the same separator, and the `/_` namespace is reserved, see [Topics and streams](/docs/design/streams#names).
- **Encoding.** Paths and query values are percent-decoded once. A value that does not decode to UTF-8 is a `400`, never rewritten into another name.
- **Partitions.** A topic has one partition unless it was created or grown with more. Every request to a topic with several partitions names one with `?partition=N`, and leaving it out is a `400`.
- **Sequences.** Record offsets within a partition, starting at `0` and stable forever.
- **Headers.** Custom headers use the `Pico-` prefix.
- **Redirects.** A request for a partition owned by another node returns `307` with the owner's address in `Location`. Standard HTTP clients drop `Authorization` on cross-origin redirects, so clients follow `307`s themselves and re-attach the credential. A hop must stay on `http` or `https` and never downgrade from `https` to `http`, since the credential rides along, and the clients refuse such a hop as `unsafe_redirect`. `picomq-client`, the Go and TypeScript clients, and the CLI do all of this.
- **Auth.** With auth required, every request sends `Authorization: Bearer <token>`. See [Authorization](/docs/design/auth) for scopes.

## Errors

Errors on stream and group routes are JSON with a code:

```json
{ "error": "conflict", "message": "producer epoch 3 is stale", "next_seq": 42 }
```

`message` and `next_seq` appear when they help. `next_seq` is also sent as `Pico-Next-Seq`.

| Status | `error` | Meaning |
| --- | --- | --- |
| `400` | `bad_request`, `schema_violation` | Malformed request, or records the bound schema rejects |
| `401` | `unauthenticated` | No valid credential, with a `WWW-Authenticate: Bearer` challenge |
| `403` | `permission_denied` | Insufficient scope |
| `403` | `fenced` | Stale producer epoch, with the current one in `Pico-Producer-Epoch` |
| `404` | `not_found` | No such topic, partition, or group |
| `405` | `method_not_allowed` | A method this protocol does not serve |
| `409` | `conflict` | Create with different settings |
| `409` | `sequence_gap` | Producer sequence gap, with `Pico-Expected-Seq` and `Pico-Received-Seq` |
| `409` | `closed` | The topic is sealed, with `Pico-Closed: true` |
| `412` | `match_failed` | `Pico-Match-Seq` did not match the tail |
| `413` | | Body larger than `--max-request-size` |
| `500` | `durability`, `internal` | The WAL write failed, or an unexpected error |
| `503` | `unavailable` | The partition is moving between nodes, the node is stopping, or metadata is unreachable. Retry |
| `504` | `timeout` | The operation timed out. Retry |

Retrying `503` and `504` is safe. How the same failures look over Durable Streams and Kafka is in [Protocols](/docs/design/protocols#errors).

The config and schema routes, `/_streams` and `/_schemas`, are a control surface and answer errors as `{"error": "<message>"}`, like the admin API.

## Topic settings

A topic's settings are given as headers on create. Some can change later through the [topic config](#topic-config) route, and the rest are fixed for the topic's life.

| Setting | Create header | Config field | After create |
| --- | --- | --- | --- |
| Content type | `Content-Type`, default `application/octet-stream` | | Fixed |
| Partitions | `Pico-Partitions`, default `1` | `partitions` | Grows, never shrinks, by at most `10000` at a time |
| Expiry | `Pico-TTL`, seconds without appends or reads, or `Pico-Expires-At`, a fixed time. Not both | | Fixed |
| [Retention](/docs/design/streams#retention) | `Pico-Retention-Ms`, `Pico-Retention-Bytes`, unlimited by default | `retentionMs`, `retentionBytes` | Set, or removed with `null` |
| [Schema](/docs/schemas#bind) | `Pico-Schema` names a registered schema, `Pico-Schema-Validate: true` validates appends | `schema`, `schemaValidate` | Rebound, or unbound with `null` |
| Sealed | `Pico-Closed: true` | | Sealed by an append with `Pico-Closed`, never reopened |
| Kafka name | Follows from the topic name | `kafkaTopic` | Fixed |

Limits must be positive, and a partition count of `0` is a `400`.

## Create

```
PUT /{topic}
```

Creates a topic with the [settings](#topic-settings) in its headers, with no body. `201` when created, `200` when it already exists with the same settings, `409` when the settings differ. The response carries the same headers as [Inspect](#inspect).

## Append

```
POST /{topic}
```

Appends records. The body is a single record in the topic's content type, a JSON batch (`application/vnd.picomq.batch+json`), or a binary batch (`application/vnd.picomq.batch`). Returns `200`.

| Request header | Meaning |
| --- | --- |
| `Pico-Key` | Record key for a single-record body. Batch bodies carry keys inline |
| `Pico-Producer-Id`, `Pico-Producer-Epoch`, `Pico-Producer-Seq` | Idempotent producer identity. A repeated seq is acknowledged without writing |
| `Pico-Match-Seq` | Conditional append: succeeds only if the tail is at this sequence |
| `Pico-Closed` | `true` seals the topic after this append. With no body, it only seals |
| `Pico-Trim-Seq` | Turns the request into a trim, dropping records below this sequence. No body |

| Response header | Meaning |
| --- | --- |
| `Pico-Start-Seq` | Sequence of the first record in this append, or the new start after a trim |
| `Pico-Next-Seq` | The partition's next sequence after the append |
| `Pico-Timestamp` | Timestamp assigned to the records |
| `Pico-Producer-Epoch`, `Pico-Producer-Seq` | The producer state the append was accepted at |

## Read

```
GET /{topic}?seq=0
```

Reads records from a sequence. Without `live` it returns what exists and stops. `seq=now` starts at the tail, and an SSE reconnect can supply `Last-Event-ID` instead of `seq`.

| Query parameter | Meaning |
| --- | --- |
| `seq` | Start position: a number, or `now`. Defaults to `0` |
| `count`, `bytes` | Caps on records and bytes returned |
| `format` | `json` (default), `binary` batch, or `raw` concatenated bodies |
| `live` | `long-poll` waits for the next record, `sse` holds the response open as an event stream |

A JSON record is `seq`, `timestamp`, `key`, `headers`, and `body`. Non-UTF-8 bytes use `key_b64`, `body_b64`, or `headers_b64`.

| Response header | Meaning |
| --- | --- |
| `Pico-Next-Seq` | Where to resume. Pass it as the next `seq` |
| `Pico-Up-To-Date` | `true` when the response reaches the tail |
| `Pico-Closed` | `true` when the topic is sealed and fully read |
| `Pico-Cursor` | Opaque resume token for paginated catch-up reads |

Catch-up responses include an `ETag` and honor `If-None-Match` with `304`, so polling readers that are caught up transfer nothing. A long poll that times out with no data returns `204`. SSE delivers each record as an event with its sequence as the event id, and ends with a control event when the topic is sealed. A node that is shutting down ends live reads with `503`, and the client reconnects.

## Inspect

```
HEAD /{topic}
```

Returns the topic's metadata in headers with no body, or `404`.

| Response header | Meaning |
| --- | --- |
| `Content-Type` | The topic's content type |
| `Pico-Partitions` | Number of partitions |
| `Pico-Start-Seq`, `Pico-Next-Seq` | The partition's readable range. On a topic with several partitions, only with `?partition=` |
| `Pico-TTL`, `Pico-Expires-At` | Expiry settings, when set |
| `Pico-Retention-Ms`, `Pico-Retention-Bytes` | Retention limits, when set |
| `Pico-Closed` | `true` when sealed |
| `Pico-Schema` | The bound schema, when set |
| `Pico-Kafka-Topic` | The topic's Kafka name, when it has one |

## Delete

```
DELETE /{topic}
```

Removes the topic, all of its partitions, and their records. `204` on success, `404` when it does not exist. The name is reusable at once.

## List

```
GET /?prefix=/logs/&limit=100
```

Lists topics as JSON. `prefix` filters by name prefix, `limit` caps the page, and `start_after` continues after a name from the previous page. Each entry has `name`, `content_type`, `partitions`, `closed`, and, for single-partition topics, `start_seq` and `next_seq`. With auth, only topics the token may read are listed.

## Topic config

```
GET /_streams/{topic}
PATCH /_streams/{topic}
```

Reads and changes a topic's settings as JSON, with the config fields from [Topic settings](#topic-settings). Both routes serve either HTTP protocol, and cover topics created over Kafka too. In a cluster they follow the ownership of partition `0`.

```json
{ "schema": null, "schemaValidate": false, "kafkaTopic": "orders", "partitions": 3, "retentionMs": 604800000, "retentionBytes": null }
```

- **`GET`** returns every field, with `null` for an unset one. `404` when the topic does not exist.
- **`PATCH`** takes only the fields to change and returns the result. An absent field is left alone, and `null` removes a setting. A `partitions` that does not grow the topic, or a `kafkaTopic`, is a `400`.
- **Auth.** `GET` needs `head` on the topic and `PATCH` needs `create`. Errors take the control surface's [shape](#errors).

## Consumer groups

How groups work is in [Consumer groups](/docs/design/groups). Group routes are served on a Pico listener, not on Durable Streams, and redirect to the group's coordinator like topic requests.

| Route | Does |
| --- | --- |
| `GET /_groups` | Lists every group in the cluster: `{"groups": ["a", "b"]}` |
| `GET /_groups/{group}` | Describes a group: `group`, `state`, `generation`, and `members` |
| `DELETE /_groups/{group}` | Deletes a group without members and its offsets. `204` |
| `POST /_groups/{group}/members` | Joins. Returns once the rebalance completes |
| `GET /_groups/{group}/members/{member}?generation=` | The member's current assignment |
| `POST /_groups/{group}/members/{member}/heartbeat` | Keeps the member alive. `204` |
| `DELETE /_groups/{group}/members/{member}` | Leaves. `204` |
| `PUT /_groups/{group}/offsets` | Commits offsets. `204` |
| `GET /_groups/{group}/offsets?stream=` | Fetches committed offsets, for the named topics or all of them |

A join sends the topics to subscribe to, at least one, and optionally the member's previous id, a static `instanceId`, a `clientId`, and timeouts:

```json
{ "subscription": ["/orders", "/payments"], "sessionTimeoutMs": 30000 }
```

and gets back the member id, generation, and the topics assigned to it:

```json
{ "memberId": "m-1", "generation": 3, "assignment": ["/orders"], "members": ["m-1", "m-2"] }
```

Heartbeats send `{"generation": 3}`. A commit sends offsets for at least one topic, with the member id and generation when committing as a member:

```json
{ "offsets": { "/orders": { "position": 42, "metadata": "checkpoint" } }, "memberId": "m-1", "generation": 3 }
```

Group errors use the error format above:

| Status | `error` | Meaning |
| --- | --- | --- |
| `404` | `unknown_member`, `not_found` | The member or group does not exist |
| `409` | `rebalance_in_progress` | Rejoin to get the new assignment |
| `409` | `illegal_generation` | The member is behind the group's generation. Rejoin |
| `409` | `inconsistent_protocol` | The group is in use by Kafka members |
| `409` | `conflict` | Delete of a group with members, or a join replaced by a newer one |
| `403` | `fenced` | Another member holds this static `instanceId` |
| `429` | `capacity_exceeded` | The coordinator's group or member limits were hit |
| `503` | `coordinator_unavailable` | The coordinator is moving. Retry |

## Durable Streams

A listener started with `--protocol ds` speaks the Durable Streams open protocol on its exact wire vocabulary, with `Stream-*` and `Producer-*` headers, raw record bodies, plain text errors, and one record per append. The [topic settings](#topic-settings) the protocol has no header for are extensions under the same prefix: `Stream-Partitions`, `Stream-Retention-Ms`, `Stream-Retention-Bytes`, `Stream-Schema`, and `Stream-Schema-Validate`. Topics created through either HTTP protocol or Kafka are readable through all three.
