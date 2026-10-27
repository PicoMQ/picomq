# Admin API

Every node runs an admin listener next to its protocol listener, on `9090` by default. It serves the health probes, a JSON API over the whole cluster, Prometheus metrics, and the [console](/docs/operations/console). The console and the [CLI admin commands](/docs/operations/cli#admin-commands) are clients of this API, so anything they show is available to scripts and monitoring directly.

## Any node answers

Every route works on every node. Most are answered from the node's in-memory metadata view, never from the database, so polling them puts no load on Postgres. The rest need live state that one node holds, and the node that received the request forwards it there with the caller's token.

| Answered by | Routes |
| --- | --- |
| The receiving node, from metadata | cluster, overview, transfers, nodes, topics, tokens, group names |
| The partition's owner | partition offsets, records, produce, trim |
| The group's coordinator | group detail, members, offsets, resets, deletes, and each group's state in the group list |
| The named node | `/admin/nodes/{id}/metrics` |

- **Address.** Forwarding uses the admin address each node registers, `--admin-address` (`PICO_ADMIN_ADDRESS`). It defaults to `http://{admin-listen}`, which other nodes reach only when the listener binds a routable address, so set it to the URL the other nodes should use.
- **TLS.** Nodes whose certificates come from a private CA also need [`PICO_TLS_CA_CERTIFICATE`](/docs/operations/tls#clients) to trust each other.
- **Internal routes.** Nodes gather group states and live offsets from each other through `POST /admin/nodes/{id}/groups` and `POST /admin/nodes/{id}/offsets`, which check the same permissions as the routes they serve.

| Status | Forwarding failed because |
| --- | --- |
| `404` | the target node is not registered |
| `502` | the target runs with `--no-admin`, or its admin address could not be reached |
| `503` | the request was already forwarded once and the target moved on, retry |
| `504` | the target did not answer within `10` s |

## Routes

- **Permissions.** Each route checks the permissions listed against its resources, from the table in [Authorization](/docs/design/auth#scopes). With auth off every route is open.
- **Errors.** Errors come back as `{"error": "<message>"}` with a meaningful status, so a rejected transfer says why, not just that it failed.
- **Lists.** Every list pages the same way as the data plane, see [Lists](/docs/api#lists).

### Cluster and nodes

| Route | Permission | What it does |
| --- | --- | --- |
| `GET /health` | open | Liveness, answers whenever the process is up |
| `GET /ready` | open | `200` when ready, `503` when not, with `ready`, `serving`, `registered`, `fenced`, `appliedIndex`, and `nodeId` |
| `GET /admin/cluster` | `cluster_read` | Identity and epoch, `registered` and `fenced`, applied index, stream and object counts, the `gc` backlog, KV size, snapshot stats, lease holder, `pendingTransferCount` |
| `GET /admin/overview` | `cluster_read` | The ten largest topics by stored bytes and the ten busiest by write rate, recomputed at most every `60` s when asked, with `computedAtMs`. Rates compare two passes, so `writeRateSinceMs` is `null` until a node's second pass |
| `GET /admin/transfers` | `cluster_read` | Pending transfers, each `{log, fromNode, toNode}` |
| `POST /admin/transfers` | `transfer_stream` | Starts a transfer, body `{"topic": name, "partition": n, "toNode": id}`. `202`, completion is asynchronous |
| `GET /admin/nodes?fenced=` | `node_read` | Registered nodes with `nodeEpoch`, `advertisedAddress`, `slots`, `openingCount` (logs open on the node), `placedCount` (closed logs placed on it), `local`, and `fenced` |
| `GET /admin/nodes/{id}` | `node_read` | One node in the same shape |
| `POST /admin/nodes/{id}` | `update_node_slots` | Sets the node's placement slots, body `{"slots": n}` |
| `GET /metrics` | `node_read` | This node's [metrics](#metrics) |
| `GET /admin/nodes/{id}/metrics` | `node_read` | Another node's metrics, through this one |

### Topics

| Route | Permission | What it does |
| --- | --- | --- |
| `GET /admin/topics?prefix=` | `list` | Topics the token may list, with partitions, content type, Kafka name, retention, TTL, and `closed` |
| `POST /admin/topics` | `create` | Creates a topic, body `{name, partitions, contentType, retentionMs, retentionBytes, ttlSeconds}` with only `name` required. `201`, or `200` when it already exists the same way |
| `GET /admin/topics/{name}` | `head` | The topic plus `storedBytes` and how many partitions each node owns |
| `PATCH /admin/topics/{name}` | `create` | Raises `partitions` or changes `retentionMs` and `retentionBytes`, `null` clearing a limit |
| `DELETE /admin/topics/{name}` | `delete` | Deletes the topic. `204` |
| `GET /admin/topics/{name}/partitions?node=` | `head` | Partitions with owner, epoch, state, start and next offset, stored bytes, and any pending transfer |
| `GET /admin/topics/{name}/partitions/{index}` | `head` | One partition in the same shape |
| `POST /admin/topics/{name}/partitions/{index}/trim` | `trim` | Deletes records before an offset, body `{"before": offset}`, answers the new `startOffset` |

### Records

| Route | Permission | What it does |
| --- | --- | --- |
| `GET /admin/topics/{name}/partitions/{index}/records` | `read` | A page of records in offset order, with each field cut to a 4 KiB preview |
| `GET /admin/topics/{name}/partitions/{index}/records/{offset}` | `read` | One record, fields up to 1 MiB |
| `POST /admin/topics/{name}/partitions/{index}/records` | `append` | Appends one record. `201` with its `offset` and `timestamp` |

Listing takes these parameters:

| Parameter | Meaning |
| --- | --- |
| `offset` | Start at this offset |
| `timestamp` | Start at the first record at or after these epoch milliseconds, never with `offset` |
| `key`, `value` | Keep records whose key or value contains this text |
| `header`, `headerValue` | Keep records with this header, and with `headerValue` only when its value contains that text |
| `limit` | Records per page, as in [Lists](/docs/api#lists) |
| `wait` | Milliseconds to wait for new records when none are there yet, at most `5000`, for a live tail |

- **Start.** Listing starts at offset `0` unless `offset` or `timestamp` say otherwise, and the cursor continues after the last record returned.
- **Filters.** They run on the owner under a scan budget, so a selective filter returns short pages with `next` set rather than scanning the whole partition in one request.

Each record is `{offset, timestamp, key, value, headers}`.

- **Fields.** A key, a value, and each header value is `null` or `{data, encoding, size, truncated}`.
- **Encoding.** `utf8` for valid UTF-8, `base64` for other bytes.
- **Size.** `size` is the full length in bytes, and `truncated` says the preview was cut.
- **Headers.** A list in record order, so duplicate names survive.

```json
{
  "offset": 1042,
  "timestamp": 1793001600000,
  "key": { "data": "customer-1042", "encoding": "utf8", "size": 13, "truncated": false },
  "value": { "data": "{\"order\":\"o-77\",\"total\":42.5}", "encoding": "utf8", "size": 30, "truncated": false },
  "headers": [
    { "name": "trace-id", "value": { "data": "4bf92f35", "encoding": "utf8", "size": 8, "truncated": false } },
    { "name": "source", "value": { "data": "checkout", "encoding": "utf8", "size": 8, "truncated": false } }
  ]
}
```

Producing takes the same layout without the read-only parts: `{key, value, headers}`, each field `{data, encoding}` or `null`, at most 2 MiB in all. A `null` value is a tombstone.

### Consumer groups

| Route | Permission | What it does |
| --- | --- | --- |
| `GET /admin/groups?prefix=` | `describe_group` | Groups the token may describe, with state, generation, member count, and coordinator |
| `GET /admin/groups/{group}` | `describe_group` | One group, with protocol, generation, members, and `committedPartitions` |
| `DELETE /admin/groups/{group}` | `delete_group` | Deletes a group with no members and its offsets. `204` |
| `GET /admin/groups/{group}/members` | `describe_group` | Members with client id, host, and decoded assignment |
| `GET /admin/groups/{group}/offsets?topic=` | `describe_group` | Committed offsets with each partition's next offset and lag, only for topics the token may `head` |
| `POST /admin/groups/{group}/offsets/reset` | `consume`, `read` on the topic | Moves offsets, see below |
| `DELETE /admin/groups/{group}/offsets?topic=` | `delete_group` | Forgets one topic's offsets. `204` |

A reset takes `{"topic": name, "to": target, "partitions": [n]}`, where `partitions` defaults to all of them and `to` is `"earliest"`, `"latest"`, `{"offset": n}`, or `{"timestamp": ms}`. It answers the new positions as `{"partitions": [{"partition": n, "offset": n}]}`. Resets and deletes need the group to have no members, like Kafka, and answer `409` while consumers are connected.

### Tokens

The token routes, `GET` and `POST /admin/tokens` and `DELETE /admin/tokens/{id}`, are covered in [Issuing tokens](/docs/operations/auth#issuing-tokens).

## Metrics

`GET /metrics` returns the node's counters in the Prometheus text format.

- **Per node.** Each node reports only itself, unlike the metadata routes, so scrape every node. `/admin/nodes/{id}/metrics` returns another node's through the one asked.
- **No history.** Values count from process start and the broker keeps no history. Prometheus keeps it, and the console charts rates only while a page is open.

| Metric | Type | Labels |
| --- | --- | --- |
| `pico_requests_total` | counter | `protocol`, `operation`, `status` (`2xx` and so on, `ok` or `error` for Kafka) |
| `pico_request_duration_seconds` | histogram | `protocol`, `operation` |
| `pico_records_in_total`, `pico_bytes_in_total` | counter | none, records and bytes appended |
| `pico_records_out_total`, `pico_bytes_out_total` | counter | none, records and bytes read |
| `pico_group_rebalances_total` | counter | none |
| `pico_transfers_total` | counter | `status`, `completed` or `failed` |
| `pico_auth_failures_total` | counter | `protocol`, `reason` |
| `pico_connections` | gauge | `listener`, `stream`, `kafka`, or `admin` |
| `pico_live_readers` | gauge | none, open long polls and live reads |
| `pico_groups_coordinated` | gauge | none |
| `pico_logs_owned` | gauge | none, partitions this node owns |

## Interpreting the numbers

- **Applied index.** The cluster's logical clock, the position of the last metadata command this node has applied. It grows with all activity, including background work, so steady growth on an idle-looking cluster is normal. Two nodes briefly showing different values just means one is a moment behind on the log.
- **Readiness.** A node is ready once it is serving and registered, and never while it is fenced. A fenced node stops on its own, see [Failover](/docs/design/failover), and its row keeps `fenced: true` until the node restarts and registers again. A row that stays fenced means that node is gone, and its streams have already moved.
- **GC backlog.** The number of objects marked for deletion that the cleaner has not yet processed. It should hover near zero, and sustained growth means cleanup is not keeping up or the lease holder is unhealthy. `leaseHolder` says whether this node currently runs that maintenance.

## Exposure

| Setting | Effect on the admin listener |
| --- | --- |
| `--auth required` | Every `/admin` route and `/metrics` needs a bearer token whose scope covers the operation and includes the `admin` audience, so the listener can be exposed like any authenticated API, see [Authentication](/docs/operations/auth). Routes take full names, so a token with `autoPrefix` is refused |
| `--auth off` | Wide open, and the node refuses to bind it anywhere but loopback unless `--insecure-allow-remote` opts out |
| `--no-admin` | No listener at all, for nodes that should expose nothing but the protocol, at the cost of the probes, the metrics, and forwarding to that node |

- **Open paths.** The probes stay open so orchestrators need no credentials. The console's static assets stay open because they hold no data and are what asks for the token.
- **TLS.** Tokens travel as bearer headers, so serve the listener over [TLS](/docs/operations/tls) when it leaves a private network.
