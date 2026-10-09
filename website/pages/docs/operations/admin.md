# Admin API and dashboard

Every node runs an admin listener next to its protocol listener, on `9090` by default. It serves the health probes, a small JSON API over the cluster state, and the dashboard. The [CLI admin commands](/docs/operations/cli#admin-commands) are a thin client over this API, so anything the CLI shows is available to scripts and monitoring directly.

Reads are answered from the node's in-memory metadata view. Administrative writes use the existing metadata and stream services; creating or deleting a stream follows the same lifecycle as the native protocol.

## Endpoints

| Method and path | What it does |
| --- | --- |
| `GET /health` | Liveness, answers whenever the process is up. |
| `GET /ready` | Readiness, `true` once the node is serving and registered. |
| `GET /admin/cluster` | Cluster overview: identity, applied index, stream and object counts, destruction backlog, lease holder, pending transfers. |
| `GET /admin/nodes` | Every registered node with epoch, address, slots, and stream counts. |
| `GET /admin/streams/{name}` | One stream: owner, state, epoch, offsets, content type, pending transfer. |
| `PUT /admin/streams/{name}` | Create a stream through the native lifecycle service; `201` if created, `200` if the configuration already matches. |
| `DELETE /admin/streams/{name}` | Delete a stream through the native lifecycle service; `204` if deleted, `404` if missing. |
| `POST /admin/transfer` | Start a stream transfer, body `{"stream": name, "toNode": id}`. |
| `POST /admin/nodes/{id}` | Update a node's placement slots, body `{"slots": n}`. |
| `GET /admin/tokens` | List token records visible to the caller, with a `count`, informational only. |
| `POST /admin/tokens` | Issue a token narrowed from the caller's scope. |
| `DELETE /admin/tokens/{id}` | Revoke a token, effective on the next request. |

Errors come back as JSON with an `error` message and a meaningful status, so a rejected transfer says why, not just that it failed.

Because the metadata state is replicated, any node's admin API describes the whole cluster. The per-node parts are the identity fields and the `local` markers, everything else reads the same regardless of which node answered. A useful consequence is that one scrape target per cluster is enough for cluster-level facts, and per-node targets add only liveness.

## Creating and deleting streams

Lifecycle requests require the `admin` audience and the existing `create` or `delete` operation, with a stream scope that matches the requested name. The `stream.write` group also grants those operations. The `admin.write` group alone does not. A token with only the `pico` audience cannot use these admin routes; the native protocol routes keep their existing authorization requirements.

The path identifies an absolute stored stream name, just like admin stream inspection. `autoPrefixStreams` does not rewrite admin paths. Omit the stream's leading slash after `/admin/streams/`, and percent-encode each path segment. Native stored names preserve URI escapes: to operate on `/orders/eu%20west`, use `/admin/streams/orders/eu%2520west`. Root and reserved namespaces (`/_sys`, `/_schemas`, `/_streams`, and `/_groups`, including descendants) are rejected. Literal spaces and other characters that cannot appear in a native URI path must already be encoded in the stored name.

Neither method accepts a body. Creation accepts the native headers: `Content-Type`, `Pico-Kafka-Topic`, `Pico-TTL`, `Pico-Expires-At`, `Pico-Closed`, `Pico-Schema`, and `Pico-Schema-Validate`. TTL and absolute expiration are mutually exclusive. The response uses the native Pico metadata headers, including `Pico-Next-Seq`; mismatched existing configuration or an unavailable Kafka alias returns `409`.

```sh
curl -X PUT 'http://localhost:9090/admin/streams/orders/eu' \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Pico-Kafka-Topic: orders-eu'

curl -X DELETE 'http://localhost:9090/admin/streams/orders/eu' \
  -H "Authorization: Bearer $TOKEN"
```

A new stream can be created on the receiving node. For an existing stream, both methods require its recorded owner, including when the stream is closed. A request to another node returns `409` with `code: "owner_required"` and `ownerNodeId`. Connect explicitly to that node's **admin listener** and review the operation again. Registered node addresses identify protocol listeners, so these operations do not redirect credentials or infer admin ports.

During a pending transfer, both methods return `409` with `code: "transfer_pending"`, `fromNode`, and `toNode`; wait for the handoff to finish. These refusals occur before lifecycle mutation. Ownership checks use the published metadata view and retain the existing stream service's concurrency semantics. Other administrative operations, such as requesting a transfer, remain available through any node's admin listener.

## Interpreting the numbers

The applied index is the cluster's logical clock, the position of the last metadata command this node has applied. It grows with all activity, including background work, so steady growth on an idle-looking cluster is normal. Two nodes briefly showing different values just means one is a moment behind on the log.

The destruction backlog is the number of objects marked for deletion that the cleaner has not yet processed. It should hover near zero, and sustained growth means cleanup is not keeping up or the lease holder is unhealthy. The lease holder field says which node currently runs that maintenance.

## The dashboard

The dashboard is served at the admin listener's root, embedded in the binary, so `http://node:9090/` works with no files to deploy. Overview shows node readiness, placement slots, stream counts, and pending transfers. Discovery browses streams and their metadata, Watch tails selected streams, Publish sends messages, and Tokens manages scoped credentials. Administrative writes show a confirmation before submission.

A binary built without the dashboard assets serves a hint page at the root instead, while the JSON API keeps working. The published Docker images always include the dashboard.

When auth is required, the dashboard prompts for an admin token and keeps it in session storage for the tab. Stream browsing, watching, and publishing use a separately configured protocol connection. Discovery requires an explicit admin pairing for ownership, transfer, and create/delete operations, so the stream token is not silently reused as an admin credential. See the dashboard README for connection and optional gateway setup.

## Exposure

With `--auth required`, every `/admin` route needs a bearer token whose scope covers the operation and includes the `admin` audience, so the listener can be exposed like any authenticated API. The probes and the dashboard's static assets stay open. Probes stay open so orchestrators need no credentials, and the assets contain no data and are what prompts for the token. Details are in [Authentication](/docs/operations/auth).

With auth off the listener is wide open, and the node refuses to bind it anywhere but loopback unless `--insecure-allow-remote` opts out. `--no-admin` disables the listener entirely for nodes that should expose nothing but the protocol, at the cost of the probes. TLS in front remains a deployment concern either way, since tokens travel as bearer headers.
