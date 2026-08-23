# Admin API and dashboard

Every node runs an admin listener next to its protocol listener, on `9090` by default. It serves the health probes, a small JSON API over the cluster state, and the dashboard. The [CLI admin commands](/docs/operations/cli#admin-commands) are a thin client over this API, so anything the CLI shows is available to scripts and monitoring directly.

Reads cost nothing. They are answered from the node's in-memory metadata view, never from the database, so polling the admin API aggressively puts no load on Postgres. Writes are metadata commands like any other cluster change.

## Endpoints

| Method and path | What it does |
| --- | --- |
| `GET /health` | Liveness, answers whenever the process is up. |
| `GET /ready` | Readiness, `true` once the node is serving and registered. |
| `GET /admin/cluster` | Cluster overview: identity, applied index, stream and object counts, destruction backlog, lease holder, pending transfers. |
| `GET /admin/nodes` | Every registered node with epoch, address, slots, and stream counts. |
| `GET /admin/streams/{name}` | One stream: owner, state, epoch, offsets, content type, pending transfer. |
| `POST /admin/transfer` | Start a stream transfer, body `{"stream": name, "toNode": id}`. |
| `POST /admin/nodes/{id}` | Update a node's placement slots, body `{"slots": n}`. |
| `GET /admin/tokens` | List token records visible to the caller, with a `count`, informational only. |
| `POST /admin/tokens` | Issue a token narrowed from the caller's scope. |
| `DELETE /admin/tokens/{id}` | Revoke a token, effective on the next request. |

Errors come back as JSON with an `error` message and a meaningful status, so a rejected transfer says why, not just that it failed.

Because the metadata state is replicated, any node's admin API describes the whole cluster. The per-node parts are the identity fields and the `local` markers, everything else reads the same regardless of which node answered. A useful consequence is that one scrape target per cluster is enough for cluster-level facts, and per-node targets add only liveness.

## Interpreting the numbers

The applied index is the cluster's logical clock, the position of the last metadata command this node has applied. It grows with all activity, including background work, so steady growth on an idle-looking cluster is normal. Two nodes briefly showing different values just means one is a moment behind on the log.

The destruction backlog is the number of objects marked for deletion that the cleaner has not yet processed. It should hover near zero, and sustained growth means cleanup is not keeping up or the lease holder is unhealthy. The lease holder field says which node currently runs that maintenance.

## The dashboard

The dashboard is served at the admin listener's root, embedded in the binary, so `http://node:9090/` works with no files to deploy. It polls the admin API every `2` seconds and shows three panels: this node's identity and readiness, the node list with slots and stream counts, and pending transfers as they move.

A binary built without the dashboard assets serves a hint page at the root instead, while the JSON API keeps working. The published Docker images always include the dashboard.

When auth is required, the dashboard prompts for a token on first rejection and keeps it in session storage for the tab, sending it as a bearer header on every API call. Token management itself stays on the JSON API.

## Exposure

With `--auth required`, every `/admin` route needs a bearer token whose scope covers the operation and includes the `admin` audience, so the listener can be exposed like any authenticated API. The probes and the dashboard's static assets stay open. Probes stay open so orchestrators need no credentials, and the assets contain no data and are what prompts for the token. Details are in [Authentication](/docs/operations/auth).

With auth off the listener is wide open, and the node refuses to bind it anywhere but loopback unless `--insecure-allow-remote` opts out. `--no-admin` disables the listener entirely for nodes that should expose nothing but the protocol, at the cost of the probes. TLS in front remains a deployment concern either way, since tokens travel as bearer headers.
