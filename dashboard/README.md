# Dashboard

The dashboard uses Preact, TypeScript, and Vite. It remains embedded in PicoMQ
when built. No additional packages are required. Stream lifecycle actions use
the general-purpose admin PUT/DELETE operations described below.

## Development

```sh
npm ci
npm run dev
```

Vite forwards `/admin`, `/health`, and `/ready` to `http://127.0.0.1:9090`.
Its development-only `/pico/` proxy strips `/pico` and forwards to the existing
Pico protocol listener at `http://127.0.0.1:4437`. Override that target with
`PICO_DASHBOARD_STREAM_TARGET` when starting Vite. No production server routes
are installed by this configuration.

## Connections and deployment

Overview uses the existing admin token gate. The other tabs have a separate
Stream connection form. Endpoint and token are stored in sessionStorage.
Missing or expired admin access locks Overview and reports errors for protected
admin operations; stream watches, publish requests, and drafts stay active.
Applying a stream connection is disabled while a publish, create, delete, or
transfer request is pending, including when switching tabs. Reapplying an
unchanged connection preserves active watches and the last publish outcome.
Stream reads, listing, and appends require the `pico` audience plus the relevant
operations and stream scopes. Schema/config reads require the `admin` audience.
A single appropriately scoped token may include both audiences.

The existing Pico listener does not provide complete browser CORS support.
For a production build, configure an external same-origin reverse proxy for the
existing protocol listener (for example `/pico/`, stripping that prefix), then
enter its path in Stream connection. An optional gateway using only Node built-ins
is included below. The embedded admin listener alone does
not supply that proxy. An explicit cross-origin endpoint only works when its
deployment adds the necessary CORS allow/expose headers. The default production
endpoint uses the current hostname and port 4437; it is not a connectivity guarantee.

Ownership redirects are deliberately not followed by the browser: manual
redirects hide their destination, while automatic cross-origin redirects drop
credentials. For a cluster, the external proxy must route to the stream owner
and handle ownership changes, or connect to the owner directly through a
browser-accessible endpoint. Do not use the advertised loopback address as a
remote browser endpoint. Durable Streams-only listeners cannot provide native
Pico listing or record envelopes and are not supported by these tabs.

### Optional gateway for the embedded dashboard

Build the dashboard, then rebuild your PicoMQ binary/image to embed those assets.
With that node running, start this separate process:

```sh
node scripts/dashboard-gateway.mjs
```

Open `http://127.0.0.1:9080`, expand **Stream connection**, and apply `/pico`
with the stream token. The gateway forwards the dashboard, assets and admin API
to `http://127.0.0.1:9090`; `/pico/` forwards to `http://127.0.0.1:4437`.
It serves the embedded dashboard from the admin listener; it does not serve Vite
or add a route to PicoMQ. Updating the gateway alone does not update the embedded UI.

Configure upstreams or a cluster using these environment variables:

```sh
PICO_GATEWAY_ADMIN=http://127.0.0.1:9090 \
PICO_GATEWAY_STREAM=http://127.0.0.1:4437 \
PICO_GATEWAY_OWNERS=http://127.0.0.1:4438,http://127.0.0.1:4439 \
node scripts/dashboard-gateway.mjs
```

`PICO_GATEWAY_HOST` defaults to `127.0.0.1`; `PICO_GATEWAY_PORT` defaults to `9080`.
For direct tailnet access, explicitly bind an appropriate local Tailscale IP.
The script does not register a Tailscale service or configure TLS. For an HTTPS
deployment, put the gateway behind your existing HTTPS reverse proxy.
Upstreams must be HTTP(S) origins, without URL credentials or path prefixes.

Only native 307/308 ownership redirects are followed, at most three hops, with
the method, body, token and exact path/query preserved. A destination must be
the configured stream origin or explicitly listed in `PICO_GATEWAY_OWNERS`.
Configure those origins to match the nodes' advertised, gateway-reachable
protocol addresses. Redirects to any other origin or path fail without forwarding
credentials. Admin redirects are rejected. The gateway never retries errors,
limits request bodies to 1 MiB, and allows 40 seconds for upstream inactivity
so native long polls can complete. Tokens are supplied by the browser and are
not stored by the gateway.

## Tabs

- **Overview:** existing cluster status, nodes and pending transfers. **Edit
  slots** reviews the node and new placement weight before confirming the
  existing `POST /admin/nodes/{id}` action. Zero slots are allowed. Requires
  the dashboard admin token with `admin` audience and `update_node_slots`.
- **Discovery:** paginated prefix search; live HEAD metadata; a bounded sample
  requesting up to 20 recent records / 256 KiB; declared schema when available; observed
  JSON types and field occurrence counts. Shape traversal is bounded to 200
  paths, depth 6, and 20 array elements per sampled array. Listing offsets are
  intentionally not displayed as live message counts.
  Stream details also show the routing owner node, advertised owner address,
  and stream epoch through the existing `GET /admin/streams/{name}` API.
  Open **Admin connection** at the top of Discovery and choose **Use dashboard admin** to use the
  dashboard's admin listener and Overview token, or enter an admin endpoint
  and its own token. Pair it with the same cluster as the stream connection;
  the protocol API does not expose a cluster identity to verify that pairing.
  The selected admin source is shown with the ownership details. No stream
  token is automatically sent to an admin endpoint. Admin access requires the
  `admin` audience and `stream_inspect` permission.
  Pairing stays in memory across stream selections and is cleared when the
  stream endpoint changes. **Refresh details** reloads ownership as well as
  the protocol metadata. Admin failures do not hide samples or protocol details.
  An epoch of `-1` means the stream has never opened; a missing engine row is
  shown as unavailable. The owner is the admin API's routing owner, which can
  differ from the persisted engine owner during a transfer. Advertised owner
  addresses are shown as text and may not be reachable from the browser.
  **Transfer stream** lists registered nodes with positive slots from the paired
  admin API and confirms the stream, destination and endpoint before the existing
  `POST /admin/transfer` action. Requires `node_read` to choose a node and
  `transfer_stream` to request the move. The server requires an opened stream;
  unopened streams show an explanation instead of offering a failing action.
  A 202 means accepted, not completed.
  Read-only status checks compare the acknowledged stream ID, persisted owner
  and pending transfer until the handoff completes; routing ownership is reported
  separately. Failed ownership reads keep the receipt visible. Selecting another
  stream or admin endpoint after submission stops that stream's status view.
  **Create stream** reviews name, content type and optional Kafka alias before
  `PUT /admin/streams/{name}`. **Delete stream** requires typing the selected
  stream name before `DELETE /admin/streams/{name}`. Both use the explicitly
  paired admin connection, require the `admin` audience plus `create` or `delete`
  and a matching stream scope, and reuse the server's existing lifecycle service.
  The `admin` permission group alone does not grant create or delete. Pairing is
  available before selecting a stream, so an empty cluster can be initialized.
  Admin names are absolute stored stream names; `autoPrefixStreams` is not applied.
  Enter the complete stored name even if a protocol token uses an automatic prefix.
  Each URL path segment is encoded for the admin route, preserving literal percent
  escapes in stored names. Reserved schema/config/group/system paths are rejected
  by the stream form. No initial messages are sent with create.
  Writes to a stream owned by another node fail with `409 owner_required` and
  its node ID. Connect the paired Admin connection to that node's admin listener
  and review the action again; the UI never guesses an admin address or forwards
  credentials. A `409 transfer_pending` asks you to wait for the handoff and
  refresh ownership. The server does not redirect these writes to the protocol
  listener. Native Pico create/delete authorization remains unchanged for other
  clients. The external protocol gateway is still needed for browser reads and
  publishing when the listener lacks CORS support.
  Stream selection and connection changes stay locked while writes are pending.
- **Watch:** up to 8 exact stream names in individual rows with Add stream and
  Remove controls (duplicates watched once), or
  JavaScript regex over stream names, with an optional discovery prefix.
  New rows receive keyboard focus; removing a row focuses a nearby stream.
  Stop watching to edit the rows. Exact rows and the regex pattern retain
  separate drafts when switching match modes.
  Each stream has its own cursor and status; every message shows its source.
  Each stream uses native `live=long-poll` GET reads, the same mechanism as
  `pico tail -f`, with a 35-second client deadline for the server's default
  25-second wait. Empty 204 responses preserve the resume cursor. Each stream
  starts its next request one second after its own previous read finishes,
  displaying results without waiting for other streams. Closed streams finish
  reading their backlog and stop when the server reports closed and caught up.
  Regex listings refresh separately every 5 seconds, scan up to
  5,000 names, and allow at most 8 matches. A worker time limit prevents a slow
  regex from blocking the UI. Newly discovered streams start at the earliest
  retained position; short-lived streams between scans can be missed. Messages
  from existing matches keep arriving during temporary discovery failures.
  Discovery retries after 5, 10, 20, then at most 30 seconds, with a visible
  warning, preserving cursors and matches until a complete scan succeeds.
  Invalid patterns, scan/match limits, and authorization failures stop the
  watch with an explicit error. Records
  stay ordered per stream; merged rows use arrival order. The display retains
  at most 250 records / 2 million serialized characters. Stop cancels pending
  reads. Start creates a new watch from the selected starting policy. Clearing
  displayed rows preserves active cursors. Watch continues across tabs.
- **Publish:** sends one JSON/text payload, optional key, and string headers to
  an existing stream through the existing JSON batch append format. HEAD checks
  existence/closed status first, so the token needs head as well as append.
  It never creates streams or retries writes. On an ambiguous response, inspect
  the original stream/endpoint shown in the error before retrying manually.
  Applying another connection is blocked until the request finishes.
- **Tokens:** list public token records and inspect scopes, issue a scoped token,
  and revoke a token with confirmation through the existing `/admin/tokens` API.
  Uses only the dashboard admin connection with `admin` audience and the relevant
  `list_tokens`, `issue_token` or `revoke_token` permissions. The scope editor uses
  repeatable audience, permission and exact/prefix matcher rows; access to all
  resources is an explicit choice. The server enforces scope narrowing. The
  issued secret appears once, stays in page memory across tabs, and is removed
  when dismissed or the page closes; it is never stored in sessionStorage or
  recovered by listing. If issuance succeeds but its response is lost, refresh
  the list and revoke the orphaned ID before issuing a replacement. The reserved
  anonymous token grants public access and cannot include the admin audience.

All administrative and lifecycle writes require a review/confirmation and are
never automatically retried. A timeout, network failure, server failure or invalid
success response leaves the outcome unknown: the UI retains the target and asks
you to inspect it before deciding to repeat the action. Scope failures do not
discard work on other tabs. Prometheus charts and consumer analytics are outside
this dashboard change.

The JSON record API exposes numeric sequences. This dashboard rejects record
sequences beyond JavaScript's safe integer range rather than silently rounding
them. Resume positions obtained from response headers remain strings.

Ordinary requests time out after 15 seconds; live reads use 35 seconds. Stream
JSON responses are bounded to 4 MiB; a larger
response reports an error without advancing the read cursor. PicoMQ may return
an oversized first record despite the requested byte cap. Received record
previews retain their sequence but truncate bodies to 16,384 characters and
bound key/header previews, with a visible notice. Truncated records are excluded
from shape inference. Schema previews are bounded to 100,000 bytes.

## Demo traffic

With a local Pico node running, send one varied JSON message to every open
stream every two seconds:

```sh
node scripts/demo-traffic.mjs
```

New streams are discovered each round; closed streams are skipped. Orders,
logs, and other events use different synthetic payloads. Stop with Ctrl+C.
Records rotate through four variants: both key and headers, key only, headers
only, and neither. Keys use `demo-N`; record headers include `source`,
`event-type`, `content-type`, and `run-id`, separate from the JSON body.
Expand a message in Watch to inspect its key and headers.
The script uses only Node built-ins, the existing list/append APIs, and the
optional `PICO_TOKEN` environment variable. It never creates streams.

For a limited run or a subset of streams:

```sh
node scripts/demo-traffic.mjs --prefix /demo/ --interval 3 --rounds 10
```

## Build and validation

```sh
npm run build
node --test tests/*.test.mjs
```

Optional browser regressions need Node 22+ and an installed Chrome/Chromium.
With the Vite preview running, use:

```sh
node tests/browser-check.mjs http://localhost:5173
node tests/actions-browser-check.mjs http://localhost:5173
node tests/transfer-browser-check.mjs http://localhost:5173
```

Set `CHROME_BIN` to override the browser executable. This check uses isolated
browser API responses and does not write to a PicoMQ instance. It verifies
admin-access isolation, pending-publish connection guards, delivery feedback,
and mobile layouts. Action checks cover confirmations, scoped credential use,
unknown write outcomes, pending connection/selection locks, token secret lifetime,
and transfer acknowledgement/status behavior. Gateway unit checks start ephemeral
loopback HTTP fixtures to check forwarding and credential restrictions.
It also checks explicit ownership pairing, independent admin errors, credential
changes, and suppression of stale ownership responses after selecting another
stream or endpoint. Unit checks use Node's built-in test runner and the existing
TypeScript compiler; they require no running PicoMQ service or added packages.

Build output goes to `../picomq/pico-http/_dashboard` for Rust embedding.
Changing dashboard assets requires rebuilding the binary or Docker image to
update the embedded UI. The Vite development preview needs neither.
