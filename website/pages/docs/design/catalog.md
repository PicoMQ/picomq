# Catalog changelog

While the list API exists, clients that need stream CDC events read `/_sys/catalog`. Create and delete still finish when their metadata commands apply. The catalog is projected afterwards, and never on the request path.

The source of truth stays the metadata log described in [Metadata](/docs/design/metadata). The catalog is a projection of registry mutations onto one named stream for the whole cluster.

<div class="pico-diagram">
<svg viewBox="0 20 720 340" width="720" role="img" aria-label="Client create or delete proposes to the metadata log. The lease-holder projector reads applied rows, appends JSON events to /_sys/catalog, and raises flushable_idx so snapshots may truncate only projected log.">
  <defs>
    <marker id="arrc" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="40" width="150" height="52" class="box"/>
  <text x="95" y="63" text-anchor="middle" class="label">client</text>
  <text x="95" y="80" text-anchor="middle" class="sub">create / delete</text>
  <rect x="240" y="40" width="200" height="52" class="box-accent"/>
  <text x="340" y="63" text-anchor="middle" class="label">metadata log</text>
  <text x="340" y="80" text-anchor="middle" class="sub">source of truth</text>
  <rect x="520" y="40" width="180" height="52" class="box"/>
  <text x="610" y="63" text-anchor="middle" class="label">registry KV</text>
  <text x="610" y="80" text-anchor="middle" class="sub">applied view</text>
  <path d="M170 66 L232 66" class="edge" marker-end="url(#arrc)"/>
  <path d="M440 66 L512 66" class="edge" marker-end="url(#arrc)"/>
  <text x="340" y="28" text-anchor="middle" class="sub">request path returns here</text>
  <rect x="240" y="160" width="200" height="70" class="box-accent"/>
  <text x="340" y="186" text-anchor="middle" class="label">catalog projector</text>
  <text x="340" y="204" text-anchor="middle" class="sub">lease holder only</text>
  <text x="340" y="220" text-anchor="middle" class="sub">filter registry Put/Delete</text>
  <path d="M340 92 L340 152" class="edge" marker-end="url(#arrc)"/>
  <text x="352" y="128" class="sub">fetch_after(cursor)</text>
  <rect x="520" y="168" width="180" height="54" class="box"/>
  <text x="610" y="191" text-anchor="middle" class="label">/_sys/catalog</text>
  <text x="610" y="208" text-anchor="middle" class="sub">JSON create/update/delete</text>
  <path d="M440 195 L512 195" class="edge" marker-end="url(#arrc)"/>
  <rect x="520" y="270" width="180" height="52" class="box"/>
  <text x="610" y="293" text-anchor="middle" class="label">consumer</text>
  <text x="610" y="310" text-anchor="middle" class="sub">read offsets, parse JSON</text>
  <path d="M610 222 L610 262" class="edge" marker-end="url(#arrc)"/>
  <rect x="20" y="270" width="420" height="52" class="box"/>
  <text x="230" y="293" text-anchor="middle" class="label">flushable_idx = last projected applied_idx</text>
  <text x="230" y="310" text-anchor="middle" class="sub">snapshot truncates only through the watermark</text>
  <path d="M240 230 L140 270" class="edge-soft" marker-end="url(#arrc)"/>
</svg>
</div>

## Projection

The projector runs only on the lease holder, the same election that gates object cleanup in [Leases](/docs/design/leases). On leadership it ensures `/_sys/catalog` exists, takes ownership of the stream, and resumes from the last catalog record's `applied_idx`. On step-down the loop aborts.

It waits for the applied view to advance, fetches metadata log rows past its cursor, and decodes each command batch into `create`, `update`, or `delete` events. Keys under `auth/`, `idx/`, and `/_sys/` are ignored. Producer-sequence-only rewrites of a registry entry are ignored too.

Events are appended with producer id `catalog` and consecutive sequences so a restart does not rewrite history. Client writes and deletes against `/_sys/` are rejected. List and TTL sweep skip reserved names, so the catalog does not appear in ordinary listings.

## Events

Each catalog record is one JSON object with content type `application/json`:

```json
{
  "op": "create",
  "name": "/orders/live",
  "stream_id": 42,
  "applied_idx": 1203
}
```

`op` is `create`, `update`, or `delete`. `name` is the registry path. `stream_id` is the internal id from [Streams](/docs/design/streams). `applied_idx` is the metadata log index that produced the event, and is also the projector's cursor.

## Truncation

Metadata snapshots still write on their own schedule. Truncating the log is separate. After a snapshot lands, the sink deletes rows only through `min(applied, flushable_idx)`, and only when that watermark is greater than zero. The projector raises `flushable_idx` as it projects, so unprojected rows cannot be deleted out from under it.

## Consuming

Read `/_sys/catalog` like any other stream. Start at `seq=0` or resume from the last `Pico-Next-Seq` you stored:

```bash
curl 'http://localhost:4437/_sys/catalog?seq=0&format=json'
```

Each record body is the JSON event above. Follow the tail with `live=long-poll` or `live=sse` the same way as any other Pico read in the [HTTP API](/docs/api).
