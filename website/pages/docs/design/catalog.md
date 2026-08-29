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

The projector runs only on the lease holder, the same election that gates object cleanup in [Leases](/docs/design/leases). On leadership it ensures `/_sys/catalog` exists, takes ownership of the stream, and folds the catalog's records into a shadow of the registry: last event per name wins, cursor is the last record's `applied_idx`. On step-down the loop aborts.

It replays metadata log rows past the cursor against the shadow with the same conditional semantics as the apply path. A create that lost its race, a delete whose condition failed, or a producer-state rewrite emits nothing. Keys under `auth/`, `idx/`, and `/_sys/` are ignored. A row's events are appended atomically, so every projected create, update, and delete lands exactly once, across failovers included.

Events are appended with producer id `catalog` and consecutive sequences. Client writes, deletes, and trims against `/_sys/` are rejected. List and TTL sweep skip reserved names.

A metadata log that predates the projector has no history to replay: first leadership emits one `create` per live registry entry at the current applied index, opened by a `baseline` record and closed by a checkpoint. A baseline whose checkpoint never landed is redone in full on the next leadership. The baseline is at-least-once.

## Events

Each catalog record is one JSON object with content type `application/json`:

```json
{
  "op": "create",
  "name": "/orders/live",
  "stream_id": 42,
  "content_type": "text/plain",
  "closed": false,
  "value_hash": "9f8b…",
  "applied_idx": 1203
}
```

`op` is `create`, `update`, or `delete`. `name` is the registry path. `stream_id` is the internal id from [Streams](/docs/design/streams). `value_hash` is the sha256 of the registry entry's bytes, kept so a recovered shadow can evaluate conditional deletes exactly. `applied_idx` is the metadata log index that produced the event, and is also the projector's cursor.

Records with `op: "checkpoint"` or `op: "baseline"` carry only `applied_idx` and mark progress and baseline boundaries. Consumers skip them.

## Truncation

Metadata snapshots write on their own schedule. After a snapshot lands, the sink deletes rows only through `min(applied, flushable_idx)`, and only when that watermark is greater than zero. The projector raises `flushable_idx` only at indices covered by a durable catalog record, so rows the catalog cannot recover from are never deleted. Checkpoints bound the lag over event-less stretches.

## Consuming

Read `/_sys/catalog` like any other stream. Start at `seq=0` or resume from the last `Pico-Next-Seq` you stored:

```bash
curl 'http://localhost:4437/_sys/catalog?seq=0&format=json'
```

Each record body is the JSON event above. Follow the tail with `live=long-poll` or `live=sse` the same way as any other Pico read in the [HTTP API](/docs/api).

In [Kafka mode](/docs/kafka) the same stream is the read-only internal topic `__catalog`, batch-encoded at fetch time:

```bash
kcat -b 127.0.0.1:9092 -t __catalog -C
```
