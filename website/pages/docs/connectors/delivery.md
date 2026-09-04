# Delivery guarantees

Every connector delivers at least once. A record that a source read, or a sink was handed, reaches its destination one or more times. The only way it can reach it zero times is a plugin that reports success for a write it did not do.

This page is precise about where the "or more" comes from, how wide the window is, and what each sink does with a duplicate when it arrives.

There is no exactly-once mode. PicoMQ does not implement Kafka transactions, and the systems on the other side of a source have no shared commit to join. The runtime offers a small, well-defined duplicate window instead, and sinks that make duplicates harmless where the destination allows it.

## Two cursors, two commit points

| Side | Cursor | Kept by | Moves when |
| --- | --- | --- | --- |
| Sink | Consumer group offset | The broker | The plugin has confirmed the write |
| Source | State blob, plus whatever the external system tracks itself | The state store, and the external system | Every record of the batch has a delivery report |

Each side has exactly one point at which its cursor moves, and that point is after the write it covers has been confirmed.

<div class="pico-diagram">
<svg viewBox="0 30 720 250" width="720" role="img" aria-label="Two timelines. The sink timeline runs fetch, consume, commit offset, with the duplicate window marked between consume completing and the commit landing. The source timeline runs poll, produce, save state, ack, with the window marked between the last delivery report and the state save landing.">
  <defs>
    <marker id="arrd" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="20" y="60" class="label">sink</text>
  <path d="M20 90 L700 90" class="edge" marker-end="url(#arrd)"/>
  <rect x="60" y="72" width="100" height="36" class="box"/>
  <text x="110" y="95" text-anchor="middle" class="label">fetch</text>
  <rect x="220" y="72" width="120" height="36" class="box-accent"/>
  <text x="280" y="95" text-anchor="middle" class="label">consume</text>
  <rect x="480" y="72" width="130" height="36" class="box"/>
  <text x="545" y="95" text-anchor="middle" class="label">commit offset</text>
  <path d="M340 122 L480 122" class="edge-soft"/>
  <path d="M340 116 L340 128" class="edge-soft"/>
  <path d="M480 116 L480 128" class="edge-soft"/>
  <text x="410" y="142" text-anchor="middle" class="sub">crash here: batch redelivered</text>
  <text x="20" y="190" class="label">source</text>
  <path d="M20 220 L700 220" class="edge" marker-end="url(#arrd)"/>
  <rect x="60" y="202" width="90" height="36" class="box-accent"/>
  <text x="105" y="225" text-anchor="middle" class="label">poll</text>
  <rect x="200" y="202" width="120" height="36" class="box"/>
  <text x="260" y="225" text-anchor="middle" class="label">produce</text>
  <rect x="440" y="202" width="110" height="36" class="box"/>
  <text x="495" y="225" text-anchor="middle" class="label">save state</text>
  <rect x="590" y="202" width="90" height="36" class="box-accent"/>
  <text x="635" y="225" text-anchor="middle" class="label">ack</text>
  <path d="M320 252 L550 252" class="edge-soft"/>
  <path d="M320 246 L320 258" class="edge-soft"/>
  <path d="M550 246 L550 258" class="edge-soft"/>
  <text x="435" y="272" text-anchor="middle" class="sub">crash here: batch re-read and re-produced</text>
</svg>
</div>

## The duplicate window

| | Sink | Source |
| --- | --- | --- |
| Opens | Plugin returns from `consume()` | Last delivery report arrives |
| Closes | Offset commit reaches the broker | State save completes |
| Width | One asynchronous commit, milliseconds | One state store write |
| On a crash inside it | Destination has the batch, group does not. Restarted sink is handed the same batch | PicoMQ has the batch, plugin's committed state does not. Restarted source re-reads and re-produces it |
| Bounded to | One batch. The next is not fetched until the commit is issued | One batch. A later batch cannot save state ahead of an earlier one that failed |

Both windows can also open without a crash.

- A sink plugin that wrote successfully and then returned an error, a network blip after the `INSERT` committed for instance, is retried and writes again.
- A source whose state save fails after a successful produce is nacked, re-reads, and produces again.

Each is one batch of duplicates. Each is preferable to advancing a cursor past data whose fate is unknown.

## Failure matrix

| What fails | Sink | Source |
| --- | --- | --- |
| Runtime killed between write and commit | Batch redelivered on restart | Batch re-read and re-produced on restart |
| Runtime killed at any other point | Resumes at last committed offset, nothing repeated | Resumes at last saved state, nothing repeated |
| Broker unreachable | Fetch stalls, no writes, resumes when the broker returns | Produce fails, batch nacked, retried with backoff. Thirty consecutive failures stop the source with `Error` |
| Plugin returns an error | Same batch retried up to five times, 200 ms to 5 s backoff, offset unmoved. Fifth failure stops the sink with `Error` | A failed `poll()` is logged and polled again |
| Plugin reports success for a write it did not make | Data lost | Data lost |
| Destination unreachable | Plugin's own retries, then an error as above | Plugin's own retries inside `poll()` |
| Routing fails for a record with no `fallback` | Not applicable | Whole batch nacked and re-read |
| Topic creation fails | Not applicable | Batch nacked and re-read |
| File state store unwritable | Not applicable | Batch nacked, source marked `Error`, re-read on next poll |
| HTTP state store write outcome unknown | Not applicable | Write kept pending and retried with an idempotency key before the next batch. Batches rejected until it resolves |
| HTTP state store rejects the write permanently | Not applicable | Provider latches. Every batch rejected until the runtime restarts |
| Plugin panics | Process aborts. Resumes from the committed offset on restart | Process aborts. Resumes from the saved state on restart |

The last row is worth stating plainly. Panics do not cross the plugin boundary, so a plugin bug takes the runtime down rather than one connector. That is intended. A half-alive runtime is harder to reason about than a dead one, and it is why the runtime is meant to run under a supervisor.

## What a duplicate becomes

The runtime's contribution ends at "at least once". What the destination ends up holding depends on the sink.

<div class="pico-diagram">
<svg viewBox="0 30 720 210" width="720" role="img" aria-label="The same batch arrives twice at two kinds of sink. An upserting sink derives the row id from topic, partition and offset, so the second arrival rewrites the same rows. An append-only sink writes the batch a second time.">
  <defs>
    <marker id="arrdup" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="100" width="130" height="56" class="box-accent"/>
  <text x="85" y="124" text-anchor="middle" class="label">batch 100..149</text>
  <text x="85" y="142" text-anchor="middle" class="sub">delivered twice</text>
  <rect x="230" y="50" width="150" height="56" class="box"/>
  <text x="305" y="74" text-anchor="middle" class="label">upserting sink</text>
  <text x="305" y="92" text-anchor="middle" class="sub">id = topic:0:offset</text>
  <rect x="230" y="150" width="150" height="56" class="box"/>
  <text x="305" y="174" text-anchor="middle" class="label">append-only sink</text>
  <text x="305" y="192" text-anchor="middle" class="sub">no id to collide on</text>
  <rect x="460" y="50" width="240" height="56" class="box"/>
  <text x="580" y="74" text-anchor="middle" class="label">50 rows</text>
  <text x="580" y="92" text-anchor="middle" class="sub">second pass rewrites the same rows</text>
  <rect x="460" y="150" width="240" height="56" class="box"/>
  <text x="580" y="174" text-anchor="middle" class="label">100 rows</text>
  <text x="580" y="192" text-anchor="middle" class="sub">second pass appends again</text>
  <path d="M150 118 L222 82" class="edge" marker-end="url(#arrdup)"/>
  <path d="M150 138 L222 174" class="edge" marker-end="url(#arrdup)"/>
  <path d="M380 78 L452 78" class="edge" marker-end="url(#arrdup)"/>
  <path d="M380 178 L452 178" class="edge" marker-end="url(#arrdup)"/>
</svg>
</div>

Sinks that write to stores with primary keys derive a record identity from `topic:partition:offset`. It is stable across replays, since a redelivered record arrives at the same offset. They upsert on it, and the destination is indistinguishable from one that saw the batch once.

Sinks that write append-only cannot dedupe on the way in, and a replayed batch appears twice. Some of those destinations have their own tools, noted below. Where the destination offers nothing, consumers of it must tolerate a repeated batch.

| Sink | On replay |
| --- | --- |
| Postgres | Upsert on `topic:partition:offset`, no visible duplicate |
| Elasticsearch | Document `_id` from `topic:partition:offset`, no visible duplicate |
| MongoDB | `_id` from `topic:partition:offset`, no visible duplicate |
| Meilisearch | Primary key from `topic:partition:offset`, no visible duplicate |
| SurrealDB | Record id from `topic:partition:offset`, no visible duplicate |
| Redshift | `id` column from `topic:partition:offset`. Rows repeat unless deduplicated downstream |
| ClickHouse | Rows repeat. A `ReplacingMergeTree` keyed on the `pico_*` columns collapses them |
| InfluxDB | Points with identical measurement, tags and timestamp overwrite. Others repeat |
| Doris | Rows repeat. Stream Load labels are unique per attempt |
| Iceberg, Delta | Appended files repeat rows |
| S3 | A second object holding the same records |
| HTTP | The endpoint receives the batch again |
| Quickwit | Documents repeat |
| stdout | Printed again |

## What is not covered

- A record that reaches PicoMQ is durable in object storage before the source is acknowledged. Nothing here depends on a node staying up.
- Ordering within a topic is preserved through both loops. Every topic has one partition and each batch is a contiguous range from one topic.
- Ordering across topics is not promised. A source fanning one stream out across many topics produces them in whatever order delivery reports arrive.
- The guarantee stops at the plugin. A Postgres pooler with its own retry, an HTTP endpoint that returns 200 before persisting, an S3 lifecycle rule, none of these are visible to the runtime.

At-least-once is what the connectors give you. What the destination does with it is the destination's contract.
