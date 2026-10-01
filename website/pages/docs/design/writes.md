# Writes

An append is durable when the record is in the write-ahead log, which lives in object storage or in Postgres. There is no node-local disk in the write path, so a node that dies loses nothing that was acknowledged. The cost of that guarantee is one round trip to the WAL, and the write path is shaped around paying it once for many records instead of once per record.

## The append path

An append arrives at the stream's owning node as a `POST`. The node checks the producer's epoch and sequence against the registry entry, assigns the next offsets, and submits the record to the storage engine. Submission happens under a per-stream lock so the order of offsets is fixed, but the caller waits for durability outside the lock. Requests pipelined on the same stream stack up in the WAL and share the same flush.

Inside the engine the record goes to two places. It enters the log cache, which serves tail reads, and it enters the WAL, which makes it durable.

<div class="pico-diagram">
<svg viewBox="0 40 590 240" width="590" role="img" aria-label="An append is ordered under the stream gate, buffered into a WAL bulk with other records, written as one WAL write, and acknowledged when the write completes.">
  <defs>
    <marker id="arrw" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="60" width="130" height="56" class="box"/>
  <text x="85" y="84" text-anchor="middle" class="label">append</text>
  <text x="85" y="102" text-anchor="middle" class="sub">POST record</text>
  <rect x="210" y="60" width="140" height="56" class="box"/>
  <text x="280" y="84" text-anchor="middle" class="label">stream gate</text>
  <text x="280" y="102" text-anchor="middle" class="sub">order, offsets</text>
  <rect x="410" y="60" width="140" height="56" class="box"/>
  <text x="480" y="84" text-anchor="middle" class="label">WAL bulk</text>
  <text x="480" y="102" text-anchor="middle" class="sub">records batched</text>
  <rect x="410" y="200" width="140" height="56" class="box-accent"/>
  <text x="480" y="224" text-anchor="middle" class="label">WAL write</text>
  <text x="480" y="242" text-anchor="middle" class="sub">one PUT or one commit</text>
  <rect x="210" y="200" width="140" height="56" class="box"/>
  <text x="280" y="224" text-anchor="middle" class="label">ack</text>
  <text x="280" y="242" text-anchor="middle" class="sub">durable, in order</text>
  <path d="M150 88 L202 88" class="edge" marker-end="url(#arrw)"/>
  <path d="M350 88 L402 88" class="edge" marker-end="url(#arrw)"/>
  <path d="M480 116 L480 192" class="edge" marker-end="url(#arrw)"/>
  <path d="M410 228 L358 228" class="edge" marker-end="url(#arrw)"/>
  <text x="490" y="158" class="sub">group commit</text>
  <text x="85" y="180" text-anchor="middle" class="sub">tail also enters the</text>
  <text x="85" y="198" text-anchor="middle" class="sub">log cache for reads</text>
</svg>
</div>

## The WAL

Incoming records accumulate into a bulk, and each bulk becomes one write to the WAL: a `PUT` on the object store, or a committed `INSERT` in Postgres. Writes are pipelined, so several bulks can be in flight, but acknowledgements are delivered in submission order. A record is acknowledged only when its bulk and every bulk before it are stored.

Group commit is what makes this affordable. One write of a few hundred kilobytes contains every record that arrived while the previous write was in flight, so per-record cost drops as concurrency rises. A single append on an idle stream pays one round trip, tens of milliseconds on S3, a millisecond or two in Postgres.

Records are framed with a checksum and every WAL write carries the node's epoch. A node that lost its registration cannot extend its WAL past a takeover, which is part of the fencing described in [Streams](/docs/design/streams). The layout of each backend, the Postgres ring of tables, and what each survives are on the [Write-ahead log](/docs/design/wal) page.

## From WAL to committed objects

The WAL is a staging area, not the long-term layout. A background upload task drains sealed log cache blocks into read-optimized objects and commits them through the metadata log.

<div class="pico-diagram">
<svg viewBox="0 30 720 260" width="720" role="img" aria-label="Sealed cache blocks are written as stream-set or stream objects, committed through the metadata log, and the covered WAL objects are deleted.">
  <defs>
    <marker id="arrw2" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="150" height="60" class="box"/>
  <text x="95" y="106" text-anchor="middle" class="label">sealed block</text>
  <text x="95" y="124" text-anchor="middle" class="sub">records from cache</text>
  <rect x="220" y="80" width="200" height="60" class="box"/>
  <text x="320" y="106" text-anchor="middle" class="label">upload</text>
  <text x="320" y="124" text-anchor="middle" class="sub">stream-set or stream object</text>
  <rect x="470" y="80" width="180" height="60" class="box-accent"/>
  <text x="560" y="106" text-anchor="middle" class="label">commit</text>
  <text x="560" y="124" text-anchor="middle" class="sub">one metadata command</text>
  <rect x="470" y="210" width="180" height="60" class="box"/>
  <text x="560" y="236" text-anchor="middle" class="label">WAL objects</text>
  <text x="560" y="254" text-anchor="middle" class="sub">covered prefix deleted</text>
  <path d="M170 110 L212 110" class="edge" marker-end="url(#arrw2)"/>
  <path d="M420 110 L462 110" class="edge" marker-end="url(#arrw2)"/>
  <path d="M560 140 L560 202" class="edge" marker-end="url(#arrw2)"/>
  <text x="95" y="50" text-anchor="middle" class="sub">background</text>
  <text x="570" y="176" class="sub">advances end offsets</text>
</svg>
</div>

A block holding records from many streams becomes a stream-set object, with large per-stream runs split out into their own stream objects. Object ids are reserved through the metadata log before the upload, so a crashed upload leaves only an unreferenced id that expires. The commit itself is a single command that registers the objects, advances the end offset of every stream involved, and does so atomically. Readers either see none of the commit or all of it.

Once the commit is applied, the WAL objects it covers hold no unique data and are deleted. The WAL stays short, which bounds recovery time.

## Recovery

When a stream is opened after a crash, the engine lists the WAL objects under the previous session's prefix and replays the records past the last committed offset. Acknowledged records are recovered because acknowledgement required the WAL upload to complete. Records that were in flight but never acknowledged may be absent, which is the standard contract: an append without an acknowledgement was never promised.
