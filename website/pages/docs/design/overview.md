# Overview

A PicoMQ deployment has three components. Nodes serve clients and hold only caches. An object store holds the data objects. A SQL database holds the metadata log that coordinates the nodes. The write-ahead log lives in either the object store or the SQL database.

| Component | Holds | Options |
| --- | --- | --- |
| Node | Caches | `pico serve` process, or a background worker inside Postgres |
| Object store | Data objects, WAL by default | S3 and compatible stores, local filesystem |
| SQL database | Metadata log, optionally the WAL | Postgres, SQLite for a single node |

## Anatomy of a node

Each node runs the same stack. An HTTP listener speaks the Pico protocol or Durable Streams, a Kafka listener serves Kafka clients, and an admin listener serves the admin API and the dashboard. Behind them:

- The ownership router decides whether this node serves a partition or redirects, see [Ownership and routing](/docs/design/ownership).
- The topic layer maps names to topics, partitions, and logs, see [Topics and streams](/docs/design/streams), and the group coordinator runs [consumer groups](/docs/design/groups).
- The `s3stream` engine moves records to and from storage.

The same stack runs as a process or as the [Postgres extension](/docs/operations/deployment/postgres).

<div class="pico-diagram">
<svg viewBox="0 0 559 434" width="559" role="img" aria-label="A node runs listeners, routing, topics and groups, and the engine. The engine writes to object storage. Topics and groups propose commands to the SQL metadata log.">
  <defs>
    <marker id="arr2" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(34 6)">
    <rect x="-14" y="14" width="519" height="288" class="edge-soft"/>
<text x="-4" y="30" class="sub">pico node</text>
    <rect x="0" y="40" width="216" height="56" class="box"/>
<text x="108" y="64" text-anchor="middle" class="label">protocol listener</text>
<text x="108" y="82" text-anchor="middle" class="sub">Pico, Durable Streams, Kafka</text>
    <rect x="276" y="40" width="216" height="56" class="box"/>
<text x="383" y="64" text-anchor="middle" class="label">admin listener</text>
<text x="383" y="82" text-anchor="middle" class="sub">API, dashboard</text>
    <rect x="0" y="136" width="216" height="56" class="box"/>
<text x="108" y="160" text-anchor="middle" class="label">ownership router</text>
<text x="108" y="178" text-anchor="middle" class="sub">serve here or 307</text>
    <rect x="276" y="136" width="216" height="56" class="box"/>
<text x="383" y="160" text-anchor="middle" class="label">topics and groups</text>
<text x="383" y="178" text-anchor="middle" class="sub">partitions, logs, offsets</text>
    <rect x="0" y="232" width="216" height="56" class="box-accent"/>
<text x="108" y="256" text-anchor="middle" class="label">s3stream engine</text>
<text x="108" y="274" text-anchor="middle" class="sub">WAL, objects, caches</text>
    <rect x="276" y="352" width="216" height="56" class="box"/>
<text x="383" y="376" text-anchor="middle" class="label">SQL database</text>
<text x="383" y="394" text-anchor="middle" class="sub">command log, lease, optional WAL</text>
    <rect x="0" y="352" width="216" height="56" class="box"/>
<text x="108" y="376" text-anchor="middle" class="label">object storage</text>
<text x="108" y="394" text-anchor="middle" class="sub">data objects, WAL by default</text>
    <path d="M108 96 L108 132" class="edge" marker-end="url(#arr2)"/>
    <path d="M216 164 L272 164" class="edge" marker-end="url(#arr2)"/>
    <path d="M306 192 L306 260 L220 260" class="edge" marker-end="url(#arr2)"/>
    <path d="M108 288 L108 348" class="edge" marker-end="url(#arr2)"/>
    <path d="M383 192 L383 348" class="edge" marker-end="url(#arr2)"/>
    <text x="116" y="331" text-anchor="start" class="sub">records</text>
    <text x="391" y="331" text-anchor="start" class="sub">metadata commands</text>
    <text x="246" y="156" text-anchor="middle" class="sub">owner</text>
  </g>
</svg>
</div>

The engine is the only writer of record data. It appends to a [write-ahead log](/docs/design/wal) for durability, batches records into larger data objects in the background, and serves reads from caches when it can.

## The metadata log

All cluster state changes are commands, such as registering a node, opening a stream, committing an object, or fencing a dead node. The full list is in [Metadata](/docs/design/metadata). A node proposes a command to the SQL database, where an ordered log table assigns it a position. Every node tails that table and applies each command to an in-memory state, so all nodes converge on the same view without talking to each other.

<div class="pico-diagram">
<svg viewBox="10 10 710 280" width="710" role="img" aria-label="Nodes propose commands to the SQL log. The log orders them. All nodes tail the log and apply commands to the same in-memory state.">
  <defs>
    <marker id="arr3" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="30" y="30" width="150" height="56" class="box-accent"/>
  <text x="105" y="55" text-anchor="middle" class="label">node 1</text>
  <text x="105" y="72" text-anchor="middle" class="sub">propose + tail</text>
  <rect x="30" y="214" width="150" height="56" class="box-accent"/>
  <text x="105" y="239" text-anchor="middle" class="label">node 2</text>
  <text x="105" y="256" text-anchor="middle" class="sub">propose + tail</text>
  <rect x="290" y="112" width="270" height="76" class="box"/>
  <text x="425" y="136" text-anchor="middle" class="label">command log</text>
  <rect x="308" y="148" width="38" height="26" class="box-accent"/>
  <rect x="350" y="148" width="38" height="26" class="box-accent"/>
  <rect x="392" y="148" width="38" height="26" class="box-accent"/>
  <rect x="434" y="148" width="38" height="26" class="box-accent"/>
  <rect x="476" y="148" width="38" height="26" class="box-accent"/>
  <text x="327" y="165" text-anchor="middle" class="sub">17</text>
  <text x="369" y="165" text-anchor="middle" class="sub">18</text>
  <text x="411" y="165" text-anchor="middle" class="sub">19</text>
  <text x="453" y="165" text-anchor="middle" class="sub">20</text>
  <text x="495" y="165" text-anchor="middle" class="sub">21</text>
  <rect x="620" y="112" width="80" height="76" class="box"/>
  <text x="660" y="144" text-anchor="middle" class="label">state</text>
  <text x="660" y="162" text-anchor="middle" class="sub">applied</text>
  <text x="660" y="176" text-anchor="middle" class="sub">index 21</text>
  <path d="M180 70 L282 128" class="edge" marker-end="url(#arr3)"/>
  <path d="M180 230 L282 172" class="edge" marker-end="url(#arr3)"/>
  <path d="M282 140 L188 82" class="edge-soft" marker-end="url(#arr3)"/>
  <path d="M282 160 L188 218" class="edge-soft" marker-end="url(#arr3)"/>
  <path d="M560 150 L612 150" class="edge" marker-end="url(#arr3)"/>
  <text x="216" y="106" class="sub">propose</text>
  <text x="216" y="206" class="sub">tail + apply</text>
  <text x="586" y="140" text-anchor="middle" class="sub">apply</text>
</svg>
</div>

The database provides the ordering, which is the property a consensus protocol would otherwise supply. Applying a command is deterministic, so replaying the log always produces the same state. Every `1024` applied commands a node writes a snapshot row and the log below it can be truncated, which keeps replay on startup short. The position of the last applied command is the applied index seen in the admin API.

The applied state answers every routing and placement question: which node owns a stream, which objects exist, which transfers are in flight. Nodes read it from an immutable in-memory view, never from SQL directly, so reads cost nothing and a slow database only delays new commands.

## Request routing

Any node accepts any request. A request for a partition owned elsewhere gets a `307` redirect to its owner over HTTP, or `NOT_LEADER_OR_FOLLOWER` over Kafka. Fencing backs this up, so a stale view can cost a hop but never split a stream. The rules are in [Ownership and routing](/docs/design/ownership).

## Background work

A few loops run outside the request path. Some run on every node, the rest only on the node holding the SQL [lease](/docs/design/leases). None of them is a special role: whichever node holds the lease does that work, and losing it moves the work elsewhere.

| Loop | Runs on | Does |
| --- | --- | --- |
| Tailer | Every node | Applies the metadata log and publishes views |
| Heartbeat | Every node | Keeps the node live, and stops it once it is fenced |
| Transfer watcher | Every node | Hands off or takes over streams in a [transfer](/docs/design/transfers) |
| Deleted cleanup | Every node | Destroys owned streams whose topic was deleted |
| Compaction check | Every node | Starts compaction for streams with too many objects |
| Snapshot | Whichever node crosses the interval | Writes the next [snapshot](/docs/design/metadata#snapshots) |
| Failover | Lease holder | Fences silent nodes and moves their streams, see [Failover](/docs/design/failover) |
| Object expiry and cleaner | Lease holder | Reclaims abandoned uploads and deletes destroyed objects, see [Garbage collection](/docs/design/gc) |
| TTL sweep | Lease holder | Deletes topics whose TTL lapsed |
| Token expiry | Lease holder | Removes expired tokens |
