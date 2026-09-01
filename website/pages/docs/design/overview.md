# Overview

A PicoMQ deployment has three components. Nodes serve clients and hold only caches. An object store holds every record. A SQL database holds the metadata log that coordinates the nodes.

## Anatomy of a node

Each node runs the same stack. An HTTP listener speaks the Pico protocol or Durable Streams, and a Kafka listener serves Kafka clients. An admin listener serves the admin API and the dashboard. Behind them, an ownership router decides whether this node serves a stream or redirects, the stream service manages the registry of names and per-stream state, and the `s3stream` engine moves records to and from object storage.

<div class="pico-diagram">
<svg viewBox="60 0 620 420" width="620" role="img" aria-label="A node runs listeners, routing, the stream service, and the engine. The engine writes to object storage. The service proposes commands to the SQL metadata log.">
  <defs>
    <marker id="arr2" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="150" y="20" width="420" height="252" fill="none" class="edge-soft"/>
  <text x="166" y="42" class="sub">pico node</text>
  <rect x="180" y="56" width="180" height="52" class="box"/>
  <text x="270" y="79" text-anchor="middle" class="label">protocol listener</text>
  <text x="270" y="96" text-anchor="middle" class="sub">Pico or Durable Streams, plus Kafka</text>
  <rect x="380" y="56" width="160" height="52" class="box"/>
  <text x="460" y="79" text-anchor="middle" class="label">admin listener</text>
  <text x="460" y="96" text-anchor="middle" class="sub">API + dashboard</text>
  <rect x="180" y="136" width="160" height="52" class="box"/>
  <text x="260" y="159" text-anchor="middle" class="label">ownership router</text>
  <text x="260" y="176" text-anchor="middle" class="sub">serve here or 307</text>
  <rect x="380" y="136" width="160" height="52" class="box"/>
  <text x="460" y="159" text-anchor="middle" class="label">stream service</text>
  <text x="460" y="176" text-anchor="middle" class="sub">registry, sessions</text>
  <rect x="280" y="204" width="160" height="52" class="box-accent"/>
  <text x="360" y="227" text-anchor="middle" class="label">s3stream engine</text>
  <text x="360" y="244" text-anchor="middle" class="sub">WAL, objects, caches</text>
  <rect x="80" y="330" width="240" height="70" class="box"/>
  <text x="200" y="356" text-anchor="middle" class="label">SQL database</text>
  <text x="200" y="374" text-anchor="middle" class="sub">command log, snapshot, lease</text>
  <rect x="420" y="330" width="240" height="70" class="box"/>
  <text x="540" y="356" text-anchor="middle" class="label">object storage</text>
  <text x="540" y="374" text-anchor="middle" class="sub">WAL objects, data objects</text>
  <path d="M270 108 L264 128" class="edge" marker-end="url(#arr2)"/>
  <path d="M340 162 L372 162" class="edge" marker-end="url(#arr2)"/>
  <path d="M448 188 L400 196" class="edge" marker-end="url(#arr2)"/>
  <path d="M320 272 L240 322" class="edge" marker-end="url(#arr2)"/>
  <path d="M400 260 L500 322" class="edge" marker-end="url(#arr2)"/>
  <text x="238" y="302" text-anchor="end" class="sub">metadata commands</text>
  <text x="490" y="302" text-anchor="start" class="sub">records</text>
</svg>
</div>

The engine is the only writer of record data. It appends to a write-ahead log on object storage for durability, batches records into larger data objects in the background, and serves reads from caches when it can.

## The metadata log

All cluster state changes are commands: register a node, create a stream, open it, commit an object, transfer ownership. A node proposes a command to the SQL database, where an ordered log table assigns it a position. Every node tails that table and applies each command to an in-memory state, so all nodes converge on the same view without talking to each other.

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

Stream names look like URL paths and any node accepts any request. The receiving node checks the view. If the stream is open on another node the client gets a `307` redirect to its advertised address. If it is unowned or local the node serves it directly. Epoch fencing backs this up: a node that lost its registration cannot commit anything, so a stale redirect can waste a hop but never split a stream.

## Background work

A few loops run outside the request path. Every node tails the metadata log and watches for stream transfers involving it. One node at a time holds a SQL lease and runs maintenance: expiring abandoned object uploads and deleting destroyed objects from storage. Any node crossing a snapshot interval writes the next snapshot. None of these tasks are special roles, whichever node holds the lease does the work and losing it just moves the work elsewhere.
