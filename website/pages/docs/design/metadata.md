# Metadata

Every change to cluster state is a command: register a node, create a stream, open it at a new epoch, commit an object, record a transfer, delete a key. Commands are small binary values with a versioned encoding. Nothing writes cluster state any other way, so the command set is the complete list of things that can happen to a cluster.

## The log

Commands are stored in one SQL table, an ordered log of `(idx, payload)` rows. Appending is optimistic: a writer reads the last index it knows, inserts at the next one, and retries at a higher index if another writer took the slot. The database's uniqueness guarantee on `idx` is the only coordination primitive in the system.

Proposals from one process are group committed. A flusher task drains the propose queue, packs up to `256` commands into a single row, and appends them together. Under load this collapses many proposals into one insert per round trip.

<div class="pico-diagram">
<svg viewBox="0 22 650 248" width="650" role="img" aria-label="A proposed command goes through a queue to the flusher, which appends a batch row to the SQL log. The tailer fetches rows, applies them to state, publishes a view, and delivers the result to the proposer.">
  <defs>
    <marker id="arrm" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="40" width="130" height="52" class="box"/>
  <text x="85" y="63" text-anchor="middle" class="label">propose</text>
  <text x="85" y="80" text-anchor="middle" class="sub">one command</text>
  <rect x="210" y="40" width="130" height="52" class="box"/>
  <text x="275" y="63" text-anchor="middle" class="label">flusher</text>
  <text x="275" y="80" text-anchor="middle" class="sub">group commit</text>
  <rect x="400" y="40" width="150" height="52" class="box-accent"/>
  <text x="475" y="63" text-anchor="middle" class="label">log row idx n</text>
  <text x="475" y="80" text-anchor="middle" class="sub">up to 256 commands</text>
  <rect x="400" y="180" width="150" height="52" class="box"/>
  <text x="475" y="203" text-anchor="middle" class="label">tailer</text>
  <text x="475" y="220" text-anchor="middle" class="sub">fetch, apply</text>
  <rect x="210" y="180" width="130" height="52" class="box"/>
  <text x="275" y="203" text-anchor="middle" class="label">view</text>
  <text x="275" y="220" text-anchor="middle" class="sub">applied index n</text>
  <rect x="20" y="180" width="130" height="52" class="box"/>
  <text x="85" y="203" text-anchor="middle" class="label">result</text>
  <text x="85" y="220" text-anchor="middle" class="sub">back to proposer</text>
  <path d="M150 66 L202 66" class="edge" marker-end="url(#arrm)"/>
  <path d="M340 66 L392 66" class="edge" marker-end="url(#arrm)"/>
  <path d="M475 92 L475 172" class="edge" marker-end="url(#arrm)"/>
  <path d="M400 206 L348 206" class="edge" marker-end="url(#arrm)"/>
  <path d="M210 206 L158 206" class="edge" marker-end="url(#arrm)"/>
  <text x="484" y="136" class="sub">SQL append, then fetch</text>
  <text x="357" y="250" text-anchor="middle" class="sub">publish first, then deliver</text>
</svg>
</div>

A proposal returns only after the local tailer has applied its row. The view is published before the result is delivered, so a caller that gets an answer is guaranteed to see its own write in the next view load.

## Applying commands

A tailer task on every node fetches rows past its applied index and applies each command to an in-memory state. Apply is a pure function: the same log always produces the same state on every node, with no clocks and no node-local input.

Apply is also where the rules are enforced. Each command validates against the current state and either mutates it or returns an error to the proposer. Duplicate work is answered with a distinct redundant result, which makes retries safe. Commands that include a node identity are checked against the node's registered epoch, so a restarted node at a higher epoch invalidates everything still in flight from its predecessor.

The state is built from persistent maps. Cloning it for a published view is cheap structural sharing, not a copy, so publishing a new view per applied row costs little even with large state.

## Views

Readers never query SQL. Each node holds one immutable view, swapped atomically by the tailer, holding the state and the applied index. Anything answering requests loads the current view without locks. Code that needs to observe a future write waits for the applied index to reach a target instead of polling the database.

This is what makes a slow metadata database tolerable. Reads keep their latency regardless of the database, and only new commands wait.

## Snapshots

The log would otherwise grow without bound, so a node periodically encodes the whole state into a single snapshot row and truncates the log below it. There is one snapshot row per cluster, not an archive. Any node may run the cycle, whichever crosses the interval first.

The cycle runs on its own task, never on the apply path: it forks the latest published view (an O(1) clone of the persistent maps) and encodes off the async workers, so appliers and readers never wait on a snapshot regardless of state size. A snapshot is due when `1024` rows have accumulated since the last one and a minimum interval (default 30 s) has elapsed, so a busy cluster is not re-shipping its full state every few seconds.

<div class="pico-diagram">
<svg viewBox="-7 15 707 188" width="707" role="img" aria-label="A snapshot row covers the log prefix, which is truncated. A cold start loads the snapshot and replays the remaining tail.">
  <defs>
    <marker id="arrs" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="40" y="60" width="150" height="56" class="box-accent"/>
  <text x="115" y="85" text-anchor="middle" class="label">snapshot</text>
  <text x="115" y="102" text-anchor="middle" class="sub">state at idx 2048</text>
  <rect x="250" y="60" width="46" height="56" class="box"/>
  <rect x="300" y="60" width="46" height="56" class="box"/>
  <rect x="350" y="60" width="46" height="56" class="box"/>
  <rect x="400" y="60" width="46" height="56" class="box"/>
  <text x="273" y="93" text-anchor="middle" class="sub">2049</text>
  <text x="323" y="93" text-anchor="middle" class="sub">2050</text>
  <text x="373" y="93" text-anchor="middle" class="sub">2051</text>
  <text x="423" y="93" text-anchor="middle" class="sub">2052</text>
  <text x="348" y="44" text-anchor="middle" class="sub">remaining log tail</text>
  <text x="115" y="44" text-anchor="middle" class="sub">one row, replaces idx 1 to 2048</text>
  <rect x="530" y="60" width="150" height="56" class="box"/>
  <text x="605" y="85" text-anchor="middle" class="label">cold start</text>
  <text x="605" y="102" text-anchor="middle" class="sub">decode + replay tail</text>
  <path d="M190 88 L242 88" class="edge" marker-end="url(#arrs)"/>
  <path d="M446 88 L522 88" class="edge" marker-end="url(#arrs)"/>
  <path d="M115 160 L115 124" class="edge" marker-end="url(#arrs)"/>
  <text x="115" y="180" text-anchor="middle" class="sub">written every 1024 applied rows</text>
</svg>
</div>

A starting node decodes the snapshot and replays the tail, so recovery time is bounded by the interval rather than the cluster's history. A node that lags so far behind that its next row was truncated detects the gap and reinstalls from the newer snapshot instead of continuing from a fork. An empty log tail is ambiguous — nothing new, or the tail was folded into a snapshot — so a tailer that fetches nothing also checks the stored snapshot index and reinstalls when it has fallen behind it.

A log row that fails to decode is treated as unrecoverable. Skipping it would silently fork that node from every other reader, so the sink stops, fails all waiting proposals, and the node stops accepting metadata writes.

## Growth bounds

Snapshots bound the log. Two more mechanisms bound the state itself.

Each node pages through the object catalog in the background and triggers a compaction pass for any stream holding more than `64` live objects. Streams open on another node are ignored by the engine, so no ownership tracking is needed. The catalog stays proportional to live streams, not to write history.

Deleted and compacted objects queue in a FIFO until the maintenance lease holder removes them from object storage. `/admin/cluster` reports the backlog depth and head sequence. A head that does not advance while the depth grows means the cleaner is stuck.
