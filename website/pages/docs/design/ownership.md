# Ownership and routing

Each internal stream is served by exactly one node at a time. Single ownership is what keeps the rest of the design simple: offsets are assigned in one place, the tail caches are coherent because only the owner writes, and appends never coordinate across nodes. The work splits into two problems, deciding who owns a stream and getting requests to that node.

## Node identity

A node has a stable id, set in its configuration, and an epoch for each process that runs it. On startup the node registers through the metadata log with its id, a requested epoch taken from the wall clock, its advertised addresses, its slot count, and where its [write-ahead log](/docs/design/wal) lives.

The log assigns the epoch: the requested one, or one past the stored epoch when that is not higher. Every registration therefore fences the incarnations before it. A replaced process that is still running finds its commands rejected and stops, and two processes claiming one node id resolve to whichever registered last.

Every command a node proposes carries its id and epoch, see [Metadata](/docs/design/metadata). How a node is declared dead and fenced is in [Failover](/docs/design/failover).

## Placement

A stream needs an owner when its partition is created, or when its owner is gone. Placement is a metadata command, and applying it is deterministic: the stream goes to the live node with the lowest ratio of streams to slots, ties going to the lowest node id. Fenced nodes and nodes with `0` slots take nothing.

Slots are a capacity weight, `1` by default, so a node with `4` slots takes four times the streams of a node with `1`. They can be changed at runtime through the admin API, which shifts where future placements land without moving anything already placed. A placement can also name a node, which Kafka `CreatePartitions` uses for explicit replica assignments.

Because the choice is a pure function of the replicated state, every node computes the same answer. Two nodes racing to place one stream propose two commands, the log orders them, and the second changes nothing.

## Routing

Any node accepts any request. The receiving node finds the partition's internal stream and checks its local view, in order.

<div class="pico-diagram">
<svg viewBox="0 0 660 340" width="660" role="img" aria-label="A request is checked against the view: unregistered logs are served locally, pending transfers to a live node route to the target, streams opened or placed on a live node route to the owner, and streams whose owner is fenced or never placed are served locally.">
  <defs>
    <marker id="arro" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="132" width="170" height="56" class="box"/>
  <text x="105" y="156" text-anchor="middle" class="label">request</text>
  <text x="105" y="174" text-anchor="middle" class="sub">check the local view</text>
  <rect x="290" y="20" width="170" height="48" class="box"/>
  <text x="375" y="49" text-anchor="middle" class="sub">log not registered</text>
  <rect x="290" y="88" width="170" height="48" class="box"/>
  <text x="375" y="117" text-anchor="middle" class="sub">transfer to a live node</text>
  <rect x="290" y="156" width="170" height="48" class="box"/>
  <text x="375" y="185" text-anchor="middle" class="sub">on a live node</text>
  <rect x="290" y="224" width="170" height="48" class="box"/>
  <text x="375" y="253" text-anchor="middle" class="sub">owner fenced or none</text>
  <rect x="510" y="20" width="130" height="48" class="box-accent"/>
  <text x="575" y="49" text-anchor="middle" class="label">serve here</text>
  <rect x="510" y="88" width="130" height="48" class="box"/>
  <text x="575" y="112" text-anchor="middle" class="label">307</text>
  <text x="575" y="128" text-anchor="middle" class="sub">to target</text>
  <rect x="510" y="156" width="130" height="48" class="box"/>
  <text x="575" y="180" text-anchor="middle" class="label">307</text>
  <text x="575" y="196" text-anchor="middle" class="sub">to owner</text>
  <rect x="510" y="224" width="130" height="48" class="box-accent"/>
  <text x="575" y="253" text-anchor="middle" class="label">serve here</text>
  <path d="M190 150 L282 48" class="edge" marker-end="url(#arro)"/>
  <path d="M190 156 L282 114" class="edge" marker-end="url(#arro)"/>
  <path d="M190 164 L282 182" class="edge" marker-end="url(#arro)"/>
  <path d="M190 170 L282 246" class="edge" marker-end="url(#arro)"/>
  <path d="M460 44 L502 44" class="edge" marker-end="url(#arro)"/>
  <path d="M460 112 L502 112" class="edge" marker-end="url(#arro)"/>
  <path d="M460 180 L502 180" class="edge" marker-end="url(#arro)"/>
  <path d="M460 248 L502 248" class="edge" marker-end="url(#arro)"/>
  <text x="330" y="310" text-anchor="middle" class="sub">a stream owned by this node is always served here</text>
</svg>
</div>

| The stream is | The request |
| --- | --- |
| Not registered yet | Is served here, which is how a create starts |
| In a transfer to a live node | Redirects to the target, see [Transfers](/docs/design/transfers) |
| Opened, or placed, on a live node | Redirects to that node with a `307` to its advertised address |
| Owned by a fenced node, or by no node | Is served here, and opening it moves it here |

Kafka has no redirects. A Kafka client asks for metadata, which names each partition's owner as its leader, and sends requests there. A request that reaches the wrong node answers `NOT_LEADER_OR_FOLLOWER`, and the client refreshes its metadata.

## Why a stale view is safe

Routing reads the node's local view, which can lag the log by a moment. That is fine because routing is only a performance decision. Correctness comes from fencing: opening a stream bumps its epoch through the log, and only the current owner may commit to it, close it, trim it, or delete it. The worst a stale view produces is an extra redirect while the views catch up. It can misdirect a request, it cannot produce two writers.
