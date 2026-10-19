# Reads

Reads are served by the partition's owning node and answered from the fastest tier that holds the data. A consumer following the tail almost never touches the object store. A consumer replaying history streams out of it through a readahead cache.

Every read is bounded by the stream's confirm offset, the highest offset whose WAL upload has completed. A record becomes readable at the same moment it becomes durable, never before, so a reader cannot observe data that a crash could take back.

## The tiers

A read at offset `n` walks down until something answers.

<div class="pico-diagram">
<svg viewBox="0 22 720 262" width="720" role="img" aria-label="A read checks the tail cache, then the log cache, then the block cache, and finally fetches from the object store with readahead.">
  <defs>
    <marker id="arrr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="40" width="190" height="56" class="box"/>
  <text x="115" y="64" text-anchor="middle" class="label">read at n</text>
  <text x="115" y="82" text-anchor="middle" class="sub">bounded by confirm offset</text>
  <rect x="270" y="40" width="200" height="56" class="box"/>
  <text x="370" y="64" text-anchor="middle" class="label">tail cache</text>
  <text x="370" y="82" text-anchor="middle" class="sub">recent records, per stream</text>
  <rect x="270" y="130" width="200" height="56" class="box"/>
  <text x="370" y="154" text-anchor="middle" class="label">log cache</text>
  <text x="370" y="172" text-anchor="middle" class="sub">not yet uploaded blocks</text>
  <rect x="270" y="220" width="200" height="56" class="box"/>
  <text x="370" y="244" text-anchor="middle" class="label">block cache</text>
  <text x="370" y="262" text-anchor="middle" class="sub">pages of committed objects</text>
  <rect x="530" y="220" width="170" height="56" class="box-accent"/>
  <text x="615" y="244" text-anchor="middle" class="label">object store</text>
  <text x="615" y="262" text-anchor="middle" class="sub">GET with readahead</text>
  <path d="M210 68 L262 68" class="edge" marker-end="url(#arrr)"/>
  <path d="M370 96 L370 122" class="edge" marker-end="url(#arrr)"/>
  <path d="M370 186 L370 212" class="edge" marker-end="url(#arrr)"/>
  <path d="M470 248 L522 248" class="edge" marker-end="url(#arrr)"/>
  <text x="380" y="115" class="sub">miss</text>
  <text x="380" y="205" class="sub">miss</text>
  <text x="480" y="240" class="sub">miss</text>
</svg>
</div>

The tail cache holds the most recently appended records of each stream in memory on the serving node, so a consumer keeping up with a producer reads without entering the engine at all. Behind it, the log cache holds every record that has not yet been uploaded into committed objects, which covers readers that are seconds behind. Both are filled by the write path, not by reads.

The block cache is the cold path. It holds fixed-size pages of committed objects, fetched with one `GET` per page no matter how many readers wait on it, and evicted by LRU with a TTL. A sequential reader gets a readahead window that grows as it keeps reading, so replay throughput approaches raw object store throughput after the first few requests.

Locating the right objects requires no listing. The metadata state indexes every committed object by stream and offset range, so "objects covering stream S from offset a" is a bounded lookup in the node's local view.

## Waiting at the tail

A consumer that has read everything should not busy-poll. A read that arrives at the confirm offset returns immediately with an empty body and an up-to-date marker, and the client can instead ask to wait.

With long polling the request parks on a per-stream waiter until new data is durable, the stream closes, or the timeout lapses, `25` seconds by default. The waiter is woken by the write path at the moment an append's WAL upload completes, so delivery latency is the durability latency and nothing more.

The same waiter feeds SSE. A client that accepts `text/event-stream` gets each new record as an event on a held-open response, with the connection recycled after `55` seconds so proxies and load balancers never see an idle connection they might kill. The client reconnects from its last offset without losing anything, because offsets, not the connection, track position.

A Kafka fetch with a wait time parks on the same waiter. When a node shuts down it ends every waiting read with `503 unavailable`, or Kafka `NOT_LEADER_OR_FOLLOWER`, so clients reconnect to a live node instead of holding the stopping one open, see [Failover](/docs/design/failover#clean-shutdown).

## Reading through a move

A read reaching a node that does not own the partition follows the rules in [Ownership and routing](/docs/design/ownership): a `307` to the owner over HTTP, `NOT_LEADER_OR_FOLLOWER` over Kafka. When a partition moves, through a transfer or failover, offsets are stable across the move, so a consumer resumes on the new owner from exactly where it left off. The new owner's caches start cold, and the first reads after a move pay object store latency until the tail warms up again.
