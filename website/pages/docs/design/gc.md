# Garbage collection

Records land in object storage and stay there until something says otherwise, so every path that makes data unreachable needs a matching path that reclaims the space. PicoMQ separates the two: making data unreachable is a metadata change, reclaiming space is a background job. Nothing in the request path ever waits on a delete.

## Where garbage comes from

Trimming a stream advances its start offset, whether by an explicit trim, `DeleteRecords`, or [retention](/docs/design/streams#retention). The trim itself marks the stream's single-stream objects that end below the new start for destruction. Its data inside stream-set objects is dropped when the node's compaction next rewrites them. An object that straddles the boundary is kept, since it still holds live records past the trim point.

<div class="pico-diagram">
<svg viewBox="0 20 660 190" width="660" role="img" aria-label="A trim to offset 900 destroys the object entirely below it and keeps the object straddling the boundary.">
  <defs>
    <marker id="arrg" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="40" y="90" width="170" height="48" fill="none" class="edge-soft"/>
  <text x="125" y="112" text-anchor="middle" class="sub">object, 0 to 500</text>
  <text x="125" y="128" text-anchor="middle" class="sub">destroyed</text>
  <rect x="230" y="90" width="190" height="48" class="box"/>
  <text x="325" y="112" text-anchor="middle" class="sub">object, 500 to 1200</text>
  <text x="325" y="128" text-anchor="middle" class="sub">kept, straddles the trim</text>
  <rect x="440" y="90" width="180" height="48" class="box"/>
  <text x="530" y="112" text-anchor="middle" class="sub">object, 1200 to 2000</text>
  <text x="530" y="128" text-anchor="middle" class="sub">kept</text>
  <path d="M298 60 L298 90" class="edge" marker-end="url(#arrg)"/>
  <text x="298" y="48" text-anchor="middle" class="sub">trim, start offset 900</text>
  <text x="330" y="180" text-anchor="middle" class="sub">reads below 900 are gone, the straddling bytes wait for compaction</text>
</svg>
</div>

Deleting a topic, or the lease holder expiring one by its TTL, removes its keys first. The owner of each internal stream then deletes it, which marks all of its objects, so space is reclaimed shortly after the name is gone. Compaction rewrites small objects into larger ones and marks the originals. An object id that was reserved for an upload that never committed expires after its TTL and is marked as well.

WAL objects are the one exception. Their writer deletes them once a commit covers them, as described in [Writes](/docs/design/writes), and after [failover](/docs/design/failover) the lease holder does the same for the dead node's WAL.

## Two-phase deletion

Marking and deleting are separate steps with the metadata log between them.

The mark is part of whatever command created the garbage. Applying a trim, a delete, or a compaction commit atomically removes the objects from the live indexes and appends them to a destruction queue in the replicated state. From that moment no reader can reach them, but the bytes still exist.

The clean runs on the lease holder, described in [Leases](/docs/design/leases). Each pass peeks a batch from the queue, deletes those objects from storage, and proposes a command that removes them from the queue and the catalog. Objects whose mark says their data is shared by a newer composite object are dropped from the catalog without touching storage.

<div class="pico-diagram">
<svg viewBox="0 25 700 165" width="700" role="img" aria-label="A metadata command marks objects into a destruction queue. The lease holder deletes them from storage and proposes the clean that removes them from the catalog.">
  <defs>
    <marker id="arrg2" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="70" width="160" height="56" class="box"/>
  <text x="100" y="94" text-anchor="middle" class="label">mark</text>
  <text x="100" y="112" text-anchor="middle" class="sub">trim, delete, compact</text>
  <rect x="240" y="70" width="170" height="56" class="box-accent"/>
  <text x="325" y="94" text-anchor="middle" class="label">destruction queue</text>
  <text x="325" y="112" text-anchor="middle" class="sub">in replicated state</text>
  <rect x="470" y="70" width="210" height="56" class="box"/>
  <text x="575" y="94" text-anchor="middle" class="label">cleaner, lease holder</text>
  <text x="575" y="112" text-anchor="middle" class="sub">delete bytes, then clean</text>
  <path d="M180 98 L232 98" class="edge" marker-end="url(#arrg2)"/>
  <path d="M410 98 L462 98" class="edge" marker-end="url(#arrg2)"/>
  <text x="350" y="170" text-anchor="middle" class="sub">the clean command removes the batch from queue and catalog</text>
</svg>
</div>

The ordering matters. Bytes are deleted from storage before the catalog entry goes away, so a crash between the two steps leaves an entry pointing at a deleted object, and the next pass simply cleans it again. The reverse order could leak bytes forever. Every step is idempotent, so retries and duplicate passes are harmless.

## Compaction

Left alone, a busy node produces a steady stream of small objects, and reading history through thousands of them costs a request each. Compaction runs on each node for the streams it owns, merging small committed objects into fewer, larger, stream-major ones. The rewrite is committed through the metadata log like any other object change: new objects in, old objects marked, one atomic step.

Compaction also completes what trim starts. The straddling object a trim left behind is rewritten without its dead prefix on the next compaction round, which is when those bytes are actually reclaimed.
