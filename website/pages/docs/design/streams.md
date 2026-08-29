# Streams

A stream in PicoMQ has two layers. Clients see a named stream with a content type, an optional TTL, and records at consecutive offsets. Underneath it sits an internal stream, a numeric id with an epoch, an offset range, and a set of objects. The named layer handles the API surface. The internal layer handles durability and ownership.

## Names and the registry

Stream names are paths, such as `logs/api/prod`. Each name maps to a registry entry stored in the metadata KV, which is part of the replicated state described in [Metadata](/docs/design/metadata). The entry records the internal stream id, the content type, TTL and expiry, whether the stream is closed, and the state of each idempotent producer.

Creating a stream writes two things through the metadata log. First an internal stream is allocated, then a registry entry is stored under the name with the new stream id. Create is idempotent. Creating a name that already exists returns the existing stream unchanged, and two nodes racing to create the same name settle through the log, with the loser adopting the winner's entry.

<div class="pico-diagram">
<svg viewBox="0 32 690 258" width="690" role="img" aria-label="A stream name maps to a registry entry in the metadata KV, which points at an internal stream row, which indexes objects in the object store.">
  <defs>
    <marker id="arrst" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="90" width="160" height="60" class="box"/>
  <text x="100" y="116" text-anchor="middle" class="label">name</text>
  <text x="100" y="134" text-anchor="middle" class="sub">logs/api/prod</text>
  <rect x="230" y="90" width="200" height="60" class="box"/>
  <text x="330" y="116" text-anchor="middle" class="label">registry entry</text>
  <text x="330" y="134" text-anchor="middle" class="sub">id, content type, producers</text>
  <rect x="470" y="90" width="200" height="60" class="box-accent"/>
  <text x="570" y="116" text-anchor="middle" class="label">internal stream</text>
  <text x="570" y="134" text-anchor="middle" class="sub">epoch, offsets, node</text>
  <rect x="470" y="210" width="200" height="60" class="box"/>
  <text x="570" y="236" text-anchor="middle" class="label">objects</text>
  <text x="570" y="254" text-anchor="middle" class="sub">records in the object store</text>
  <path d="M180 120 L222 120" class="edge" marker-end="url(#arrst)"/>
  <path d="M430 120 L462 120" class="edge" marker-end="url(#arrst)"/>
  <path d="M570 150 L570 202" class="edge" marker-end="url(#arrst)"/>
  <text x="100" y="60" text-anchor="middle" class="sub">client API</text>
  <text x="330" y="60" text-anchor="middle" class="sub">metadata KV</text>
  <text x="570" y="60" text-anchor="middle" class="sub">metadata state</text>
</svg>
</div>

## Epochs

Every internal stream has an epoch, a counter that increases each time the stream is opened. A stream that was created but never opened sits at epoch `-1`. Opening a stream assigns it to a node and bumps the epoch through the metadata log, which fences any writer still holding the previous epoch. Data written under a stale epoch is rejected, so a node that lost ownership cannot corrupt the stream even if it keeps running.

This is the same fencing idea the nodes themselves use, applied per stream. The epoch never resets, so the pair of stream id and epoch uniquely identifies one continuous session of ownership.

## Offsets

Records occupy consecutive offsets starting at `0`. Two numbers in the metadata state bound the readable range. The start offset moves forward when a stream is trimmed, and offsets below it are gone. The end offset is the committed tail, advanced when objects holding new records are committed through the log.

Offsets are assigned at append time by the owning node and never change. A record's offset is stable across node restarts, transfers, and compaction, so readers can store an offset and resume from it at any time.

## Lifecycle

A stream is opened lazily, on the first request that needs it rather than at creation. It stays open on its owner until it is closed, either explicitly, by a TTL expiring, or as part of an ownership change. Closing flushes buffered data and marks the internal stream closed in the metadata state. A later request reopens it at the next epoch, possibly on a different node.

Deleting a stream removes the registry entry, so the name is immediately reusable, and marks the stream's objects for garbage collection. Deletion of the data itself is asynchronous, handled by the cleanup pass described in [Garbage collection](/docs/design/gc). Registry changes are published to the [catalog changelog](/docs/design/catalog).

## Producers

The registry entry also tracks idempotent producers. Each producer has an id, an epoch, and a last accepted sequence number. An append that repeats an already accepted sequence is acknowledged without writing again, and an append from a producer at a stale epoch is rejected. This state moves with the registry entry, so producer deduplication survives node restarts and stream transfers.
