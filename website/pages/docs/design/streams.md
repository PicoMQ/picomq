# Topics and streams

Clients name a **topic**. A topic has one or more **partitions**, numbered from `0`. Each partition is a **log**, and each log is backed by one **internal stream**, the unit the engine stores, fences, and moves between nodes. Consumer groups have logs too, see [Consumer groups](/docs/design/groups).

<div class="pico-diagram">
<svg viewBox="0 32 690 258" width="690" role="img" aria-label="A topic name maps to a topic record with partitions. Each partition is a log entry that points at an internal stream, which indexes objects in the object store.">
  <defs>
    <marker id="arrst" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="90" width="160" height="60" class="box"/>
  <text x="100" y="116" text-anchor="middle" class="label">topic</text>
  <text x="100" y="134" text-anchor="middle" class="sub">/logs/api, 3 partitions</text>
  <rect x="230" y="90" width="200" height="60" class="box"/>
  <text x="330" y="116" text-anchor="middle" class="label">log, partition 1</text>
  <text x="330" y="134" text-anchor="middle" class="sub">stream id, producers</text>
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

## Names

Topic names are paths, such as `/logs/api`. Slashes and dots are the same separator: `/logs/api`, `/logs.api`, and `logs.api` all name one topic, whose canonical form is `logs.api`. A topic remembers the spelling it was created with and answers to every spelling.

The canonical form is also the topic's Kafka name, when it is a legal Kafka name: at most `249` characters from letters, digits, `.`, `_`, and `-`. A topic whose canonical form is not legal in Kafka exists only over HTTP.

## What is stored

Everything about a topic lives in the metadata KV, part of the replicated state described in [Metadata](/docs/design/metadata).

| Key | Holds |
| --- | --- |
| `topic/{canonical name}` | The topic: id, partition count, content type, TTL or expiry, closed, bound [schema](/docs/schemas) |
| `topic-id/{topic id}` | The name of a topic id |
| `log/partition/{topic id}/{index}` | A partition's log entry: its internal stream and its producers |
| `log/group/{group}` | A consumer group's log entry |
| `idx/sid/{stream id}` | The log an internal stream belongs to |

## Creating a topic

1. A fresh topic id is recorded.
2. Each partition gets its log: an internal stream is registered, its index entry written, and it is placed on the least loaded live node. A create that carries records opens partition `0` on the receiving node instead, so the records land there.
3. The topic record is written, only if the name is still free.

Creating a name that exists with the same settings returns the existing topic. Different settings are a conflict. Two nodes racing to create one name settle on the third step, and the loser removes the partitions it made.

Partitions can be added later, never removed, up to `10000` in one step.

## Epochs

Every internal stream has an epoch, a counter that increases each time the stream is opened. A stream that was created but never opened sits at epoch `-1`. Opening a stream assigns it to a node and bumps the epoch through the metadata log, which fences any writer still holding the previous epoch. A node that lost a stream cannot write to it again, even if it keeps running. The pair of stream id and epoch identifies one continuous session of ownership.

## Offsets

Records in a partition occupy consecutive offsets starting at `0`. Two numbers bound the readable range:

- The start offset moves forward when the partition is trimmed. Offsets below it are gone.
- The end offset is the committed tail, advanced when objects holding new records are committed through the log.

Offsets are assigned at append time by the owning node and never change, across restarts, transfers, failover, and compaction. A reader can store an offset and resume from it at any time.

## Lifecycle

Two kinds of closing are easy to confuse. **Sealing** a topic is a client action that ends it for writers. **Releasing** an internal stream is a node handing back ownership, and clients never see it.

| Event | What happens |
| --- | --- |
| First request for a partition | Its owner opens the internal stream at the next epoch |
| Release | The owner closes the internal stream, uploading buffered records first. It happens on [transfer](/docs/design/transfers), [shutdown](/docs/design/failover#clean-shutdown), and after a failed append. The next request reopens it at the next epoch |
| Seal | The topic is marked closed. Appends are refused, reads continue to the end |
| TTL | A topic with a TTL expires when nothing appends to it or reads it for that long. One with an expiry time expires at that time. The lease holder deletes expired topics |
| Delete | The topic, id, log, and index entries are removed, so the name is reusable at once. The owner then destroys each internal stream, and its objects go to [garbage collection](/docs/design/gc) |

## Producers

Each partition's log entry tracks its idempotent producers, so a retried append is never written twice.

| | HTTP producers | Kafka producers |
| --- | --- | --- |
| Identity | `Pico-Producer-Id` and `Pico-Producer-Epoch` | Producer id and epoch from `InitProducerId` |
| Deduplication | A sequence already accepted is acknowledged without writing | The last `5` batches per producer, as in Kafka |
| Persisted | After each append is durable | Every `4096` offsets, and rebuilt on load by replaying the log after that point |

An append from a producer at a stale epoch is rejected. Producers idle for `24` hours are forgotten, and a partition tracks at most `1024`. Producer state moves with the log entry, so deduplication survives restarts, transfers, and failover.
