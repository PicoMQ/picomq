# Why not Kafka?

PicoMQ [serves the Kafka protocol](/docs/kafka). This page explains why Pico's own data model is not Kafka's. Log workloads fall into two types with different storage requirements.

| Type | Description | Examples |
| --- | --- | --- |
| Funneling | Many producers append to a small number of shared logs. Consumers read each log in full | Telemetry, clickstream ingestion, warehouse loads |
| Routing | Each entity has its own ordered log. Consumers read individual logs by name | Messaging, feeds, per-entity state, agent sessions |

Both need a durable log. Kafka supports funneling only. PicoMQ supports both.

## Why Kafka is an excellent funnel

Kafka was built in 2011 to move telemetry from LinkedIn's servers into Hadoop. Early messages carried no keys, and the original implementation discarded the partitioning key after computing a partition. The data model reflects that origin: a small number of wide topics, each split into partitions for parallelism, with consumers reading every partition in order.

<div class="pico-diagram">
<svg viewBox="0 0 680 260" width="680" role="img" aria-label="Many producers pour events into one wide partitioned topic, and a consumer group drains the whole pipe.">
  <defs>
    <marker id="wnk1" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="30" width="120" height="44" class="box"/>
  <text x="80" y="56" text-anchor="middle" class="label">service</text>
  <rect x="20" y="106" width="120" height="44" class="box"/>
  <text x="80" y="132" text-anchor="middle" class="label">service</text>
  <rect x="20" y="182" width="120" height="44" class="box"/>
  <text x="80" y="208" text-anchor="middle" class="label">service</text>
  <rect x="230" y="30" width="240" height="196" fill="none" class="edge-soft"/>
  <text x="246" y="52" class="sub">topic: user-events</text>
  <rect x="250" y="66" width="200" height="38" class="box-accent"/>
  <text x="350" y="90" text-anchor="middle" class="sub">partition 0</text>
  <rect x="250" y="118" width="200" height="38" class="box-accent"/>
  <text x="350" y="142" text-anchor="middle" class="sub">partition 1</text>
  <rect x="250" y="170" width="200" height="38" class="box-accent"/>
  <text x="350" y="194" text-anchor="middle" class="sub">partition 2</text>
  <rect x="540" y="106" width="120" height="44" class="box"/>
  <text x="600" y="126" text-anchor="middle" class="label">consumer</text>
  <text x="600" y="142" text-anchor="middle" class="sub">reads everything</text>
  <path d="M140 52 L222 90" class="edge" marker-end="url(#wnk1)"/>
  <path d="M140 128 L222 128" class="edge" marker-end="url(#wnk1)"/>
  <path d="M140 204 L222 170" class="edge" marker-end="url(#wnk1)"/>
  <path d="M470 128 L532 128" class="edge" marker-end="url(#wnk1)"/>
  <text x="340" y="250" text-anchor="middle" class="sub">funneling: many producers, few wide topics, consumers read everything</text>
</svg>
</div>

Storage systems can be measured by read, write, and space amplification, and improving all three requires restricting the access pattern. Kafka restricts in favor of the funnel. Records land in append-only, immutable segment files that are deleted wholesale when they age out of retention, so write amplification is about 1x. The only read API is a sequential scan in write order, so a full scan also costs about 1x read amplification. For funneling this is close to optimal.

## Why Kafka is a bad router

Routing inverts the access pattern. Consumers read a specific subset of the data, usually one entity: one user, one session, one workflow run. The underlying storage needs hundreds of thousands or millions of small ordered logs rather than one large one.

Partitions are too heavy to give each entity its own, so entities are interleaved into shared partitions. The partition key controls placement, not access, and there is no read path that returns one key's records. Reconstructing one entity requires scanning the partition and discarding the rest, so read amplification is the ratio of the partition size to the records requested:

```
             |partition|
  α_read = ─────────────
              |record|
```

<div class="pico-diagram">
<svg viewBox="0 0 680 250" width="680" role="img" aria-label="One user's records are scattered as small slivers across every partition, so reading that user means scanning all of them.">
  <defs>
    <marker id="wnk2" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="30" y="30" width="380" height="180" fill="none" class="edge-soft"/>
  <text x="46" y="52" class="sub">topic: user-events</text>
  <rect x="50" y="66" width="340" height="34" class="box"/>
  <rect x="106" y="66" width="14" height="34" class="box-accent"/>
  <rect x="298" y="66" width="14" height="34" class="box-accent"/>
  <text x="62" y="88" class="sub" text-anchor="start">p0</text>
  <rect x="50" y="118" width="340" height="34" class="box"/>
  <rect x="204" y="118" width="14" height="34" class="box-accent"/>
  <text x="62" y="140" class="sub" text-anchor="start">p1</text>
  <rect x="50" y="170" width="340" height="34" class="box"/>
  <rect x="130" y="170" width="14" height="34" class="box-accent"/>
  <rect x="342" y="170" width="14" height="34" class="box-accent"/>
  <text x="62" y="192" class="sub" text-anchor="start">p2</text>
  <rect x="520" y="96" width="130" height="52" class="box"/>
  <text x="585" y="118" text-anchor="middle" class="label">reader</text>
  <text x="585" y="135" text-anchor="middle" class="sub">wants user 1042</text>
  <path d="M512 108 L398 84" class="edge" marker-end="url(#wnk2)"/>
  <path d="M512 122 L398 135" class="edge" marker-end="url(#wnk2)"/>
  <path d="M512 138 L398 186" class="edge" marker-end="url(#wnk2)"/>
  <text x="340" y="236" text-anchor="middle" class="sub">user 1042's records (accented) are spread across all partitions</text>
</svg>
</div>

This is close to the worst case, and the same layout causes problems beyond reads. Offsets are tracked per partition, not per entity, so one bad record blocks every entity behind it. Changing partition counts moves data and disrupts every consumer. A hot key produces a hot partition that cannot be split in place.

## The industry's answers

Most Kafka alternatives compete on the funnel: cheaper brokers, tiered storage, object-native backends. The primitive is unchanged.

Some newer systems address routing by changing the storage layout. Records are kept in an LSM tree keyed by `(key, sequence)`, so one key's log is found by binary search instead of a partition scan, and millions of keyed logs fit on one node. This fixes read amplification, but the log remains an entry inside a larger structure. It cannot be listed, sized, placed, or deleted on its own, and continuous compaction is required to keep each key's records collocated.

## Pico: the stream is the primitive

Pico makes the per-entity log a first class object. Every entity gets its own stream, named like a path and backed by its own log. Idle streams have no cost and a deployment holds millions of them, so unrelated entities never share a log and no compaction is needed to keep an entity's records together.

<div class="pico-diagram">
<svg viewBox="0 0 680 270" width="680" role="img" aria-label="Producers write to exact streams under a users prefix. One reader tails a single stream, another works the whole prefix.">
  <defs>
    <marker id="wnk3" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="96" width="140" height="52" class="box"/>
  <text x="90" y="118" text-anchor="middle" class="label">producer</text>
  <text x="90" y="135" text-anchor="middle" class="sub">writes one stream</text>
  <rect x="240" y="30" width="220" height="210" fill="none" class="edge-soft"/>
  <text x="256" y="52" class="sub">prefix: /users/</text>
  <rect x="260" y="66" width="180" height="38" class="box-accent"/>
  <text x="350" y="90" text-anchor="middle" class="sub">/users/1042</text>
  <rect x="260" y="118" width="180" height="38" class="box-accent"/>
  <text x="350" y="142" text-anchor="middle" class="sub">/users/1043</text>
  <rect x="260" y="170" width="180" height="38" class="box-accent"/>
  <text x="350" y="194" text-anchor="middle" class="sub">/users/1044 ...</text>
  <rect x="530" y="46" width="140" height="52" class="box"/>
  <text x="600" y="68" text-anchor="middle" class="label">router read</text>
  <text x="600" y="85" text-anchor="middle" class="sub">tail one stream</text>
  <rect x="530" y="160" width="140" height="52" class="box"/>
  <text x="600" y="182" text-anchor="middle" class="label">funnel read</text>
  <text x="600" y="199" text-anchor="middle" class="sub">list prefix, fan in</text>
  <path d="M160 122 L252 90" class="edge" marker-end="url(#wnk3)"/>
  <path d="M440 80 L522 72" class="edge" marker-end="url(#wnk3)"/>
  <path d="M468 90 L522 172" class="edge" marker-end="url(#wnk3)"/>
  <path d="M468 142 L522 182" class="edge" marker-end="url(#wnk3)"/>
  <path d="M468 192 L522 192" class="edge" marker-end="url(#wnk3)"/>
  <text x="345" y="262" text-anchor="middle" class="sub">one stream per entity, aggregate reads list the prefix and fan in</text>
</svg>
</div>

Producers write to a named stream, so there is no partitioner. Reading one entity means tailing one stream, which costs 1x read amplification. Reading the aggregate means listing a prefix and fanning in across its streams, scoped to one entity, one customer's subtree, or everything.

| | Partitions (Kafka) | Streams (Pico) |
| --- | --- | --- |
| **Point reads** | Finding one entity's records requires scanning the whole partition | The entity is the stream. It is addressed by name and only its records are read |
| **Isolation** | Offsets are per partition, so one bad record blocks every entity behind it | Offsets and failures are scoped to one stream |
| **Rescaling** | Changing partition counts moves data and disrupts all consumers. Hot partitions cannot be split in place | The stream is the unit of placement. A hot stream moves to another node on its own and offsets do not change |
| **Lifecycle** | Retention applies per topic. One entity's data cannot be trimmed or deleted independently | Retention, trimming, and deletion are per stream operations |

Funneling works the same way on PicoMQ. A topic with N partitions is N streams under one prefix. Producers hash to a stream, consumers fan in across the prefix. Reads are 1x sequential and retention deletes whole segments, as in Kafka. Kafka clients connect through the [Kafka protocol](/docs/kafka).
