---
title: Interactive cluster
---

<script setup>
import Simulation from '../../../.vitepress/theme/simulation/Simulation.vue'
</script>

# Interactive cluster

::: info Note
An interactive model of a PicoMQ cluster. It is not a bit-exact replica of
runtime behavior. Thresholds are compressed (`10` log rows, `4` objects, flush
only on trigger). It is meant to show the shape of the write path: propose
through `meta_log`, WAL staging, sealed commit, snapshot / compact / GC, and
ownership change after a kill.
:::

<ClientOnly>
  <Simulation />
</ClientOnly>

## Experiment

1. **Create stream.** Four commands in one `meta_log` row: `CreateStream`,
   `PlaceStream`, `OpenStream`, `PutKv`. Append is separate. Each append encodes a
   batch into the owning node's buffer and log cache. Not durable. The
   producer ack waits for the WAL PUT. Metadata is not involved.
2. **Flush WAL.** At `3` buffered records here, every `~5 ms` window in a real
   cluster. One WAL object under the node's session prefix. End offsets in
   metadata stay put.
3. **Commit sealed.** Sealed WAL blocks become stream-set objects.
   `PrepareObject` + `CommitStreamSetObject` advance end offsets through
   metadata. Covered WAL objects are deleted.
4. **Snapshot, compact, clean.** Snapshot every `10` log rows here, `1024` rows
   plus a `30 s` floor in production. Compact at `4` committed stream-set objects
   here, `64` in production. Compact queues destroyed objects. Clean objects is
   the lease holder's GC tick.
5. **Kill / restart.** Unflushed buffer is lost. Sealed WAL on S3 is recovered
   when the stream opens on a survivor. The sim collapses pending-transfer /
   source-drain into a force-reassign with an epoch bump. Restart loads the
   snapshot and replays the log tail.
