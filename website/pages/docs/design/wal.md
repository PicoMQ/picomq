# Write-ahead log

An append is acknowledged when its record is in the WAL. The WAL is the durable part of the write path. It lives in the object store or in a Postgres database. Caches, committed objects, and compaction are the same for both.

## Contract

A WAL backend implements four operations.

| Operation | When | Contract |
| --- | --- | --- |
| Append | Every write | Store a framed batch, acknowledge in submission order |
| Read | Recovery | Return records from an offset forward |
| Trim | After a commit to data objects | Release everything below the committed offset |
| Reset | After recovery is uploaded | Start a new session from an empty log |

Records use the same frame in both backends: a header with the offset, length, and a CRC32 over the body, then the batch of records. The container around the frame differs.

| | Object store | Postgres |
| --- | --- | --- |
| Unit of write | One object per bulk | One row per batch |
| Location | `{prefix}{epoch}/wal/{start}-{end}` | Slot table `pico_wal_{cluster}_{node}_{slot}` |
| Fence | Epoch in the key | Epoch on the node row, checked per insert |
| Trim | Delete covered objects | Truncate the slot table |
| Append latency | Tens of ms on S3, single digits on S3 Express | One commit |

<div class="pico-diagram">
<svg viewBox="0 0 720 300" width="720" role="img" aria-label="The engine hands a framed batch to the WAL. On the object store it becomes one PUT under the node's epoch prefix. In Postgres it becomes one INSERT into a slot table while holding a share lock on the node row. Both acknowledge in order.">
  <defs>
    <marker id="arrwal" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="270" y="20" width="180" height="56" class="box"/>
  <text x="360" y="44" text-anchor="middle" class="label">framed batch</text>
  <text x="360" y="62" text-anchor="middle" class="sub">header, crc, records</text>
  <rect x="20" y="130" width="300" height="140" class="edge-soft"/>
  <text x="30" y="148" class="sub">object store</text>
  <rect x="40" y="160" width="260" height="56" class="box-accent"/>
  <text x="170" y="184" text-anchor="middle" class="label">PUT</text>
  <text x="170" y="202" text-anchor="middle" class="sub">{prefix}{epoch}/wal/{start}-{end}</text>
  <text x="170" y="250" text-anchor="middle" class="sub">epoch in the key, prefix deleted on trim</text>
  <rect x="400" y="130" width="300" height="140" class="edge-soft"/>
  <text x="410" y="148" class="sub">postgres</text>
  <rect x="420" y="160" width="260" height="56" class="box-accent"/>
  <text x="550" y="184" text-anchor="middle" class="label">INSERT</text>
  <text x="550" y="202" text-anchor="middle" class="sub">slot table, node row FOR SHARE</text>
  <text x="550" y="250" text-anchor="middle" class="sub">epoch checked per row, slot truncated on trim</text>
  <path d="M320 76 L170 152" class="edge" marker-end="url(#arrwal)"/>
  <path d="M400 76 L550 152" class="edge" marker-end="url(#arrwal)"/>
</svg>
</div>

## Object store

The WAL is a sequence of small objects written by one node under its own key prefix.

- Incoming records accumulate into a bulk. Each bulk is one `PUT`.
- Uploads are pipelined. Acknowledgements are delivered in submission order, once a bulk and every bulk before it are stored.
- One upload contains every record that arrived while the previous upload was in flight, so per-record cost drops as concurrency rises.
- The object key carries the node's epoch. A node that lost its registration cannot extend its WAL past a takeover, see [Streams](/docs/design/streams).
- Trim deletes the covered objects.

## Postgres

The WAL is a set of tables in a Postgres database. `--wal` with a `postgres://` URL selects it. The [Postgres extension](/docs/operations/deployment/postgres) uses it by default. An append costs one Postgres commit.

### Tables

| Table | Rows | Columns |
| --- | --- | --- |
| `pico_wal_node` | One per node, keyed by cluster and node id | `epoch`, `trim_offset`, `segments`, `segment_bytes`, `timeline` |
| `pico_wal_{cluster}_{node}_{slot}` | One per batch | `start_offset`, `end_offset`, `epoch`, `body` |

The cluster part of a slot table name is a CRC32 of the cluster id. `body` is stored `EXTERNAL`, so a row read is one TOAST fetch with no decompression.

### Ring

- Offsets are divided into segments of `segmentBytes`. The segment number of an offset is its generation.
- A generation maps to slot `generation % segments`. A slot holds one generation at a time.
- When the trim offset passes the end of a generation its slot is truncated.
- If the head reaches a slot whose generation has not been trimmed, appends wait. The ring bounds how far the WAL can run ahead of the upload to data objects.

<div class="pico-diagram">
<svg viewBox="0 0 720 200" width="720" role="img" aria-label="Eight slot tables. Slots hold generations 16 through 21, the trim offset sits in generation 17, the head is in generation 21, and two slots are empty and reusable.">
  <defs>
    <marker id="arrring" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="70" width="80" height="56" class="box-accent"/>
  <text x="60" y="94" text-anchor="middle" class="label">slot 0</text>
  <text x="60" y="112" text-anchor="middle" class="sub">gen 16</text>
  <rect x="106" y="70" width="80" height="56" class="box-accent"/>
  <text x="146" y="94" text-anchor="middle" class="label">slot 1</text>
  <text x="146" y="112" text-anchor="middle" class="sub">gen 17</text>
  <rect x="192" y="70" width="80" height="56" class="box-accent"/>
  <text x="232" y="94" text-anchor="middle" class="label">slot 2</text>
  <text x="232" y="112" text-anchor="middle" class="sub">gen 18</text>
  <rect x="278" y="70" width="80" height="56" class="box-accent"/>
  <text x="318" y="94" text-anchor="middle" class="label">slot 3</text>
  <text x="318" y="112" text-anchor="middle" class="sub">gen 19</text>
  <rect x="364" y="70" width="80" height="56" class="box-accent"/>
  <text x="404" y="94" text-anchor="middle" class="label">slot 4</text>
  <text x="404" y="112" text-anchor="middle" class="sub">gen 20</text>
  <rect x="450" y="70" width="80" height="56" class="box-accent"/>
  <text x="490" y="94" text-anchor="middle" class="label">slot 5</text>
  <text x="490" y="112" text-anchor="middle" class="sub">gen 21</text>
  <rect x="536" y="70" width="80" height="56" class="box"/>
  <text x="576" y="94" text-anchor="middle" class="label">slot 6</text>
  <text x="576" y="112" text-anchor="middle" class="sub">empty</text>
  <rect x="622" y="70" width="80" height="56" class="box"/>
  <text x="662" y="94" text-anchor="middle" class="label">slot 7</text>
  <text x="662" y="112" text-anchor="middle" class="sub">empty</text>
  <path d="M146 30 L146 62" class="edge" marker-end="url(#arrring)"/>
  <text x="146" y="22" text-anchor="middle" class="sub">trim</text>
  <path d="M490 30 L490 62" class="edge" marker-end="url(#arrring)"/>
  <text x="490" y="22" text-anchor="middle" class="sub">head</text>
  <text x="60" y="164" text-anchor="middle" class="sub">truncated next</text>
  <text x="576" y="164" text-anchor="middle" class="sub">gen 22 goes here</text>
</svg>
</div>

| Parameter | Default | Constraint |
| --- | --- | --- |
| `segments` | `8` | At least 2 |
| `segmentBytes` | `64 MiB` | At least `maxBytesInBatch`, at most 4 GiB |
| Capacity, `(segments - 1) * segmentBytes` | `448 MiB` | Must exceed `maxUnflushedBytes` |

A change to `segments` or `segmentBytes` is applied at the first start where the WAL is fully drained.

### Fencing

- Every batch insert runs as `INSERT ... SELECT ... FROM pico_wal_node WHERE epoch = $epoch FOR SHARE`.
- If a newer process has claimed the node row with a higher epoch, the select returns no row, the insert writes nothing, and the writer reports itself fenced.
- Claiming the node row is an upsert that succeeds only when the new epoch is at least the stored one.
- Trim is guarded by the same epoch predicate.

### Commit path

| Parameter | Default | Effect |
| --- | --- | --- |
| `batchInterval` | `1` ms | How long records accumulate before a batch is inserted |
| `maxBytesInBatch` | `1 MiB` | Size that closes a batch early |
| `maxInflight` | `4` | Batches inserted concurrently, each in its own transaction |
| `maxUnflushedBytes` | `128 MiB` | Bytes acknowledged but not yet uploaded to data objects before appends wait |
| `synchronousCommit` | `on` | `synchronous_commit` on the WAL connections: `on`, `remote_write`, `remote_apply`, or `local` |

Acknowledgements are released in order as commits return. A failed insert is retried against the same table. If a row already exists at that offset it is compared with the batch: a match is success, a mismatch is corruption.

## Recovery

When a stream is opened after a crash the engine reads the WAL past the last committed offset and replays it. Acknowledged records are recovered because acknowledgement required the write to complete. Records in flight but never acknowledged may be absent.

| | Object store | Postgres |
| --- | --- | --- |
| Read | List under the previous session's prefix | Scan from the trim offset forward across slot tables, stop at the first gap |
| Integrity | Body CRC per record | Body CRC per record, row epoch must match the epoch in its offset |

The Postgres backend also compares the database timeline with the one recorded at the last start.

| Timeline | Meaning | Outcome |
| --- | --- | --- |
| Equal | Normal restart | Recovery proceeds |
| Lower | Database restored from a backup | Records after the backup point are gone. Logged as a warning |
| Higher, `synchronous_standby_names` set | Standby promoted | Recovery proceeds. Logged |
| Higher, `synchronous_standby_names` empty | Standby promoted | Records acknowledged after the standby's last replayed commit are lost. Logged as an error |

## Durability

| Event | Object store WAL | Postgres WAL, no synchronous standby | Postgres WAL, synchronous standby |
| --- | --- | --- | --- |
| Node crash or restart | Nothing lost | Nothing lost | Nothing lost |
| Postgres crash, same disk | Not applicable | Nothing lost with `fsync = on` | Nothing lost |
| Primary host lost | Not applicable | Records after the last replicated commit lost | Nothing lost after promotion |
| Zone lost | Depends on bucket class | Lost unless the standby is elsewhere | Nothing lost if the standby is elsewhere |

Checks the Postgres WAL runs at start:

| Condition | Result |
| --- | --- |
| `pg_is_in_recovery()` is true | Refuses to start |
| `fsync = off` | Refuses to start unless the URI carries `allowUnsafe=true`, then warns |
| `synchronous_standby_names` empty | Warns |
