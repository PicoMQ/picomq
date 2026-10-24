# Write-ahead log

An append is acknowledged when its record is in the WAL. The WAL is the durable part of the write path, a sequence of small objects that each node writes to the object store under its own key prefix. Caches, committed objects, and compaction sit behind it.

## Contract

The engine drives the WAL through four operations.

| Operation | When | Contract |
| --- | --- | --- |
| Append | Every write | Store a framed batch, acknowledge in submission order |
| Read | Recovery | Return records from an offset forward |
| Trim | After a commit to data objects | Release everything below the committed offset |
| Reset | After recovery is uploaded | Start a new session from an empty log |

A batch is framed with a header carrying its offset, its length, and a CRC32 over the body, then the records.

## Layout

| | |
| --- | --- |
| Unit of write | One object per bulk, pipelined, acknowledged in order, see [Writes](/docs/design/writes#the-wal) |
| Key | `{prefix}{epoch}/wal/{start}-{end}`, under the node's prefix and the epoch it writes at |
| Reservation | `reservation/_kafka_{cluster}/{node}`, a small object naming the node and epoch that may write |
| Trim | The covered objects are deleted |
| Append latency | Tens of ms on S3, single digits on S3 Express |

<div class="pico-diagram">
<svg viewBox="0 0 720 300" width="720" role="img" aria-label="The engine hands a framed batch to the WAL. It becomes one PUT under the node's epoch prefix. Before the batch is acknowledged the writer reads its reservation back, and the acknowledgement is released in order.">
  <defs>
    <marker id="arrwal" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="270" y="20" width="180" height="56" class="box"/>
  <text x="360" y="44" text-anchor="middle" class="label">framed batch</text>
  <text x="360" y="62" text-anchor="middle" class="sub">header, crc, records</text>
  <rect x="60" y="130" width="600" height="150" class="edge-soft"/>
  <text x="70" y="148" class="sub">object store</text>
  <rect x="90" y="170" width="260" height="56" class="box-accent"/>
  <text x="220" y="194" text-anchor="middle" class="label">PUT</text>
  <text x="220" y="212" text-anchor="middle" class="sub">{prefix}{epoch}/wal/{start}-{end}</text>
  <rect x="380" y="170" width="250" height="56" class="box"/>
  <text x="505" y="194" text-anchor="middle" class="label">reservation</text>
  <text x="505" y="212" text-anchor="middle" class="sub">read back before each ack</text>
  <text x="360" y="262" text-anchor="middle" class="sub">acknowledged in order, deleted on trim</text>
  <path d="M330 76 L220 162" class="edge" marker-end="url(#arrwal)"/>
  <path d="M350 198 L372 198" class="edge" marker-end="url(#arrwal)"/>
</svg>
</div>

## Fencing

- Starting the WAL writes the reservation for the node's epoch.
- After each completed bulk, and before acknowledging it, the writer reads the reservation back. If it names anyone else, or cannot be read, the writer is fenced and every later append fails.
- A takeover rewrites the reservation for the dead node before reading anything, so the dead writer can never acknowledge again.

## Commit path

| Parameter | Default | Effect |
| --- | --- | --- |
| `batchInterval` | `250` ms | How long records accumulate before a bulk is written |
| `maxBytesInBatch` | `8 MiB` | Size that closes a bulk early |
| `maxUnflushedBytes` | `1 GiB` | Bytes acknowledged but not yet uploaded to data objects before appends wait |
| `maxInflightUploadCount` | `50` | Bulks in flight at once |

Parameters go on the `--wal` URI, see [Configuration](/docs/operations/configuration#storage) and [Tuning](/docs/operations/tuning#append-latency).

## Takeover

Each node registers where its WAL lives. When a node is fenced, the lease holder opens that WAL as the dead node, in recovery mode, see [Failover](/docs/design/failover#taking-over). The takeover claim fences the dead node's writer before anything is read.

| The dead node's WAL is in | The lease holder can take it over if |
| --- | --- |
| An object store bucket | It can reach the bucket and sees the dead node's reservation at that epoch. The bucket may differ from its own |
| `file://` | It runs on the same host, which makes `file://` a single host setup |

A takeover that is refused is retried on each failover tick, and the dead node's open streams stay where they are until it succeeds or the node restarts.

## Recovery

A node recovers its own WAL when it starts, and the lease holder recovers a fenced node's WAL by taking it over. Recovery lists the node's WAL objects past the last committed offset, checks the CRC of every record, uploads the records to data objects, and closes the streams they belong to. Acknowledged records are recovered because acknowledgement required the write to complete. Records in flight but never acknowledged may be absent.

## Durability

| Event | Outcome |
| --- | --- |
| Node crash or restart | Nothing lost |
| Node host lost | Nothing lost, the WAL is in the bucket |
| Zone lost | Depends on the bucket's storage class |
