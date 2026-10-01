# Tuning

The mental model for all tuning: append latency is bounded below by one WAL write, and object store cost is roughly proportional to how many `PUT`s are made. Every knob in this page moves along that line, trading latency against request count, or memory against object store reads.

## Append latency

The WAL seals a batch when it reaches the size cap or when the batch interval lapses. A lone append on a quiet stream pays up to that interval on top of the write itself, so the batch interval is the dominant latency knob for low-throughput streams. It is set as a parameter on the WAL URI.

| WAL | Batch interval | Size cap | Write cost |
| --- | --- | --- | --- |
| Object store | `250` ms | `8` MiB | One `PUT`, tens of ms on S3 |
| Postgres | `1` ms | `1` MiB | One commit, low single-digit ms |

```bash
--wal '0@s3://picomq?region=us-east-1&batchInterval=5'
```

A `5` ms interval on the object store gives near-floor latency at the price of one `PUT` per flush. Busy streams are insensitive to this setting because size seals the batch first, and group commit keeps the per-record cost low either way. The remaining URI parameters (`maxBytesInBatch`, `maxUnflushedBytes`, `maxInflightUploadCount` on the object store, `maxInflight` on Postgres) rarely need to move.

On the Postgres WAL the floor is the commit itself. `synchronousCommit=remote_apply` adds the standby round trip to every acknowledgement in exchange for records surviving the loss of the primary. `remote_write` is the cheaper middle.

## Producer throughput

Throughput comes from keeping many records in flight, not from faster individual appends.

Batch appends are the first lever, many records in one request and one durability wait. Pipelining is the second: requests on the same stream are ordered by the server, so a producer can send the next append before the previous acknowledgement arrives. HTTP/2 (`--http2` on the CLI) makes deep pipelines practical over one connection. `pico bench` reports what a given combination achieves against a real endpoint.

Spreading load across streams is the third lever, since every stream is an independent pipeline and placement spreads them across nodes.

## Engine memory

| Flag | Default | What it holds |
| --- | --- | --- |
| `--wal-cache-size` | `200` MiB | Records not yet packed into committed objects. Readers up to this far behind the tail read from memory. |
| `--block-cache-size` | `100` MiB | Pages of committed objects for cold reads, with readahead. |
| `--wal-upload-threshold` | `100` MiB | How much WAL accumulates before being packed into committed objects. Clamped to `2/5` of the WAL cache. |
| `--wal-upload-interval-ms` | `0` (off) | Packs buffered WAL periodically even below the threshold. |

The WAL cache is the one to grow on nodes with many consumers slightly behind their producers, since a consumer that falls out of it drops to the block cache and, past that, to object store reads. The block cache matters for replay-heavy workloads, where a larger cache and its readahead keep sequential replays at object store throughput without repeated fetches.

A larger upload threshold produces fewer, larger committed objects, which reads and compaction both prefer. The cost is a longer WAL, which is replayed on open after a crash, so recovery time grows with it. On low-traffic nodes the threshold may take a long time to fill, and enabling the upload interval keeps the WAL short and recovery fast anyway.

## Object store spend

Request count, not bytes, dominates the bill on most object stores. The two levers that matter are the WAL batch interval, one `PUT` per flush, and the upload threshold, which sets how often WAL data is rewritten into committed objects. With the WAL in Postgres the first lever disappears from the bill and only the upload threshold remains. Relaxing latency on quiet streams and letting batches fill is the single most effective cost change. Compaction and garbage collection add a steady background of requests proportional to churn, not to traffic.

## Read behavior

Long poll and SSE limits are `25` and `55` seconds, covered in [Configuration](/docs/operations/configuration). They are proxy-compatibility settings rather than performance ones. Delivery latency for waiting readers is the append durability latency, since the write path wakes waiters directly, so tightening the WAL batch interval improves end-to-end delivery for live consumers as a side effect.
