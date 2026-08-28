# Introduction

PicoMQ is a durable stream server. Clients create named streams, append records, and read them back over HTTP. Records are stored on S3-compatible object storage and cluster coordination goes through a SQL database. A node is a single binary with no local state worth backing up.

<div class="pico-diagram">
<svg viewBox="0 0 720 300" width="720" role="img" aria-label="Clients talk HTTP to pico nodes. Nodes write records to object storage and coordinate through a SQL metadata log.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="114" width="130" height="72" class="box"/>
  <text x="85" y="146" text-anchor="middle" class="label">clients</text>
  <text x="85" y="164" text-anchor="middle" class="sub">HTTP</text>
  <rect x="270" y="42" width="160" height="72" class="box-accent"/>
  <text x="350" y="74" text-anchor="middle" class="label">pico node 1</text>
  <text x="350" y="92" text-anchor="middle" class="sub">serve + admin</text>
  <rect x="270" y="186" width="160" height="72" class="box-accent"/>
  <text x="350" y="218" text-anchor="middle" class="label">pico node 2</text>
  <text x="350" y="236" text-anchor="middle" class="sub">serve + admin</text>
  <rect x="540" y="42" width="160" height="72" class="box"/>
  <text x="620" y="68" text-anchor="middle" class="label">SQL metadata log</text>
  <text x="620" y="86" text-anchor="middle" class="sub">Postgres or SQLite</text>
  <text x="620" y="101" text-anchor="middle" class="sub">who owns what</text>
  <rect x="540" y="186" width="160" height="72" class="box"/>
  <text x="620" y="212" text-anchor="middle" class="label">object storage</text>
  <text x="620" y="230" text-anchor="middle" class="sub">S3 compatible</text>
  <text x="620" y="245" text-anchor="middle" class="sub">every record</text>
  <path d="M150 138 L262 90" class="edge" marker-end="url(#arr)"/>
  <path d="M150 162 L262 210" class="edge" marker-end="url(#arr)"/>
  <path d="M430 78 L532 78" class="edge" marker-end="url(#arr)"/>
  <path d="M430 222 L532 222" class="edge" marker-end="url(#arr)"/>
  <path d="M430 100 L532 200" class="edge" marker-end="url(#arr)"/>
  <path d="M430 200 L532 100" class="edge" marker-end="url(#arr)"/>
</svg>
</div>

## Vision

PicoMQ treats a stream as a small, disposable unit. Streams are named like URL paths, created with one request, and cost nothing while idle. A deployment can hold ten streams or millions, one per order, per session, per device, or per job.

Object storage is what makes that granularity economical. Every record goes there, including the write-ahead log, so durability never depends on a node and an idle stream is a registry entry plus its objects. Coordination goes through a SQL database. There is no consensus protocol and no broker disks.

The structure follows from that. A node can be stopped and replaced at any time because it holds no unique state. Adding capacity is starting another process. Losing a node causes a few seconds of rerouting, not a data rebalance.

## Features

- **Zero-disk nodes.** Records are stored on S3-compatible storage, including the write-ahead log. A node keeps caches, nothing more.
- **SQL as the control plane.** Cluster metadata is an ordered command log in Postgres, or SQLite for a single node. Nodes tail it and rebuild the same state.
- **Three wire protocols.** The native Pico protocol, the Durable Streams open protocol, and the Kafka wire protocol for standard Kafka clients. Same engine underneath.
- **Just HTTP.** Create with `PUT`, append with `POST`, read with `GET`, tail with long polling or SSE. Any HTTP client is a PicoMQ client, and any Kafka client is too.
- **Live stream transfer.** Ownership of a stream moves between nodes without losing writes, with seconds of handoff.
- **Fencing everywhere.** Node epochs and stream epochs keep zombie processes from corrupting anything.
- **One binary.** `pico` is the server, the client, the admin CLI, and the benchmark tool. The admin dashboard is embedded in it.

## Use cases

PicoMQ suits anything modeled as many ordered, resumable streams: a stream per user session or chat, per device, per workflow run, or per agent conversation. Readers resume from any position, so it also serves audit trails, per-entity event history, and real-time delivery to many concurrent readers.

It is not built for single-digit millisecond appends. Durability comes from object storage, so an append costs one round trip there, typically tens of milliseconds.