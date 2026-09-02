# Protocols

PicoMQ speaks three client protocols.

- **Pico protocol.** The native HTTP API, with all custom headers under the `Pico-*` prefix.
- **Durable Streams.** An open HTTP protocol implemented on its exact wire vocabulary, with `Stream-*` and `Producer-*` headers.
- **[Kafka wire protocol](/docs/kafka).** Serves standard Kafka clients over TCP.

A node serves one of the two HTTP protocols on its stream listener, chosen in its configuration, and Kafka on a second listener unless that listener is off. Each frontend is a thin translation over the same stream service, documented in [Protocol facades](/docs/extending). Nothing in the storage or metadata layers knows which protocol a record arrived through.

This page covers the resource model and the stored record format. The Kafka frontend's own surface has [its own page](/docs/kafka).

## The resource model

The deliberate choice here is that a stream is just a URL and the standard methods keep their usual meaning: `PUT` creates, `POST` appends, `GET` reads, `DELETE` removes. There is no session, no handshake, and no client library required, so anything that can issue an HTTP request is a full client. The exact endpoints and status codes are in the [API reference](/docs/api).

The second choice is that all position state travels in response headers: the next offset, whether the reader is at the tail, a cursor for resuming. The server keeps nothing about its consumers, so a reader can disappear for a week, come back with its last offset, and continue. This is also what makes reads through redirects and transfers safe, since any node can answer from just the request.

## Records

Every stream is a log of Kafka **RecordBatch v2** batches.

<div class="pico-diagram">
<svg viewBox="0 30 680 150" width="680" role="img" aria-label="A RecordBatch v2 is a header with base offset, CRC32C, attributes, timestamps, producer identity and record count, followed by records that each carry an offset delta, a timestamp delta, an optional key, a value and headers.">
  <rect x="20" y="70" width="330" height="52" class="box"/>
  <text x="185" y="93" text-anchor="middle" class="label">batch header</text>
  <text x="185" y="110" text-anchor="middle" class="sub">base offset, CRC32C, attributes, timestamps, producer id, count</text>
  <rect x="350" y="70" width="310" height="52" class="box-accent"/>
  <text x="505" y="93" text-anchor="middle" class="label">records</text>
  <text x="505" y="110" text-anchor="middle" class="sub">offset delta, timestamp delta, key, value, headers</text>
  <text x="340" y="52" text-anchor="middle" class="sub">one append, one batch</text>
  <text x="340" y="152" text-anchor="middle" class="sub">Kafka RecordBatch v2, byte for byte</text>
</svg>
</div>

A record has an optional key, a value, ordered headers, and a timestamp. HTTP appends are encoded by the owning node with a per-stream monotonic `LogAppendTime`. Kafka produces keep the client's `CreateTime`. A Kafka fetch returns the stored bytes after the same base-offset patch a Kafka broker writes. HTTP reads decode those batches.

A Pico record is a Kafka record. A Durable Streams record is a value. A Kafka record with a key and headers reads over Pico with them intact and over DS as its value.

Appends over Pico come in three shapes: a single body, optionally keyed through `Pico-Key`, a JSON batch, and a binary batch. Reads return JSON, the binary batch, or the raw concatenated values. Durable Streams appends are one body per request, and on a JSON stream a top-level array is one record per element. Records in a batch occupy consecutive offsets and are acknowledged once the whole batch is durable.

## Topic names

Stream names are paths. Kafka topic names cannot contain `/`. Each stream may carry one topic alias, unique across the cluster. Create over HTTP derives one by turning slashes into dots when that name is legal and free. Otherwise set `Pico-Kafka-Topic` on create or `kafkaTopic` on `PATCH /_streams/{name}`. A Kafka `CreateTopics` call creates the stream of the same name with that alias. Reserved streams under `/_sys`, `/_schemas` and `/_streams` never carry a topic.

## Producers

Exactly-once appends over HTTP need the server to remember, because a client that times out cannot know whether its write landed. Both HTTP protocols solve this the same way: a producer identifies itself with an id, an epoch, and a per-record sequence number, the server accepts each sequence once, acknowledges repeats without writing, and rejects stale epochs. A mismatch response includes what the server expected next to what it received, so a producer can tell a lost acknowledgement from a real gap. Kafka's idempotent producer is the same mechanism, keyed by the producer id, epoch and base sequence in the batch header. The state behind both is in the stream's registry entry, described in [Streams](/docs/design/streams), and survives restarts and transfers.

## Routing at the edge

All frontends share one routing step in front of every handler, so the protocol code never thinks about ownership. Creates are always served locally, since create is what places a stream in the first place, and everything else follows the decision table in [Ownership and routing](/docs/design/ownership), redirecting to the owner when the stream is served elsewhere. A routing failure returns an error rather than a guess.

SSE connections are capped at `55` seconds and long polls at `25` by default, both below common proxy idle timeouts, so intermediaries see regular traffic instead of connections worth killing. Clients resume from their last offset or cursor and lose nothing across reconnects.
