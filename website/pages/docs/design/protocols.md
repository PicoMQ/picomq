# Protocols

PicoMQ speaks three client protocols.

- **Pico protocol.** The native HTTP API, with all custom headers under the `Pico-*` prefix.
- **Durable Streams.** An open HTTP protocol implemented on its exact wire vocabulary, with `Stream-*` and `Producer-*` headers.
- **[Kafka wire protocol](/docs/kafka).** Serves standard Kafka clients over TCP.

A node serves one of the three, chosen in its configuration, and each is a thin translation over the same stream service. Nothing in the storage or metadata layers knows which protocol a record arrived through.

This page covers the two HTTP protocols, which share a resource model and a stored record format. The Kafka frontend makes different choices for compatibility's sake and has [its own page](/docs/kafka).

## The resource model

The deliberate choice here is that a stream is just a URL and the standard methods keep their usual meaning: `PUT` creates, `POST` appends, `GET` reads, `DELETE` removes. There is no session, no handshake, and no client library required, so anything that can issue an HTTP request is a full client. The exact endpoints and status codes are in the [API reference](/docs/api).

The second choice is that all position state travels in response headers: the next offset, whether the reader is at the tail, a cursor for resuming. The server keeps nothing about its consumers, so a reader can disappear for a week, come back with its last offset, and continue. This is also what makes reads through redirects and transfers safe, since any node can answer from just the request.

## Records on the wire

Every record is stored as an envelope so nothing about it is lost between protocols.

<div class="pico-diagram">
<svg viewBox="0 30 680 150" width="680" role="img" aria-label="An envelope is a version byte, a timestamp, a header count with name and value pairs, and the body bytes.">
  <rect x="20" y="70" width="90" height="52" class="box"/>
  <text x="65" y="93" text-anchor="middle" class="label">version</text>
  <text x="65" y="110" text-anchor="middle" class="sub">u8</text>
  <rect x="110" y="70" width="120" height="52" class="box"/>
  <text x="170" y="93" text-anchor="middle" class="label">timestamp</text>
  <text x="170" y="110" text-anchor="middle" class="sub">i64, ms</text>
  <rect x="230" y="70" width="230" height="52" class="box"/>
  <text x="345" y="93" text-anchor="middle" class="label">headers</text>
  <text x="345" y="110" text-anchor="middle" class="sub">count, then name and value pairs</text>
  <rect x="460" y="70" width="200" height="52" class="box-accent"/>
  <text x="560" y="93" text-anchor="middle" class="label">body</text>
  <text x="560" y="110" text-anchor="middle" class="sub">the record bytes</text>
  <text x="340" y="52" text-anchor="middle" class="sub">one record, one envelope</text>
  <text x="340" y="152" text-anchor="middle" class="sub">length-prefixed fields, no padding</text>
</svg>
</div>

The envelope is the reason the two HTTP protocols can share storage. A record appended through one protocol reads back through the other with its timestamp and metadata intact, because the stored form belongs to neither. The timestamp is assigned by the owning node and is monotonic per stream, so equal wall-clock readings still order correctly. Headers are the record's own key-value metadata, distinct from HTTP headers.

Kafka streams are the deliberate exception: the Kafka frontend stores the client's record batches verbatim rather than re-encoding them into envelopes, so fetches return the exact bytes Kafka clients expect with no per-record work. The two stored forms coexist because streams carry a content type, and a stream is written and read through the protocol family that created it.

Appends come in three shapes, a single body, a JSON batch, and a binary batch, each with its own content type. Records in a batch are ordered under the stream's gate together, so they always occupy consecutive offsets, and the append is acknowledged once the whole batch is durable.

## Producers

Exactly-once appends over HTTP need the server to remember, because a client that times out cannot know whether its write landed. Both protocols solve this the same way: a producer identifies itself with an id, an epoch, and a per-record sequence number, the server accepts each sequence once, acknowledges repeats without writing, and rejects stale epochs. A mismatch response includes what the server expected next to what it received, so a producer can tell a lost acknowledgement from a real gap. The state behind this is in the stream's registry entry, described in [Streams](/docs/design/streams), and survives restarts and transfers.

## Routing at the edge

All frontends share one routing step in front of every handler, so the protocol code never thinks about ownership. Creates are always served locally, since create is what places a stream in the first place, and everything else follows the decision table in [Ownership and routing](/docs/design/ownership), redirecting to the owner when the stream is served elsewhere. A routing failure returns an error rather than a guess.

SSE connections are capped at `55` seconds and long polls at `25` by default, both below common proxy idle timeouts, so intermediaries see regular traffic instead of connections worth killing. Clients resume from their last offset or cursor and lose nothing across reconnects.
