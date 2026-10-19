# Transfers

A transfer moves ownership of a stream from one node to another. No record data moves, everything durable is already in the object store, so a transfer is a handoff of session state: close on the source, reopen on the target. It is the tool for rebalancing load and for moving chosen streams. Stopping a node does not need it: a clean shutdown hands off every stream at once, see [Failover](/docs/design/failover#clean-shutdown).

## Starting a transfer

A transfer begins as a metadata command, proposed through the admin API or the CLI. Apply validates it: the stream must be open on the source, the target must be a live node with slots, and the stream must not already be transferring. What lands in the state is a pending transfer record, the pair of source and target node ids keyed by stream id. Proposing the same transfer twice is a no-op, and proposing a conflicting one is rejected.

The pending record changes routing immediately, as long as the target stays live. From this point new requests for the stream route to the target, where opens wait for the handoff to settle instead of failing.

## The handoff

There is no node-to-node call anywhere in the process. Each node watches the published metadata view, and the pending record tells every participant what to do next.

<div class="pico-diagram">
<svg viewBox="20 0 660 350" width="660" role="img" aria-label="The source sees the pending transfer, drains and closes the stream, and proposes completion. The log re-places the stream on the target, which pre-warms it at the next epoch.">
  <defs>
    <marker id="arrt" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="40" y="20" width="140" height="44" class="box"/>
  <text x="110" y="47" text-anchor="middle" class="label">source node</text>
  <rect x="280" y="20" width="140" height="44" class="box"/>
  <text x="350" y="47" text-anchor="middle" class="label">metadata log</text>
  <rect x="520" y="20" width="140" height="44" class="box"/>
  <text x="590" y="47" text-anchor="middle" class="label">target node</text>
  <path d="M110 64 L110 310" class="edge-soft"/>
  <path d="M350 64 L350 310" class="edge-soft"/>
  <path d="M590 64 L590 310" class="edge-soft"/>
  <rect x="280" y="88" width="140" height="36" class="box-accent"/>
  <text x="350" y="110" text-anchor="middle" class="sub">transfer recorded</text>
  <path d="M118 158 L342 158" class="edge" marker-end="url(#arrt)"/>
  <text x="230" y="148" text-anchor="middle" class="sub">drain, close at epoch e</text>
  <path d="M118 204 L342 204" class="edge" marker-end="url(#arrt)"/>
  <text x="230" y="194" text-anchor="middle" class="sub">complete transfer at e</text>
  <rect x="280" y="224" width="140" height="36" class="box-accent"/>
  <text x="350" y="246" text-anchor="middle" class="sub">re-placed on target</text>
  <path d="M358 290 L582 290" class="edge" marker-end="url(#arrt)"/>
  <text x="470" y="280" text-anchor="middle" class="sub">view update, open at e + 1</text>
  <text x="350" y="334" text-anchor="middle" class="sub">requests arriving during the transfer route to the target and wait</text>
</svg>
</div>

The source sees the pending record in its view, stops accepting new appends for the stream, flushes what is buffered, and closes it, recording the epoch it closed at. It then proposes the completion command with that epoch. Apply accepts the completion only if the stream is closed at exactly that epoch, then re-points the stream at the target and removes the pending record in one atomic step.

The target sees the pending record disappear and the stream now placed on it, and opens the stream immediately at the next epoch. This pre-warm means the first client request after the transfer finds the stream already open instead of paying the open cost.

## During the transfer

Requests that arrive mid-handoff route to the target. An open attempt on the target parks and polls until the transfer settles, up to `10` seconds, so clients see added latency rather than errors. A request that reaches any other node while the transfer is pending is rejected as transferring, HTTP `409` or Kafka `NOT_LEADER_OR_FOLLOWER`, and the client retries against the target.

The unavailability window is the source's drain plus two metadata round trips, typically well under a second for a quiet stream and bounded by the buffered data for a busy one.

## Crashes

Every step is idempotent and driven by state that survives crashes, so the process resumes wherever it stopped.

| Crash | Recovery |
| --- | --- |
| Source, before completing | If it restarts, its WAL recovery closes the stream and its watcher completes the transfer. If it stays down, [failover](/docs/design/failover) recovers its WAL and completes the transfer itself |
| Source, after proposing completion | The completion either landed or it did not. The retry settles it, and the epoch check makes a duplicate harmless |
| Target, before completion | Routing ignores a transfer to a fenced node. The completion still re-points the stream, and failover then places it on a live node |

The watcher loop on every node retries once per second.

The epoch requirement ties each completion to one specific close. A completion left over from an earlier attempt is rejected once the stream has moved on, so retries are always safe and a stale completion cannot undo later state.
