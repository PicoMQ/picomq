# Failover

A node that stops serving, by crashing, hanging, or losing its network, must not hold its streams hostage. Failover detects such a node, fences it so it can never write again, recovers what it had buffered, and moves its streams to live nodes. The lease holder runs it, see [Leases](/docs/design/leases), and every step goes through the metadata log.

## Liveness

Every node proposes a heartbeat command every `--heartbeat-interval-sec` (default `5`). Applying it increments a counter on the node's row. The lease holder checks the rows once a second.

The clock it uses is not the wall clock. It is the holder's own heartbeat count, as applied in the log. A node is dead once the holder has heartbeated `ceil(node timeout / heartbeat interval)` times without seeing a heartbeat from it. With the defaults, `30` and `5` seconds, that is `6` heartbeats.

| Situation | Result |
| --- | --- |
| A node stops heartbeating | Fenced after the timeout |
| The lease holder is paused or cut off from the log | Its own heartbeats stop, so its clock stops. Nobody is fenced |
| The metadata store is down | No heartbeats land, including the holder's. Nobody is fenced |
| A node restarts | It registers at a new epoch, which restarts its clock |
| The lease holder is itself fenced | It skips failover until it is live again |

Counting in the log means a node is only ever judged against heartbeats the holder actually saw land. Time the holder could not observe never counts against anyone. The node timeout must be greater than the heartbeat interval, and startup refuses any other combination.

## Fencing

Fencing a node is the `FenceNode` command. It names the node, its epoch, and the heartbeat count the proposer saw, and it applies only if the row still matches: same epoch, same count, not yet fenced. A node that heartbeated or re-registered in the meantime is left alone, so a holder acting on an old view cannot fence a node that is alive.

A node can also fence itself, which is how a clean shutdown hands off, below. Fencing itself is unconditional.

Once a node is fenced:

- Every command it proposes is rejected, see [Metadata](/docs/design/metadata).
- Routing stops sending requests to it, see [Ownership and routing](/docs/design/ownership).
- `/ready` and `/admin/nodes` report `fenced: true`.
- The node notices the fence in its view and stops.

## Taking over

On each tick the holder walks every fenced node's streams and moves them on.

<div class="pico-diagram">
<svg viewBox="20 0 680 380" width="680" role="img" aria-label="The lease holder fences the silent node. Closed streams are re-placed at once. The holder takes over the dead node's WAL, recovers buffered records, and closes its open streams. The next tick re-places them and a live node opens them at the next epoch.">
  <defs>
    <marker id="arrf" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="40" y="20" width="140" height="44" class="box"/>
  <text x="110" y="47" text-anchor="middle" class="label">lease holder</text>
  <rect x="280" y="20" width="140" height="44" class="box"/>
  <text x="350" y="47" text-anchor="middle" class="label">metadata log</text>
  <rect x="520" y="20" width="160" height="44" class="box"/>
  <text x="600" y="47" text-anchor="middle" class="label">dead node's WAL</text>
  <path d="M110 64 L110 340" class="edge-soft"/>
  <path d="M350 64 L350 340" class="edge-soft"/>
  <path d="M600 64 L600 340" class="edge-soft"/>
  <path d="M118 100 L342 100" class="edge" marker-end="url(#arrf)"/>
  <text x="230" y="90" text-anchor="middle" class="sub">fence node at epoch, count</text>
  <path d="M118 150 L342 150" class="edge" marker-end="url(#arrf)"/>
  <text x="230" y="140" text-anchor="middle" class="sub">re-place closed streams</text>
  <path d="M118 200 L592 200" class="edge" marker-end="url(#arrf)"/>
  <text x="355" y="190" text-anchor="middle" class="sub">take over: its writer is fenced from here on</text>
  <rect x="520" y="216" width="160" height="36" class="box-accent"/>
  <text x="600" y="238" text-anchor="middle" class="sub">recover, upload</text>
  <path d="M592 270 L358 270" class="edge" marker-end="url(#arrf)"/>
  <text x="475" y="260" text-anchor="middle" class="sub">close its open streams</text>
  <path d="M118 316 L342 316" class="edge" marker-end="url(#arrf)"/>
  <text x="230" y="306" text-anchor="middle" class="sub">next tick: re-place them</text>
  <text x="350" y="366" text-anchor="middle" class="sub">a live node opens each stream at the next epoch</text>
</svg>
</div>

| A stream on the fenced node that is | Becomes |
| --- | --- |
| Closed | Placed on the least loaded live node at once |
| In a pending transfer | Completed to the transfer target, see [Transfers](/docs/design/transfers) |
| Deleted, with its index entry already gone | Destroyed |
| Open | Recovered first, then placed on the next tick |

An open stream may have acknowledged records that exist only in the dead node's WAL. The holder therefore takes over that WAL before the stream moves:

1. It opens the WAL as the dead node, which fences the dead node's writer. How each backend fences is in [Write-ahead log](/docs/design/wal).
2. It replays the WAL and uploads the records that were not yet in data objects.
3. It closes the streams the dead node had open.
4. The next tick places them on live nodes, which open them at the next epoch.

Until step 3, requests for such a stream get a retryable error: HTTP `409`, or Kafka `NOT_LEADER_OR_FOLLOWER`. No acknowledged record is lost, and nothing is written under the dead node's epoch after the takeover.

## When a node stops

| Trigger | How the node notices |
| --- | --- |
| Its row is fenced, or replaced by a newer registration | Its view |
| Its WAL is taken over | Its next WAL write fails as fenced |
| `SIGTERM` or `Ctrl-C` | The signal, followed by a clean shutdown |

A fenced node stops and exits so a supervisor can restart it. The restart registers at a new epoch and the node rejoins empty. [Docker](/docs/operations/deployment/docker) covers what the runtime does.

## Clean shutdown

A clean shutdown hands off without waiting for the node timeout.

1. Readiness fails, so load balancers stop sending traffic.
2. The node waits `--shutdown-drain-sec`.
3. Live reads end. Long polls, SSE subscriptions, and Kafka fetches waiting for data answer `503 unavailable`, or Kafka `NOT_LEADER_OR_FOLLOWER`, and their clients reconnect elsewhere.
4. Listeners stop accepting, and in-flight requests finish.
5. The node closes its streams, which uploads their buffered records first.
6. It fences itself. The next failover tick, about a second later, places its streams on live nodes.

| | Clean stop | Crash or partition |
| --- | --- | --- |
| Detected by | The node fencing itself | Missed heartbeats |
| Streams move after | About a second | The node timeout, then WAL recovery |
| Buffered records | Flushed by the node | Recovered from its WAL by the lease holder |

## Metadata outages

If the metadata store is unreachable, nodes keep serving the streams they hold. The engine's own metadata commands, such as committing objects, retry with backoff until the store is back, and acknowledged appends stay durable in the WAL meanwhile. Operations that change metadata directly, such as creating a topic or issuing a token, fail with `503 unavailable`, or Kafka `NOT_LEADER_OR_FOLLOWER`, and the client retries. Nobody is fenced, because no heartbeats land, the lease holder's included.

## Why it is safe

The lease decides who runs failover, not whether failover is correct. Two overlapping holders, a stale view, or a node that comes back from a long pause cannot produce two writers:

- `FenceNode` applies only if the node is exactly as the proposer saw it.
- A fenced node's commands are rejected by apply.
- A new owner opens a stream at a higher epoch, which fences writes at the old one.
- A taken over WAL rejects its original writer, which then stops.
- A holder that is not live itself does not run failover.
