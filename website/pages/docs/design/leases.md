# Leases

Some background work should run on one node at a time. Expiring abandoned object reservations and deleting destroyed objects from storage would be wasteful, not incorrect, if every node did them at once. PicoMQ elects that one node with a lease in the SQL database, the same database that already holds the metadata log, so the election needs no extra infrastructure.

## The lease row

The lease is a single row holding the current holder, an epoch, and an expiry time. Acquiring it is a conditional update: take the row if it is free or its TTL has lapsed, bumping the epoch. Renewing is a conditional update at the holder's own epoch. The database's transaction guarantees make both atomic, which is all the election needs.

The TTL is `10` seconds and each node tries to acquire or renew every quarter of that. The epoch is a fencing token. A holder that stalls, loses connectivity, or gets paused past the TTL finds its renewal rejected because a successor bumped the epoch, and it demotes itself instead of acting on stale leadership.

<div class="pico-diagram">
<svg viewBox="40 48 620 180" width="620" role="img" aria-label="Node A holds the lease and renews it, then stops. After the TTL runs out node B acquires the lease at the next epoch.">
  <rect x="60" y="100" width="250" height="40" class="box-accent"/>
  <text x="185" y="124" text-anchor="middle" class="label">node A, epoch 7</text>
  <rect x="310" y="100" width="140" height="40" fill="none" class="edge-soft"/>
  <text x="380" y="124" text-anchor="middle" class="sub">TTL, 10 s</text>
  <rect x="450" y="100" width="190" height="40" class="box-accent"/>
  <text x="545" y="124" text-anchor="middle" class="label">node B, epoch 8</text>
  <path d="M130 88 L130 100" class="edge"/>
  <path d="M200 88 L200 100" class="edge"/>
  <path d="M270 88 L270 100" class="edge"/>
  <text x="200" y="78" text-anchor="middle" class="sub">renews every 2.5 s</text>
  <path d="M310 140 L310 160" class="edge"/>
  <text x="310" y="178" text-anchor="middle" class="sub">node A stops renewing</text>
  <path d="M450 140 L450 160" class="edge-soft"/>
  <text x="450" y="206" text-anchor="middle" class="sub">node B acquires, epoch 8</text>
</svg>
</div>

## What leadership gates

The leadership state feeds a watch channel, and the maintenance loops follow it. They start when the node becomes the holder and stop when it is not. Under the lease run:

- the expiry tick, which reclaims object ids whose upload never committed
- the object cleaner, which deletes destroyed objects from storage
- the [catalog changelog](/docs/design/catalog) projector

The first two are described in [Garbage collection](/docs/design/gc).

Losing the lease is not an event worth reacting to beyond stopping the loops. The work is queued in the replicated state, not in the holder's memory, so the next holder picks up exactly where the last one stopped.

## Handover

A node that shuts down cleanly releases the lease, so a successor takes over on its next attempt rather than waiting out the TTL. A node that crashes leaves the row to expire, and the gap until takeover is at most the TTL plus one check interval. A holder that cannot reach the database keeps its leadership only for as long as the lease cannot have expired underneath it, then self-demotes.

## Why correctness never depends on it

The lease is an optimization, not a safety mechanism. Every action a leader takes goes through the metadata log like any other command, applies are deterministic, and the cleanup commands are idempotent. If a handover briefly overlaps two holders, the result is duplicate proposals where one is applied and the other is answered as redundant. Nothing diverges, some work is done twice.
