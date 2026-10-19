# Consumer groups

A consumer group shares the work of reading a set of topics among its members and remembers how far it got. One coordinator serves groups for both protocols: Kafka clients use the standard group APIs, and HTTP clients use the `/_groups` routes in the [HTTP API](/docs/api).

## The group log

Each group has a log of its own, an internal stream like a topic partition's but invisible to clients. The log holds the group's committed offsets. Membership is not stored in it.

| State | Where it lives | Survives a coordinator move |
| --- | --- | --- |
| Committed offsets | The group log | Yes |
| Members, generation, assignment | The coordinator's memory | No, members rejoin |

Offset commits append small delta records. Every `64` commits the coordinator appends a snapshot of the full offset table and trims the log below it, so loading a group replays at most one snapshot and `64` deltas. A snapshot leaves out offsets for topics that were deleted.

## The coordinator

The coordinator of a group is the node that owns its log, decided like any other ownership, see [Ownership and routing](/docs/design/ownership). The log is created on the node that handles the group's first request, so a group starts out coordinated wherever its first member connects.

- **Kafka:** `FindCoordinator` returns the owning node, and a request sent elsewhere answers `NOT_COORDINATOR`.
- **HTTP:** a request sent elsewhere is redirected to the coordinator with a `307`, like a stream request.

When the coordinator's node stops, [failover](/docs/design/failover) moves the log like any stream. The new coordinator rebuilds the offsets from the log. Members find the new coordinator, see their member id is unknown, and rejoin.

## Members

A group runs in one of two modes, fixed by its first member. A member of the other kind is refused with `inconsistent protocol`.

| | Kafka members | HTTP members |
| --- | --- | --- |
| Assignment | The group leader assigns partitions, as in Kafka | The coordinator assigns whole topics |
| Topics | Any number of partitions | Single partition topics only |
| Protocol | JoinGroup, SyncGroup, Heartbeat, LeaveGroup | Join, heartbeat, leave routes |

HTTP members subscribe to a list of topics. The coordinator gives each topic to exactly one member, assigning the topics with the fewest subscribers first, each to the member that holds the fewest so far.

Every join, leave, or expiry starts a rebalance and bumps the group's generation. A member that misses heartbeats for its session timeout, between `1` and `300` seconds, is removed. A commit made as a member is checked against its member id and generation, so a member that was removed or missed a rebalance cannot overwrite the group's progress. Commits made without joining, as Kafka standalone consumers do, are not checked.

## Listing and deleting

- **List:** HTTP `GET /_groups` returns every group in the cluster, read from the group logs in the metadata. Kafka `ListGroups` answers per broker, as Kafka does, with the groups that node coordinates and that have members.
- **Describe:** state, generation, and members, from the coordinator.
- **Delete:** only a group without members can be deleted. Deleting removes its log and its offsets.

## Limits

| Limit | Value |
| --- | --- |
| Groups loaded on one coordinator | `1000`, idle groups are unloaded after `5` minutes and reload from their log |
| Members per group | `10000` |
| Topics or partitions per group | `10000` each |
| Group id | `255` bytes |
