# TypeScript client

`@picomq/client` is the TypeScript SDK for the HTTP protocols. It speaks the native Pico protocol and [Durable Streams](/docs/design/protocols) behind one `StreamApi` surface, and adds a batching producer and consumer group members on Pico. It runs on Node 20+ and anywhere else `fetch` and web streams exist.

## Install

```bash
npm install @picomq/client
```

## Usage

```ts
import { connect } from '@picomq/client'

const pico = connect('pico', 'http://127.0.0.1:4437')
const stream = pico.stream('/orders/1042')

await stream.create('application/json')
await stream.append([JSON.stringify({ item: 'widget' })])

for await (const record of stream.records()) {
  console.log(record.position, new TextDecoder().decode(record.body))
}
```

Records accept `Uint8Array`, `string`, or `{ body, key, headers, timestamp }`. Header values are strings or `Uint8Array`. Records read back carry the server timestamp, and the key when one was set. `records()` starts at the beginning, or at a position you pass, and `records(from, { live: true })` keeps tailing after catching up. Positions are protocol-specific strings, so always continue from the `next` a call returned.

## Partitions

A topic created over Kafka can have several partitions. Calls that address one take a `partition` option: `head`, `append`, `read`, `records`, `subscribe`, `trim`, and producers.

```ts
for await (const record of stream.records(pico.beginning(), { partition: 2 })) {
  console.log(record.position)
}
```

## Producers

A producer batches sends, sequences them under a producer id and epoch, and dedupes retries server side.

```ts
const producer = stream.producer('writer-1')
const seq = await producer.sendDurable('x')
await producer.close()
```

`send()` returns a `Pending` whose `durable()` resolves to the assigned sequence number. If a batch fails terminally the producer is poisoned and further sends throw, so open a new producer with a higher `epoch` to continue. For one-off idempotent appends, `appendAs` sends records under an explicit producer id, epoch, and sequence.

## Consumer groups

A `GroupMember` joins, heartbeats in the background, and follows rebalances. See [Consumer groups](/docs/design/groups) for how assignment works.

```ts
import { GroupMember } from '@picomq/client'

const member = await GroupMember.join(pico, 'billing', ['/orders', '/payments'])
for (const topic of member.assignment().streams) {
  await member.commit({ [topic]: { position: 42 } })
}
await member.leave()
```

`assignments()` yields each new assignment after a rebalance, and `error()` reports a member that could not rejoin. The lower-level calls are on the Pico client: `joinGroup`, `heartbeat`, `groupAssignment`, `leaveGroup`, `commitOffsets`, `fetchOffsets`, `describeGroup`, `listGroups`, and `deleteGroup`.

## Live subscriptions

`subscribe` is the SSE path, delivering data and control events as they arrive:

```ts
for await (const event of stream.subscribe(pico.beginning())) {
  if (event.type === 'data') {
    for (const record of event.records) console.log(record.position)
  }
  if (event.type === 'control' && event.upToDate) break
}
```

Subscriptions reconnect with exponential backoff by default. Tune with `reconnect`, `maxReconnectAttempts`, `reconnectDelayMs`, and `maxReconnectDelayMs`, and cancel with an `AbortSignal`.

## Configuration

`connect` takes an optional `ClientConfig`:

| Field | Meaning |
| --- | --- |
| `token` | Bearer token sent on every request, including each redirect hop |
| `http2` | Speak cleartext HTTP/2 (h2c) for multiplexed appends. The server must support it |
| `retry` | `RetryPolicy` for idempotent calls: `create`, `head`, `read`, `list`, `close`, `delete`, `trim`. Plain `append` is never retried, because it has no dedupe key |

The client follows ownership redirects itself and re-attaches the credential on every hop, see the [HTTP API conventions](/docs/api#conventions).

## Errors

Every failure is a `ClientError` with a stable `kind`, such as `not_found`, `closed`, `conflict`, `stale_epoch`, `permission_denied`, `transport`, or `unsupported`, plus the HTTP `status` and the server's `code`. Durable Streams has no equivalent for `list`, `trim`, or groups, which throw `unsupported`.
