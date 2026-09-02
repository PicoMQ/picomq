# TypeScript client

`@picomq/client` is the TypeScript SDK for the HTTP protocols. It speaks the native Pico protocol and [Durable Streams](/docs/design/protocols) behind one `StreamApi` surface, and includes a batching producer for high-throughput appends. It runs on Node 20+ and anywhere else `fetch` and web streams exist.

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

Records accept `Uint8Array`, `string`, or `{ body, key, headers }`. Header values are strings or `Uint8Array`. Records read back carry the server timestamp and the key when one was set. Pass `{ live: true }` to `records()` to keep tailing after catching up instead of returning.

## Configuration

`connect` takes an optional `ClientConfig`:

| Field | Meaning |
| --- | --- |
| `token` | Bearer token sent on every request, including each redirect hop. |
| `http2` | Speak cleartext HTTP/2 (h2c) for multiplexed appends. Opt-in, the server must support it. |
| `retry` | `RetryPolicy` applied to idempotent calls (`create`, `head`, `read`, `list`, `close`, `delete`, `trim`). Plain `append` is never retried because it has no dedupe key, use a producer when you need safe retries. |

The client follows ownership redirects (`307`) itself and re-attaches the credential on every hop, which standard HTTP clients refuse to do across origins. See the [HTTP API conventions](/docs/api#conventions).

## Producers

For exactly-once appends, a producer batches sends, sequences them under a producer id and epoch, and dedupes retries server side.

```ts
const producer = stream.producer('writer-1')
const seq = await producer.sendDurable('x')
await producer.close()
```

`send()` returns a `Pending` whose `durable()` resolves to the assigned sequence number. If a batch fails terminally the session is poisoned and further sends throw, so open a new producer with a higher epoch to continue.

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

## Errors

Every failure is a `ClientError` with a stable `kind` (`not_found`, `closed`, `conflict`, `stale_epoch`, `transport`, and friends), the HTTP `status`, and a machine readable `code`.

## Protocol differences

The `StreamApi` surface exposes the union of both protocols and throws an `unsupported` error where one side has no equivalent. Notably, listing streams is Pico-only, batch appends over Durable Streams are limited to one record per request, and position tokens are protocol-specific strings, so always feed back the `next` value a call returned rather than constructing one.
