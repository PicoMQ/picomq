# `@picomq/client`

TypeScript client for PicoMQ over HTTP. Supports the Pico protocol and Durable Streams with one `StreamApi` surface.

```bash
cd client/typescript
npm install
npm run build
npm test
```

## Quick start

```ts
import { connect } from '@picomq/client'

const pico = connect('pico', 'http://127.0.0.1:4437')
const stream = pico.stream('/demo')

await stream.create('application/octet-stream')
await stream.append(['hi', { body: 'with metadata', headers: { kind: 'greeting' } }])

for await (const record of stream.records()) {
  console.log(record.position, new TextDecoder().decode(record.body))
}
```

Records accept `Uint8Array`, `string`, or `{ body, headers, timestamp }`. Pass `{ live: true }` to `records()` to keep tailing after catching up.

## Producers

A producer batches sends, sequences them under a producer id and epoch, and dedupes retries server side.

```ts
const producer = stream.producer('writer-1')
const seq = await producer.sendDurable('x')
await producer.close()
```

`send()` returns a `Pending` whose `durable()` resolves to the assigned sequence number. If a batch fails terminally the session is poisoned and further sends throw, so open a new producer with a higher epoch to continue.

## Retries

Pass a `RetryPolicy` to retry idempotent calls (`create`, `head`, `read`, `list`, `close`, `delete`, `trim`) on transport errors, 429s, and 5xx responses. Plain `append` is never retried by the client because it has no dedupe key, use a producer when you need safe retries.

```ts
import { connect, RetryPolicy } from '@picomq/client'

const pico = connect('pico', 'http://127.0.0.1:4437', { retry: RetryPolicy.attempts(5) })
```

## Live subscriptions

```ts
for await (const event of stream.subscribe(pico.beginning())) {
  if (event.type === 'data') {
    for (const record of event.records) console.log(record.position)
  }
  if (event.type === 'control' && event.upToDate) break
}
```

Subscriptions reconnect with exponential backoff by default. Tune with `reconnect`, `maxReconnectAttempts`, `reconnectDelayMs`, and `maxReconnectDelayMs`, and cancel with an `AbortSignal`.

## Auth

Pass `token` to send `Authorization: Bearer` on every request, including ownership redirects to another node's hostname.

```ts
const pico = connect('pico', 'http://127.0.0.1:4437', { token: process.env.PICO_TOKEN })
```

## Errors

Every failure is a `ClientError` with a stable `kind` (`not_found`, `closed`, `conflict`, `stale_epoch`, `transport`, and friends), the HTTP `status`, and a machine readable `code`.
