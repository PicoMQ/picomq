# `@picomq/client`

TypeScript client for PicoMQ over HTTP. Supports the Pico protocol and Durable Streams with one `StreamApi` surface.

```bash
npm install @picomq/client
```

## Quick start

```ts
import { connect } from '@picomq/client'

const pico = connect('pico', 'http://127.0.0.1:4437')
const stream = pico.stream('/demo')

await stream.create()
await stream.append(['hi', { body: 'with metadata', headers: { kind: 'greeting' } }])

for await (const record of stream.records()) {
  console.log(record.position, new TextDecoder().decode(record.body))
}
```

Partitions, producers, consumer groups, live subscriptions, configuration, and errors are covered in the [TypeScript client documentation](https://picomq.com/docs/client/typescript).

## Development

```bash
cd client/typescript
npm install
npm run build
npm test
```
