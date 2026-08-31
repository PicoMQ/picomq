# `@picomq/client`

```bash
cd client/typescript
npm install
npm run build
npm test
```

```ts
import { connect, Producer } from '@picomq/client'

const pico = connect('pico', 'http://127.0.0.1:4437')
await pico.create('/demo', 'application/octet-stream')
await pico.append('/demo', [new TextEncoder().encode('hi')], 'application/octet-stream')

for await (const ev of pico.subscribe('/demo', '0', { reconnect: false })) {
  if (ev.type === 'control' && ev.upToDate) break
}

const producer = new Producer(pico, '/demo', 'writer-1')
await (await producer.send(new TextEncoder().encode('x'))).durable()
await producer.close()
```
