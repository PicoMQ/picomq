import { connect } from '../../dist/index.js'

const ENDPOINT = process.env.PICO_ENDPOINT ?? 'http://127.0.0.1:4437'
const CT = 'application/octet-stream'

const pico = connect('pico', ENDPOINT)
const base = `/bench-${Date.now().toString(36)}`

function payload(size, tag) {
  const body = new Uint8Array(size)
  body.set(new TextEncoder().encode(tag))
  return body
}

function fmt(n) {
  return n >= 1000 ? `${(n / 1000).toFixed(1)}k` : n.toFixed(0)
}

async function timed(label, fn) {
  const started = performance.now()
  const out = await fn()
  const seconds = (performance.now() - started) / 1000
  return { label, seconds, ...out }
}

function report({ label, seconds, records, bytes }) {
  const rps = records / seconds
  const mbps = bytes / seconds / (1024 * 1024)
  console.log(
    `${label.padEnd(46)} ${String(records).padStart(7)} rec  ${seconds.toFixed(2).padStart(6)}s  ${fmt(rps).padStart(8)} rec/s  ${mbps.toFixed(1).padStart(7)} MB/s`,
  )
}

async function sequentialAppends(count, size) {
  const stream = pico.stream(`${base}/seq`)
  await stream.create(CT)
  const body = payload(size, 'seq')
  return timed(`sequential append (${size}B, 1 rec/req)`, async () => {
    for (let i = 0; i < count; i++) {
      await stream.append([body])
    }
    return { records: count, bytes: count * size }
  })
}

async function producerRun(name, count, size, config) {
  const stream = pico.stream(`${base}/${name}`)
  await stream.create(CT)
  const producer = stream.producer(`bench-${name}`, config)
  const body = payload(size, name)
  const result = await timed(
    `producer ${name} (linger ${config.lingerMs}ms, inflight ${config.maxInflight})`,
    async () => {
      const pendings = new Array(count)
      for (let i = 0; i < count; i++) {
        pendings[i] = await producer.send(body)
      }
      const seqs = await Promise.all(pendings.map((p) => p.durable()))
      await producer.close()
      for (let i = 0; i < count; i++) {
        if (seqs[i] !== i) throw new Error(`ordering violated at ${i}: got ${seqs[i]}`)
      }
      return { records: count, bytes: count * size }
    },
  )
  const head = await stream.head()
  if (head.next !== String(count)) throw new Error(`expected next ${count}, got ${head.next}`)
  return result
}

async function readBack(name, expected, size) {
  const stream = pico.stream(`${base}/${name}`)
  return timed(`read back ${name} (pages of 1000)`, async () => {
    let records = 0
    let bytes = 0
    for await (const record of stream.records('0', { batch: { count: 1000 } })) {
      records += 1
      bytes += record.body.byteLength
    }
    if (records !== expected) throw new Error(`expected ${expected} records, read ${records}`)
    return { records, bytes: bytes || records * size }
  })
}

async function main() {
  console.log(`endpoint ${ENDPOINT}`)
  console.log('')

  report(await sequentialAppends(300, 128))
  report(await producerRun('batch-1', 5000, 128, { lingerMs: 5, maxInflight: 1 }))
  report(await producerRun('batch-8', 5000, 128, { lingerMs: 5, maxInflight: 8 }))
  report(await producerRun('large-8', 1000, 8192, { lingerMs: 5, maxInflight: 8 }))
  report(await readBack('batch-8', 5000, 128))
  report(await readBack('large-8', 1000, 8192))

  const listing = await pico.list(`${base}/`, 100)
  for (const info of listing.streams) {
    await pico.delete(info.name)
  }
  console.log('')
  console.log(`cleaned up ${listing.streams.length} bench streams`)
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
