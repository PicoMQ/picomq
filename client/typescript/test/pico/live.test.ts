import { describe, expect, it } from 'vitest'
import { connect, ClientError, RetryPolicy } from '../../src/index'
import type { StreamRecord } from '../../src/types'

const ENDPOINT = process.env.PICO_ENDPOINT ?? 'http://127.0.0.1:4437'

async function serverUp(): Promise<boolean> {
  try {
    const response = await fetch(ENDPOINT, { signal: AbortSignal.timeout(1500) })
    return response.ok
  } catch {
    return false
  }
}

const up = await serverUp()
const text = (record: StreamRecord) => new TextDecoder().decode(record.body)

describe.runIf(up)('live pico server', () => {
  const pico = connect('pico', ENDPOINT, { retry: RetryPolicy.attempts(3) })
  const base = `/it-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`
  const CT = 'application/octet-stream'

  it('creates streams idempotently and reports head', async () => {
    const stream = pico.stream(`${base}/basic`)
    expect(await stream.create(CT)).toBe(true)
    expect(await stream.create(CT)).toBe(false)
    const info = await stream.head()
    expect(info).toMatchObject({ name: `${base}/basic`, start: '0', next: '0', closed: false })
  })

  it('appends strings, bytes, and headers and reads them back', async () => {
    const stream = pico.stream(`${base}/mixed`)
    await stream.create(CT)
    const ack = await stream.append([
      'one',
      { body: 'two', headers: { kind: 'demo', n: '2' } },
      new Uint8Array([1, 2, 3]),
    ])
    expect(ack.start).toBe('0')
    expect(ack.next).toBe('3')

    const page = await stream.read('0', 'off')
    expect(page.records).toHaveLength(3)
    expect(page.upToDate).toBe(true)
    expect(text(page.records[0]!)).toBe('one')
    expect(page.records[0]!.position).toBe('0')
    expect(text(page.records[1]!)).toBe('two')
    expect(page.records[1]!.headers).toEqual({ kind: 'demo', n: '2' })
    expect(page.records[2]!.body).toEqual(new Uint8Array([1, 2, 3]))
    expect(page.records[2]!.position).toBe('2')
  })

  it('paginates records() across many pages', async () => {
    const stream = pico.stream(`${base}/paged`)
    await stream.create(CT)
    const total = 53
    await stream.append(Array.from({ length: total }, (_, i) => `r${i}`))

    const seen: string[] = []
    for await (const record of stream.records('0', { batch: { count: 7 } })) {
      seen.push(text(record))
    }
    expect(seen).toEqual(Array.from({ length: total }, (_, i) => `r${i}`))
  })

  it('tails live records while a writer appends', { timeout: 20_000 }, async () => {
    const stream = pico.stream(`${base}/tail`)
    await stream.create(CT)
    const controller = new AbortController()
    const seen: string[] = []

    const reader = (async () => {
      try {
        for await (const record of stream.records('0', { live: true, signal: controller.signal })) {
          seen.push(text(record))
          if (seen.length === 5) controller.abort()
        }
      } catch (error) {
        if (!(error instanceof ClientError) || error.kind !== 'aborted') throw error
      }
    })()

    for (let i = 0; i < 5; i++) {
      await stream.append([`live${i}`])
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    await reader
    expect(seen).toEqual(['live0', 'live1', 'live2', 'live3', 'live4'])
  })

  it('subscribes over sse and catches up', { timeout: 20_000 }, async () => {
    const stream = pico.stream(`${base}/sse`)
    await stream.create(CT)
    await stream.append(['a', 'b', 'c'])

    const bodies: string[] = []
    for await (const event of stream.subscribe('0', { reconnect: false })) {
      if (event.type === 'data') {
        for (const record of event.records) bodies.push(text(record))
      }
      if (event.type === 'control' && event.upToDate) break
    }
    expect(bodies).toEqual(['a', 'b', 'c'])
  })

  it('producer assigns strictly ordered seqs across pipelined batches', { timeout: 30_000 }, async () => {
    const name = `${base}/producer`
    const stream = pico.stream(name)
    await stream.create(CT)

    const producer = stream.producer('writer-1', { lingerMs: 2, maxInflight: 4 })
    const total = 200
    const pendings = []
    for (let i = 0; i < total; i++) {
      pendings.push(await producer.send(`p${i}`))
    }
    const seqs = await Promise.all(pendings.map((p) => p.durable()))
    await producer.close()

    expect(seqs).toEqual(Array.from({ length: total }, (_, i) => i))

    const bodies: string[] = []
    for await (const record of stream.records('0', { batch: { count: 500 } })) {
      bodies.push(text(record))
    }
    expect(bodies).toEqual(Array.from({ length: total }, (_, i) => `p${i}`))
  })

  it('deduplicates producer batches resent with the same seq', async () => {
    const stream = pico.stream(`${base}/dedupe`)
    await stream.create(CT)

    const ref = { id: 'writer-d', epoch: 0, seq: 0 }
    const first = await stream.appendAs(['x', 'y'], ref)
    expect(first.applied).toBe(true)
    expect(first.duplicate).toBe(false)

    const again = await stream.appendAs(['x', 'y'], ref)
    expect(again.applied).toBe(false)
    expect(again.duplicate).toBe(true)
    expect(again.ack.next).toBe(first.ack.next)
  })

  it('rejects stale producer epochs', async () => {
    const stream = pico.stream(`${base}/epoch`)
    await stream.create(CT)

    await stream.appendAs(['e1'], { id: 'writer-e', epoch: 5, seq: 0 })
    await expect(
      stream.appendAs(['e0'], { id: 'writer-e', epoch: 4, seq: 0 }),
    ).rejects.toMatchObject({ kind: 'stale_epoch' })
  })

  it('trims, closes, and deletes a stream', { timeout: 20_000 }, async () => {
    const stream = pico.stream(`${base}/lifecycle`)
    await stream.create(CT)
    await stream.append(['0', '1', '2', '3', '4'])

    let start = await stream.trim(2)
    const deadline = Date.now() + 15_000
    while (start !== '2' && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 250))
      start = await stream.trim(2)
    }
    expect(start).toBe('2')
    expect((await stream.head())!.start).toBe('2')

    const tail: string[] = []
    for await (const record of stream.records('2')) tail.push(text(record))
    expect(tail).toEqual(['2', '3', '4'])

    const next = await stream.close()
    expect(next).toBe('5')
    await expect(stream.append(['late'])).rejects.toMatchObject({ kind: 'closed' })

    expect(await stream.delete()).toBe(true)
    expect(await stream.head()).toBeNull()
    expect(await stream.delete()).toBe(false)
  })

  it('lists streams under a prefix', async () => {
    const listing = await pico.list(`${base}/`, 100)
    const names = listing.streams.map((s) => s.name)
    expect(names).toContain(`${base}/basic`)
    expect(names).toContain(`${base}/mixed`)
    expect(names).not.toContain(`${base}/never-created`)
  })

  it('maps missing streams to not_found', async () => {
    await expect(pico.read(`${base}/nope`, '0', 'off')).rejects.toMatchObject({
      kind: 'not_found',
      status: 404,
    })
  })

  it('aborts long-poll reads promptly', { timeout: 10_000 }, async () => {
    const stream = pico.stream(`${base}/poll`)
    await stream.create(CT)
    const controller = new AbortController()
    setTimeout(() => controller.abort(), 200)
    const started = Date.now()
    await expect(
      stream.read('0', 'long-poll', {}, { signal: controller.signal }),
    ).rejects.toMatchObject({ kind: 'aborted' })
    expect(Date.now() - started).toBeLessThan(5000)
  })

  it('cleans up its test streams', async () => {
    const listing = await pico.list(`${base}/`, 200)
    for (const info of listing.streams) {
      await pico.delete(info.name)
    }
  })
})

describe.runIf(!up)('live pico server (skipped)', () => {
  it('skips because no server is reachable at ' + ENDPOINT, () => {
    expect(up).toBe(false)
  })
})
