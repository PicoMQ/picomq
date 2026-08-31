import { describe, expect, it } from 'vitest'
import { Producer, type ProducerClient } from '../src/producer'
import { RetryPolicy } from '../src/retry'
import { toEnvelopes } from '../src/record'
import { ClientError } from '../src/error'
import type { AppendInput, ProducerAck, ProducerRef } from '../src/types'

interface Call {
  records: AppendInput[]
  producer: ProducerRef
}

function fakeClient(
  respond: (call: Call, index: number) => ProducerAck | Error,
): { client: ProducerClient; calls: Call[] } {
  const calls: Call[] = []
  const client: ProducerClient = {
    async appendAs(_name, records, producer) {
      const call = { records, producer }
      calls.push(call)
      const result = respond(call, calls.length - 1)
      if (result instanceof Error) throw result
      return result
    },
  }
  return { client, calls }
}

function okAck(start: number, count: number): ProducerAck {
  return {
    applied: true,
    duplicate: false,
    ack: { start: String(start), next: String(start + count) },
  }
}

describe('Producer', () => {
  it('rejects oversized records before queueing', async () => {
    const { client } = fakeClient(() => okAck(0, 1))
    const producer = new Producer(client, '/s', 'id', { maxBufferedBytes: 8 })
    await expect(producer.send(new Uint8Array(16))).rejects.toMatchObject({
      code: 'record_too_large',
    })
    await producer.close().catch(() => undefined)
  })

  it('batches queued records and resolves sequential seqs', async () => {
    let next = 0
    const { client, calls } = fakeClient((call) => {
      const ack = okAck(next, call.records.length)
      next += call.records.length
      return ack
    })
    const producer = new Producer(client, '/s', 'id', { lingerMs: 5 })
    const a = await producer.send('one')
    const b = await producer.send('two')
    const c = await producer.send('three')
    expect(await a.durable()).toBe(0)
    expect(await b.durable()).toBe(1)
    expect(await c.durable()).toBe(2)
    expect(calls.length).toBeLessThanOrEqual(2)
    const total = calls.reduce((n, call) => n + call.records.length, 0)
    expect(total).toBe(3)
    await producer.close()
  })

  it('resolves duplicates from the ack next seq', async () => {
    const { client } = fakeClient((call) => ({
      applied: false,
      duplicate: true,
      ack: { start: '7', next: String(7 + call.records.length) },
    }))
    const producer = new Producer(client, '/s', 'id', { lingerMs: 0 })
    const seq = await producer.sendDurable('x')
    expect(seq).toBe(7)
    await producer.close()
  })

  it('retries sequence gaps until the batch applies', async () => {
    const { client, calls } = fakeClient((_call, index) => {
      if (index === 0) {
        return new ClientError('conflict', 'gap', { status: 409, code: 'sequence_gap' })
      }
      return okAck(0, 1)
    })
    const producer = new Producer(client, '/s', 'id', {
      lingerMs: 0,
      retry: new RetryPolicy(3, 0, 0, 1),
    })
    expect(await producer.sendDurable('x')).toBe(0)
    expect(calls).toHaveLength(2)
    await producer.close()
  })

  it('poisons the session after a terminal failure', async () => {
    const { client } = fakeClient(
      () => new ClientError('bad_request', 'rejected', { status: 400 }),
    )
    const producer = new Producer(client, '/s', 'id', { lingerMs: 0, retry: RetryPolicy.none() })
    const pending = await producer.send('x')
    await expect(pending.durable()).rejects.toMatchObject({ status: 400 })
    await expect(producer.send('y')).rejects.toMatchObject({ code: 'producer_poisoned' })
    await expect(producer.close()).rejects.toMatchObject({ code: 'producer_poisoned' })
  })

  it('does not trigger unhandled rejections for unawaited sends', async () => {
    const rejections: unknown[] = []
    const onRejection = (reason: unknown) => {
      rejections.push(reason)
    }
    process.on('unhandledRejection', onRejection)
    try {
      const { client } = fakeClient(
        () => new ClientError('bad_request', 'rejected', { status: 400 }),
      )
      const producer = new Producer(client, '/s', 'id', { lingerMs: 0, retry: RetryPolicy.none() })
      await producer.send('x')
      await producer.close().catch(() => undefined)
      await new Promise((resolve) => setTimeout(resolve, 20))
      expect(rejections).toEqual([])
    } finally {
      process.off('unhandledRejection', onRejection)
    }
  })

  it('completes the first batch before pipelining later ones', async () => {
    const events: string[] = []
    let releaseFirst!: () => void
    const firstDone = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
    const client: ProducerClient = {
      async appendAs(_name, records, producer) {
        events.push(`start:${producer.seq}`)
        if (producer.seq === 0) await firstDone
        events.push(`end:${producer.seq}`)
        return okAck(producer.seq, records.length)
      },
    }
    const producer = new Producer(client, '/s', 'id', { lingerMs: 0, maxInflight: 8 })
    const a = await producer.send('a')
    await new Promise((resolve) => setTimeout(resolve, 10))
    const b = await producer.send('b')
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(events).toEqual(['start:0'])
    releaseFirst()
    await a.durable()
    await b.durable()
    expect(events).toEqual(['start:0', 'end:0', 'start:1', 'end:1'])
    await producer.close()
  })

  it('accepts record objects with headers', async () => {
    const { client, calls } = fakeClient((call) => okAck(0, call.records.length))
    const producer = new Producer(client, '/s', 'id', { lingerMs: 0 })
    await producer.sendDurable({ body: 'x', headers: { a: 'b' } })
    const envelopes = toEnvelopes(calls[0]!.records)
    expect(envelopes[0]!.headers).toEqual({ a: 'b' })
    await producer.close()
  })
})
