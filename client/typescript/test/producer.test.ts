import { describe, expect, it } from 'vitest'
import { PicoClient } from '../src/pico'
import { Producer } from '../src/producer'

describe('Producer', () => {
  it('rejects oversized records before queueing', async () => {
    const client = new PicoClient('http://127.0.0.1:4437')
    const producer = new Producer(client, '/s', 'id', { maxBufferedBytes: 8 })
    await expect(producer.send(new Uint8Array(16))).rejects.toMatchObject({
      code: 'record_too_large',
    })
    await producer.close().catch(() => undefined)
  })
})
