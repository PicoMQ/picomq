import { describe, expect, it, vi } from 'vitest'
import { Stream } from '../src/stream'
import type { ReadPage, StreamApi, StreamRecord } from '../src/types'

function record(position: string, body: string): StreamRecord {
  return { position, headers: {}, body: new TextEncoder().encode(body) }
}

function page(records: StreamRecord[], next: string, upToDate: boolean, closed = false): ReadPage {
  return { records, next, upToDate, closed }
}

function fakeApi(pages: ReadPage[]): StreamApi {
  let index = 0
  return {
    protocol: () => 'pico',
    beginning: () => '0',
    now: () => {
      throw new Error('unsupported')
    },
    stream: () => {
      throw new Error('not used')
    },
    create: vi.fn(),
    head: vi.fn(),
    append: vi.fn(),
    read: async () => {
      const next = pages[Math.min(index, pages.length - 1)]!
      index += 1
      return next
    },
    subscribe: () => {
      throw new Error('not used')
    },
    list: vi.fn(),
    close: vi.fn(),
    delete: vi.fn(),
    closeTransport: vi.fn(),
  } as unknown as StreamApi
}

describe('Stream.records', () => {
  it('pages through history and stops when up to date', async () => {
    const api = fakeApi([
      page([record('0', 'a'), record('1', 'b')], '2', false),
      page([record('2', 'c')], '3', true),
    ])
    const stream = new Stream(api, '/s')
    const bodies: string[] = []
    for await (const rec of stream.records()) {
      bodies.push(new TextDecoder().decode(rec.body))
    }
    expect(bodies).toEqual(['a', 'b', 'c'])
  })

  it('stops on a closed stream', async () => {
    const api = fakeApi([page([record('0', 'a')], '1', true, true)])
    const stream = new Stream(api, '/s')
    const positions: string[] = []
    for await (const rec of stream.records()) {
      positions.push(rec.position)
    }
    expect(positions).toEqual(['0'])
  })

  it('stops when an empty page does not advance', async () => {
    const api = fakeApi([page([], '0', false)])
    const stream = new Stream(api, '/s')
    const seen: StreamRecord[] = []
    for await (const rec of stream.records()) {
      seen.push(rec)
    }
    expect(seen).toEqual([])
  })

  it('keeps polling in live mode until closed', async () => {
    const api = fakeApi([
      page([record('0', 'a')], '1', true),
      page([], '1', true),
      page([record('1', 'b')], '2', true, true),
    ])
    const stream = new Stream(api, '/s')
    const bodies: string[] = []
    for await (const rec of stream.records(undefined, { live: true })) {
      bodies.push(new TextDecoder().decode(rec.body))
    }
    expect(bodies).toEqual(['a', 'b'])
  })
})

describe('Stream handle', () => {
  it('binds the stream name for api calls', async () => {
    const api = fakeApi([])
    const stream = new Stream(api, '/s')
    await stream.create('text/plain')
    await stream.head()
    await stream.delete()
    expect(api.create).toHaveBeenCalledWith('/s', 'text/plain', undefined, undefined)
    expect(api.head).toHaveBeenCalledWith('/s', undefined)
    expect(api.delete).toHaveBeenCalledWith('/s', undefined)
  })
})
