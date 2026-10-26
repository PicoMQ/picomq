import { afterEach, describe, expect, it, vi } from 'vitest'
import { DsClient } from '../../src/ds/client'
import { ClientError } from '../../src/error'

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('DsClient', () => {
  const client = new DsClient('http://127.0.0.1:4437')

  it('rejects multi-record append', async () => {
    await expect(
      client.append('/s', [new Uint8Array([1]), new Uint8Array([2])]),
    ).rejects.toBeInstanceOf(ClientError)
  })

  it('rejects record headers', async () => {
    await expect(
      client.append('/s', [{ body: 'x', headers: [{ name: 'a', value: 'b' }] }]),
    ).rejects.toMatchObject({ kind: 'unsupported' })
  })

  it('rejects null record bodies', async () => {
    await expect(client.append('/s', [{ body: null }])).rejects.toMatchObject({
      kind: 'unsupported',
    })
  })

  it('rejects list', async () => {
    await expect(client.list('/')).rejects.toBeInstanceOf(ClientError)
  })
})

describe('DsClient partitions', () => {
  function respond(headers: Record<string, string>): ReturnType<typeof vi.fn> {
    return vi.fn().mockImplementation(async () => new Response(null, { status: 200, headers }))
  }

  function urls(fetch: ReturnType<typeof vi.fn>): string[] {
    return fetch.mock.calls.map((call) => call[0] as string)
  }

  it('sends the partition on head, append, and read when set', async () => {
    const fetch = respond({ 'Stream-Next-Offset': '1', 'Stream-Up-To-Date': 'true' })
    vi.stubGlobal('fetch', fetch)

    const client = new DsClient('http://example.test')

    await client.head('/s', { partition: 1 })
    await client.append('/s', ['x'], { partition: 1 })
    await client.read('/s', '-1', 'long-poll', {}, { partition: 1 })

    expect(urls(fetch)).toEqual([
      'http://example.test/s?partition=1',
      'http://example.test/s?partition=1',
      'http://example.test/s?offset=-1&live=long-poll&partition=1',
    ])
  })

  it('omits the partition when unset', async () => {
    const fetch = respond({ 'Stream-Next-Offset': '1', 'Stream-Up-To-Date': 'true' })
    vi.stubGlobal('fetch', fetch)

    const client = new DsClient('http://example.test')

    await client.head('/s')
    await client.append('/s', ['x'])
    await client.read('/s', '-1', 'off')

    expect(urls(fetch)).toEqual([
      'http://example.test/s',
      'http://example.test/s',
      'http://example.test/s?offset=-1',
    ])
  })

  it('sends the partition on subscribe only when set', async () => {
    const fetch = vi.fn().mockImplementation(
      async () =>
        new Response('event: control\ndata: {"streamNextOffset":"1","streamClosed":true}\n\n', {
          status: 200,
          headers: { 'Content-Type': 'text/event-stream' },
        }),
    )
    vi.stubGlobal('fetch', fetch)

    const client = new DsClient('http://example.test')
    const events = []
    for await (const event of client.subscribe('/s', '-1', { partition: 0, reconnect: false })) {
      events.push(event)
    }
    for await (const event of client.subscribe('/s', '-1', { reconnect: false })) {
      events.push(event)
    }

    expect(events).toHaveLength(2)
    expect(urls(fetch)).toEqual([
      'http://example.test/s?offset=-1&live=sse&partition=0',
      'http://example.test/s?offset=-1&live=sse',
    ])
  })

  it('decodes partitions from head with absent offsets', async () => {
    vi.stubGlobal('fetch', respond({ 'Stream-Partitions': '4' }))
    const client = new DsClient('http://example.test')

    expect(await client.head('/s')).toEqual({ name: '/s', partitions: 4, closed: false })
  })

  it('decodes a single-partition head with offsets', async () => {
    vi.stubGlobal('fetch', respond({ 'Stream-Partitions': '1', 'Stream-Next-Offset': '7' }))
    const client = new DsClient('http://example.test')

    expect(await client.head('/s')).toEqual({
      name: '/s',
      partitions: 1,
      start: '-1',
      next: '7',
      closed: false,
    })
  })
})
