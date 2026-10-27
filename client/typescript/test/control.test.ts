import { afterEach, describe, expect, it, vi } from 'vitest'
import { DsClient } from '../src/ds/client'
import { PicoClient } from '../src/pico/client'

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('topic settings', () => {
  it('sends create options as each protocol headers', async () => {
    const fetch = vi.fn().mockImplementation(async () => new Response(null, { status: 201 }))
    vi.stubGlobal('fetch', fetch)
    const options = { partitions: 3, retentionMs: 60000 }

    expect(await new PicoClient('http://example.test').create('/orders', options)).toBe(true)
    await new DsClient('http://example.test').create('/orders', options)

    const pico = fetch.mock.calls[0]![1]!.headers as Headers
    expect(pico.get('Content-Type')).toBe('application/octet-stream')
    expect(pico.get('Pico-Partitions')).toBe('3')
    expect(pico.get('Pico-Retention-Ms')).toBe('60000')
    expect(pico.get('Pico-TTL')).toBeNull()
    const ds = fetch.mock.calls[1]![1]!.headers as Headers
    expect(ds.get('Stream-Partitions')).toBe('3')
    expect(ds.get('Stream-Retention-Ms')).toBe('60000')
  })

  it('patches the fields it is given, null removing a setting', async () => {
    const fetch = vi.fn().mockResolvedValue(
      Response.json({
        kafkaTopic: 'orders',
        partitions: 4,
        retentionMs: null,
        retentionBytes: 4096,
      }),
    )
    vi.stubGlobal('fetch', fetch)

    const config = await new PicoClient('http://example.test').updateConfig('/orders', {
      retentionMs: null,
      retentionBytes: 4096,
      partitions: 4,
    })

    const [url, init] = fetch.mock.calls[0]!
    expect(url).toBe('http://example.test/_streams/orders')
    expect(init.method).toBe('PATCH')
    expect(JSON.parse(init.body as string)).toEqual({
      retentionMs: null,
      retentionBytes: 4096,
      partitions: 4,
    })
    expect(config).toEqual({
      kafkaTopic: 'orders',
      partitions: 4,
      retentionBytes: 4096,
    })
  })

  it('escapes names on data routes as on control routes', async () => {
    const fetch = vi.fn().mockImplementation(async () => new Response(null, { status: 404 }))
    vi.stubGlobal('fetch', fetch)
    const client = new PicoClient('http://example.test')

    await client.head('/streams/a%41 #1')
    await client.config('/streams/a%41 #1')

    expect(fetch.mock.calls[0]![0]).toBe('http://example.test/streams/a%2541%20%231')
    expect(fetch.mock.calls[1]![0]).toBe('http://example.test/_streams/streams/a%2541%20%231')
  })

  it('drops only one leading slash', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response(null, { status: 404 }))
    vi.stubGlobal('fetch', fetch)
    const client = new PicoClient('http://example.test')

    await client.config('/orders')
    await client.config('//orders')

    expect(fetch.mock.calls[0]![0]).toBe('http://example.test/_streams/orders')
    expect(fetch.mock.calls[1]![0]).toBe('http://example.test/_streams//orders')
  })

  it('refuses dot segments without sending a request', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetch)
    const client = new PicoClient('http://example.test')

    for (const name of ['..', '../orders', 'a/./b', 'a/..']) {
      await expect(client.delete(name)).rejects.toMatchObject({ kind: 'bad_request' })
    }
    expect(fetch).not.toHaveBeenCalled()
  })

  it('decodes control errors and missing topics', async () => {
    const fetch = vi.fn().mockImplementation(async (_url: string, init: RequestInit) =>
      init.method === 'PATCH'
        ? Response.json({ error: 'partitions must grow' }, { status: 400 })
        : new Response(null, { status: 404 }),
    )
    vi.stubGlobal('fetch', fetch)
    const client = new DsClient('http://example.test')

    await expect(client.updateConfig('/orders', { partitions: 1 })).rejects.toMatchObject({
      kind: 'bad_request',
      message: 'partitions must grow',
    })
    expect(await client.config('/orders')).toBeNull()
  })
})
