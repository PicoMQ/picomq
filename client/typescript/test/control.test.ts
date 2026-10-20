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
    const options = {
      partitions: 3,
      retentionMs: 60000,
      schema: 'order.json',
      schemaValidate: true,
    }

    expect(await new PicoClient('http://example.test').create('/orders', options)).toBe(true)
    await new DsClient('http://example.test').create('/orders', options)

    const pico = fetch.mock.calls[0]![1]!.headers as Headers
    expect(pico.get('Content-Type')).toBe('application/octet-stream')
    expect(pico.get('Pico-Partitions')).toBe('3')
    expect(pico.get('Pico-Retention-Ms')).toBe('60000')
    expect(pico.get('Pico-Schema-Validate')).toBe('true')
    expect(pico.get('Pico-TTL')).toBeNull()
    const ds = fetch.mock.calls[1]![1]!.headers as Headers
    expect(ds.get('Stream-Partitions')).toBe('3')
    expect(ds.get('Stream-Schema')).toBe('order.json')
  })

  it('patches the fields it is given, null removing a setting', async () => {
    const fetch = vi.fn().mockResolvedValue(
      Response.json({
        schema: null,
        schemaValidate: false,
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
      schemaValidate: false,
      kafkaTopic: 'orders',
      partitions: 4,
      retentionBytes: 4096,
    })
  })

  it('escapes schema names per path segment', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetch)

    await new PicoClient('http://example.test').deleteSchema('team/orders#v2 final.json')

    expect(fetch.mock.calls[0]![0]).toBe(
      'http://example.test/_schemas/team/orders%23v2%20final.json',
    )
  })

  it('decodes control errors and missing topics and schemas', async () => {
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
    expect(await client.getSchema('order.json')).toBeNull()
    expect(await client.deleteSchema('order.json')).toBe(false)
  })
})
