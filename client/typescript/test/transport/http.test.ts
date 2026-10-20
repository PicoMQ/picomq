import { afterEach, describe, expect, it, vi } from 'vitest'
import { Http } from '../../src/transport/http'

function redirect(status: number, location: string): Response {
  return new Response(null, { status, headers: { location } })
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('Http', () => {
  it('keeps authorization on same-origin redirects', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(redirect(307, 'http://a.test/moved'))
      .mockResolvedValueOnce(new Response('ok', { status: 200 }))
    vi.stubGlobal('fetch', fetch)

    const http = new Http('http://a.test', 'secret')
    const response = await http.send({ method: 'GET', url: 'http://a.test/x' })
    expect(response.status).toBe(200)
    expect(fetch).toHaveBeenCalledTimes(2)
    expect(String(fetch.mock.calls[1]![0])).toBe('http://a.test/moved')
    const headers = fetch.mock.calls[1]![1]!.headers as Headers
    expect(headers.get('authorization')).toBe('Bearer secret')
  })

  it('keeps authorization on cross-origin ownership redirects', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(redirect(308, 'http://b.test/moved'))
      .mockResolvedValueOnce(new Response('ok', { status: 200 }))
    vi.stubGlobal('fetch', fetch)

    const http = new Http('http://a.test', 'secret')
    await http.send({ method: 'GET', url: 'http://a.test/x' })
    const headers = fetch.mock.calls[1]![1]!.headers as Headers
    expect(headers.get('authorization')).toBe('Bearer secret')
  })

  it('goes straight to a learned owner until it is gone', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(redirect(307, 'http://b.test/orders?partition=1&seq=0'))
      .mockResolvedValueOnce(new Response('ok', { status: 200 }))
      .mockResolvedValueOnce(new Response('ok', { status: 200 }))
      .mockRejectedValueOnce(new TypeError('fetch failed'))
      .mockResolvedValueOnce(new Response('ok', { status: 200 }))
    vi.stubGlobal('fetch', fetch)

    const http = new Http('http://a.test')
    await http.send({ method: 'GET', url: 'http://a.test/orders?partition=1&seq=0' })
    await http.send({ method: 'GET', url: 'http://a.test/orders?partition=1&seq=5' })
    expect(String(fetch.mock.calls[2]![0])).toBe('http://b.test/orders?partition=1&seq=5')

    await expect(
      http.send({ method: 'GET', url: 'http://a.test/orders?partition=1&seq=6' }),
    ).rejects.toMatchObject({ kind: 'transport' })
    await http.send({ method: 'GET', url: 'http://a.test/orders?partition=1&seq=6' })
    expect(String(fetch.mock.calls[4]![0])).toBe('http://a.test/orders?partition=1&seq=6')
  })

  it('forgets an owner once its stream is deleted', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(redirect(307, 'http://b.test/orders'))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
    vi.stubGlobal('fetch', fetch)

    const http = new Http('http://a.test')
    await http.send({ method: 'DELETE', url: 'http://a.test/orders' })
    await http.send({ method: 'HEAD', url: 'http://a.test/orders' })
    expect(String(fetch.mock.calls[2]![0])).toBe('http://a.test/orders')
  })

  it('keeps an owner path prefix for learned routes', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(redirect(307, 'https://gw.test/node2/orders?seq=0'))
      .mockResolvedValueOnce(new Response('ok', { status: 200 }))
      .mockResolvedValueOnce(new Response('ok', { status: 200 }))
    vi.stubGlobal('fetch', fetch)

    const http = new Http('https://gw.test/node1/')
    await http.send({ method: 'GET', url: 'https://gw.test/node1/orders?seq=0' })
    await http.send({ method: 'GET', url: 'https://gw.test/node1/orders?seq=7' })
    expect(String(fetch.mock.calls[2]![0])).toBe('https://gw.test/node2/orders?seq=7')
  })

  it('fails after too many redirects', async () => {
    const fetch = vi.fn().mockResolvedValue(redirect(307, 'http://a.test/loop'))
    vi.stubGlobal('fetch', fetch)

    const http = new Http('http://a.test')
    await expect(http.send({ method: 'GET', url: 'http://a.test/x' })).rejects.toMatchObject({
      code: 'too_many_redirects',
    })
  })

  it('maps network failures to transport errors', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockRejectedValue(new TypeError('fetch failed')),
    )

    const http = new Http('http://a.test')
    await expect(http.send({ method: 'GET', url: 'http://a.test/x' })).rejects.toMatchObject({
      kind: 'transport',
    })
  })
})
