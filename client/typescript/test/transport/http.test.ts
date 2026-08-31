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

    const http = new Http('secret')
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

    const http = new Http('secret')
    await http.send({ method: 'GET', url: 'http://a.test/x' })
    const headers = fetch.mock.calls[1]![1]!.headers as Headers
    expect(headers.get('authorization')).toBe('Bearer secret')
  })

  it('fails after too many redirects', async () => {
    const fetch = vi.fn().mockResolvedValue(redirect(307, 'http://a.test/loop'))
    vi.stubGlobal('fetch', fetch)

    const http = new Http()
    await expect(http.send({ method: 'GET', url: 'http://a.test/x' })).rejects.toMatchObject({
      code: 'too_many_redirects',
    })
  })

  it('maps network failures to transport errors', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockRejectedValue(new TypeError('fetch failed')),
    )

    const http = new Http()
    await expect(http.send({ method: 'GET', url: 'http://a.test/x' })).rejects.toMatchObject({
      kind: 'transport',
    })
  })
})
