import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, AuthRequired, admin, request, token } from '../src/api/client'

const calls: { url: string; init: RequestInit }[] = []

function respond(status: number, body: unknown) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string, init: RequestInit) => {
      calls.push({ url, init })
      return new Response(body === undefined ? null : JSON.stringify(body), { status })
    }),
  )
}

beforeEach(() => {
  const stored = new Map<string, string>()
  vi.stubGlobal('sessionStorage', {
    getItem: (key: string) => stored.get(key) ?? null,
    setItem: (key: string, value: string) => stored.set(key, value),
    removeItem: (key: string) => stored.delete(key),
  })
  calls.length = 0
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('admin', () => {
  it('encodes every segment so topic names with slashes stay one segment', () => {
    expect(admin('topics', '/orders/eu', 'partitions', 3)).toBe('/admin/topics/%2Forders%2Feu/partitions/3')
  })
})

describe('request', () => {
  it('sends the stored token, the JSON body and only the defined query values', async () => {
    respond(200, { name: '/orders' })
    token.set('secret')

    const reply = await request<{ name: string }>('POST', '/admin/topics', {
      query: { prefix: '/o', cursor: undefined, limit: 10 },
      body: { name: '/orders' },
    })

    expect(reply).toEqual({ name: '/orders' })
    expect(calls[0]?.url).toBe('/admin/topics?prefix=%2Fo&limit=10')
    const headers = new Headers(calls[0]?.init.headers)
    expect(headers.get('Authorization')).toBe('Bearer secret')
    expect(headers.get('Content-Type')).toBe('application/json')
    expect(calls[0]?.init.body).toBe('{"name":"/orders"}')
  })

  it('sends no authorization header without a token', async () => {
    respond(204, undefined)

    expect(await request<void>('DELETE', '/admin/topics/x')).toBeUndefined()
    expect(new Headers(calls[0]?.init.headers).has('Authorization')).toBe(false)
  })

  it('asks for a token only on 401', async () => {
    respond(401, { error: 'missing token' })
    await expect(request('GET', '/admin/cluster')).rejects.toBeInstanceOf(AuthRequired)

    respond(403, { error: 'denied' })
    const denied = await request('GET', '/admin/cluster').catch((error: unknown) => error)
    expect(denied).toBeInstanceOf(ApiError)
    expect(denied).not.toBeInstanceOf(AuthRequired)
    expect(denied).toMatchObject({ status: 403, message: 'denied' })
  })

  it('falls back to the status when the error body is not the API shape', async () => {
    respond(502, 'upstream')

    await expect(request('GET', '/admin/cluster')).rejects.toMatchObject({ status: 502, message: '502' })
  })
})
