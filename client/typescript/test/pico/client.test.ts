import { afterEach, describe, expect, it, vi } from 'vitest'
import { decodeBatchAppend } from '../../src/pico/codec'
import { PicoClient } from '../../src/pico/client'
import { RetryPolicy } from '../../src/retry'

afterEach(() => {
  vi.unstubAllGlobals()
})

function ackResponse(headers: Record<string, string>): Response {
  return new Response(null, { status: 200, headers })
}

describe('PicoClient.append', () => {
  it('encodes headers and string bodies on the wire', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue(ackResponse({ 'Pico-Start-Seq': '4', 'Pico-Next-Seq': '6' }))
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient('http://example.test')
    const ack = await client.append('/s', [
      { body: 'hello', headers: { kind: 'greeting' } },
      new TextEncoder().encode('raw'),
    ])

    expect(ack).toEqual({ start: '4', next: '6' })
    const body = fetch.mock.calls[0]![1]!.body as ArrayBuffer
    const decoded = decodeBatchAppend(new Uint8Array(body))
    expect(decoded).toHaveLength(2)
    expect(decoded[0]!.headers).toEqual({ kind: 'greeting' })
    expect(new TextDecoder().decode(decoded[0]!.body)).toBe('hello')
    expect(new TextDecoder().decode(decoded[1]!.body)).toBe('raw')
  })

  it('does not retry appends', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response('boom', { status: 500 }))
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient('http://example.test', undefined, false, RetryPolicy.attempts(3))
    await expect(client.append('/s', ['x'])).rejects.toMatchObject({ status: 500 })
    expect(fetch).toHaveBeenCalledTimes(1)
  })

  it('retries reads with the configured policy', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response('boom', { status: 500 }))
      .mockResolvedValueOnce(
        new Response(null, {
          status: 200,
          headers: { 'Pico-Next-Seq': '0', 'Pico-Up-To-Date': 'true' },
        }),
      )
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient(
      'http://example.test',
      undefined,
      false,
      new RetryPolicy(3, 0, 0, 1),
    )
    const page = await client.read('/s', '0', 'off')
    expect(page.upToDate).toBe(true)
    expect(fetch).toHaveBeenCalledTimes(2)
  })

  it('flags duplicates from producer appends', async () => {
    const fetch = vi.fn().mockResolvedValue(ackResponse({ 'Pico-Next-Seq': '9' }))
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient('http://example.test')
    const result = await client.appendAs('/s', ['x'], { id: 'p', epoch: 0, seq: 3 })
    expect(result.applied).toBe(false)
    expect(result.duplicate).toBe(true)
    expect(result.ack.next).toBe('9')

    const headers = new Headers(fetch.mock.calls[0]![1]!.headers as HeadersInit)
    expect(headers.get('Pico-Producer-Id')).toBe('p')
    expect(headers.get('Pico-Producer-Epoch')).toBe('0')
    expect(headers.get('Pico-Producer-Seq')).toBe('3')
  })
})
