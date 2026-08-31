import { describe, expect, it, vi } from 'vitest'
import { PicoClient } from '../src/pico'
import { DsClient } from '../src/ds'

function sseResponse(text: string, headers: Record<string, string> = {}): Response {
  return new Response(text, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      ...headers,
    },
  })
}

describe('PicoClient.subscribe', () => {
  it('yields data and control then stops when closed', async () => {
    const fetch = vi.fn().mockResolvedValue(
      sseResponse(
        [
          'event: data',
          'id: 2',
          'data:[{"seq":1,"timestamp":1,"headers":{},"body":"hi"}]',
          '',
          'event: control',
          'id: 2',
          'data:{"next_seq":2,"up_to_date":true,"closed":true}',
          '',
          '',
        ].join('\n'),
      ),
    )
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient('http://example.test')
    const events = []
    for await (const event of client.subscribe('s', '0', { reconnect: false })) {
      events.push(event)
    }

    expect(fetch).toHaveBeenCalledOnce()
    expect(String(fetch.mock.calls[0]![0])).toContain('/s?seq=0&live=sse')
    expect(events).toHaveLength(2)
    expect(events[0]).toMatchObject({
      type: 'data',
      id: '2',
      records: [{ position: '1', body: new TextEncoder().encode('hi') }],
    })
    expect(events[1]).toEqual({
      type: 'control',
      id: '2',
      next: '2',
      upToDate: true,
      closed: true,
    })

    vi.unstubAllGlobals()
  })

  it('passes AbortSignal to fetch', async () => {
    const fetch = vi.fn().mockResolvedValue(sseResponse(''))
    vi.stubGlobal('fetch', fetch)
    const ac = new AbortController()
    const client = new PicoClient('http://example.test')
    const iter = client.subscribe('s', '0', { signal: ac.signal, reconnect: false })[
      Symbol.asyncIterator
    ]()
    await iter.next()
    expect(fetch.mock.calls[0]![1]).toMatchObject({ signal: ac.signal })
    vi.unstubAllGlobals()
  })
})

describe('DsClient.subscribe', () => {
  it('decodes base64 data and control offsets', async () => {
    const payload = btoa('hello')
    const fetch = vi.fn().mockResolvedValue(
      sseResponse(
        [
          'event: data',
          `data:${payload}`,
          '',
          'event: control',
          'data:{"streamNextOffset":"abc","upToDate":true,"streamClosed":true}',
          '',
          '',
        ].join('\n'),
        { 'Stream-SSE-Data-Encoding': 'base64' },
      ),
    )
    vi.stubGlobal('fetch', fetch)

    const client = new DsClient('http://example.test')
    const events = []
    for await (const event of client.subscribe('s', '-1', { reconnect: false })) {
      events.push(event)
    }

    expect(events[0]).toMatchObject({
      type: 'data',
      records: [{ position: '', body: new TextEncoder().encode('hello') }],
    })
    expect(events[1]).toEqual({
      type: 'control',
      next: 'abc',
      upToDate: true,
      closed: true,
    })
    vi.unstubAllGlobals()
  })
})
