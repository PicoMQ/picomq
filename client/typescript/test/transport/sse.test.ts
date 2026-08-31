import { describe, expect, it } from 'vitest'
import { iterateSse } from '../../src/transport/sse'
import { ClientError } from '../../src/error'

function streamOf(text: string): ReadableStream<Uint8Array> {
  const bytes = new TextEncoder().encode(text)
  return new ReadableStream({
    start(controller) {
      controller.enqueue(bytes)
      controller.close()
    },
  })
}

describe('iterateSse', () => {
  it('parses data and control events', async () => {
    const body = streamOf(
      [
        'event: data',
        'id: 3',
        'data:[{"seq":2,"body":"hi"}]',
        '',
        'event: control',
        'id: 3',
        'data:{"next_seq":3,"up_to_date":true}',
        '',
        '',
      ].join('\n'),
    )
    const events = []
    for await (const event of iterateSse(body)) {
      events.push(event)
    }
    expect(events).toEqual([
      { event: 'data', id: '3', data: '[{"seq":2,"body":"hi"}]' },
      { event: 'control', id: '3', data: '{"next_seq":3,"up_to_date":true}' },
    ])
  })

  it('joins multiline data', async () => {
    const body = streamOf(['event: data', 'data:a', 'data:b', '', ''].join('\n'))
    const events = []
    for await (const event of iterateSse(body)) {
      events.push(event)
    }
    expect(events).toEqual([{ event: 'data', data: 'a\nb' }])
  })

  it('aborts mid-stream', async () => {
    const controller = new AbortController()
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new TextEncoder().encode('event: data\ndata:one\n\n'))
      },
    })
    const iter = iterateSse(body, controller.signal)
    const first = await iter.next()
    expect(first.value?.data).toBe('one')
    controller.abort()
    await expect(iter.next()).rejects.toMatchObject({ kind: 'aborted' })
  })
})
