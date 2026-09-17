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

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function request(fetch: ReturnType<typeof vi.fn>, index = 0) {
  const [url, init] = fetch.mock.calls[index]! as [string, RequestInit]
  const body = init.body === undefined || init.body === null ? undefined : JSON.parse(init.body as string)
  return { url, method: init.method, headers: new Headers(init.headers), body }
}

describe('PicoClient groups', () => {
  it('joins with the json body and decodes the membership', async () => {
    const fetch = vi.fn().mockResolvedValue(
      json({ memberId: 'm-1', generation: 2, assignment: ['/a'], members: ['m-1', 'm-2'] }),
    )
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient('http://example.test')
    const joined = await client.joinGroup('orders/eu', ['/a', '/b'], {
      memberId: 'm-1',
      instanceId: 'i-1',
      clientId: 'test',
      sessionTimeoutMs: 6000,
      rebalanceTimeoutMs: 9000,
    })

    expect(joined).toEqual({
      memberId: 'm-1',
      generation: 2,
      assignment: ['/a'],
      members: ['m-1', 'm-2'],
    })

    const sent = request(fetch)
    expect(sent.method).toBe('POST')
    expect(sent.url).toBe('http://example.test/_groups/orders%2Feu/members')
    expect(sent.headers.get('Content-Type')).toBe('application/json')
    expect(sent.body).toEqual({
      subscription: ['/a', '/b'],
      memberId: 'm-1',
      instanceId: 'i-1',
      clientId: 'test',
      sessionTimeoutMs: 6000,
      rebalanceTimeoutMs: 9000,
    })
  })

  it('rejects a join response without a member id', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(json({ generation: 1 })))
    const client = new PicoClient('http://example.test')

    await expect(client.joinGroup('g', ['/a'])).rejects.toMatchObject({ code: 'invalid_response' })
  })

  it('heartbeats, fetches the assignment, and leaves with the fence', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(json({ generation: 3, assignment: ['/a'] }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient('http://example.test')
    const fence = { memberId: 'm/1', generation: 3, instanceId: 'i-1' }

    await client.heartbeat('g', fence)
    expect(await client.groupAssignment('g', fence)).toEqual({ generation: 3, assignment: ['/a'] })
    await client.leaveGroup('g', 'm/1', 'i-1')

    const heartbeat = request(fetch, 0)
    expect(heartbeat.method).toBe('POST')
    expect(heartbeat.url).toBe('http://example.test/_groups/g/members/m%2F1/heartbeat')
    expect(heartbeat.body).toEqual({ generation: 3, instanceId: 'i-1' })

    const assignment = request(fetch, 1)
    expect(assignment.method).toBe('GET')
    expect(assignment.url).toBe(
      'http://example.test/_groups/g/members/m%2F1?generation=3&instanceId=i-1',
    )

    const leave = request(fetch, 2)
    expect(leave.method).toBe('DELETE')
    expect(leave.url).toBe('http://example.test/_groups/g/members/m%2F1?instanceId=i-1')
  })

  it('commits with an optional fence and fetches offsets per stream', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(
        json({ offsets: { '/a': { position: 7, metadata: 'ck' }, '/b c': { position: 1 } } }),
      )
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient('http://example.test')
    const offsets = { '/a': { position: 7, metadata: 'ck' } }

    await client.commitOffsets('g', offsets, { memberId: 'm', generation: 3 })
    await client.commitOffsets('g', offsets)
    const fetched = await client.fetchOffsets('g', ['/a', '/b c'])

    const fenced = request(fetch, 0)
    expect(fenced.method).toBe('PUT')
    expect(fenced.url).toBe('http://example.test/_groups/g/offsets')
    expect(fenced.body).toEqual({ offsets, memberId: 'm', generation: 3 })
    expect(request(fetch, 1).body).toEqual({ offsets })

    const fetchRequest = request(fetch, 2)
    expect(fetchRequest.method).toBe('GET')
    expect(fetchRequest.url).toBe('http://example.test/_groups/g/offsets?stream=%2Fa&stream=%2Fb%20c')
    expect(fetched).toEqual({ '/a': { position: 7, metadata: 'ck' }, '/b c': { position: 1 } })
  })

  it('maps group error codes onto client errors', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(json({ error: 'illegal_generation', message: 'stale' }, 409)),
    )
    const client = new PicoClient('http://example.test')

    await expect(
      client.commitOffsets('g', {}, { memberId: 'm', generation: 0 }),
    ).rejects.toMatchObject({ kind: 'conflict', code: 'illegal_generation', status: 409 })
  })

  it('describes and lists groups with retries', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response('boom', { status: 503 }))
      .mockResolvedValueOnce(
        json({
          group: 'g',
          state: 'Stable',
          generation: 2,
          protocolType: 'consumer',
          members: [
            { memberId: 'm', instanceId: null, clientId: 'c', subscription: ['/a'], assignment: ['/a'] },
          ],
        }),
      )
      .mockResolvedValueOnce(json({ groups: [{ group: 'g', state: 'Stable' }] }))
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient('http://example.test', undefined, false, new RetryPolicy(3, 0, 0, 1))

    const described = await client.describeGroup('g')
    expect(described).toEqual({
      group: 'g',
      state: 'Stable',
      generation: 2,
      protocolType: 'consumer',
      members: [{ memberId: 'm', clientId: 'c', subscription: ['/a'], assignment: ['/a'] }],
    })
    expect(request(fetch, 1).url).toBe('http://example.test/_groups/g')

    expect(await client.listGroups()).toEqual([{ group: 'g', state: 'Stable' }])
    expect(request(fetch, 2).url).toBe('http://example.test/_groups')
    expect(fetch).toHaveBeenCalledTimes(3)
  })

  it('does not retry joins, heartbeats, or commits', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response('boom', { status: 503 }))
    vi.stubGlobal('fetch', fetch)

    const client = new PicoClient('http://example.test', undefined, false, RetryPolicy.attempts(3))
    const fence = { memberId: 'm', generation: 1 }

    await expect(client.joinGroup('g', ['/a'])).rejects.toMatchObject({ status: 503 })
    await expect(client.heartbeat('g', fence)).rejects.toMatchObject({ status: 503 })
    await expect(client.commitOffsets('g', {}, fence)).rejects.toMatchObject({ status: 503 })

    expect(fetch).toHaveBeenCalledTimes(3)
  })
})
