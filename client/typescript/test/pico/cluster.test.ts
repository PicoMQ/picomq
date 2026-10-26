import { describe, expect, it } from 'vitest'
import { connect, GroupMember, type PicoClient } from '../../src/index'

const NODE1 = process.env.PICO_ENDPOINT ?? 'http://127.0.0.1:4437'
const NODE2 = process.env.PICO_ENDPOINT_2 ?? 'http://127.0.0.1:4438'
const TOKEN =
  process.env.PICO_TOKEN ?? 'ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY'
const CT = 'application/octet-stream'

async function reachable(url: string): Promise<boolean> {
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(1500) })
    return response.status < 500
  } catch {
    return false
  }
}

const up = (await reachable(NODE1)) && (await reachable(NODE2))
const text = (body: Uint8Array) => new TextDecoder().decode(body)

async function head(node: string, path: string): Promise<Response> {
  return fetch(`${node}${path}`, {
    method: 'HEAD',
    headers: { Authorization: `Bearer ${TOKEN}` },
    redirect: 'manual',
  })
}

async function ownerOf(path: string): Promise<string> {
  const deadline = Date.now() + 15_000
  for (;;) {
    const [first, second] = await Promise.all([head(NODE1, path), head(NODE2, path)])
    for (const [owner, own, redirect] of [
      [NODE1, first, second],
      [NODE2, second, first],
    ] as const) {
      if (own.status === 200 && redirect.status === 307) {
        expect(redirect.headers.get('location')).toContain(`:${new URL(owner).port}/`)
        return owner
      }
    }
    if (Date.now() >= deadline) {
      throw new Error(`no single owner for ${path} (${first.status}, ${second.status})`)
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
}

async function waitForGroup(client: PicoClient, group: string): Promise<void> {
  const deadline = Date.now() + 15_000
  for (;;) {
    try {
      await client.describeGroup(group)
      return
    } catch (error) {
      if (Date.now() >= deadline) throw error
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
}

describe.runIf(up)('live pico cluster', () => {
  const node1 = connect('pico', NODE1, { token: TOKEN })
  const node2 = connect('pico', NODE2, { token: TOKEN })
  const anon = connect('pico', NODE2)

  async function locate(path: string): Promise<{ owner: PicoClient; other: PicoClient }> {
    return (await ownerOf(path)) === NODE1
      ? { owner: node1, other: node2 }
      : { owner: node2, other: node1 }
  }
  const base = `/cluster-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`

  it('rejects unauthenticated writes when auth is required', async () => {
    const name = `${base}/denied`
    await node1.stream(name).create({ contentType: CT })
    await expect(anon.append(name, ['nope'])).rejects.toMatchObject({
      kind: 'unauthenticated',
      status: 401,
    })
  })

  it('follows a cross-node 307 and keeps the bearer token', async () => {
    const name = `${base}/owned`
    expect(await node1.stream(name).create({ contentType: CT })).toBe(true)
    const { owner, other } = await locate(name)

    const ack = await other.append(name, ['from-other-node'])
    expect(ack.start).toBe('0')
    expect(ack.next).toBe('1')

    const page = await other.read(name, '0', 'off')
    expect(page.records).toHaveLength(1)
    expect(text(page.records[0]!.body)).toBe('from-other-node')

    const viaOwner = await owner.read(name, '0', 'off')
    expect(text(viaOwner.records[0]!.body)).toBe('from-other-node')
  })

  it('creates on node 2 and reads through a redirect from the non-owner', async () => {
    const name = `${base}/other-owner`
    await node2.stream(name).create({ contentType: CT })
    await node2.append(name, ['seed'])
    const { other } = await locate(name)

    const page = await other.read(name, '0', 'off')
    expect(page.records).toHaveLength(1)
    expect(text(page.records[0]!.body)).toBe('seed')
  })

  it('produces through the non-owner and keeps sequence', async () => {
    const name = `${base}/producer`
    await node1.stream(name).create({ contentType: CT })
    const { other } = await locate(name)
    const producer = other.stream(name).producer('cluster-writer', { lingerMs: 0, maxInflight: 2 })
    expect(await producer.sendDurable('a')).toBe(0)
    expect(await producer.sendDurable('b')).toBe(1)
    await producer.close()

    const seen: string[] = []
    for await (const record of other.stream(name).records()) {
      seen.push(text(record.body))
    }
    expect(seen).toEqual(['a', 'b'])
  })

  it('drives one group membership through both nodes', { timeout: 20_000 }, async () => {
    const stream = `${base}/group-a`
    await node1.create(stream, { contentType: CT })
    const group = `cluster-ops-${base.slice(9)}`

    const joined = await node1.joinGroup(group, [stream], {
      clientId: 'ts-cluster',
      sessionTimeoutMs: 2000,
    })
    const fence = { memberId: joined.memberId, generation: joined.generation }

    await waitForGroup(node2, group)
    await node2.heartbeat(group, fence)
    expect((await node2.groupAssignment(group, fence)).assignment).toEqual([stream])

    await node2.commitOffsets(group, { [stream]: { position: 5 } }, fence)
    expect((await node2.fetchOffsets(group))[stream]!.position).toBe(5)
    expect((await node2.describeGroup(group)).state).toBe('Stable')

    expect((await node1.listGroups()).groups).toContain(group)
    expect((await node2.listGroups()).groups).toContain(group)

    await node2.leaveGroup(group, joined.memberId)
    expect((await node1.describeGroup(group)).state).toBe('Empty')
  })

  it('rebalances when the second member joins via the node2 node', { timeout: 30_000 }, async () => {
    const streams = ['a', 'b', 'c', 'd'].map((s) => `${base}/gm/${s}`)
    for (const name of streams) await node1.create(name, { contentType: CT })
    const group = `cluster-shared-${base.slice(9)}`
    const config = { sessionTimeoutMs: 2000, heartbeatIntervalMs: 100 }

    const first = await GroupMember.join(node1, group, streams, config)
    expect(first.assignment()).toEqual({ generation: 1, streams })

    await waitForGroup(node2, group)
    const [second, onFirst] = await Promise.all([
      GroupMember.join(node2, group, streams, config),
      (async () => {
        for await (const assignment of first.assignments()) {
          if (assignment.generation > 1) return assignment
        }
        throw new Error(`no rebalance; error=${first.error()?.code}`)
      })(),
    ])

    expect(onFirst.generation).toBe(2)
    expect(second.assignment().generation).toBe(2)
    expect(onFirst.streams).toHaveLength(2)
    expect([...onFirst.streams, ...second.assignment().streams].sort()).toEqual(streams)

    const mine = second.assignment().streams[0]!
    await second.commit({ [mine]: { position: 9 } })
    expect((await first.fetchOffsets([mine]))[mine]!.position).toBe(9)

    await second.leave()
    for await (const assignment of first.assignments()) {
      if (assignment.generation > 2) {
        expect(assignment.streams).toEqual(streams)
        break
      }
    }
    expect(first.error()).toBeUndefined()

    await first.leave()
    expect((await node2.describeGroup(group)).state).toBe('Empty')
  })

  it('maps a bad token to unauthenticated', async () => {
    const bad = connect('pico', NODE1, { token: 'not-a-real-token' })
    await expect(bad.head(`${base}/owned`)).rejects.toMatchObject({
      kind: 'unauthenticated',
    })
  })

  it('cleans up its test streams', { timeout: 20_000 }, async () => {
    for (const client of [node1, node2]) {
      const listing = await client.list(`${base}/`, 200)
      for (const info of listing.streams) {
        await client.delete(info.name)
      }
    }
  })
})

describe.runIf(!up)('live pico cluster (skipped)', () => {
  it(`skips because ${NODE1} and ${NODE2} are not both reachable`, () => {
    expect(up).toBe(false)
  })
})
