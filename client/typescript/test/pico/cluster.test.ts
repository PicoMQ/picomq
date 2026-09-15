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

async function waitForRedirect(from: string, path: string, ownerHost: string): Promise<string> {
  const deadline = Date.now() + 15_000
  let last = 0
  while (Date.now() < deadline) {
    const response = await fetch(`${from}${path}`, {
      method: 'HEAD',
      headers: { Authorization: `Bearer ${TOKEN}` },
      redirect: 'manual',
    })
    last = response.status
    if (response.status === 307) {
      const location = response.headers.get('location')
      if (!location) throw new Error('307 without location')
      expect(location).toContain(ownerHost)
      return location
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(`no ownership redirect from ${from} for ${path} (last status ${last})`)
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
  const owner = connect('pico', NODE1, { token: TOKEN })
  const other = connect('pico', NODE2, { token: TOKEN })
  const anon = connect('pico', NODE2)
  const base = `/cluster-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`

  it('rejects unauthenticated writes when auth is required', async () => {
    const name = `${base}/denied`
    await owner.stream(name).create(CT)
    await expect(anon.append(name, ['nope'])).rejects.toMatchObject({
      kind: 'unauthenticated',
      status: 401,
    })
  })

  it('follows a cross-node 307 and keeps the bearer token', async () => {
    const name = `${base}/owned`
    expect(await owner.stream(name).create(CT)).toBe(true)

    await waitForRedirect(NODE2, name, 'localhost:4437')

    const ack = await other.append(name, ['from-other-node'])
    expect(ack.start).toBe('0')
    expect(ack.next).toBe('1')

    const page = await other.read(name, '0', 'off')
    expect(page.records).toHaveLength(1)
    expect(text(page.records[0]!.body)).toBe('from-other-node')

    const viaOwner = await owner.read(name, '0', 'off')
    expect(text(viaOwner.records[0]!.body)).toBe('from-other-node')
  })

  it('creates on node 2 and reads through a redirect from node 1', async () => {
    const name = `${base}/other-owner`
    await other.stream(name).create(CT)
    await other.append(name, ['seed'])

    await waitForRedirect(NODE1, name, 'localhost:4438')

    const page = await owner.read(name, '0', 'off')
    expect(page.records).toHaveLength(1)
    expect(text(page.records[0]!.body)).toBe('seed')
  })

  it('produces through the non-owner and keeps sequence', async () => {
    const name = `${base}/producer`
    await owner.stream(name).create(CT)
    await waitForRedirect(NODE2, name, 'localhost:4437')
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
    await owner.create(stream, CT)
    const group = `cluster-ops-${base.slice(9)}`

    const joined = await owner.joinGroup(group, [stream], {
      clientId: 'ts-cluster',
      sessionTimeoutMs: 2000,
    })
    const fence = { memberId: joined.memberId, generation: joined.generation }
    await waitForGroup(other, group)
    await other.heartbeat(group, fence)
    expect((await other.groupAssignment(group, fence)).assignment).toEqual([stream])
    await other.commitOffsets(group, { [stream]: { position: 5 } }, fence)
    expect((await other.fetchOffsets(group))[stream]!.position).toBe(5)
    expect((await other.describeGroup(group)).state).toBe('Stable')
    const listed = [...(await owner.listGroups()), ...(await other.listGroups())]
    expect(listed.map((g) => g.group)).toContain(group)
    await other.leaveGroup(group, joined.memberId)
    expect((await owner.describeGroup(group)).state).toBe('Empty')
  })

  it('rebalances when the second member joins via the other node', { timeout: 30_000 }, async () => {
    const streams = ['a', 'b', 'c', 'd'].map((s) => `${base}/gm/${s}`)
    for (const name of streams) await owner.create(name, CT)
    const group = `cluster-shared-${base.slice(9)}`
    const config = { sessionTimeoutMs: 2000, heartbeatIntervalMs: 100 }

    const first = await GroupMember.join(owner, group, streams, config)
    expect(first.assignment()).toEqual({ generation: 1, streams })
    await waitForGroup(other, group)
    const [second, onFirst] = await Promise.all([
      GroupMember.join(other, group, streams, config),
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
    expect((await other.describeGroup(group)).state).toBe('Empty')
  })

  it('maps a bad token to unauthenticated', async () => {
    const bad = connect('pico', NODE1, { token: 'not-a-real-token' })
    await expect(bad.head(`${base}/owned`)).rejects.toMatchObject({
      kind: 'unauthenticated',
    })
  })

  it('cleans up its test streams', { timeout: 20_000 }, async () => {
    for (const client of [owner, other]) {
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
