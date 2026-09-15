import { describe, expect, it } from 'vitest'
import { GroupMember, type Assignment, type GroupClient } from '../src/group'
import { ClientError } from '../src/error'
import { RetryPolicy } from '../src/retry'
import type { GroupMembership, JoinOptions, MemberFence, Offsets } from '../src/types'

type Call =
  | { op: 'join'; options: JoinOptions }
  | { op: 'heartbeat'; fence: MemberFence }
  | { op: 'leave'; memberId: string; instanceId: string | undefined }
  | { op: 'commit'; offsets: Offsets; fence: MemberFence | undefined }

function membership(memberId: string, generation: number, assignment: string[]): GroupMembership {
  return { memberId, generation, assignment, members: [memberId] }
}

function groupError(code: string, status = 409): ClientError {
  return new ClientError('conflict', code, { status, code })
}

class FakeGroup implements GroupClient {
  readonly calls: Call[] = []
  joins: Array<GroupMembership | ClientError> = []
  heartbeats: Array<ClientError | undefined> = []

  async joinGroup(_group: string, _subscription: string[], options: JoinOptions = {}) {
    this.calls.push({ op: 'join', options })
    const next = this.joins.shift()
    if (next === undefined) throw new Error('unexpected join')
    if (next instanceof Error) throw next
    return next
  }

  async heartbeat(_group: string, fence: MemberFence) {
    this.calls.push({ op: 'heartbeat', fence })
    const next = this.heartbeats.shift()
    if (next !== undefined) throw next
  }

  async leaveGroup(_group: string, memberId: string, instanceId?: string) {
    this.calls.push({ op: 'leave', memberId, instanceId })
  }

  async commitOffsets(_group: string, offsets: Offsets, fence?: MemberFence) {
    this.calls.push({ op: 'commit', offsets, fence })
  }

  async fetchOffsets(): Promise<Offsets> {
    return {}
  }

  ops(): string[] {
    return this.calls.map((call) => call.op)
  }
}

const fast = { sessionTimeoutMs: 300, heartbeatIntervalMs: 5, retry: new RetryPolicy(3, 0, 0, 1) }

async function nextGeneration(member: GroupMember, after: number): Promise<Assignment> {
  for await (const assignment of member.assignments()) {
    if (assignment.generation > after) return assignment
  }
  throw new Error(`assignments ended before generation ${after + 1}; error=${member.error()?.code}`)
}

async function settled(member: GroupMember): Promise<ClientError> {
  let last: Assignment | undefined
  for await (const assignment of member.assignments()) last = assignment
  expect(last).toBeDefined()
  const error = member.error()
  if (error === undefined) throw new Error('member did not fail')
  return error
}

describe('GroupMember', () => {
  it('joins with the configured session and exposes the assignment', async () => {
    const client = new FakeGroup()
    client.joins.push(membership('m-1', 1, ['/a', '/b']))
    const member = await GroupMember.join(client, 'g', ['/a', '/b'], {
      ...fast,
      instanceId: 'i-1',
      clientId: 'tests',
      rebalanceTimeoutMs: 700,
    })
    expect(member.memberId()).toBe('m-1')
    expect(member.assignment()).toEqual({ generation: 1, streams: ['/a', '/b'] })
    expect(member.error()).toBeUndefined()
    expect(client.calls[0]).toMatchObject({
      op: 'join',
      options: {
        sessionTimeoutMs: 300,
        rebalanceTimeoutMs: 700,
        instanceId: 'i-1',
        clientId: 'tests',
      },
    })
    expect((client.calls[0] as { options: JoinOptions }).options.memberId).toBeUndefined()

    await member.leave()
    expect(client.calls.at(-1)).toEqual({ op: 'leave', memberId: 'm-1', instanceId: 'i-1' })
  })

  it('retries the initial join on retryable errors', async () => {
    const client = new FakeGroup()
    client.joins.push(
      new ClientError('transport', 'down', { code: 'transport' }),
      membership('m-1', 1, ['/a']),
    )
    const member = await GroupMember.join(client, 'g', ['/a'], fast)
    expect(member.memberId()).toBe('m-1')
    expect(client.ops()).toEqual(['join', 'join'])
    await member.leave()
  })

  it('surfaces a non-retryable initial join failure', async () => {
    const client = new FakeGroup()
    client.joins.push(groupError('inconsistent_protocol'))
    await expect(GroupMember.join(client, 'g', ['/a'], fast)).rejects.toMatchObject({
      code: 'inconsistent_protocol',
    })
    expect(client.ops()).toEqual(['join'])
  })

  it('rejoins with the same member id on rebalance and publishes the new assignment', async () => {
    const client = new FakeGroup()
    client.joins.push(membership('m-1', 1, ['/a', '/b']), membership('m-1', 2, ['/a']))
    client.heartbeats.push(undefined, groupError('rebalance_in_progress'))
    const member = await GroupMember.join(client, 'g', ['/a', '/b'], fast)

    const assignment = await nextGeneration(member, 1)
    expect(assignment).toEqual({ generation: 2, streams: ['/a'] })
    expect(member.assignment()).toEqual(assignment)
    expect(member.memberId()).toBe('m-1')
    const rejoin = client.calls.filter((call) => call.op === 'join')[1] as { options: JoinOptions }
    expect(rejoin.options.memberId).toBe('m-1')

    await member.commit({ '/a': { position: 4 } })
    expect(client.calls.at(-1)).toEqual({
      op: 'commit',
      offsets: { '/a': { position: 4 } },
      fence: { memberId: 'm-1', generation: 2 },
    })
    await member.leave()
  })

  it('rejoins with a fresh id when the coordinator forgot the member', async () => {
    const client = new FakeGroup()
    client.joins.push(membership('m-1', 1, ['/a']), membership('m-2', 3, ['/a']))
    client.heartbeats.push(groupError('unknown_member'))
    const member = await GroupMember.join(client, 'g', ['/a'], fast)

    expect(await nextGeneration(member, 1)).toEqual({ generation: 3, streams: ['/a'] })
    expect(member.memberId()).toBe('m-2')
    const rejoin = client.calls.filter((call) => call.op === 'join')[1] as { options: JoinOptions }
    expect(rejoin.options.memberId).toBeUndefined()

    await member.leave()
    expect(client.calls.at(-1)).toEqual({ op: 'leave', memberId: 'm-2', instanceId: undefined })
  })

  it('drops the member id when the rejoin itself reports unknown_member', async () => {
    const client = new FakeGroup()
    client.joins.push(
      membership('m-1', 1, ['/a']),
      groupError('unknown_member'),
      membership('m-9', 2, ['/a']),
    )
    client.heartbeats.push(groupError('illegal_generation'))
    const member = await GroupMember.join(client, 'g', ['/a'], fast)
    expect(await nextGeneration(member, 1)).toEqual({ generation: 2, streams: ['/a'] })
    const joins = client.calls.filter((call) => call.op === 'join') as Array<{ options: JoinOptions }>
    expect(joins.map((join) => join.options.memberId)).toEqual([undefined, 'm-1', undefined])
    await member.leave()
  })

  it('backs off on retryable heartbeat errors and recovers', async () => {
    const client = new FakeGroup()
    client.joins.push(membership('m-1', 1, ['/a']))
    client.heartbeats.push(
      new ClientError('transport', 'blip', { code: 'transport' }),
      new ClientError('other', 'busy', { status: 503, code: 'http_503' }),
      undefined,
      undefined,
    )
    const member = await GroupMember.join(client, 'g', ['/a'], fast)
    await new Promise((resolve) => setTimeout(resolve, 60))
    expect(member.error()).toBeUndefined()
    expect(client.ops().filter((op) => op === 'heartbeat').length).toBeGreaterThanOrEqual(4)
    await member.leave()
  })

  it('fails the session on a fatal heartbeat error and refuses commits', async () => {
    const client = new FakeGroup()
    client.joins.push(membership('m-1', 1, ['/a']))
    client.heartbeats.push(groupError('fenced', 403))
    const member = await GroupMember.join(client, 'g', ['/a'], fast)

    const error = await settled(member)
    expect(error.code).toBe('fenced')
    expect(member.error()?.code).toBe('fenced')
    await expect(member.commit({ '/a': { position: 1 } })).rejects.toMatchObject({ code: 'fenced' })

    await member.leave()
    expect(client.ops()).not.toContain('leave')
  })

  it('fails when retryable heartbeat errors exhaust the policy', async () => {
    const client = new FakeGroup()
    client.joins.push(membership('m-1', 1, ['/a']))
    const outage = new ClientError('transport', 'down', { code: 'transport' })
    client.heartbeats.push(outage, outage, outage, outage)
    const member = await GroupMember.join(client, 'g', ['/a'], {
      ...fast,
      retry: new RetryPolicy(2, 0, 0, 1),
    })
    expect((await settled(member)).kind).toBe('transport')
    await member.leave()
  })

  it('stops heartbeating when the signal aborts', async () => {
    const client = new FakeGroup()
    client.joins.push(membership('m-1', 1, ['/a']))
    const controller = new AbortController()
    const member = await GroupMember.join(client, 'g', ['/a'], { ...fast, signal: controller.signal })
    await new Promise((resolve) => setTimeout(resolve, 20))
    controller.abort()
    const seen = client.ops().filter((op) => op === 'heartbeat').length
    expect(seen).toBeGreaterThan(0)
    await new Promise((resolve) => setTimeout(resolve, 30))
    expect(client.ops().filter((op) => op === 'heartbeat').length).toBe(seen)
    expect(member.error()).toBeUndefined()
    await member.leave()
    expect(client.calls.at(-1)).toMatchObject({ op: 'leave', memberId: 'm-1' })
  })

  it('rejects joining with an already aborted signal', async () => {
    const client = new FakeGroup()
    const controller = new AbortController()
    controller.abort()
    await expect(
      GroupMember.join(client, 'g', ['/a'], { signal: controller.signal }),
    ).rejects.toMatchObject({ kind: 'aborted' })
    expect(client.calls).toEqual([])
  })
})
