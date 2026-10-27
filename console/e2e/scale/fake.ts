import type { Page, Route } from '@playwright/test'

export const SCALE = {
  topics: 100_000,
  partitions: 10_000,
  groups: 100_000,
  members: 10_000,
  offsets: 10_000,
  tokens: 100_000,
  nodes: 100,
  records: 1_000_000,
}

const PAGE = 100

export interface Fake {
  cursors: (path: string) => (string | null)[]
  prefixes: (path: string) => (string | null)[]
}

type Make = (index: number) => object

const name = (base: string, index: number) => `${base}-${String(index).padStart(6, '0')}`

const TOPIC = {
  contentType: 'application/json',
  kafkaTopic: null,
  retentionMs: null,
  retentionBytes: null,
  ttlSeconds: null,
  expiresAtMs: null,
  closed: false,
}

const SCOPE = {
  streams: [{ prefix: '' }],
  tokens: [],
  groups: [],
  access: {
    stream: { read: true, write: false },
    tokens: { read: false, write: false },
    group: { read: false, write: false },
    admin: { read: false, write: false },
  },
  ops: [],
  audiences: ['pico'],
  autoPrefix: false,
  expiresAtMs: null,
}

export async function fakeAdmin(page: Page): Promise<Fake> {
  const seen: { path: string; cursor: string | null; prefix: string | null }[] = []

  const paged = (route: Route, url: URL, total: number, make: Make, named?: (index: number) => string) => {
    const prefix = url.searchParams.get('prefix')
    const cursor = url.searchParams.get('cursor')
    const limit = Number(url.searchParams.get('limit') ?? PAGE)
    seen.push({ path: url.pathname, cursor, prefix })

    const first = cursor === null ? 0 : Number(cursor) + 1
    const items: object[] = []
    let index = first
    for (; index < total && items.length < limit; index += 1) {
      if (prefix === null || named === undefined || unrooted(named(index)).startsWith(unrooted(prefix))) {
        items.push(make(index))
      }
    }
    const next = index < total ? String(index - 1) : null
    return route.fulfill({ json: { items, next } })
  }

  await page.route('**/admin/**', (route) => {
    const url = new URL(route.request().url())
    const parts = url.pathname.split('/').slice(2).map(decodeURIComponent)
    const [area, id, child, index, leaf] = parts

    if (area === 'cluster') {
      return route.fulfill({
        json: {
          clusterId: 'scale',
          nodeId: 1,
          nodeEpoch: 1,
          advertisedAddress: 'http://node-1',
          registered: true,
          fenced: false,
          appliedIndex: 1,
          streamCount: SCALE.topics,
          objectCount: 0,
          streamSetObjectCount: 0,
          streamObjectCount: 0,
          preparedObjectCount: 0,
          gc: { backlog: 0, oldestSeq: null, nextSeq: 0 },
          kvEntryCount: 0,
          kvBytes: 0,
          pendingTransferCount: 0,
          leaseHolder: true,
          snapshot: { lastAppliedIndex: 0, lastBytes: 0, lastDurationMs: 0, lastAtMs: 0, taken: 0, failed: 0 },
        },
      })
    }
    if (area === 'overview') {
      return route.fulfill({ json: { computedAtMs: Date.now(), topicsBySize: [], topicsByWriteRate: [], writeRateSinceMs: null } })
    }
    if (area === 'nodes' && id === undefined) {
      return paged(route, url, SCALE.nodes, (each) => ({
        nodeId: each + 1,
        nodeEpoch: 1,
        advertisedAddress: `http://node-${each + 1}`,
        slots: 1,
        local: each === 0,
        openingCount: 0,
        placedCount: SCALE.partitions / SCALE.nodes,
        fenced: false,
      }))
    }
    if (area === 'topics' && id === undefined) {
      return paged(route, url, SCALE.topics, (each) => ({ ...TOPIC, id: each.toString(16).padStart(32, '0'), name: `/${name('topic', each)}`, partitions: 1 }), (each) =>
        `/${name('topic', each)}`,
      )
    }
    if (area === 'topics' && child === undefined) {
      return route.fulfill({
        json: { ...TOPIC, id: 'f'.repeat(32), name: `/${id}`, partitions: SCALE.partitions, storedBytes: 0, owners: [{ nodeId: 1, partitions: SCALE.partitions }] },
      })
    }
    if (area === 'topics' && child === 'partitions' && index === undefined) {
      return paged(route, url, SCALE.partitions, (each) => partition(each))
    }
    if (area === 'topics' && child === 'partitions' && leaf === undefined) {
      return route.fulfill({ json: partition(Number(index)) })
    }
    if (area === 'topics' && leaf === 'records') {
      const offset = url.searchParams.get('offset')
      if (url.searchParams.get('cursor') === null && offset !== null) {
        url.searchParams.set('cursor', String(Number(offset) - 1))
      }
      return paged(route, url, SCALE.records, (each) => ({
        offset: each,
        timestamp: 1_700_000_000_000 + each,
        key: null,
        headers: [],
        value: { data: `{"n":${each}}`, encoding: 'utf8', size: 8, truncated: false },
      }))
    }
    if (area === 'groups' && id === undefined) {
      return paged(
        route,
        url,
        SCALE.groups,
        (each) => ({
          groupId: name('group', each),
          coordinatorNodeId: 1,
          current: true,
          state: 'Stable',
          protocolType: 'consumer',
          protocolName: 'range',
          generation: 1,
          members: 1,
        }),
        (each) => name('group', each),
      )
    }
    if (area === 'groups' && child === undefined) {
      return route.fulfill({
        json: {
          groupId: id,
          coordinatorNodeId: 1,
          committedPartitions: SCALE.offsets,
          state: 'Stable',
          protocolType: 'consumer',
          protocolName: 'range',
          generation: 1,
          members: SCALE.members,
        },
      })
    }
    if (area === 'groups' && child === 'members') {
      return paged(route, url, SCALE.members, (each) => ({
        memberId: name('member', each),
        instanceId: null,
        clientId: 'scale',
        clientHost: '10.0.0.1',
        assignment: { topics: [{ topic: 'topic-000000', partitions: [each] }] },
      }))
    }
    if (area === 'groups' && child === 'offsets') {
      return paged(route, url, SCALE.offsets, (each) => ({
        topic: '/topic-000000',
        partition: each,
        committedOffset: 10,
        metadata: null,
        nextOffset: 20,
        lag: 10,
        current: true,
      }))
    }
    if (area === 'tokens') {
      return paged(
        route,
        url,
        SCALE.tokens,
        (each) => ({ id: `svc/${name('token', each)}`, scope: SCOPE, createdAtMs: 1_700_000_000_000, issuedBy: 'dev/root' }),
        (each) => `svc/${name('token', each)}`,
      )
    }
    return route.fulfill({ status: 404, json: { error: `no fake for ${url.pathname}` } })
  })

  return {
    cursors: (path) => seen.filter((each) => each.path === path).map((each) => each.cursor),
    prefixes: (path) => seen.filter((each) => each.path === path).map((each) => each.prefix),
  }
}

function unrooted(text: string): string {
  return text.replace(/^\//, '')
}

function partition(index: number) {
  return {
    index,
    streamId: index,
    ownerNodeId: 1,
    epoch: 1,
    state: 'opened',
    pendingTransfer: null,
    startOffset: 0,
    nextOffset: SCALE.records,
    current: true,
    storedBytes: 0,
  }
}
