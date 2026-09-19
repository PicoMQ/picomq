import assert from 'node:assert/strict'
import { afterEach, test } from 'node:test'
import { readFile } from 'node:fs/promises'
import ts from 'typescript'

// Keep the dashboard checks on Node's built-in runner and its existing compiler.
// The HTTP boundary is isolated; no PicoMQ service or test dependency is needed.
async function moduleUrl(path, imports = {}) {
  let source = (await readFile(new URL(path, import.meta.url), 'utf8')).replace('import.meta.env.DEV', 'true')
  for (const [specifier, url] of Object.entries(imports)) source = source.replaceAll(`'${specifier}'`, `'${url}'`)
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
  })
  return `data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}`
}
const streamsUrl = await moduleUrl('../src/streams.ts')
const api = await import(await moduleUrl('../src/api.ts', { './streams': streamsUrl }))
const originalFetch = globalThis.fetch
const originalStorage = globalThis.sessionStorage
afterEach(() => {
  globalThis.fetch = originalFetch
  if (originalStorage === undefined) delete globalThis.sessionStorage
  else globalThis.sessionStorage = originalStorage
})

const connection = { endpoint: 'https://admin.example:9090/proxy', token: 'explicit-admin-token' }
const ownership = (overrides = {}) => ({
  name: '/demo/orders', ownerNodeId: 42, ownerAdvertisedAddress: 'http://owner.example:4437', epoch: 9,
  ...overrides,
})
const json = (value, status = 200) => new Response(JSON.stringify(value), {
  status, headers: { 'Content-Type': 'application/json' },
})

test('ownership uses the existing admin route and preserves encoded native stream names', async () => {
  const name = '/demo/a%20b/%2Fpart'
  globalThis.sessionStorage = { getItem: () => 'unrelated-dashboard-token' }
  let calls = 0
  globalThis.fetch = async (url, init) => {
    calls++
    assert.equal(String(url), 'https://admin.example:9090/proxy/admin/streams/demo/a%2520b/%252Fpart')
    assert.equal(new Headers(init.headers).get('Authorization'), 'Bearer explicit-admin-token')
    assert.equal(init.method ?? 'GET', 'GET')
    assert.equal(init.redirect, 'manual')
    assert.equal(init.cache, 'no-store')
    return json(ownership({ name }))
  }
  assert.deepEqual(await api.fetchStreamOwnership(connection, name), ownership({ name }))
  assert.equal(calls, 1)
})

test('an explicitly empty admin token never falls back to the dashboard token', async () => {
  globalThis.sessionStorage = { getItem: () => 'unrelated-dashboard-token' }
  globalThis.fetch = async (_url, init) => {
    assert.equal(new Headers(init.headers).has('Authorization'), false)
    return json(ownership())
  }
  await api.fetchStreamOwnership({ ...connection, token: '' }, '/demo/orders')
})

test('ownership preserves unavailable, unopened, and zero epochs and an unresolved owner', async () => {
  for (const epoch of [null, -1, 0]) {
    globalThis.fetch = async () => json(ownership({ ownerNodeId: -1, ownerAdvertisedAddress: '', epoch }))
    const result = await api.fetchStreamOwnership(connection, '/demo/orders')
    assert.equal(result.epoch, epoch)
    assert.equal(result.ownerNodeId, -1)
    assert.equal(result.ownerAdvertisedAddress, '')
  }
  // Configured node IDs are signed integers, including -1 when it has an address.
  globalThis.fetch = async () => json(ownership({ ownerNodeId: -2 }))
  assert.equal((await api.fetchStreamOwnership(connection, '/demo/orders')).ownerNodeId, -2)
  globalThis.fetch = async () => json(ownership({ ownerNodeId: -1 }))
  assert.equal((await api.fetchStreamOwnership(connection, '/demo/orders')).ownerAdvertisedAddress, 'http://owner.example:4437')
})

test('ownership rejects unsafe numeric identities and metadata for a different stream', async () => {
  for (const invalid of [
    { epoch: 9007199254740992 }, { epoch: -2 }, { epoch: '9' },
    { ownerNodeId: 9007199254740992 },
    { name: '/different/stream' },
  ]) {
    globalThis.fetch = async () => json(ownership(invalid))
    await assert.rejects(api.fetchStreamOwnership(connection, '/demo/orders'), Error, JSON.stringify(invalid))
  }
})

test('ownership authentication failures retain the server status without retrying', async () => {
  for (const status of [401, 403]) {
    let calls = 0
    globalThis.fetch = async () => { calls++; return new Response('Unauthorized', { status }) }
    await assert.rejects(api.fetchStreamOwnership(connection, '/demo/orders'), (error) => {
      assert.ok(error instanceof api.AuthRequired)
      assert.equal(error.status, status)
      return true
    })
    assert.equal(calls, 1)
  }
})

test('ownership redirects and non-API responses fail without forwarding credentials', async () => {
  for (const response of [
    new Response(null, { status: 307, headers: { Location: 'https://different.example/admin/streams/demo/orders' } }),
    new Response('<html>Sign in</html>', { headers: { 'Content-Type': 'text/html' } }),
    new Response('Gateway unavailable', { status: 503 }),
  ]) {
    let calls = 0
    globalThis.fetch = async (_url, init) => { calls++; assert.equal(init.redirect, 'manual'); return response }
    await assert.rejects(api.fetchStreamOwnership(connection, '/demo/orders'), Error)
    assert.equal(calls, 1)
  }
})

test('changing selection can abort an in-flight ownership lookup', async () => {
  const controller = new AbortController()
  let observedSignal
  globalThis.fetch = async (_url, init) => {
    observedSignal = init.signal
    return new Promise((_resolve, reject) => {
      init.signal.addEventListener('abort', () => reject(init.signal.reason), { once: true })
    })
  }
  const pending = api.fetchStreamOwnership(connection, '/demo/orders', controller.signal)
  controller.abort()
  await assert.rejects(pending, Error)
  assert.equal(observedSignal.aborted, true)
})

test('ownership preserves the persisted identity separately from routing during handoff', async () => {
  for (const fields of [
    { streamId: 12, nodeId: -2, state: 'opened', pendingTransfer: { fromNode: -2, toNode: 42 } },
    { streamId: 12, nodeId: 42, state: 'closed', pendingTransfer: null },
    { streamId: 12, nodeId: null, state: null, pendingTransfer: null },
  ]) {
    globalThis.fetch = async () => json(ownership(fields))
    assert.deepEqual(await api.fetchStreamOwnership(connection, '/demo/orders'), ownership(fields))
  }
  // Older responses may omit these fields, but cannot prove transfer completion.
  globalThis.fetch = async () => json(ownership())
  assert.equal((await api.fetchStreamOwnership(connection, '/demo/orders')).streamId, undefined)
})

test('ownership rejects unsafe persisted identities and malformed pending transfer states', async () => {
  for (const fields of [
    { streamId: -1 }, { streamId: null }, { streamId: 9007199254740992 },
    { nodeId: 2147483648 }, { nodeId: -2147483649 }, { nodeId: '42' },
    { state: 'transferring' },
    { pendingTransfer: { fromNode: -2, toNode: 2147483648 } },
    { pendingTransfer: { fromNode: '-2', toNode: 42 } },
  ]) {
    globalThis.fetch = async () => json(ownership(fields))
    await assert.rejects(api.fetchStreamOwnership(connection, '/demo/orders'))
  }
})
