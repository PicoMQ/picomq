import assert from 'node:assert/strict'
import { afterEach, test } from 'node:test'
import { readFile } from 'node:fs/promises'
import ts from 'typescript'

// Isolate the existing admin HTTP contract with Node's built-in runner.
async function moduleUrl(path, imports = {}) {
  let source = (await readFile(new URL(path, import.meta.url), 'utf8')).replace('import.meta.env.DEV', 'true')
  for (const [specifier, url] of Object.entries(imports)) source = source.replaceAll(`'${specifier}'`, `'${url}'`)
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
  })
  return `data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}`
}
const streamsUrl = await moduleUrl('../src/streams.ts')
const apiUrl = await moduleUrl('../src/api.ts', { './streams': streamsUrl })
const { AuthRequired } = await import(apiUrl)
const actions = await import(await moduleUrl('../src/admin-actions.ts', { './api': apiUrl, './streams': streamsUrl }))
const originalFetch = globalThis.fetch
const originalStorage = globalThis.sessionStorage
afterEach(() => {
  globalThis.fetch = originalFetch
  if (originalStorage === undefined) delete globalThis.sessionStorage
  else globalThis.sessionStorage = originalStorage
})
const connection = { endpoint: 'https://admin.example/proxy/', token: 'supplied-admin-token' }
const json = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
const node = (overrides = {}) => ({ nodeId: -1, nodeEpoch: 0, advertisedAddress: null, slots: 12, local: true, openingCount: 0, placedCount: 1, ...overrides })
const scope = { streams: [{ prefix: '/demo/' }], ops: ['read', 'head', 'list'], audiences: ['pico'] }
const record = { id: 'svc/read', scope, createdAtMs: 123, issuedBy: 'root' }
const issued = { id: record.id, token: 'secret-once', scope, createdAtMs: 123 }
const accepted = { stream: '/demo/orders', streamId: 1, toNode: -1, pending: true }

function uncertain(error) {
  assert.ok(error instanceof actions.AdminWriteError)
  assert.equal(error.uncertain, true)
  assert.match(error.message, /outcome is unknown/)
  return true
}

test('admin reads use only explicit credentials and preserve signed node IDs', async () => {
  globalThis.sessionStorage = { getItem: () => 'unrelated-dashboard-token' }
  for (const token of [connection.token, '']) {
    globalThis.fetch = async (url, init) => {
      assert.equal(url, 'https://admin.example/proxy/admin/nodes')
      assert.equal(init.method, 'GET')
      assert.equal(init.redirect, 'manual')
      assert.equal(init.cache, 'no-store')
      assert.equal(new Headers(init.headers).get('Authorization'), token ? `Bearer ${token}` : null)
      return json({ nodes: [node()] })
    }
    assert.deepEqual(await actions.fetchAdminNodes({ ...connection, token }), { nodes: [node()] })
  }
})

test('transfer uses the existing JSON route and returns pending, not completed', async () => {
  let calls = 0
  globalThis.fetch = async (url, init) => {
    calls++
    assert.equal(url, 'https://admin.example/proxy/admin/transfer')
    assert.equal(init.method, 'POST')
    assert.equal(new Headers(init.headers).get('Content-Type'), 'application/json')
    assert.deepEqual(JSON.parse(init.body), { stream: '/demo/orders', toNode: -1 })
    return json(accepted, 202)
  }
  assert.deepEqual(await actions.transferStream(connection, '/demo/orders', -1), accepted)
  assert.equal(calls, 1)
})

test('node slot updates retain zero and the full u32 range without backend truncation', async () => {
  for (const [nodeId, slots] of [[-2147483648, 0], [2147483647, 4294967295]]) {
    globalThis.fetch = async (url, init) => {
      assert.equal(url, `https://admin.example/proxy/admin/nodes/${nodeId}`)
      assert.equal(init.method, 'POST')
      assert.deepEqual(JSON.parse(init.body), { slots })
      return json(node({ nodeId, slots }))
    }
    assert.equal((await actions.updateNodeSlots(connection, nodeId, slots)).slots, slots)
  }
})

test('invalid node IDs, slots, and stream paths cannot reach a write endpoint', async () => {
  globalThis.fetch = async () => assert.fail('Invalid input must not be sent')
  for (const value of [-2147483649, 2147483648, NaN, 1.5, '1']) {
    await assert.rejects(async () => actions.transferStream(connection, '/demo/orders', value))
    await assert.rejects(async () => actions.updateNodeSlots(connection, value, 0))
  }
  for (const slots of [-1, 4294967296, NaN, 1.5, '1']) await assert.rejects(async () => actions.updateNodeSlots(connection, 1, slots))
  for (const name of ['/', 'orders', '/demo/../orders', '/demo/orders?other=true']) {
    await assert.rejects(async () => actions.transferStream(connection, name, 1))
  }
})

test('token list returns public records; issuance returns the one-time secret separately', async () => {
  globalThis.fetch = async (url, init) => {
    assert.equal(url, 'https://admin.example/proxy/admin/tokens')
    if (init.method === 'GET') return json({ count: 1, tokens: [{ ...record, token: 'never-list-this-secret' }] })
    assert.equal(init.method, 'POST')
    assert.deepEqual(JSON.parse(init.body), { id: record.id, scope })
    return json(issued, 201)
  }
  assert.deepEqual(await actions.listTokens(connection), { count: 1, tokens: [record] })
  assert.deepEqual(await actions.issueToken(connection, record.id, scope), issued)
})

test('token revocation encodes opaque IDs once without changing the destination route', async () => {
  const id = 'svc/a b%2F雪?#'
  let calls = 0
  globalThis.fetch = async (url, init) => {
    calls++
    assert.equal(url, 'https://admin.example/proxy/admin/tokens/svc%2Fa%20b%252F%E9%9B%AA%3F%23')
    assert.equal(init.method, 'DELETE')
    assert.equal(init.body, undefined)
    return new Response(null, { status: 204 })
  }
  await actions.revokeToken(connection, id)
  assert.equal(calls, 1)
  for (const id of ['.', '..']) await assert.rejects(async () => actions.revokeToken(connection, id), /cannot be addressed safely/)
  assert.equal(calls, 1)
})

test('scope validation rejects unknown keys and unsafe values while preserving deny defaults', () => {
  assert.deepEqual(actions.validateTokenScope({}), {})
  const full = { ...scope, tokens: [{ exact: 'svc/a' }], groups: { admin: { read: true }, tokens: { write: false } }, autoPrefixStreams: true, expiresAtMs: 0 }
  assert.deepEqual(actions.validateTokenScope(full), full)
  for (const invalid of [
    null, [], { admin: true }, { ops: ['fly'] }, { audiences: ['kafka'] }, { streams: [{ regex: '.*' }] },
    { streams: [{ exact: '/a', prefix: '/b' }] }, { tokens: [{}] }, { groups: { unknown: {} } },
    { groups: { stream: { read: 1 } } }, { groups: { admin: { future: true } } },
    { expiresAtMs: 'soon' }, { expiresAtMs: 9007199254740992 }, { autoPrefixStreams: 'true' },
  ]) assert.throws(() => actions.validateTokenScope(invalid), /Invalid token scope/)
})

test('token issuance rejects dead scopes, invalid auto-prefix and anonymous admin access before sending', async () => {
  globalThis.fetch = async () => assert.fail('Invalid token must not be sent')
  for (const invalid of [{}, { ops: ['read'] }, { audiences: ['pico'] }, { ...scope, autoPrefixStreams: true, streams: [{ exact: '/a' }] }]) {
    await assert.rejects(async () => actions.issueToken(connection, 'reader', invalid))
  }
  await assert.rejects(async () => actions.issueToken(connection, 'anonymous', { ops: ['cluster_read'], audiences: ['admin'] }), /anonymous/)
  for (const id of ['', 'x'.repeat(97), '雪'.repeat(33)]) await assert.rejects(async () => actions.issueToken(connection, id, scope), /1–96/)
  actions.validateTokenId('雪'.repeat(32))
})

test('write authentication and definite rejections retain server status without retry', async () => {
  for (const status of [401, 403, 400, 404, 409, 429]) {
    let calls = 0
    globalThis.fetch = async () => { calls++; return json({ error: 'request denied' }, status) }
    await assert.rejects(actions.transferStream(connection, '/demo/orders', -1), (error) => {
      assert.equal(error.status, status)
      if (status === 401 || status === 403) assert.ok(error instanceof AuthRequired)
      else { assert.ok(error instanceof actions.AdminWriteError); assert.equal(error.uncertain, false) }
      return true
    })
    assert.equal(calls, 1)
  }
})

test('network errors, gateway failures and redirects mark writes uncertain and never retry', async () => {
  for (const result of [null, 408, 500, 503, 307]) {
    let calls = 0
    globalThis.fetch = async (_url, init) => {
      calls++
      assert.equal(init.redirect, 'manual')
      if (result === null) throw new TypeError('Network gone')
      return new Response(null, { status: result, headers: { Location: 'https://different.example/admin/transfer' } })
    }
    await assert.rejects(actions.transferStream(connection, '/demo/orders', -1), uncertain)
    assert.equal(calls, 1)
  }
})

test('unexpected success or mismatched acknowledgement never reports a completed write', async () => {
  for (const response of [
    json(accepted, 200), json({ ...accepted, toNode: 99 }, 202), json({ ...accepted, streamId: 9007199254740992 }, 202),
    new Response('<html>proxy</html>', { status: 202, headers: { 'Content-Type': 'text/html' } }),
    new Response('{bad', { status: 202, headers: { 'Content-Type': 'application/json' } }),
    json({ ...accepted, oversized: 'x'.repeat(64 * 1024) }, 202),
  ]) {
    globalThis.fetch = async () => response
    await assert.rejects(actions.transferStream(connection, '/demo/orders', -1), uncertain)
  }
  globalThis.fetch = async () => json(node({ slots: 13 }))
  await assert.rejects(actions.updateNodeSlots(connection, -1, 12), uncertain)
  globalThis.fetch = async () => json({ ...issued, id: 'another-id' }, 201)
  await assert.rejects(actions.issueToken(connection, record.id, scope), uncertain)
  globalThis.fetch = async () => json({ ok: true })
  await assert.rejects(actions.revokeToken(connection, record.id), uncertain)
})

test('read responses are bounded and validated, and in-flight reads can be aborted', async () => {
  for (const value of [{ nodes: [node({ nodeId: 2147483648 })] }, { nodes: [node({ openingCount: 9007199254740992 })] }, { nodes: [], extra: 'x'.repeat(1024 * 1024) }]) {
    globalThis.fetch = async () => json(value)
    await assert.rejects(actions.fetchAdminNodes(connection))
  }
  globalThis.fetch = async () => json({ count: 2, tokens: [record] })
  await assert.rejects(actions.listTokens(connection), /Invalid token list/)
  const controller = new AbortController()
  globalThis.fetch = async (_url, init) => new Promise((_resolve, reject) => {
    init.signal.addEventListener('abort', () => reject(init.signal.reason), { once: true })
  })
  const pending = actions.listTokens(connection, controller.signal)
  controller.abort()
  await assert.rejects(pending, { name: 'AbortError' })
})

test('a write deadline or lost success body preserves an unknown outcome without retry', async (t) => {
  const deadline = new AbortController()
  t.mock.method(AbortSignal, 'timeout', () => deadline.signal)
  let calls = 0
  globalThis.fetch = async (_url, init) => {
    calls++
    return new Promise((_resolve, reject) => {
      init.signal.addEventListener('abort', () => reject(init.signal.reason), { once: true })
    })
  }
  const pending = actions.issueToken(connection, record.id, scope)
  deadline.abort(new DOMException('Timed out', 'TimeoutError'))
  await assert.rejects(pending, (error) => { uncertain(error); assert.match(error.message, /timed out/); return true })
  assert.equal(calls, 1)
  t.mock.restoreAll()

  globalThis.fetch = async () => new Response(new ReadableStream({
    pull(controller) { controller.error(new TypeError('Disconnected while reading the reply')) },
  }), { status: 201, headers: { 'Content-Type': 'application/json' } })
  await assert.rejects(actions.issueToken(connection, record.id, scope), uncertain)
})
