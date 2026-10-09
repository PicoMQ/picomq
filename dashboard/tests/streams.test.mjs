import assert from 'node:assert/strict'
import { afterEach, test } from 'node:test'
import { readFile } from 'node:fs/promises'
import ts from 'typescript'

// Exercise the production TypeScript with the repository's existing compiler.
// No test framework or runtime dependency is added.
async function moduleFrom(path) {
  const source = (await readFile(new URL(path, import.meta.url), 'utf8')).replace('import.meta.env.DEV', 'true')
  const { outputText } = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } })
  return import(`data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}`)
}
const api = await moduleFrom('../src/streams.ts')
const { inferShape } = await moduleFrom('../src/shape.ts')
const originalFetch = globalThis.fetch
afterEach(() => { globalThis.fetch = originalFetch })
const connection = { endpoint: '/pico', token: 'test-token' }
const json = (body, headers = {}) => new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json', ...headers } })

test('listing passes prefix and cursor to the existing root API', async () => {
  globalThis.fetch = async (url, init) => {
    assert.equal(new URL(url, 'http://test').pathname, '/pico/')
    const params = new URL(url, 'http://test').searchParams
    assert.equal(params.get('prefix'), '/orders & events/')
    assert.equal(params.get('start_after'), '/orders/last')
    assert.equal(init.headers.get('Authorization'), 'Bearer test-token')
    assert.equal(init.redirect, 'manual')
    return json({ streams: [{ name: '/orders/next', closed: false }], has_more: true })
  }
  const page = await api.listStreams(connection, '/orders & events/', '/orders/last')
  assert.equal(page.has_more, true)
  assert.equal(page.streams[0].name, '/orders/next')
})

test('HEAD positions remain exact strings even beyond safe integers', async () => {
  globalThis.fetch = async (url, init) => {
    assert.equal(url, '/pico/demo/orders'); assert.equal(init.method, 'HEAD')
    return new Response(null, { headers: { 'Pico-Start-Seq': '0', 'Pico-Next-Seq': '9007199254740993', 'Pico-Closed': 'true' } })
  }
  const info = await api.inspectStream(connection, '/demo/orders')
  assert.equal(info.next, '9007199254740993'); assert.equal(info.closed, true)
})

test('reads use the response cursor and preserve record envelopes', async () => {
  globalThis.fetch = async (url) => {
    const params = new URL(url, 'http://test').searchParams
    assert.equal(params.get('seq'), '5'); assert.equal(params.get('bytes'), '262144')
    return json([{ seq: 5, body_b64: '/w==', key: 'order', headers: { source: 'test' } }], { 'Pico-Next-Seq': '6', 'Pico-Closed': 'true' })
  }
  const page = await api.readStream(connection, '/demo/orders', '5')
  assert.equal(page.next, '6'); assert.equal(page.closed, true)
  assert.equal(page.records[0].body_b64, '/w==')
})

test('unsafe JSON sequence values fail rather than silently rounding', async () => {
  globalThis.fetch = async () => new Response('[{"seq":9007199254740993,"body":"x"}]', { headers: { 'Content-Type': 'application/json', 'Pico-Next-Seq': '9007199254740994' } })
  await assert.rejects(api.readStream(connection, '/demo/orders', '0'), /safe integer/)
})

test('missing exposed position headers and non-API HTML produce useful errors', async () => {
  globalThis.fetch = async () => new Response(null)
  await assert.rejects(api.inspectStream(connection, '/demo/orders'), /Pico-Start-Seq/)
  globalThis.fetch = async () => new Response('<html></html>', { headers: { 'Content-Type': 'text/html' } })
  await assert.rejects(api.listStreams(connection, '/'), /Expected the Pico JSON API/)
})

test('publishing uses one native batch append and never retries an ambiguous failure', async () => {
  let attempts = 0
  globalThis.fetch = async (url, init) => {
    attempts++
    assert.equal(url, '/pico/demo/orders'); assert.equal(init.method, 'POST')
    assert.equal(init.headers.get('Content-Type'), 'application/vnd.picomq.batch+json')
    assert.deepEqual(JSON.parse(init.body), { records: [{ body: '{"ok":true}', headers: { source: 'テスト' }, key: '🔑' }] })
    throw new TypeError('Disconnected')
  }
  await assert.rejects(api.appendMessage(connection, '/demo/orders', '{"ok":true}', '🔑', { source: 'テスト' }), /Could not reach/)
  assert.equal(attempts, 1)
})

test('schema and config lookups use only existing endpoints', async () => {
  const paths = []
  globalThis.fetch = async (url) => {
    paths.push(url)
    return url.includes('_streams') ? json({ schema: 'orders', schemaValidate: false }) : new Response('{"type":"object"}')
  }
  await api.streamConfig(connection, '/demo/orders')
  await api.streamConfig(connection, '/demo/a%20b')
  await api.fetchSchema(connection, 'orders')
  assert.deepEqual(paths, ['/pico/_streams/demo/orders', '/pico/_streams/demo/a%2520b', '/pico/_schemas/orders'])
})

test('ownership redirects never forward a credential to another origin', async () => {
  let calls = 0
  globalThis.fetch = async () => { calls++; return new Response(null, { status: 307, headers: { Location: 'http://other-node/orders' } }) }
  await assert.rejects(api.inspectStream(connection, '/orders'), /another node/)
  assert.equal(calls, 1)
})

test('stream paths preserve encoded names and reject URL escape or normalization', () => {
  assert.equal(api.streamPath('/demo/a%20b'), '/demo/a%20b')
  for (const name of ['//other-host/x', '/a/../b', '/a/%2e%2e/b', '/a?seq=0', '/a#fragment', '/a\\b', '/a b', '/']) {
    assert.throws(() => api.streamPath(name), undefined, name)
  }
})

test('exact watch rows validate and deduplicate without splitting stream names', () => {
  assert.deepEqual(api.validateStreamNames([' /demo/orders ', '/demo/events', '/demo/orders', '/demo/a,b'], 8), [
    '/demo/orders', '/demo/events', '/demo/a,b',
  ])
  assert.throws(() => api.validateStreamNames([], 8), /each row/)
  assert.throws(() => api.validateStreamNames(['/demo/orders', ' '], 8), /each row/)
  assert.throws(() => api.validateStreamNames(['/demo/orders', '//other-host/x'], 8), /exact stream path/)
  assert.throws(() => api.validateStreamNames(['/demo/orders\n/demo/events'], 8), /exact stream path/)
  assert.throws(() => api.validateStreamNames(Array.from({ length: 9 }, (_, i) => `/stream/${i}`), 8), /at most 8/)
})

test('inferred fields count records, not repeated array elements', () => {
  const shape = inferShape([
    { seq: 0, body: '{"items":[{"id":1},{"id":2}],"optional":null}' },
    { seq: 1, body: '{"items":[{"id":"3"}]}' },
    { seq: 2, body: 'not json' },
    { seq: 3, body_b64: '/w==' },
  ])
  assert.equal(shape.parsed, 2)
  const id = shape.fields.find((field) => field.path === '$["items"][]["id"]')
  assert.deepEqual(id, { path: '$["items"][]["id"]', types: ['number', 'string'], samples: 2 })
  assert.equal(shape.fields.find((field) => field.path === '$["optional"]').samples, 1)
})

test('shape traversal bounds huge arrays and distinct fields', () => {
  const body = JSON.stringify(Object.fromEntries(Array.from({ length: 1000 }, (_, i) => [`field${i}`, [1, 2, 3]])))
  const shape = inferShape([{ seq: 0, body }])
  assert.ok(shape.fields.length <= 200)
  assert.equal(shape.limited, true)
})

test('oversized records retain their identity with an explicit bounded preview', () => {
  const record = api.previewRecord({ seq: 42, body: 'x'.repeat(3_000_000), key: 'k'.repeat(2000) })
  assert.equal(record.seq, 42)
  assert.equal(record.body.length, 16384)
  assert.equal(record.key.length, 1024)
  assert.equal(record.previewTruncated, true)
  assert.equal(inferShape([record]).parsed, 0)
})

test('oversized responses fail instead of advancing the cursor silently', async () => {
  globalThis.fetch = async () => json([{ seq: 1, body: 'x'.repeat(4 * 1024 * 1024) }], { 'Pico-Next-Seq': '2' })
  await assert.rejects(api.readStream(connection, '/demo/orders', '1'), /position has not been skipped/)
})

test('out-of-range Kafka timestamps cannot crash message rendering', () => {
  assert.match(api.formatTimestamp(9_223_372_036_854_776_000), /outside date range/)
  assert.equal(api.formatTimestamp(0), '1970-01-01T00:00:00.000Z')
})

test('live reads use native long polling and preserve an idle 204 resume cursor', async (t) => {
  const deadlines = []
  t.mock.method(AbortSignal, 'timeout', (milliseconds) => {
    deadlines.push(milliseconds)
    return new AbortController().signal
  })
  globalThis.fetch = async (url, init) => {
    const params = new URL(url, 'http://test').searchParams
    assert.equal(params.get('live'), 'long-poll')
    assert.equal(params.get('seq'), '9007199254740993')
    assert.equal(params.get('count'), '50')
    assert.equal(init.redirect, 'manual')
    return new Response(null, { status: 204, headers: { 'Pico-Next-Seq': '9007199254740993', 'Pico-Closed': 'false' } })
  }
  assert.deepEqual(await api.readStream(connection, '/demo/orders', '9007199254740993', undefined, 50, true), {
    records: [], next: '9007199254740993', closed: false,
  })
  assert.deepEqual(deadlines, [35_000])
})

test('live read timeout reports its longer deadline and remains retryable', async (t) => {
  const deadline = new AbortController()
  t.mock.method(AbortSignal, 'timeout', (milliseconds) => {
    assert.equal(milliseconds, 35_000)
    return deadline.signal
  })
  globalThis.fetch = async () => {
    deadline.abort(new DOMException('Simulated deadline', 'TimeoutError'))
    throw deadline.signal.reason
  }
  await assert.rejects(api.readStream(connection, '/demo/orders', '0', undefined, 50, true), (error) => {
    assert.match(error.message, /35 seconds/)
    assert.equal(error.retryable, true)
    return true
  })
})
