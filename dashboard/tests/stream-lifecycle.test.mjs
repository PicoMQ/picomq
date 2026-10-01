import assert from 'node:assert/strict'
import { afterEach, test } from 'node:test'
import { readFile } from 'node:fs/promises'
import ts from 'typescript'

// Isolate the admin lifecycle HTTP boundary with the dashboard's existing compiler and
// Node built-ins. No service or additional testing framework is required.
async function moduleUrl(path, imports = {}) {
  let source = (await readFile(new URL(path, import.meta.url), 'utf8')).replace('import.meta.env.DEV', 'true')
  for (const [specifier, url] of Object.entries(imports)) source = source.replaceAll(`'${specifier}'`, `'${url}'`)
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
  })
  return `data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}`
}
const streamsUrl = await moduleUrl('../src/streams.ts')
const lifecycle = await import(await moduleUrl('../src/stream-lifecycle.ts', { './streams': streamsUrl }))
const originalFetch = globalThis.fetch
const originalStorage = globalThis.sessionStorage
afterEach(() => {
  globalThis.fetch = originalFetch
  if (originalStorage === undefined) delete globalThis.sessionStorage
  else globalThis.sessionStorage = originalStorage
})

const connection = { endpoint: 'https://admin.example/proxy/', token: 'explicit-admin-token' }
const options = { name: '/demo/a%20b/%2Fpart', contentType: 'application/json', kafkaTopic: 'example.events' }
const created = (status = 201) => new Response(null, { status, headers: { 'Pico-Next-Seq': '0' } })

test('create uses admin PUT and headers without records or implicit credentials', async () => {
  globalThis.sessionStorage = { getItem: () => 'unrelated-admin-token' }
  let calls = 0
  globalThis.fetch = async (url, init) => {
    calls++
    assert.equal(url, 'https://admin.example/proxy/admin/streams/demo/a%2520b/%252Fpart')
    assert.equal(init.method, 'PUT')
    assert.equal(init.body, undefined)
    const headers = new Headers(init.headers)
    assert.equal(headers.get('Content-Type'), 'application/json')
    assert.equal(headers.get('Pico-Kafka-Topic'), 'example.events')
    assert.equal(headers.get('Authorization'), 'Bearer explicit-admin-token')
    assert.equal(init.redirect, 'manual')
    assert.equal(init.cache, 'no-store')
    assert.ok(init.signal instanceof AbortSignal)
    return created()
  }
  assert.deepEqual(await lifecycle.createStream(connection, options), { created: true })
  assert.equal(calls, 1)
})

test('existing streams are reported separately and blank alias/token remain omitted', async () => {
  globalThis.sessionStorage = { getItem: () => 'unrelated-admin-token' }
  globalThis.fetch = async (_url, init) => {
    const headers = new Headers(init.headers)
    assert.equal(headers.has('Pico-Kafka-Topic'), false)
    assert.equal(headers.has('Authorization'), false)
    return created(200)
  }
  assert.deepEqual(await lifecycle.createStream({ ...connection, token: '' }, { ...options, kafkaTopic: '' }), { created: false })
})

test('delete uses only admin DELETE and requires the expected response', async () => {
  let calls = 0
  globalThis.fetch = async (url, init) => {
    calls++
    assert.equal(url, 'https://admin.example/proxy/admin/streams/demo/a%2520b/%252Fpart')
    assert.equal(init.method, 'DELETE')
    assert.equal(init.body, undefined)
    assert.equal(new Headers(init.headers).get('Authorization'), 'Bearer explicit-admin-token')
    assert.equal(init.redirect, 'manual')
    return new Response(null, { status: 204 })
  }
  await lifecycle.deleteStream(connection, options.name)
  assert.equal(calls, 1)
})

test('invalid stream paths and native reserved resources cannot be mutated', async () => {
  globalThis.fetch = async () => assert.fail('Invalid or reserved paths must never reach the server')
  for (const name of ['/', '//other.example/stream', '/a/../b', '/demo/x?query', '/demo/x#fragment', '/_schemas/x', '/_streams/x', '/_groups/x', '/_sys', '/_sys/x']) {
    await assert.rejects(lifecycle.createStream(connection, { ...options, name }), Error, name)
    await assert.rejects(lifecycle.deleteStream(connection, name), Error, name)
  }
  assert.equal(lifecycle.lifecycleStreamPath('/_schemas-other'), '/_schemas-other')
})

test('invalid content types and Kafka aliases fail before a write is attempted', async () => {
  globalThis.fetch = async () => assert.fail('Invalid form values must never reach the server')
  for (const contentType of ['', 'json', 'text/plain\r\nInjected: 1', 'application/☃']) {
    await assert.rejects(lifecycle.createStream(connection, { ...options, contentType }), Error)
  }
  for (const kafkaTopic of ['.', '..', 'with/slash', 'with space', 'évents', 'x'.repeat(250)]) {
    await assert.rejects(lifecycle.createStream(connection, { ...options, kafkaTopic }), Error)
  }
  assert.deepEqual(lifecycle.validateCreateStream({ name: ' /demo/orders ', contentType: ' text/plain; charset=utf-8 ', kafkaTopic: ' ' }), {
    name: '/demo/orders', contentType: 'text/plain; charset=utf-8', kafkaTopic: '',
  })
})

test('auth, conflict, and missing-stream failures retain authoritative status and never retry', async () => {
  for (const status of [401, 403, 404, 409, 429]) {
    let calls = 0
    globalThis.fetch = async () => { calls++; return new Response('Untrusted body is not displayed', { status }) }
    await assert.rejects(lifecycle.deleteStream(connection, options.name), (error) => {
      assert.ok(error instanceof lifecycle.StreamMutationError)
      assert.equal(error.status, status)
      assert.equal(error.uncertain, false)
      return true
    })
    assert.equal(calls, 1)
  }
})

test('network, 5xx and malformed successful replies preserve uncertain write outcomes', async () => {
  const failures = [
    () => { throw new TypeError('connection reset') },
    () => new Response(null, { status: 503 }),
    () => new Response(null, { status: 408 }),
    () => new Response('<html>fallback</html>', { status: 200 }),
    () => new Response(null, { status: 202 }),
  ]
  for (const failure of failures) {
    let calls = 0
    globalThis.fetch = async () => { calls++; return failure() }
    await assert.rejects(lifecycle.createStream(connection, options), (error) => {
      assert.ok(error instanceof lifecycle.StreamMutationError)
      assert.equal(error.uncertain, true)
      return true
    })
    assert.equal(calls, 1)
  }
  globalThis.fetch = async () => new Response(null, { status: 200 })
  await assert.rejects(lifecycle.deleteStream(connection, options.name), (error) => error.uncertain === true)
})

test('redirects never send admin credentials to the advertised owner', async () => {
  for (const status of [301, 302, 303, 307, 308]) {
    let calls = 0
    globalThis.fetch = async (_url, init) => {
      calls++
      assert.equal(init.redirect, 'manual')
      return new Response(null, { status, headers: { Location: 'https://different.example/stream' } })
    }
    await assert.rejects(lifecycle.deleteStream(connection, options.name), (error) => error.uncertain === true && /redirected/.test(error.message))
    assert.equal(calls, 1)
  }
})

test('aborting a write after dispatch is uncertain and never automatically repeated', async () => {
  const controller = new AbortController()
  let calls = 0
  globalThis.fetch = async (_url, init) => {
    calls++
    return new Promise((_resolve, reject) => init.signal.addEventListener('abort', () => reject(init.signal.reason), { once: true }))
  }
  const result = lifecycle.deleteStream(connection, options.name, controller.signal)
  controller.abort()
  await assert.rejects(result, (error) => error.uncertain === true)
  assert.equal(calls, 1)
  await assert.rejects(lifecycle.deleteStream(connection, options.name, controller.signal), Error)
  assert.equal(calls, 1, 'Already canceled writes must not dispatch')
})


test('owner conflicts identify the admin listener without forwarding or retrying', async () => {
  for (const [body, expected] of [
    [{ code: 'owner_required', ownerNodeId: -7 }, /node -7’s admin listener/],
    [{ code: 'owner_required', ownerNodeId: 'untrusted.example' }, /the stream owner’s admin listener/],
    [{ code: 'transfer_pending' }, /transfer is pending/],
  ]) {
    let calls = 0
    globalThis.fetch = async () => { calls++; return new Response(JSON.stringify(body), { status: 409 }) }
    await assert.rejects(lifecycle.deleteStream(connection, options.name), (error) => {
      assert.equal(error.status, 409)
      assert.equal(error.uncertain, false)
      assert.match(error.message, expected)
      return true
    })
    assert.equal(calls, 1)
  }
})

test('oversized or malformed conflict details remain bounded and do not expose untrusted text', async () => {
  for (const body of ['not-json', JSON.stringify({ code: 'owner_required', ownerNodeId: 2, padding: 'x'.repeat(4096) })]) {
    globalThis.fetch = async () => new Response(body, { status: 409 })
    await assert.rejects(lifecycle.createStream(connection, options), (error) => {
      assert.equal(error.uncertain, false)
      assert.match(error.message, /configuration or Kafka alias conflicts/)
      assert.doesNotMatch(error.message, /not-json|node 2/)
      return true
    })
  }
})
