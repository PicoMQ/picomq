import assert from 'node:assert/strict'
import http from 'node:http'
import { test } from 'node:test'
import { createGateway } from '../scripts/dashboard-gateway.mjs'

async function listen(t, server) {
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise((resolve) => { server.closeAllConnections(); server.close(resolve) }))
  return `http://127.0.0.1:${server.address().port}`
}

test('gateway forwards embedded assets/admin and native paths with original credentials', async (t) => {
  const admin = await listen(t, http.createServer((request, response) => {
    assert.equal(request.headers.authorization, 'Bearer admin-only')
    response.setHeader('Content-Type', request.url === '/' ? 'text/html' : 'application/json')
    response.end(request.url === '/' ? '<html>Embedded dashboard</html>' : '{"nodes":[]}')
  }))
  const stream = await listen(t, http.createServer(async (request, response) => {
    assert.equal(request.url, '/demo/a%20b?format=json')
    assert.equal(request.method, 'POST')
    assert.equal(request.headers.authorization, 'Bearer stream-only')
    let body = ''; for await (const chunk of request) body += chunk
    assert.equal(body, 'payload')
    response.setHeader('Pico-Next-Seq', '42'); response.end('ok')
  }))
  const gateway = await listen(t, createGateway({ admin, stream }))
  assert.match(await (await fetch(gateway, { headers: { Authorization: 'Bearer admin-only' } })).text(), /Embedded dashboard/)
  assert.deepEqual(await (await fetch(gateway + '/admin/nodes', { headers: { Authorization: 'Bearer admin-only' } })).json(), { nodes: [] })
  const response = await fetch(gateway + '/pico/demo/a%20b?format=json', { method: 'POST', headers: { Authorization: 'Bearer stream-only' }, body: 'payload' })
  assert.equal(response.headers.get('Pico-Next-Seq'), '42')
  assert.equal(await response.text(), 'ok')
})

test('an allowlisted owner receives one unchanged write after a pre-operation redirect', async (t) => {
  let writes = 0
  const owner = await listen(t, http.createServer(async (request, response) => {
    writes++; assert.equal(request.method, 'DELETE'); assert.equal(request.url, '/demo/events')
    assert.equal(request.headers.authorization, 'Bearer scoped-token')
    response.writeHead(204); response.end()
  }))
  const stream = await listen(t, http.createServer((request, response) => {
    response.writeHead(307, { Location: owner + request.url }); response.end()
  }))
  const gateway = await listen(t, createGateway({ stream, owners: [owner] }))
  const response = await fetch(gateway + '/pico/demo/events', { method: 'DELETE', headers: { Authorization: 'Bearer scoped-token' } })
  assert.equal(response.status, 204); assert.equal(writes, 1)
})

test('redirects cannot send credentials to an unconfigured owner or change the path', async (t) => {
  let received = 0
  const owner = await listen(t, http.createServer((_request, response) => { received++; response.end('unexpected') }))
  let location = owner + '/demo/events'
  const stream = await listen(t, http.createServer((_request, response) => { response.writeHead(307, { Location: location }); response.end() }))
  const unpaired = await listen(t, createGateway({ stream }))
  assert.equal((await fetch(unpaired + '/pico/demo/events', { headers: { Authorization: 'Bearer secret' } })).status, 502)
  const paired = await listen(t, createGateway({ stream, owners: [owner] }))
  location = owner + '/admin/tokens'
  assert.equal((await fetch(paired + '/pico/demo/events', { headers: { Authorization: 'Bearer secret' } })).status, 502)
  assert.equal(received, 0)
})

test('gateway bounds redirect loops and never retries a failed write', async (t) => {
  let count = 0; let failing = false
  const stream = await listen(t, http.createServer((request, response) => {
    count++
    if (failing) { response.writeHead(503); response.end('unavailable'); return }
    response.writeHead(307, { Location: stream + request.url }); response.end()
  }))
  const gateway = await listen(t, createGateway({ stream }))
  assert.equal((await fetch(gateway + '/pico/loop')).status, 502)
  assert.equal(count, 4)
  count = 0; failing = true
  assert.equal((await fetch(gateway + '/pico/demo/events', { method: 'POST', body: 'once' })).status, 503)
  assert.equal(count, 1)
})

test('gateway rejects oversized requests before reaching PicoMQ', async (t) => {
  let count = 0
  const stream = await listen(t, http.createServer((_request, response) => { count++; response.end('unexpected') }))
  const gateway = await listen(t, createGateway({ stream }))
  const response = await fetch(gateway + '/pico/demo/events', { method: 'POST', body: 'x'.repeat(1024 * 1024 + 1) })
  assert.equal(response.status, 413); assert.equal(count, 0)
})
