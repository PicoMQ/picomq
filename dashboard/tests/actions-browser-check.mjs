// Optional action regressions: Node 22+ and Chrome/Chromium, no test dependencies.
// node tests/actions-browser-check.mjs http://localhost:5173
// Only dashboard assets use the network. All PicoMQ reads and writes are mocked.
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

const target = process.argv[2] || 'http://localhost:5173'
const profile = await mkdtemp(join(tmpdir(), 'picomq-actions-check-'))
const browser = spawn(process.env.CHROME_BIN || 'google-chrome', [
  '--headless=new', '--no-sandbox', '--disable-dev-shm-usage', '--remote-debugging-port=0',
  `--user-data-dir=${profile}`, 'about:blank',
], { stdio: 'ignore' })
let launchError
browser.on('error', (error) => { launchError = error })
let ws
let nextId = 0
const pending = new Map()
const exceptions = []

function command(method, params = {}) {
  return new Promise((resolve, reject) => {
    const id = ++nextId
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`CDP timeout: ${method}`)) }, 20_000)
    pending.set(id, { resolve, reject, timeout })
    ws.send(JSON.stringify({ id, method, params }))
  })
}
async function evaluate(expression) {
  const response = await command('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true })
  if (response.exceptionDetails) throw new Error(JSON.stringify(response.exceptionDetails))
  return response.result.value
}
async function until(expression, timeout = 12_000) {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    if (await evaluate(expression)) return
    await delay(50)
  }
  throw new Error(`Timed out: ${expression}\n${await evaluate('document.body.innerText')}`)
}
async function button(label) {
  await evaluate(`(() => {
    const node = [...document.querySelectorAll('button')].find((node) => node.textContent.trim() === ${JSON.stringify(label)} && node.getClientRects().length)
    if (!node || node.matches(':disabled')) throw new Error('Button missing or disabled: ' + ${JSON.stringify(label)})
    node.click()
  })()`)
}
async function input(label, value) {
  await evaluate(`(() => {
    const label = [...document.querySelectorAll('label')].find((node) => [...node.childNodes].filter((child) => child.nodeType === Node.TEXT_NODE).map((child) => child.textContent).join('').trim() === ${JSON.stringify(label)} && node.getClientRects().length)
    if (!label) throw new Error('Label missing: ' + ${JSON.stringify(label)})
    const node = label.querySelector('input, textarea, select')
    node.value = ${JSON.stringify(value)}
    node.dispatchEvent(new Event(node.tagName === 'SELECT' ? 'change' : 'input', { bubbles: true }))
  })()`)
}
async function details(label) {
  await evaluate(`(() => {
    const node = [...document.querySelectorAll('summary')].find((node) => node.textContent.trim().startsWith(${JSON.stringify(label)}) && node.getClientRects().length)
    if (!node) throw new Error('Details missing: ' + ${JSON.stringify(label)})
    node.parentElement.open = true
  })()`)
}
const visibleButton = (label) => `[...document.querySelectorAll('button')].find((node) => node.textContent.trim() === ${JSON.stringify(label)} && node.getClientRects().length)`
const storedValues = '[...Object.values(sessionStorage), ...Object.values(localStorage)].join(" ")'

function installApi(runId) {
  sessionStorage.clear(); localStorage.clear()
  sessionStorage.setItem('pico-stream-connection', JSON.stringify({ endpoint: '/pico', token: 'stream-token' }))
  sessionStorage.setItem('pico-admin-token', 'dashboard-admin-token')
  const originalFetch = window.fetch.bind(window)
  const streams = new Map([['/review/existing', 'application/json'], ['/review/second', 'application/json']])
  const tokens = new Map()
  const nodes = [1, 2].map((nodeId) => ({ nodeId, nodeEpoch: 1, advertisedAddress: `http://owner-${nodeId}.example:4437`, slots: 10, local: nodeId === 1, openingCount: 0, placedCount: 1 }))
  const state = window.actionsCheck = {
    runId, writes: [], pending: {}, adminStatus: 200,
    owner: { streamId: 5, nodeId: 1, ownerNodeId: 1, ownerAdvertisedAddress: nodes[0].advertisedAddress, epoch: 1, pendingTransfer: null },
  }
  const json = (value, status = 200, headers = {}) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json', ...headers } })
  const positions = { 'Pico-Start-Seq': '0', 'Pico-Next-Seq': '0' }
  function defer(kind, url, options, response) {
    state.writes.push({ kind, path: url.pathname, method: options.method, body: options.body, token: new Headers(options.headers).get('Authorization'), headers: Object.fromEntries(new Headers(options.headers)) })
    return new Promise((resolve, reject) => {
      const abort = () => { delete state.pending[kind]; reject(new DOMException('Aborted', 'AbortError')) }
      options.signal?.addEventListener('abort', abort, { once: true })
      state.pending[kind] = (fail = false) => {
        options.signal?.removeEventListener('abort', abort)
        delete state.pending[kind]
        if (fail) reject(new TypeError('Simulated response loss after commit'))
        else resolve(response)
      }
    })
  }
  window.fetch = async (resource, options = {}) => {
    const url = new URL(typeof resource === 'string' ? resource : resource.url, location.href)
    const method = options.method || 'GET'
    if (url.pathname === '/ready') return json({ ready: true, serving: true, registered: true, appliedIndex: 1, nodeId: 1 })
    if (url.pathname.startsWith('/admin/')) {
      if (state.adminStatus !== 200) return json({ error: 'Admin denied' }, state.adminStatus)
      if (url.pathname === '/admin/cluster') return json({ clusterId: 'actions-check', nodeId: 1, nodeEpoch: 1, registered: true, appliedIndex: 1, streamCount: streams.size, objectCount: 0, pendingTransfers: [], gc: { backlog: 0, oldestSeq: null, nextSeq: 0 }, leaseHolder: true })
      if (url.pathname === '/admin/nodes') return json({ nodes })
      if (/^\/admin\/nodes\/-?\d+$/.test(url.pathname)) {
        const node = nodes.find((node) => node.nodeId === Number(url.pathname.split('/').at(-1)))
        node.slots = JSON.parse(options.body).slots
        return defer('slots', url, options, json({ ...node }))
      }
      if (url.pathname.startsWith('/admin/streams/')) {
        const name = '/' + decodeURIComponent(url.pathname.slice('/admin/streams/'.length))
        if (method === 'PUT') {
          const existing = streams.has(name)
          streams.set(name, new Headers(options.headers).get('Content-Type'))
          return defer('create', url, options, new Response(null, { status: existing ? 200 : 201, headers: positions }))
        }
        if (method === 'DELETE') {
          streams.delete(name)
          return defer('delete', url, options, new Response(null, { status: 204 }))
        }
        return json({ name, ...state.owner })
      }
      if (url.pathname === '/admin/transfer') {
        const request = JSON.parse(options.body)
        state.owner.pendingTransfer = { streamId: 5, fromNode: 1, toNode: request.toNode }
        state.owner.ownerNodeId = request.toNode
        return defer('transfer', url, options, json({ stream: request.stream, streamId: 5, toNode: request.toNode, pending: true }, 202))
      }
      if (url.pathname === '/admin/tokens' && method === 'GET') return json({ count: tokens.size, tokens: [...tokens.values()] })
      if (url.pathname === '/admin/tokens' && method === 'POST') {
        const request = JSON.parse(options.body)
        const record = { ...request, createdAtMs: Date.now(), issuedBy: 'review-admin' }
        tokens.set(request.id, record)
        return defer('issue', url, options, json({ ...record, token: 'pico.mock.one-time-secret' }, 201))
      }
      if (url.pathname.startsWith('/admin/tokens/') && method === 'DELETE') {
        tokens.delete(decodeURIComponent(url.pathname.slice('/admin/tokens/'.length)))
        return defer('revoke', url, options, new Response(null, { status: 204 }))
      }
      throw new Error(`Unexpected admin route: ${method} ${url.pathname}`)
    }
    const base = ['/pico', '/other-pico'].find((base) => url.pathname.startsWith(base + '/'))
    if (!base) return originalFetch(resource, options)
    const name = url.pathname.slice(base.length)
    if (name === '/') return json({ streams: [...streams].map(([name, content_type]) => ({ name, content_type, closed: false })), has_more: false })
    if (name.startsWith('/_streams/')) return json({ schema: null, schemaValidate: false, kafkaTopic: null })
    if (!streams.has(name)) return json({ error: 'Unknown stream' }, 404)
    if (method === 'HEAD') return new Response(null, { headers: { ...positions, 'Content-Type': streams.get(name) } })
    if (method === 'GET') return json([], 200, positions)
    throw new Error(`Unexpected protocol write: ${method} ${url.pathname}`)
  }
}

let injection
async function reset() {
  if (injection) await command('Page.removeScriptToEvaluateOnNewDocument', { identifier: injection })
  const runId = `${Date.now()}-${Math.random()}`
  injection = (await command('Page.addScriptToEvaluateOnNewDocument', { source: `(${installApi.toString()})(${JSON.stringify(runId)})` })).identifier
  await command('Page.navigate', { url: target })
  await until(`window.actionsCheck?.runId === ${JSON.stringify(runId)} && document.querySelectorAll('.ss-tabs button').length === 5`)
}

try {
  let port
  for (let i = 0; i < 150; i++) {
    if (launchError) throw launchError
    if (browser.exitCode !== null) throw new Error(`Browser exited: ${browser.exitCode}`)
    try { port = Number((await readFile(join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0]); break } catch { await delay(100) }
  }
  if (!port) throw new Error('Chrome did not expose a debug port; set CHROME_BIN to Chrome/Chromium.')
  const pages = await (await fetch(`http://127.0.0.1:${port}/json`)).json()
  ws = new WebSocket(pages.find((page) => page.type === 'page').webSocketDebuggerUrl)
  await new Promise((resolve, reject) => { ws.addEventListener('open', resolve, { once: true }); ws.addEventListener('error', reject, { once: true }) })
  ws.addEventListener('message', (event) => {
    const message = JSON.parse(event.data)
    if (message.id) {
      const item = pending.get(message.id)
      if (!item) return
      clearTimeout(item.timeout); pending.delete(message.id)
      if (message.error) item.reject(new Error(JSON.stringify(message.error)))
      else item.resolve(message.result)
    } else if (message.method === 'Runtime.exceptionThrown') exceptions.push(message.params.exceptionDetails)
  })
  await command('Runtime.enable'); await command('Page.enable')
  await reset()
  await button('Discovery')
  await details('Create stream')
  assert.equal(await evaluate(`${visibleButton('Review create')}.disabled`), true)
  assert.equal(await evaluate('actionsCheck.writes.length'), 0)
  await details('Admin connection')
  await button('Use dashboard admin')
  await input('New stream name', '/review/new')
  await input('Kafka alias (optional)', 'review.new')
  await button('Review create')
  assert.equal(await evaluate('actionsCheck.writes.length'), 0)
  await details('Stream connection')
  await input('Stream API endpoint', '/other-pico')
  // A second click and connection submit happen before rendering disabled UI.
  await evaluate(`(() => {
    const confirm = ${visibleButton('Confirm create stream')};
    confirm.click(); confirm.click()
    const pairing = [...document.querySelectorAll('button')].find((node) => node.textContent.trim() === 'Use dashboard admin')
    pairing.click()
    document.querySelector('.ss-connection form').dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }))
  })()`)
  await until('typeof actionsCheck.pending.create === "function"')
  assert.equal(await evaluate('actionsCheck.writes.length'), 1)
  assert.equal(await evaluate("JSON.parse(sessionStorage.getItem('pico-stream-connection')).endpoint"), '/pico')
  assert.equal(await evaluate(`${visibleButton('Apply connection')}.disabled`), true)
  await button('Publish')
  await details('Stream connection')
  assert.equal(await evaluate(`${visibleButton('Apply connection')}.disabled`), true)
  await evaluate('actionsCheck.pending.create()')
  await button('Discovery')
  await until('document.body.innerText.includes("Created /review/new via " + location.origin + ".")')
  assert.equal(await evaluate('actionsCheck.writes[0].token'), 'Bearer dashboard-admin-token')
  assert.equal(await evaluate('actionsCheck.writes[0].path'), '/admin/streams/review/new')
  assert.equal(await evaluate('actionsCheck.writes[0].headers["pico-kafka-topic"]'), 'review.new')
  console.log('PASS create requires confirmation, blocks duplicates/connection changes across tabs, and retains its receipt')

  await details('Delete stream')
  await input('Type the exact stream name to confirm', '/review/wrong')
  assert.equal(await evaluate(`${visibleButton('Confirm delete stream')}.disabled`), true)
  assert.equal(await evaluate('actionsCheck.writes.length'), 1)
  await input('Type the exact stream name to confirm', '/review/new')
  await button('Confirm delete stream')
  await until('typeof actionsCheck.pending.delete === "function"')
  assert.equal(await evaluate('[...document.querySelectorAll(".ss-link")].filter((node) => node.getClientRects().length).every((node) => node.disabled)'), true)
  await button('Watch')
  await details('Stream connection')
  assert.equal(await evaluate(`${visibleButton('Apply connection')}.disabled`), true)
  await evaluate('actionsCheck.pending.delete()')
  await button('Discovery')
  await until('document.body.innerText.includes("Deleted /review/new via " + location.origin + ".")')
  assert.equal(await evaluate('!!document.querySelector(".ss-stream-name")'), false)
  assert.equal(await evaluate(`!!(${visibleButton('Confirm delete stream')})`), false)
  console.log('PASS delete requires the exact name, locks selection/connection, and keeps success after clearing selection')

  await input('New stream name', '/review/uncertain')
  await input('Kafka alias (optional)', '')
  await button('Review create')
  await button('Confirm create stream')
  await until('typeof actionsCheck.pending.create === "function"')
  await button('Watch')
  await evaluate('actionsCheck.pending.create(true)')
  await button('Discovery')
  await until('document.body.innerText.includes("The outcome is unknown.")')
  assert.equal(await evaluate('document.body.innerText.includes("Target: /review/uncertain via " + location.origin)'), true)
  await delay(150)
  assert.equal(await evaluate('actionsCheck.writes.filter((write) => write.kind === "create").length'), 2)
  await details('Stream connection')
  await input('Stream API endpoint', '/other-pico')
  await button('Apply connection')
  await until('[...document.querySelectorAll("label")].find((label) => label.firstChild.textContent.trim() === "New stream name").querySelector("input").value === ""')
  assert.equal(await evaluate('document.body.innerText.includes("The outcome is unknown.")'), false)
  console.log('PASS unknown create outcomes retain target without retry and connection changes clear stale drafts')

  await reset()
  await button('Discovery')
  await details('Admin connection')
  await button('Use dashboard admin')
  await details('Create stream')
  await input('New stream name', '/review/stale')
  await button('Review create')
  await evaluate("sessionStorage.setItem('pico-admin-token', 'new-admin-token')")
  await button('Confirm create stream')
  await until('document.body.innerText.includes("The admin token changed")')
  assert.equal(await evaluate('actionsCheck.writes.length'), 0)
  await button('Use dashboard admin')
  await input('New stream name', '/review/stale')
  await button('Review create')
  await button('Confirm create stream')
  await until('typeof actionsCheck.pending.create === "function"')
  assert.equal(await evaluate('actionsCheck.writes.at(-1).token'), 'Bearer new-admin-token')
  await evaluate('actionsCheck.pending.create()')
  await until('document.body.innerText.includes("Created /review/stale")')
  await details('Delete stream')
  await input('Type the exact stream name to confirm', '/review/stale')
  await evaluate("sessionStorage.setItem('pico-admin-token', 'newer-admin-token')")
  await button('Confirm delete stream')
  await until('document.body.innerText.includes("The admin token changed")')
  assert.equal(await evaluate('actionsCheck.writes.length'), 1)
  await input('Admin API endpoint', 'https://custom-admin.example')
  await input('Admin API token', '')
  await button('Connect admin API')
  await input('New stream name', '/review/custom-empty')
  await button('Review create')
  await button('Confirm create stream')
  await until('typeof actionsCheck.pending.create === "function"')
  assert.equal(await evaluate('actionsCheck.writes.at(-1).token'), null)
  await evaluate('actionsCheck.pending.create()')
  await until('document.body.innerText.includes("Created /review/custom-empty via https://custom-admin.example")')
  await input('Admin API token', 'custom-admin-token')
  await button('Connect admin API')
  await input('Type the exact stream name to confirm', '/review/custom-empty')
  await button('Confirm delete stream')
  await until('typeof actionsCheck.pending.delete === "function"')
  assert.equal(await evaluate('actionsCheck.writes.at(-1).token'), 'Bearer custom-admin-token')
  await evaluate('actionsCheck.pending.delete()')
  await until('document.body.innerText.includes("Deleted /review/custom-empty")')
  console.log('PASS lifecycle rejects changed dashboard credentials and uses only the custom admin token, including explicit empty credentials')

  for (const change of ['pair', 'stream']) {
    await reset()
    await button('Discovery')
    await details('Admin connection')
    await button('Use dashboard admin')
    await details('Create stream')
    await input('New stream name', '/review/context-change')
    await button('Review create')
    if (change === 'pair') {
      await input('Admin API endpoint', 'https://replacement-admin.example')
      await input('Admin API token', 'replacement-admin-token')
    } else {
      await details('Stream connection')
      await input('Stream API endpoint', '/other-pico')
    }
    await evaluate(`(() => {
      const confirm = ${visibleButton('Confirm create stream')};
      ${visibleButton(change === 'pair' ? 'Connect admin API' : 'Apply connection')}.click()
      confirm.click()
    })()`)
    await delay(50)
    assert.equal(await evaluate('actionsCheck.writes.length'), 0, `A same-tick ${change} change must invalidate create confirmation`)
  }
  for (const change of ['pair', 'selection', 'stream']) {
    await reset()
    await button('Discovery')
    await details('Admin connection')
    await button('Use dashboard admin')
    await until('[...document.querySelectorAll(".ss-link")].some((node) => node.textContent === "/review/existing")')
    await button('/review/existing')
    await details('Delete stream')
    await input('Type the exact stream name to confirm', '/review/existing')
    if (change === 'pair') {
      await input('Admin API endpoint', 'https://replacement-admin.example')
      await input('Admin API token', 'replacement-admin-token')
    } else if (change === 'stream') {
      await details('Stream connection')
      await input('Stream API endpoint', '/other-pico')
    }
    await evaluate(`(() => {
      const confirm = ${visibleButton('Confirm delete stream')};
      ${visibleButton(change === 'pair' ? 'Connect admin API' : change === 'stream' ? 'Apply connection' : '/review/second')}.click()
      confirm.click()
    })()`)
    await delay(50)
    assert.equal(await evaluate('actionsCheck.writes.length'), 0, `A same-tick ${change} change must invalidate delete confirmation`)
  }
  console.log('PASS pairing, stream connection and selected stream changes invalidate existing confirmations before the next render')

  await reset()
  await button('Tokens')
  await input('New token ID', 'review/reader')
  await button('Review token')
  await until('document.body.innerText.includes("Enter a stream prefix")')
  assert.equal(await evaluate('actionsCheck.writes.length'), 0)
  await input('Streams value 1', '/review/')
  await button('Review token')
  assert.equal(await evaluate('actionsCheck.writes.length'), 0)
  for (const width of [320, 375, 1366]) {
    await command('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: false })
    assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth'), true, `Token confirmation overflows at ${width}px`)
  }
  await button('Confirm issue token')
  await until('typeof actionsCheck.pending.issue === "function"')
  await button('Discovery')
  await evaluate('actionsCheck.pending.issue()')
  await button('Tokens')
  await until(`document.querySelector('[aria-label="New token secret"] textarea')?.value === 'pico.mock.one-time-secret'`)
  assert.equal(await evaluate('actionsCheck.writes.at(-1).token'), 'Bearer dashboard-admin-token')
  assert.equal(await evaluate(`${storedValues}.includes('pico.mock.one-time-secret')`), false)
  await button('Discovery')
  await button('Tokens')
  assert.equal(await evaluate(`document.querySelector('[aria-label="New token secret"] textarea').value`), 'pico.mock.one-time-secret')
  for (const width of [320, 375, 1366]) {
    await command('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: false })
    assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth'), true, `Token secret/table overflows at ${width}px`)
  }
  await button('Dismiss secret')
  assert.equal(await evaluate(`!!document.querySelector('[aria-label="New token secret"]')`), false)
  assert.equal(await evaluate(`${storedValues}.includes('pico.mock.one-time-secret')`), false)
  await until(`!!document.querySelector('[aria-label="Revoke token review/reader"]')`)
  await evaluate(`document.querySelector('[aria-label="Revoke token review/reader"]').click()`)
  assert.equal(await evaluate('actionsCheck.writes.length'), 1)
  await button('Confirm revoke token')
  await until('typeof actionsCheck.pending.revoke === "function"')
  assert.equal(await evaluate('actionsCheck.writes.at(-1).path'), '/admin/tokens/review%2Freader')
  await evaluate('actionsCheck.pending.revoke()')
  await until('document.body.innerText.includes("Revoked token review/reader.")')
  await until(`!document.querySelector('[aria-label="Revoke token review/reader"]')`)
  console.log('PASS token issuance/revocation require confirmation; issued secret survives tabs, stays out of storage, and disappears on dismissal')

  await input('New token ID', 'review/uncertain-token')
  await button('Review token')
  await evaluate("sessionStorage.setItem('pico-admin-token', 'replacement-admin-token')")
  await button('Confirm issue token')
  await until('document.body.innerText.includes("The admin token changed")')
  assert.equal(await evaluate('actionsCheck.writes.length'), 2)
  await button('Review token')
  await button('Confirm issue token')
  await until('typeof actionsCheck.pending.issue === "function"')
  assert.equal(await evaluate('actionsCheck.writes.at(-1).token'), 'Bearer replacement-admin-token')
  await button('Discovery')
  await evaluate('actionsCheck.pending.issue(true)')
  await button('Tokens')
  await until('document.body.innerText.includes("its secret cannot be recovered")')
  assert.match(await evaluate('document.body.innerText'), /Issue review\/uncertain-token/)
  assert.equal(await evaluate(`!!document.querySelector('[aria-label="New token secret"]')`), false)
  await button('Refresh tokens')
  await until(`!!document.querySelector('[aria-label="Revoke token review/uncertain-token"]')`)
  assert.equal(await evaluate('actionsCheck.writes.filter((write) => write.kind === "issue").length'), 2)
  console.log('PASS stale admin credentials invalidate confirmation; uncertain issuance keeps its ID and recovery guidance without retry')

  for (const width of [320, 375, 1366]) {
    await command('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: false })
    for (const tab of ['Overview', 'Discovery', 'Watch', 'Publish', 'Tokens']) {
      await button(tab)
      if (tab === 'Discovery') { await details('Create stream'); await details('Delete stream') }
      assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth'), true, `${tab} overflows at ${width}px`)
    }
  }
  assert.deepEqual(exceptions, [])
  console.log('PASS all five tabs and expanded lifecycle controls fit desktop/mobile; no runtime exceptions')
} finally {
  for (const item of pending.values()) clearTimeout(item.timeout)
  ws?.close()
  browser.kill('SIGTERM')
  await Promise.race([new Promise((resolve) => browser.once('exit', resolve)), delay(2_000)])
  if (browser.exitCode === null) browser.kill('SIGKILL')
  await rm(profile, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 })
}
