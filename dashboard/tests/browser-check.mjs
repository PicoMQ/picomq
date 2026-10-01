// Optional browser regression checks. Requires Node 22+ and Chrome/Chromium.
// Start the dashboard, then run: node tests/browser-check.mjs http://localhost:5173
// Set CHROME_BIN to override the browser executable. No PicoMQ server is needed:
// only dashboard assets use the network; all API responses are controlled below.
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

const target = process.argv[2] || 'http://localhost:5173'
const profile = await mkdtemp(join(tmpdir(), 'picomq-browser-check-'))
const browser = spawn(process.env.CHROME_BIN || 'google-chrome', [
  '--headless=new', '--no-sandbox', '--disable-dev-shm-usage',
  '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank',
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
    const button = [...document.querySelectorAll('button')].find((node) => node.textContent.trim() === ${JSON.stringify(label)} && node.getClientRects().length)
    if (!button || button.matches(':disabled')) throw new Error('Button missing or disabled: ' + ${JSON.stringify(label)})
    button.click()
  })()`)
}

async function input(label, value) {
  await evaluate(`(() => {
    const label = [...document.querySelectorAll('label')].find((node) => node.firstChild.textContent.trim() === ${JSON.stringify(label)} && node.getClientRects().length)
    if (!label) throw new Error('Label missing: ' + ${JSON.stringify(label)})
    const input = label.querySelector('input, textarea, select')
    input.value = ${JSON.stringify(value)}
    input.dispatchEvent(new Event(input.tagName === 'SELECT' ? 'change' : 'input', { bubbles: true }))
  })()`)
}

async function selectStream(name) {
  await evaluate(`(() => {
    const link = [...document.querySelectorAll('.ss-link')].find((node) => node.textContent === ${JSON.stringify(name)})
    if (!link) throw new Error('Stream missing: ' + ${JSON.stringify(name)})
    link.click()
  })()`)
}

function metadata(label) {
  return `[...document.querySelectorAll('dt')].find((node) => node.textContent === ${JSON.stringify(label)})?.nextElementSibling?.textContent`
}

async function openAdminConnection() {
  await evaluate(`(() => {
    const summary = [...document.querySelectorAll('summary')].find((node) => node.textContent === 'Admin connection')
    if (!summary) throw new Error('Admin connection controls missing')
    summary.parentElement.open = true
  })()`)
}

const streamInputs = `[...document.querySelectorAll('label')]
  .filter((label) => /^Stream [1-9]\\d*$/.test(label.firstChild.textContent.trim()) && label.getClientRects().length)
  .map((label) => label.querySelector('input'))`

async function streamValues() {
  return evaluate(`(${streamInputs}).map((input) => input.value)`)
}

async function removeStream(index) {
  await evaluate(`(() => {
    const button = document.querySelector('[aria-label="Remove stream ${index}"]')
    if (!button || button.matches(':disabled') || !button.getClientRects().length) throw new Error('Remove control unavailable: ${index}')
    button.click()
  })()`)
}

// Inject before app startup so both authentication-at-startup and expiry paths
// are exercised. Appends commit to this in-browser store before their response
// is released, reproducing the ambiguous-delivery case without real writes.
function installApi(initialAdminStatus, runId) {
  sessionStorage.setItem('pico-stream-connection', JSON.stringify({ endpoint: '/pico', token: 'stream-token' }))
  sessionStorage.setItem('pico-admin-token', 'dashboard-admin-token')
  const originalFetch = window.fetch.bind(window)
  const records = new Map([['/review/one', []], ['/review/two', []]])
  const state = window.browserCheck = {
    runId, adminStatus: initialAdminStatus, observedAdminStatus: null,
    appendCount: 0, abortedAppends: 0, pendingAppend: null,
    ownershipStatus: 200, ownershipRequests: [], deferredOwnership: null, pendingOwnership: [],
    ownership: {
      '/review/one': { ownerNodeId: 41, ownerAdvertisedAddress: 'http://owner-one.example:4437', epoch: 0 },
      '/review/two': { ownerNodeId: 42, ownerAdvertisedAddress: 'http://owner-two.example:4437', epoch: 7 },
    },
    push(name) {
      const list = records.get(name)
      list.push({ seq: list.length, timestamp: Date.now(), body: JSON.stringify({ name, event: list.length }) })
    },
  }
  const json = (value, status = 200, headers = {}) => new Response(JSON.stringify(value), {
    status, headers: { 'Content-Type': 'application/json', ...headers },
  })
  const positions = (next) => ({ 'Pico-Start-Seq': '0', 'Pico-Next-Seq': String(next), 'Pico-Closed': 'false' })
  window.fetch = async (resource, options = {}) => {
    const url = new URL(typeof resource === 'string' ? resource : resource.url, location.href)
    if (url.pathname.includes('/admin/streams/')) {
      const name = '/' + decodeURIComponent(url.pathname.split('/admin/streams/')[1])
      state.ownershipRequests.push({ name, endpoint: url.origin + url.pathname, token: new Headers(options.headers).get('Authorization') })
      const response = json(state.ownership[name] ? { name, ...state.ownership[name] } : { error: 'Unknown stream' }, state.ownershipStatus)
      // Ignore abort deliberately: a late transport completion must not replace
      // the currently selected stream's ownership or a changed connection.
      if (state.deferredOwnership === name) return new Promise((resolve) => { state.pendingOwnership.push(() => resolve(response)) })
      return response
    }
    if (url.pathname.startsWith('/admin/')) {
      state.observedAdminStatus = state.adminStatus
      if (state.adminStatus !== 200) return json({ error: 'Admin token unavailable' }, state.adminStatus)
      return json(url.pathname.endsWith('/nodes') ? { nodes: [] } : {
        clusterId: 'browser-check', nodeId: 'review', streamCount: records.size,
        objectCount: 0, pendingTransfers: [], gc: {},
      })
    }
    if (url.pathname === '/ready') return json({ ready: true })
    const protocolBase = ['/pico', '/other-pico'].find((base) => url.pathname.startsWith(base + '/'))
    if (!protocolBase) return originalFetch(resource, options)
    const name = url.pathname.slice(protocolBase.length)
    if (name === '/') return json({ streams: [...records.keys()].map((name) => ({ name, content_type: 'application/json', closed: false })), has_more: false })
    if (name.startsWith('/_streams/')) return json({ schema: null, schemaValidate: false, kafkaTopic: null })
    const list = records.get(name)
    if (!list) return json({ error: 'Unknown stream' }, 404)
    if (options.method === 'HEAD') return new Response(null, { headers: { 'Content-Type': 'application/json', ...positions(list.length) } })
    if (options.method === 'POST') {
      const record = JSON.parse(options.body).records[0]
      const start = list.length
      list.push({ ...record, seq: start, timestamp: Date.now() })
      state.appendCount++
      return new Promise((resolve, reject) => {
        const abort = () => { state.abortedAppends++; state.pendingAppend = null; reject(new DOMException('Aborted', 'AbortError')) }
        options.signal.addEventListener('abort', abort, { once: true })
        state.pendingAppend = (fail = false) => {
          options.signal.removeEventListener('abort', abort)
          state.pendingAppend = null
          if (fail) reject(new TypeError('Simulated response loss after commit'))
          else resolve(new Response(null, { headers: { ...positions(start + 1), 'Pico-Start-Seq': String(start) } }))
        }
      })
    }
    const from = Number(url.searchParams.get('seq'))
    const page = list.slice(from, from + Number(url.searchParams.get('count') || 50))
    return json(page, 200, positions(page.length ? page.at(-1).seq + 1 : from))
  }
}

let injection
async function reset(adminStatus) {
  if (injection) await command('Page.removeScriptToEvaluateOnNewDocument', { identifier: injection })
  const runId = `${Date.now()}-${Math.random()}`
  const response = await command('Page.addScriptToEvaluateOnNewDocument', { source: `(${installApi.toString()})(${adminStatus}, ${JSON.stringify(runId)})` })
  injection = response.identifier
  await command('Page.navigate', { url: target })
  await until(`window.browserCheck?.runId === ${JSON.stringify(runId)} && browserCheck.observedAdminStatus === ${adminStatus} && document.querySelectorAll(".ss-tabs button").length === 5`)
}

async function adminStatus(status) {
  await evaluate(`browserCheck.observedAdminStatus = null; browserCheck.adminStatus = ${status}`)
  await until(`browserCheck.observedAdminStatus === ${status}`)
  // Allow the app's state/render to commit after fetch returns the response.
  await delay(100)
}

try {
  let port
  for (let i = 0; i < 150; i++) {
    if (launchError) throw launchError
    if (browser.exitCode !== null) throw new Error(`Browser exited with code ${browser.exitCode}`)
    try { port = Number((await readFile(join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0]); break } catch { await delay(100) }
  }
  if (!port) throw new Error('Chrome did not expose a debug port; set CHROME_BIN to a working Chrome/Chromium executable.')
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

  await reset(401)
  assert.match(await evaluate('document.body.innerText'), /admin audience/)
  await button('Watch')
  assert.equal((await streamValues()).length, 1)
  assert.equal(await evaluate(`document.querySelector('[aria-label="Remove stream 1"]').disabled`), true)
  await input('Stream 1', '/review/one')
  await button('Add stream')
  await until('document.activeElement.labels?.[0]?.textContent.trim() === "Stream 2"')
  await button('Start watching')
  assert.equal(await evaluate(`(${streamInputs})[1].matches(':invalid')`), true)
  assert.equal(await evaluate(`(${streamInputs}).some((input) => input.matches(':disabled'))`), false)
  assert.equal(await evaluate('document.activeElement.labels?.[0]?.textContent.trim()'), 'Stream 2')
  await input('Stream 2', '/review/middle')
  await button('Add stream')
  await until('document.activeElement.labels?.[0]?.textContent.trim() === "Stream 3"')
  await input('Stream 3', '/review/two')
  await removeStream(2)
  await until(`(${streamInputs}).includes(document.activeElement)`)
  assert.deepEqual(await streamValues(), ['/review/one', '/review/two'])
  assert.ok(['/review/one', '/review/two'].includes(await evaluate('document.activeElement.value')))
  assert.equal(await evaluate('[...document.querySelectorAll("button")].find((button) => button.textContent === "Start watching").disabled'), false)

  await input('Match', 'regex')
  await input('Name pattern', '^/review/(one|two)$')
  await input('Match', 'exact')
  assert.deepEqual(await streamValues(), ['/review/one', '/review/two'])
  await input('Match', 'regex')
  assert.equal(await evaluate('[...document.querySelectorAll("label")].find((label) => label.firstChild.textContent.trim() === "Name pattern").querySelector("input").value'), '^/review/(one|two)$')
  await input('Match', 'exact')

  for (let index = 3; index <= 7; index++) {
    await button('Add stream')
    await until(`document.activeElement.labels?.[0]?.textContent.trim() === "Stream ${index}"`)
    await input(`Stream ${index}`, `/review/draft-${index}`)
  }
  await evaluate(`(() => {
    const add = [...document.querySelectorAll('button')].find((button) => button.textContent === 'Add stream')
    add.click(); add.click()
  })()`)
  await until('document.activeElement.labels?.[0]?.textContent.trim() === "Stream 8"')
  await input('Stream 8', '/review/draft-8')
  assert.equal((await streamValues()).length, 8)
  assert.equal(await evaluate('[...document.querySelectorAll("button")].find((button) => button.textContent === "Add stream").disabled'), true)
  await evaluate('[...document.querySelectorAll("button")].find((button) => button.textContent === "Add stream").click()')
  assert.equal((await streamValues()).length, 8)
  for (const width of [320, 375]) {
    await command('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: false })
    assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth'), true)
    assert.equal(await evaluate(`(${streamInputs}).every((input) => {
      const rect = input.getBoundingClientRect()
      return rect.left >= 0 && rect.right <= innerWidth && rect.width >= 120
    })`), true)
    assert.equal(await evaluate(`[...document.querySelectorAll('button[aria-label^="Remove stream "]')].every((button) => { const rect = button.getBoundingClientRect(); return rect.left >= 0 && rect.right <= innerWidth })`), true)
  }
  await command('Emulation.setDeviceMetricsOverride', { width: 1366, height: 900, deviceScaleFactor: 1, mobile: false })
  for (let index = 8; index >= 3; index--) {
    await removeStream(index)
    await until(`document.activeElement.labels?.[0]?.textContent.trim() === "Stream ${index - 1}"`)
  }
  assert.deepEqual(await streamValues(), ['/review/one', '/review/two'])
  console.log('PASS individual stream rows preserve values/focus, enforce eight rows, retain separate regex drafts, and fit mobile')

  await input('Start at', 'beginning')
  await button('Start watching')
  assert.equal(await evaluate(`(${streamInputs}).every((input) => input.matches(':disabled'))`), true)
  assert.equal(await evaluate(`[...document.querySelectorAll('button[aria-label^="Remove stream "]')].every((button) => button.matches(':disabled'))`), true)
  assert.equal(await evaluate('[...document.querySelectorAll("button")].find((button) => button.textContent === "Add stream").matches(":disabled")'), true)
  await evaluate("browserCheck.push('/review/one')")
  await until('document.querySelectorAll(".ss-watch-record").length === 1')
  await adminStatus(200)
  let expectedRows = 1
  for (const status of [401, 403]) {
    await adminStatus(status)
    await evaluate("browserCheck.push('/review/one'); browserCheck.push('/review/two')")
    expectedRows += 2
    await until(`document.querySelectorAll(".ss-watch-record").length === ${expectedRows}`)
  }
  assert.deepEqual(await streamValues(), ['/review/one', '/review/two'])
  const identities = await evaluate('[...document.querySelectorAll(".ss-watch-record")].map((row) => row.querySelector(".ss-record-stream").textContent + row.querySelector("summary .mono").textContent)')
  assert.equal(new Set(identities).size, expectedRows)
  await button('Stop')
  assert.equal(await evaluate(`(${streamInputs}).every((input) => !input.matches(':disabled'))`), true)
  await button('Discovery')
  await until('[...document.querySelectorAll(".ss-link")].some((link) => link.textContent === "/review/two")')
  await evaluate(`(() => {
    const row = [...document.querySelectorAll('.ss-link')].find((link) => link.textContent === '/review/two').closest('tr')
    ;[...row.querySelectorAll('button')].find((button) => button.textContent === 'Watch').click()
  })()`)
  await until(`(${streamInputs}).length === 1`)
  assert.deepEqual(await streamValues(), ['/review/two'])
  assert.equal(await evaluate(`document.querySelector('[aria-label="Remove stream 1"]').disabled`), true)
  console.log('PASS admin401 startup and admin401/403 expiry preserve independent multi-stream access, rows, and cursors')
  console.log('PASS active watches disable row controls; Discovery preselects one editable stream row')

  await reset(200)
  await evaluate("browserCheck.push('/review/one')")
  await button('Discovery')
  await until('[...document.querySelectorAll(".ss-link")].some((link) => link.textContent === "/review/one")')
  await selectStream('/review/one')
  await until('document.querySelectorAll(".ss-record").length === 1')
  assert.equal(await evaluate('browserCheck.ownershipRequests.length'), 0)
  await openAdminConnection()
  await button('Use dashboard admin')
  await until(`${metadata('Routing owner')} === '41'`)
  assert.equal(await evaluate(metadata('Stream epoch')), '0')
  assert.equal(await evaluate('browserCheck.ownershipRequests.at(-1).token'), 'Bearer dashboard-admin-token')
  assert.equal(await evaluate(metadata('Next position')), '1')

  await evaluate('browserCheck.ownershipStatus = 403')
  await button('Refresh details')
  await until('document.body.innerText.includes("The token lacks admin scope")')
  assert.equal(await evaluate(metadata('Next position')), '1')
  assert.equal(await evaluate('document.querySelectorAll(".ss-record").length'), 1)
  assert.equal(await evaluate(metadata('Routing owner')), undefined)
  await evaluate('browserCheck.ownershipStatus = 200')
  for (const [epoch, expected] of [[null, 'Not available'], [-1, 'Not opened (−1)'], [0, '0']]) {
    await evaluate(`browserCheck.ownership['/review/one'] = { ownerNodeId: -1, ownerAdvertisedAddress: '', epoch: ${JSON.stringify(epoch)} }`)
    await button('Refresh details')
    await until(`${metadata('Stream epoch')} === ${JSON.stringify(expected)}`)
    assert.equal(await evaluate(metadata('Routing owner')), 'Unassigned')
  }
  await evaluate("browserCheck.ownership['/review/one'].ownerAdvertisedAddress = 'http://negative-id-owner.example:4437'")
  await button('Refresh details')
  await until(`${metadata('Routing owner')} === '-1'`)
  console.log('PASS ownership requires explicit pairing, preserves null/-1/zero, and admin errors leave native samples usable')

  await adminStatus(401)
  await button('Overview')
  const lookupsBeforeUnlock = await evaluate('browserCheck.ownershipRequests.length')
  await evaluate(`(() => {
    const input = document.querySelector('[aria-label="Admin access token"]')
    input.value = 'replacement-dashboard-token'
    input.dispatchEvent(new Event('input', { bubbles: true }))
    browserCheck.adminStatus = 200
  })()`)
  await button('Unlock')
  await until(`browserCheck.ownershipRequests.length > ${lookupsBeforeUnlock}`)
  await button('Discovery')
  assert.equal(await evaluate('browserCheck.ownershipRequests.at(-1).token'), 'Bearer replacement-dashboard-token')
  await until(`${metadata('Stream epoch')} === '0'`)
  console.log('PASS unlocking Overview refreshes paired dashboard ownership with the new admin token')

  await evaluate("browserCheck.deferredOwnership = '/review/one'; browserCheck.ownership['/review/one'].ownerNodeId = 999")
  await button('Refresh details')
  await until('browserCheck.pendingOwnership.length === 1')
  await selectStream('/review/two')
  await until(`${metadata('Routing owner')} === '42'`)
  await evaluate('browserCheck.pendingOwnership.shift()()')
  await delay(100)
  assert.equal(await evaluate(metadata('Routing owner')), '42')
  assert.equal(await evaluate(metadata('Stream epoch')), '7')

  await evaluate("browserCheck.deferredOwnership = '/review/two'; browserCheck.ownership['/review/two'].ownerNodeId = 998")
  await button('Refresh details')
  await until('browserCheck.pendingOwnership.length === 1')
  await evaluate('document.querySelector(".ss-connection").open = true')
  await input('Stream API endpoint', '/other-pico')
  await button('Apply connection')
  await until('!document.querySelector(".ss-stream-name")')
  await until('[...document.querySelectorAll(".ss-link")].some((link) => link.textContent === "/review/one")')
  const lookupsBeforeSelect = await evaluate('browserCheck.ownershipRequests.length')
  await selectStream('/review/one')
  await until('document.querySelectorAll(".ss-record").length === 1')
  await evaluate('browserCheck.pendingOwnership.shift()(); browserCheck.deferredOwnership = null')
  await delay(100)
  assert.equal(await evaluate('browserCheck.ownershipRequests.length'), lookupsBeforeSelect)
  assert.equal(await evaluate(metadata('Routing owner')), undefined)
  console.log('PASS stale ownership cannot cross stream selection or stream endpoint changes; endpoint changes clear pairing')

  await openAdminConnection()
  await input('Admin API endpoint', 'https://admin.review.example:9090')
  await input('Admin API token', 'custom-admin-token')
  await button('Connect admin API')
  await until(`${metadata('Routing owner')} === '999'`)
  assert.equal(await evaluate('browserCheck.ownershipRequests.at(-1).token'), 'Bearer custom-admin-token')
  assert.match(await evaluate('browserCheck.ownershipRequests.at(-1).endpoint'), /^https:\/\/admin\.review\.example:9090\/admin\/streams\//)
  const lookupsBeforeEmptyToken = await evaluate('browserCheck.ownershipRequests.length')
  await input('Admin API token', '')
  await button('Connect admin API')
  await until(`browserCheck.ownershipRequests.length > ${lookupsBeforeEmptyToken}`)
  assert.equal(await evaluate('browserCheck.ownershipRequests.at(-1).token'), null)
  console.log('PASS explicit admin connections use only their own credentials, including an intentionally empty token')

  const longAddress = 'https://' + 'owner.'.repeat(40) + 'example:4437/streams'
  await evaluate(`browserCheck.ownership['/review/one'].ownerAdvertisedAddress = ${JSON.stringify(longAddress)}`)
  await button('Refresh details')
  await until(`${metadata('Owner address')} === ${JSON.stringify(longAddress)}`)
  for (const width of [320, 375, 1366]) {
    await command('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: false })
    assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth'), true, `Ownership overflows at ${width}px`)
  }
  console.log('PASS ownership details and expanded admin controls fit mobile/desktop with a long owner address')

  await reset(403)
  await button('Publish')
  await input('Stream name', '/review/one')
  await input('Message body', '{"publish":"retained"}')
  await input('Key (optional)', 'review-key')
  await evaluate('document.querySelector(".ss-connection").open = true')
  await input('Stream API endpoint', '/changed')
  // Submit before Preact can render the disabled controls. The synchronous
  // guards must block both a duplicate send and this stale connection submit.
  await evaluate(`(() => {
    const form = [...document.querySelectorAll('label')].find((label) => label.firstChild.textContent.trim() === 'Message body').closest('form')
    form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }))
    form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }))
    document.querySelector('.ss-connection form').dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }))
  })()`)
  await until('typeof browserCheck.pendingAppend === "function"')
  await evaluate('document.querySelector(".ss-connection").open = true')
  assert.equal(await evaluate('[...document.querySelectorAll("button")].find((button) => button.textContent === "Apply connection").disabled'), true)
  // Exercise the submit guard as well as the disabled button, with both the
  // identical endpoint and a changed draft. Neither may cancel the append.
  for (const endpoint of ['/pico', '/changed']) {
    await input('Stream API endpoint', endpoint)
    await evaluate('document.querySelector(".ss-connection form").dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }))')
    await delay(100)
    assert.match(await evaluate('document.querySelector(".ss-connection summary").textContent'), /\/pico/)
    assert.equal(await evaluate('browserCheck.abortedAppends'), 0)
  }
  await button('Discovery')
  await evaluate('document.querySelector(".ss-connection").open = true')
  assert.equal(await evaluate('[...document.querySelectorAll("button")].find((button) => button.textContent === "Apply connection").disabled'), true)
  await adminStatus(200)
  await adminStatus(401)
  await adminStatus(403)
  await button('Publish')
  assert.equal(await evaluate('typeof browserCheck.pendingAppend'), 'function')
  await evaluate('browserCheck.pendingAppend()')
  await until('document.querySelector(".ss-success")?.textContent.includes("Sent one message to /review/one")')
  await button('Overview'); await button('Publish')
  assert.match(await evaluate('document.querySelector(".ss-success").textContent'), /Sent one message to \/review\/one/)
  await evaluate('document.querySelector(".ss-connection").open = true')
  await input('Stream API endpoint', '/pico')
  await button('Apply connection')
  assert.match(await evaluate('document.querySelector(".ss-success")?.textContent || ""'), /Sent one message/)
  assert.equal(await evaluate('browserCheck.appendCount'), 1)
  console.log('PASS sending blocks same/changed connection application across tabs; admin expiry preserves the acknowledgement')

  await input('Message body', '{"publish":"ambiguous"}')
  await button('Send message')
  await until('typeof browserCheck.pendingAppend === "function"')
  await evaluate('browserCheck.pendingAppend(true)')
  await until('document.querySelector(".ss-notice")?.textContent.includes("is unknown")')
  await button('Discovery'); await button('Publish')
  assert.match(await evaluate('document.querySelector(".ss-notice").textContent'), /Delivery to \/review\/one via \/pico is unknown/)
  await delay(1_100)
  assert.equal(await evaluate('browserCheck.appendCount'), 2)
  assert.equal(await evaluate('browserCheck.abortedAppends'), 0)
  console.log('PASS ambiguous append outcomes persist across tabs and are never automatically retried')

  for (const width of [320, 375, 1366]) {
    await command('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: false })
    for (const tab of ['Overview', 'Discovery', 'Watch', 'Publish']) {
      await button(tab)
      const size = await evaluate('({ width: innerWidth, scroll: document.documentElement.scrollWidth })')
      assert.ok(size.scroll <= size.width, `${tab} overflows at ${width}px: ${JSON.stringify(size)}`)
    }
  }
  assert.deepEqual(exceptions, [])
  console.log('PASS all tabs fit mobile/desktop viewports; no browser runtime exceptions')
} finally {
  for (const item of pending.values()) clearTimeout(item.timeout)
  ws?.close()
  browser.kill('SIGTERM')
  await Promise.race([new Promise((resolve) => browser.once('exit', resolve)), delay(2_000)])
  if (browser.exitCode === null) browser.kill('SIGKILL')
  await rm(profile, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 })
}
