// Optional real-browser checks using controlled API responses and dashboard assets.
// Node 22+ and Chrome/Chromium; no backend service or added dependency is needed.
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

// Keep the same Chrome/CDP setup as the existing optional browser checks.
async function openBrowser(target) {
  const profile = await mkdtemp(join(tmpdir(), 'picomq-transfer-check-'))
  const process = spawn(globalThis.process.env.CHROME_BIN || 'google-chrome', [
    '--headless=new', '--no-sandbox', '--disable-dev-shm-usage',
    '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank',
  ], { stdio: 'ignore' })
  let launchError
  process.on('error', (error) => { launchError = error })
  let socket
  let nextId = 0
  let injection
  const pending = new Map()
  const exceptions = []
  function command(method, params = {}) {
    return new Promise((resolve, reject) => {
      const id = ++nextId
      const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`CDP timeout: ${method}`)) }, 20_000)
      pending.set(id, { resolve, reject, timeout })
      socket.send(JSON.stringify({ id, method, params }))
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
      const label = [...document.querySelectorAll('label')].find((node) => [...node.childNodes].filter((part) => part.nodeType === 3).map((part) => part.textContent).join('').trim() === ${JSON.stringify(label)} && node.getClientRects().length)
      if (!label) throw new Error('Label missing: ' + ${JSON.stringify(label)})
      const input = label.querySelector('input, textarea, select')
      input.value = ${JSON.stringify(value)}
      input.dispatchEvent(new Event(input.tagName === 'SELECT' ? 'change' : 'input', { bubbles: true }))
    })()`)
  }
  async function openDetails(label) {
    await evaluate(`(() => {
      const summary = [...document.querySelectorAll('summary')].find((node) => node.textContent.trim().startsWith(${JSON.stringify(label)}) && node.getClientRects().length)
      if (!summary) throw new Error('Details missing: ' + ${JSON.stringify(label)})
      summary.parentElement.open = true
    })()`)
  }
  async function selectStream(name) {
    await evaluate(`(() => {
      const node = [...document.querySelectorAll('.ss-link')].find((node) => node.textContent === ${JSON.stringify(name)})
      if (!node || node.disabled) throw new Error('Stream unavailable: ' + ${JSON.stringify(name)})
      node.click()
    })()`)
  }
  async function reset(install) {
    if (injection) await command('Page.removeScriptToEvaluateOnNewDocument', { identifier: injection })
    const runId = `${Date.now()}-${Math.random()}`
    injection = (await command('Page.addScriptToEvaluateOnNewDocument', { source: `(${install.toString()})(); window.transferCheck.runId = ${JSON.stringify(runId)}` })).identifier
    await command('Page.navigate', { url: target })
    await until(`window.transferCheck?.runId === ${JSON.stringify(runId)} && document.querySelector(".ss-tabs")`)
  }
  async function close() {
    for (const item of pending.values()) clearTimeout(item.timeout)
    socket?.close()
    process.kill('SIGTERM')
    await Promise.race([new Promise((resolve) => process.once('exit', resolve)), delay(2_000)])
    if (process.exitCode === null) process.kill('SIGKILL')
    await rm(profile, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 })
  }
  try {
    let port
    for (let attempt = 0; attempt < 150; attempt++) {
      if (launchError) throw launchError
      if (process.exitCode !== null) throw new Error(`Browser exited with code ${process.exitCode}`)
      try { port = Number((await readFile(join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0]); break } catch { await delay(100) }
    }
    if (!port) throw new Error('Chrome did not expose a debug port; set CHROME_BIN.')
    const pages = await (await fetch(`http://127.0.0.1:${port}/json`)).json()
    socket = new WebSocket(pages.find((page) => page.type === 'page').webSocketDebuggerUrl)
    await new Promise((resolve, reject) => { socket.addEventListener('open', resolve, { once: true }); socket.addEventListener('error', reject, { once: true }) })
    socket.addEventListener('message', (event) => {
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
    return { command, evaluate, until, button, input, openDetails, selectStream, reset, close, exceptions }
  } catch (error) { await close(); throw error }
}

const browser = await openBrowser(process.argv[2] || 'http://localhost:5173')
const { evaluate, until, button, input, openDetails, selectStream, reset } = browser

function installApi() {
  sessionStorage.setItem('pico-stream-connection', JSON.stringify({ endpoint: '/pico', token: 'stream-token' }))
  sessionStorage.setItem('pico-admin-token', 'admin-token')
  const originalFetch = window.fetch.bind(window)
  const json = (value, status = 200, headers = {}) => new Response(JSON.stringify(value), {
    status, headers: { 'Content-Type': 'application/json', ...headers },
  })
  const node = (nodeId, slots) => ({ nodeId, nodeEpoch: 1, slots, advertisedAddress: `http://node-${nodeId}.example:4437`, local: nodeId === 1, openingCount: 0, placedCount: 1 })
  const state = window.transferCheck = {
    nodes: [node(1, 10), node(2, 10), node(3, 0)], transfers: [], slotUpdates: [],
    finishTransfer: null, finishSlots: null, ownershipStatus: 200, ownershipReads: 0,
    ownership: { name: '/review/one', streamId: 11, nodeId: 1, state: 'opened', ownerNodeId: 1, ownerAdvertisedAddress: 'http://node-1.example:4437', epoch: 0, pendingTransfer: null },
  }
  window.fetch = async (resource, options = {}) => {
    const url = new URL(typeof resource === 'string' ? resource : resource.url, location.href)
    const method = options.method || 'GET'
    const auth = new Headers(options.headers).get('Authorization')
    if (url.pathname === '/admin/transfer') {
      const body = JSON.parse(options.body)
      state.transfers.push({ ...body, auth, method })
      return new Promise((resolve) => {
        state.finishTransfer = () => {
          state.finishTransfer = null
          resolve(json({ ...body, streamId: 11, pending: true }, 202))
        }
      })
    }
    if (/^\/admin\/nodes\/-?\d+$/.test(url.pathname)) {
      const id = Number(url.pathname.split('/').at(-1))
      const body = JSON.parse(options.body)
      state.slotUpdates.push({ nodeId: id, ...body, auth, method })
      return new Promise((resolve) => {
        state.finishSlots = () => {
          state.finishSlots = null
          state.nodes.find((item) => item.nodeId === id).slots = body.slots
          resolve(json(state.nodes.find((item) => item.nodeId === id)))
        }
      })
    }
    if (url.pathname.startsWith('/admin/streams/')) {
      state.ownershipReads++
      return json(state.ownershipStatus === 200 ? state.ownership : { error: 'Read temporarily unavailable' }, state.ownershipStatus)
    }
    if (url.pathname === '/admin/nodes') return json({ nodes: state.nodes })
    if (url.pathname === '/admin/tokens') return json({ count: 0, tokens: [] })
    if (url.pathname === '/admin/cluster') return json({ clusterId: 'transfer-review', nodeId: 1, nodeEpoch: 1, streamCount: 2, objectCount: 0, pendingTransfers: [], gc: {}, registered: true })
    if (url.pathname === '/ready') return json({ ready: true })
    if (url.pathname === '/pico/') return json({ streams: ['/review/one', '/review/two'].map((name) => ({ name, content_type: 'application/json', closed: false })), has_more: false })
    if (url.pathname.startsWith('/pico/_streams/')) return json({ schema: null, schemaValidate: false })
    if (url.pathname.startsWith('/pico/review/')) {
      const headers = { 'Pico-Start-Seq': '0', 'Pico-Next-Seq': '0', 'Pico-Closed': 'false', 'Content-Type': 'application/json' }
      if (method === 'HEAD') return new Response(null, { headers })
      return json([], 200, headers)
    }
    return originalFetch(resource, options)
  }
}

async function prepareTransfer() {
  await button('Discovery')
  await until(`document.querySelector('.ss-link')?.textContent === '/review/one'`)
  await selectStream('/review/one')
  await openDetails('Admin connection')
  await button('Use dashboard admin')
  await until(`[...document.querySelectorAll('button')].some((node) => node.textContent === 'Transfer stream' && !node.disabled)`)
  await button('Transfer stream')
  await until(`[...document.querySelectorAll('label')].find((node) => node.firstChild.textContent === 'Destination node')?.querySelector('select')?.options.length === 2`)
  await input('Destination node', '2')
  await button('Review transfer')
}

try {
  await reset(installApi)
  await until(`document.querySelectorAll('.ss-tabs button').length === 5 && [...document.querySelectorAll('button')].some((node) => node.textContent === 'Edit slots')`)
  await button('Edit slots')
  await input('Slots for node 1', '0')
  await button('Review slots')
  assert.equal(await evaluate('transferCheck.slotUpdates.length'), 0)
  assert.match(await evaluate('document.body.innerText'), /from 10 to 0 slots/)
  await evaluate(`(() => { const button = [...document.querySelectorAll('button')].find((node) => node.textContent === 'Confirm slots'); button.click(); button.click() })()`)
  await until('typeof transferCheck.finishSlots === "function"')
  assert.deepEqual(await evaluate('transferCheck.slotUpdates'), [{ nodeId: 1, slots: 0, auth: 'Bearer admin-token', method: 'POST' }])
  await button('Tokens')
  await evaluate('transferCheck.finishSlots()')
  await button('Overview')
  await until('document.body.innerText.includes("Node 1: slots updated to 0.")')
  assert.equal(await evaluate('transferCheck.slotUpdates.length'), 1)
  console.log('PASS node slots require confirmation, preserve zero, prevent duplicate writes, and survive tab changes')

  await prepareTransfer()
  assert.equal(await evaluate('transferCheck.transfers.length'), 0)
  for (const width of [320, 375, 1366]) {
    await browser.command('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: false })
    assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth'), true, `Transfer confirmation overflows ${width}px`)
  }
  assert.equal(await evaluate(`[...document.querySelectorAll('label')].find((node) => node.firstChild.textContent === 'Destination node').querySelector('select').querySelector('option[value="3"]') === null`), true)
  await evaluate(`(() => { const button = [...document.querySelectorAll('button')].find((node) => node.textContent === 'Confirm transfer'); button.click(); button.click() })()`)
  await until('typeof transferCheck.finishTransfer === "function"')
  assert.deepEqual(await evaluate('transferCheck.transfers'), [{ stream: '/review/one', toNode: 2, auth: 'Bearer admin-token', method: 'POST' }])
  assert.equal(await evaluate(`[...document.querySelectorAll('.ss-link')].every((node) => node.disabled)`), true)
  assert.equal(await evaluate(`[...document.querySelectorAll('button')].find((node) => node.textContent === 'Use dashboard admin').disabled`), true)
  await evaluate(`document.querySelectorAll('.ss-link')[1].click()`)
  assert.equal(await evaluate('document.querySelector(".ss-stream-name").textContent'), '/review/one')

  await evaluate('transferCheck.ownershipStatus = 503')
  await button('Refresh details')
  await until('document.body.innerText.includes("Ownership unavailable")')
  await button('Publish')
  await openDetails('Stream connection')
  await input('Stream API endpoint', '/other-pico')
  assert.equal(await evaluate(`[...document.querySelectorAll('button')].find((node) => node.textContent === 'Apply connection').disabled`), true)
  await evaluate(`document.querySelector('details.ss-connection form').dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }))`)
  assert.equal(await evaluate('JSON.parse(sessionStorage.getItem("pico-stream-connection")).endpoint'), '/pico')
  await button('Discovery')
  await evaluate(`transferCheck.ownershipStatus = 200; transferCheck.ownership.ownerNodeId = 2; transferCheck.ownership.pendingTransfer = { fromNode: 1, toNode: 2 }; transferCheck.finishTransfer()`)
  await until('document.body.innerText.includes("Transfer pending: node 1 → 2.")')
  assert.equal(await evaluate('document.body.innerText.includes("Transfer complete")'), false)
  assert.equal(await evaluate('transferCheck.transfers.length'), 1)
  console.log('PASS transfer confirms once and retains the acknowledgement through read errors, tab changes, and locked connection/selection')

  await evaluate('transferCheck.ownership.pendingTransfer = null')
  await until('document.body.innerText.includes("Waiting for ownership handoff")')
  assert.equal(await evaluate('document.body.innerText.includes("Transfer complete")'), false)
  await evaluate('transferCheck.ownershipStatus = 503')
  await until('document.body.innerText.includes("Transfer status unavailable")')
  await button('Refresh details')
  await until('document.body.innerText.includes("Ownership unavailable")')
  assert.match(await evaluate('document.body.innerText'), /\/review\/one → node 2/)
  await evaluate(`transferCheck.ownershipStatus = 200; transferCheck.ownership.nodeId = 2; transferCheck.ownership.state = 'closed'; transferCheck.ownership.ownerNodeId = 1`)
  await until('document.body.innerText.includes("Transfer complete: ownership handed to node 2. Routing owner currently reports node 1.")')
  assert.equal(await evaluate('transferCheck.transfers.length'), 1)
  console.log('PASS transfer completion uses persisted identity and handoff state, not an early or stale routing owner')

  await reset(installApi)
  await until(`document.querySelectorAll('.ss-tabs button').length === 5`)
  await prepareTransfer()
  await button('Confirm transfer')
  await until('typeof transferCheck.finishTransfer === "function"')
  await evaluate(`transferCheck.ownership.streamId = 12; transferCheck.ownership.nodeId = 2; transferCheck.finishTransfer()`)
  await until('document.body.innerText.includes("This stream name now refers to a different stream.")')
  assert.equal(await evaluate('document.body.innerText.includes("Transfer complete")'), false)
  console.log('PASS recreating a stream name cannot be mistaken for the original transfer completing')

  for (const width of [320, 375, 1366]) {
    await browser.command('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: false })
    for (const tab of ['Overview', 'Discovery']) {
      await button(tab)
      const size = await evaluate('({ width: innerWidth, scroll: document.documentElement.scrollWidth })')
      assert.ok(size.scroll <= size.width, `${tab} overflows at ${width}px: ${JSON.stringify(size)}`)
    }
  }
  assert.deepEqual(browser.exceptions, [])
  console.log('PASS node/transfer controls fit mobile and desktop; no runtime exceptions')
} finally {
  await browser.close()
}
