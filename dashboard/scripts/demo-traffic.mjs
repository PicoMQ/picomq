#!/usr/bin/env node
import { parseArgs } from 'node:util'
import { randomUUID } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'

const { values } = parseArgs({
  options: {
    endpoint: { type: 'string', default: 'http://127.0.0.1:4437' },
    prefix: { type: 'string', default: '/' },
    interval: { type: 'string', default: '2' },
    rounds: { type: 'string', default: '0' },
    help: { type: 'boolean', short: 'h' },
  },
})

if (values.help) {
  console.log(`Send one synthetic JSON message to every open stream each round.

Usage: node scripts/demo-traffic.mjs [options]
  --endpoint URL   Pico protocol listener (default: http://127.0.0.1:4437)
  --prefix PATH    Discover names under this prefix (default: /)
  --interval SEC   Pause between rounds (default: 2)
  --rounds N       Stop after N rounds; 0 runs until Ctrl+C (default: 0)

Uses PICO_TOKEN when authentication is enabled. Discovers new streams every
round, skips closed streams, and never creates streams or retries an append.`)
  process.exit(0)
}

const interval = Number(values.interval)
const rounds = Number(values.rounds)
if (!Number.isFinite(interval) || interval <= 0 || !Number.isSafeInteger(rounds) || rounds < 0) {
  console.error('Interval must be a positive number; rounds must be a nonnegative integer.')
  process.exit(1)
}
const endpoint = new URL(values.endpoint)
if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
  console.error('Endpoint must be an HTTP(S) URL without credentials, query, or fragment.')
  process.exit(1)
}
const base = endpoint.href.replace(/\/$/, '')
const stop = new AbortController()
process.on('SIGINT', () => stop.abort())
process.on('SIGTERM', () => stop.abort())
const runId = randomUUID()
let sent = 0

async function request(path, init = {}) {
  const headers = new Headers(init.headers)
  if (process.env.PICO_TOKEN) headers.set('Authorization', `Bearer ${process.env.PICO_TOKEN}`)
  const response = await fetch(base + path, {
    ...init, headers, redirect: 'error',
    signal: AbortSignal.any([stop.signal, AbortSignal.timeout(10_000)]),
  })
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`)
  return response
}

async function discover() {
  const streams = []
  let after = ''
  for (;;) {
    const query = new URLSearchParams({ prefix: values.prefix, limit: '100' })
    if (after) query.set('start_after', after)
    const page = await (await request(`/?${query}`)).json()
    if (!Array.isArray(page.streams)) throw new Error('Expected a Pico stream listing.')
    streams.push(...page.streams)
    if (!page.has_more) return streams
    const next = page.streams.at(-1)?.name
    if (!next || next === after) throw new Error('Stream listing did not advance.')
    after = next
  }
}

function payload(name, round) {
  const common = { demo: true, source: 'dashboard-demo-traffic', runId, round, stream: name, timestamp: new Date().toISOString() }
  if (name.endsWith('/orders')) return {
    ...common, event: ['order.created', 'order.paid', 'order.shipped'][(round - 1) % 3],
    order: { id: `demo-${round}`, customer: `customer-${1 + round % 5}`, total: Number((19.5 + round % 80 * 1.25).toFixed(2)), currency: 'USD' },
  }
  if (name.endsWith('/logs')) return {
    ...common, event: 'log.entry', level: round % 7 === 0 ? 'warn' : 'info', service: ['api', 'worker', 'scheduler'][round % 3],
    message: ['Request completed', 'Background job processed', 'Heartbeat received'][round % 3], durationMs: 10 + round % 120,
  }
  return { ...common, event: ['page.viewed', 'button.clicked', 'session.started'][round % 3], userId: `demo-user-${1 + round % 10}`, properties: { page: ['/home', '/orders', '/settings'][round % 3], synthetic: true } }
}

console.log(`Demo traffic started: ${endpoint.origin}${endpoint.pathname} · prefix ${values.prefix} · every ${interval}s · run ${runId}`)
for (let round = 1; !stop.signal.aborted && (!rounds || round <= rounds); round++) {
  try {
    const streams = await discover()
    const open = streams.filter((stream) => !stream.closed)
    if (!open.length) console.log('No open streams match; waiting for the next round.')
    for (const [index, stream] of open.entries()) {
      if (stop.signal.aborted) break
      try {
        // Names are native URI paths. Reject paths that would alter the URL target.
        if (typeof stream.name !== 'string' || !stream.name.startsWith('/') || stream.name.startsWith('//') || /[?#\\\s]/.test(stream.name) || new URL(stream.name, endpoint).pathname !== stream.name) {
          throw new Error('Stream name is not a supported native URI path.')
        }
        const message = payload(stream.name, round)
        // Rotate each stream through both, key only, headers only, and neither.
        const variant = (round - 1 + index) % 4
        const record = { body: JSON.stringify(message) }
        if (variant === 0 || variant === 1) record.key = `demo-${round}`
        if (variant === 0 || variant === 2) record.headers = {
          source: 'dashboard-demo-traffic', 'event-type': message.event,
          'content-type': 'application/json', 'run-id': runId,
        }
        const response = await request(stream.name, {
          method: 'POST', headers: { 'Content-Type': 'application/vnd.picomq.batch+json' },
          body: JSON.stringify({ records: [record] }),
        })
        await response.arrayBuffer()
        sent++
        console.log(`${new Date().toISOString()} ${stream.name} · sequence ${response.headers.get('Pico-Start-Seq') ?? '?'} · round ${round}`)
      } catch (error) {
        if (!stop.signal.aborted) console.error(`${stream.name}: ${error.message} (append not retried)`)
      }
    }
  } catch (error) {
    if (!stop.signal.aborted) console.error(`Discovery failed: ${error.message}`)
  }
  if (!stop.signal.aborted && (!rounds || round < rounds)) {
    try { await delay(interval * 1000, undefined, { signal: stop.signal }) } catch { /* Stopping. */ }
  }
}
console.log(`Demo traffic stopped. ${sent} messages acknowledged.`)
