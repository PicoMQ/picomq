import assert from 'node:assert/strict'
import { afterEach, test } from 'node:test'
import { readFile } from 'node:fs/promises'
import ts from 'typescript'

// Keep the production request and cursor code; only the browser Worker is replaced.
function moduleURL(source) {
  const { outputText } = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } })
  return `data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}`
}
const streamsURL = moduleURL((await readFile(new URL('../src/streams.ts', import.meta.url), 'utf8')).replace('import.meta.env.DEV', 'true'))
const matchURL = moduleURL('export async function matchNames(pattern, names) { const regex = new RegExp(pattern); return names.filter(name => regex.test(name)); }')
const source = (await readFile(new URL('../src/watch.ts', import.meta.url), 'utf8'))
  .replace("'./streams'", JSON.stringify(streamsURL)).replace("'./match'", JSON.stringify(matchURL))
const { watchStreams } = await import(moduleURL(source))
const originalFetch = globalThis.fetch
afterEach(() => { globalThis.fetch = originalFetch })

const json = (value, headers = {}) => new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json', ...headers } })
const head = (next = '1', start = '0') => new Response(null, { headers: { 'Pico-Start-Seq': start, 'Pico-Next-Seq': next } })
const page = (records = [], next = '1') => json(records, { 'Pico-Next-Seq': next })
const listing = (names, more = false) => json({ streams: names.map(name => ({ name, closed: false })), has_more: more })
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done }); return { promise, resolve } }
async function flush() { for (let i = 0; i < 5; i++) await new Promise(setImmediate) }

function harness(t, fetcher, options = {}) {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const output = { records: [], statuses: [], warnings: [], errors: [], progress: 0, requests: [] }
  globalThis.fetch = async (url, init) => {
    const parsed = new URL(url, 'http://review.invalid')
    const request = { name: parsed.pathname.replace(/^\/pico/, ''), method: init.method || 'GET', query: parsed.searchParams, signal: init.signal }
    output.requests.push(request)
    return fetcher(request)
  }
  const stop = watchStreams({ endpoint: '/pico', token: '' }, {
    mode: 'exact', names: ['/fast'], query: '^/', prefix: '/', start: 'beginning', ...options,
  }, {
    records: (stream, records) => output.records.push({ stream, records }),
    statuses: statuses => output.statuses.push(statuses),
    discovery: warning => output.warnings.push(warning),
    progress: () => { output.progress++ },
    error: error => output.errors.push(error),
  })
  t.after(stop)
  return { output, stop, advance: async ms => { t.mock.timers.tick(ms); await flush() } }
}

test('healthy records render and keep polling while a peer HEAD remains pending', async t => {
  const stalled = deferred()
  const { output, advance } = harness(t, request => {
    if (request.name === '/slow') return stalled.promise
    if (request.method === 'HEAD') return head('2')
    const seq = Number(request.query.get('seq'))
    return page([{ seq, body: `message ${seq}` }], String(seq + 1))
  }, { names: ['/fast', '/slow'] })
  await flush()
  assert.deepEqual(output.records.map(item => [item.stream, item.records[0].seq]), [['/fast', 0]])
  assert.equal(output.statuses.at(-1).find(item => item.name === '/slow').state, 'Connecting…')
  await advance(1000)
  assert.deepEqual(output.records.map(item => item.records[0].seq), [0, 1])
  assert.equal(output.requests.filter(item => item.name === '/slow').length, 1)
})

test('a pending GET cannot overlap another poll and Stop suppresses its late result', async t => {
  const pending = deferred()
  const { output, advance, stop } = harness(t, request => request.method === 'HEAD' ? head() : pending.promise)
  await flush()
  await advance(10_000)
  assert.equal(output.requests.length, 2)
  const count = output.statuses.length
  stop(); stop()
  assert.ok(output.requests.every(request => request.signal.aborted))
  pending.resolve(page([{ seq: 0, body: 'late' }]))
  await flush(); await advance(10_000)
  assert.equal(output.records.length, 0)
  assert.equal(output.statuses.length, count)
  assert.equal(output.requests.length, 2)
})

test('Now captures its first successful HEAD cursor and retries a failed GET from that cursor', async t => {
  let heads = 0; let reads = 0
  const { output, advance } = harness(t, request => {
    if (request.method === 'HEAD') {
      if (++heads === 1) throw new TypeError('temporary HEAD failure')
      return head(heads === 2 ? '7' : '9')
    }
    if (++reads === 1) return new Response('temporary read failure', { status: 503 })
    return page([{ seq: 7, body: 'retained cursor' }], '8')
  }, { start: 'now' })
  await flush(); await advance(1000); await advance(1000)
  assert.deepEqual(output.requests.filter(request => request.method === 'GET').map(request => request.query.get('seq')), ['7', '7'])
  assert.equal(output.records[0].records[0].seq, 7)
})

test('partial discovery failure preserves known streams, cursors, and polling until recovery', async t => {
  let phase = 'initial'; let fastNext = 3; let listCalls = 0
  const { output, advance } = harness(t, request => {
    if (request.name === '/') {
      listCalls++
      if (phase === 'initial') return listing(['/fast'])
      if (phase === 'failed') return request.query.has('start_after')
        ? new Response('listing outage', { status: 503 }) : listing(['/new'], true)
      return listing(['/fast', '/new'])
    }
    if (request.method === 'HEAD') return request.name === '/fast' ? head(String(fastNext)) : head('5', '3')
    const seq = Number(request.query.get('seq'))
    const next = request.name === '/fast' ? fastNext : 5
    return page(seq < next ? [{ seq, body: request.name }] : [], String(seq < next ? seq + 1 : next))
  }, { mode: 'regex', start: 'now' })
  await flush()
  assert.equal(output.requests.find(request => request.name === '/fast' && request.method === 'GET').query.get('seq'), '3')
  phase = 'failed'; fastNext = 4
  await advance(5000)
  assert.equal(listCalls, 3)
  assert.match(output.warnings.at(-1), /503.*Retrying in 5 seconds/)
  assert.equal(output.errors.length, 0)
  assert.equal(output.requests.some(request => request.name === '/new'), false)
  assert.ok(output.records.some(item => item.stream === '/fast' && item.records[0].seq === 3))
  const reads = output.requests.filter(request => request.name === '/fast' && request.method === 'GET').length
  await advance(1000)
  assert.ok(output.requests.filter(request => request.name === '/fast' && request.method === 'GET').length > reads)
  phase = 'recovered'; await advance(4000)
  assert.equal(output.warnings.at(-1), '')
  assert.equal(output.requests.find(request => request.name === '/new' && request.method === 'GET').query.get('seq'), '3')
  assert.ok(output.requests.filter(request => request.name === '/fast' && request.method === 'GET').slice(1).every(request => Number(request.query.get('seq')) >= 3))
})

test('discovery backoff retries initial outages, honors initial Now, and resets after success', async t => {
  let attempts = 0; const retryStatuses = [0, 408, 429, 503]
  const { output, advance } = harness(t, request => {
    if (request.name !== '/') return request.method === 'HEAD' ? head('11', '4') : page([], '11')
    attempts++
    if (attempts === 5) return listing(['/fast'])
    const status = retryStatuses[Math.min(attempts - 1, 3)]
    if (status === 0) throw new TypeError('network down')
    return new Response('discovery unavailable', { status })
  }, { mode: 'regex', start: 'now' })
  await flush()
  assert.match(output.warnings.at(-1), /Retrying in 5 seconds/)
  await advance(4999); assert.equal(attempts, 1)
  await advance(1); assert.equal(attempts, 2); assert.match(output.warnings.at(-1), /Retrying in 10 seconds/)
  await advance(10_000); assert.equal(attempts, 3); assert.match(output.warnings.at(-1), /Retrying in 20 seconds/)
  await advance(20_000); assert.equal(attempts, 4); assert.match(output.warnings.at(-1), /Retrying in 30 seconds/)
  await advance(30_000); assert.equal(attempts, 5)
  assert.equal(output.requests.find(request => request.name === '/fast' && request.method === 'GET').query.get('seq'), '11')
  assert.equal(output.warnings.at(-1), '')
  await advance(5000); assert.equal(attempts, 6)
  assert.match(output.warnings.at(-1), /Retrying in 5 seconds/)
  assert.equal(output.errors.length, 0)
})

test('known streams keep polling while a later discovery request is still pending', async t => {
  const pending = deferred(); let lists = 0
  const { output, advance, stop } = harness(t, request => {
    if (request.name === '/') return ++lists === 1 ? listing(['/fast']) : pending.promise
    return request.method === 'HEAD' ? head() : page()
  }, { mode: 'regex' })
  await flush(); await advance(5000)
  const reads = output.requests.filter(request => request.name === '/fast' && request.method === 'GET').length
  await advance(1000)
  assert.ok(output.requests.filter(request => request.name === '/fast' && request.method === 'GET').length > reads)
  stop(); pending.resolve(listing(['/new'])); await flush()
  assert.equal(output.requests.some(request => request.name === '/new'), false)
})

test('a discovery connection loss after response headers remains retryable', async t => {
  let lists = 0
  const { output, advance } = harness(t, request => {
    if (request.name !== '/') return request.method === 'HEAD' ? head() : page()
    if (++lists !== 2) return listing(['/fast'])
    const body = new ReadableStream({ start(controller) { controller.error(new TypeError('Connection terminated while reading response body')) } })
    return new Response(body, { headers: { 'Content-Type': 'application/json' } })
  }, { mode: 'regex' })
  await flush(); await advance(5000)
  assert.equal(output.errors.length, 0)
  assert.match(output.warnings.at(-1), /Retrying in 5 seconds/)
  await advance(5000)
  assert.equal(lists, 3)
  assert.equal(output.warnings.at(-1), '')
})

test('removed subscriptions abort and late responses cannot overwrite a replacement with the same name', async t => {
  const stale = deferred(); let phase = 0; let oldRead
  const { output, advance } = harness(t, request => {
    if (request.name === '/') return listing(phase === 1 ? [] : ['/fast'])
    if (request.method === 'HEAD') return head(phase === 0 ? '1' : '10', phase === 0 ? '0' : '9')
    if (phase === 0) { oldRead = request; return stale.promise }
    return page([{ seq: 9, body: 'replacement' }], '10')
  }, { mode: 'regex' })
  await flush(); phase = 1; await advance(5000)
  assert.equal(oldRead.signal.aborted, true)
  assert.deepEqual(output.statuses.at(-1), [])
  phase = 2; await advance(5000)
  assert.equal(output.records[0].records[0].seq, 9)
  stale.resolve(page([{ seq: 0, body: 'stale subscription' }], '1')); await flush()
  assert.deepEqual(output.records.map(item => item.records[0].seq), [9])
  assert.equal(output.statuses.at(-1)[0].position, '10')
})

for (const status of [401, 403]) test(`discovery ${status} is fatal and aborts existing subscriptions`, async t => {
  const pending = deferred(); let lists = 0
  const { output, advance } = harness(t, request => {
    if (request.name === '/') return ++lists === 1 ? listing(['/fast']) : new Response('denied', { status })
    return request.method === 'HEAD' ? head() : pending.promise
  }, { mode: 'regex' })
  await flush(); await advance(5000)
  assert.equal(output.errors.length, 1)
  assert.equal(output.errors[0].status, status)
  assert.ok(output.requests.every(request => request.signal.aborted))
  await advance(30_000); assert.equal(lists, 2)
})

test('an interrupted error body cannot erase a known authorization failure status', async t => {
  const { output, advance } = harness(t, () => {
    const body = new ReadableStream({ start(controller) { controller.error(new TypeError('Error response body interrupted')) } })
    return new Response(body, { status: 403 })
  }, { mode: 'regex' })
  await flush(); await advance(30_000)
  assert.equal(output.errors.length, 1)
  assert.equal(output.errors[0].status, 403)
  assert.equal(output.requests.length, 1)
})

test('invalid regex stops explicitly without starting stream reads', async t => {
  const { output, advance } = harness(t, () => listing(['/fast']), { mode: 'regex', query: '[' })
  await flush(); await advance(30_000)
  assert.equal(output.errors.length, 1)
  assert.ok(output.errors[0] instanceof SyntaxError)
  assert.equal(output.requests.length, 1)
})

test('malformed listing JSON stays a protocol error rather than an endless network retry', async t => {
  const { output, advance } = harness(t, () => new Response('{invalid', { headers: { 'Content-Type': 'application/json' } }), { mode: 'regex' })
  await flush(); await advance(30_000)
  assert.equal(output.errors.length, 1)
  assert.ok(output.errors[0] instanceof SyntaxError)
  assert.equal(output.requests.length, 1)
})

test('a later over-limit match set stops all watches without silently choosing eight', async t => {
  let lists = 0
  const { output, advance } = harness(t, request => {
    if (request.name === '/') return listing(++lists === 1 ? ['/fast'] : Array.from({ length: 9 }, (_, i) => `/stream${i}`))
    return request.method === 'HEAD' ? head() : page()
  }, { mode: 'regex' })
  await flush(); await advance(5000)
  assert.match(output.errors[0].message, /at most 8/)
  assert.equal(output.requests.some(request => request.name.startsWith('/stream')), false)
  assert.ok(output.requests.every(request => request.signal.aborted))
  const count = output.requests.length
  await advance(30_000); assert.equal(output.requests.length, count)
})

test('an idle long poll resumes from its 204 cursor and delivers later messages', async (t) => {
  let reads = 0
  const { output, advance } = harness(t, (request) => {
    if (request.method === 'HEAD') return head(reads ? '7' : '5')
    assert.equal(request.query.get('live'), 'long-poll')
    if (++reads === 1) return new Response(null, { status: 204, headers: { 'Pico-Next-Seq': '5' } })
    assert.equal(request.query.get('seq'), '5')
    return page([{ seq: 5, body: 'first after idle' }, { seq: 6, body: 'second after idle' }], '7')
  }, { start: 'now' })
  await flush()
  assert.equal(output.records.length, 0)
  assert.equal(output.statuses.at(-1)[0].position, '5')
  await advance(1000)
  assert.deepEqual(output.records.flatMap((item) => item.records.map((record) => record.seq)), [5, 6])
  assert.equal(output.statuses.at(-1)[0].position, '7')
})

test('a closed stream drains its backlog before the caught-up read stops polling', async (t) => {
  const { output, advance } = harness(t, (request) => {
    if (request.method === 'HEAD') return new Response(null, { headers: {
      'Pico-Start-Seq': '0', 'Pico-Next-Seq': '3', 'Pico-Closed': 'true',
    } })
    assert.equal(request.query.get('live'), 'long-poll')
    const seq = Number(request.query.get('seq'))
    // Native GET marks closed only at its tail, unlike HEAD which can already
    // be closed while the reader still has multiple pages left to consume.
    return json([{ seq, body: `backlog ${seq}` }], {
      'Pico-Next-Seq': String(seq + 1), ...(seq === 2 ? { 'Pico-Closed': 'true' } : {}),
    })
  })
  await flush(); await advance(1000); await advance(1000)
  assert.deepEqual(output.records.map((item) => item.records[0].seq), [0, 1, 2])
  assert.deepEqual(output.statuses.at(-1)[0], { name: '/fast', position: '3', state: 'Closed · caught up' })
  const requests = output.requests.length
  await advance(10_000)
  assert.equal(output.requests.length, requests)
})
