import type { ServerResponse } from 'node:http'
import { openai } from '@ai-sdk/openai'
import { type ModelMessage, generateText, jsonSchema, stepCountIs, tool } from 'ai'
import { Producer, type PicoClient } from '@picomq/client'
import {
  CT,
  ENDPOINT,
  broadcastTo,
  dec,
  enc,
  ensureStream,
  json,
  openSse,
  preview,
  requireKey,
  shortId,
  sseFrame,
  type RecentItem,
  type SseClient,
  type StreamRecordInfo,
} from './lib'

export const AGENT_PREFIX = process.env.PICO_AGENT_PREFIX ?? '/examples/ai-sdk/agent'

type AgentEvent =
  | { type: 'run_start'; prompt: string; timestamp: string }
  | {
      type: 'step'
      index: number
      text: string
      toolCalls: { tool: string; args: unknown }[]
      toolResults: { tool: string; result: unknown }[]
      finishReason: string
    }
  | { type: 'run_end'; text: string; steps: number; totalTokens: number; timestamp: string }

type RunState = {
  stream: string
  producerId: string
  clients: Set<SseClient>
  buffer: Array<{ event: string; data: unknown }>
  records: StreamRecordInfo[]
  done: boolean
}

const runs = new Map<string, RunState>()
let pico!: PicoClient

type Token =
  | { type: 'number'; value: number }
  | { type: 'op'; value: '+' | '-' | '*' | '/' }
  | { type: 'paren'; value: '(' | ')' }

function tokenize(input: string): Token[] {
  const tokens: Token[] = []
  const isDigit = (ch: string) => ch >= '0' && ch <= '9'
  let i = 0
  while (i < input.length) {
    const ch = input[i]!
    if (' \t\n\r'.includes(ch)) {
      i++
    } else if ('+-*/'.includes(ch)) {
      tokens.push({ type: 'op', value: ch as '+' | '-' | '*' | '/' })
      i++
    } else if (ch === '(' || ch === ')') {
      tokens.push({ type: 'paren', value: ch })
      i++
    } else if (isDigit(ch) || ch === '.') {
      const start = i
      let dots = 0
      while (i < input.length && (isDigit(input[i]!) || input[i] === '.')) {
        if (input[i] === '.') dots++
        if (dots > 1) throw new Error('Invalid number format')
        i++
      }
      const value = Number(input.slice(start, i))
      if (!Number.isFinite(value)) throw new Error(`Invalid number: "${input.slice(start, i)}"`)
      tokens.push({ type: 'number', value })
    } else {
      throw new Error(`Unexpected character: "${ch}"`)
    }
  }
  return tokens
}

function evaluateExpression(input: string): number {
  const tokens = tokenize(input)
  let index = 0
  const peek = () => tokens[index]
  const take = () => tokens[index++]

  const parseExpression = (): number => {
    let value = parseTerm()
    for (;;) {
      const t = peek()
      if (!t || t.type !== 'op' || (t.value !== '+' && t.value !== '-')) break
      take()
      const rhs = parseTerm()
      value = t.value === '+' ? value + rhs : value - rhs
    }
    return value
  }
  const parseTerm = (): number => {
    let value = parseFactor()
    for (;;) {
      const t = peek()
      if (!t || t.type !== 'op' || (t.value !== '*' && t.value !== '/')) break
      take()
      const rhs = parseFactor()
      value = t.value === '*' ? value * rhs : value / rhs
    }
    return value
  }
  const parseFactor = (): number => {
    const t = peek()
    if (!t) throw new Error('Unexpected end of expression')
    if (t.type === 'op' && (t.value === '+' || t.value === '-')) {
      take()
      const value = parseFactor()
      return t.value === '-' ? -value : value
    }
    if (t.type === 'paren' && t.value === '(') {
      take()
      const value = parseExpression()
      const close = take()
      if (!close || close.type !== 'paren' || close.value !== ')') throw new Error("Expected ')'")
      return value
    }
    if (t.type === 'number') {
      take()
      return t.value
    }
    throw new Error('Unexpected token')
  }

  const result = parseExpression()
  if (index < tokens.length) throw new Error('Unexpected extra input')
  return result
}

const agentTools = {
  lookupCompany: tool({
    description: 'Look up information about a company by name.',
    inputSchema: jsonSchema<{ name: string }>({
      type: 'object',
      properties: { name: { type: 'string', description: 'The company name to look up' } },
      required: ['name'],
    }),
    execute: async ({ name }) => {
      const db: Record<string, object> = {
        PicoMQ: {
          founded: 2025,
          sector: 'Infrastructure',
          description: 'Streaming messaging for agents and apps',
        },
        Vercel: {
          founded: 2015,
          sector: 'Developer Tools',
          hq: 'San Francisco',
          description: 'Frontend cloud and Next.js creators',
        },
        Stripe: {
          founded: 2010,
          sector: 'Fintech',
          hq: 'San Francisco',
          description: 'Online payments infrastructure',
        },
      }
      return db[name] ?? { error: `No data found for "${name}"` }
    },
  }),
  calculate: tool({
    description: 'Evaluate a mathematical expression and return the result.',
    inputSchema: jsonSchema<{ expression: string }>({
      type: 'object',
      properties: {
        expression: {
          type: 'string',
          description: "A mathematical expression, e.g. '2025 - 2010'",
        },
      },
      required: ['expression'],
    }),
    execute: async ({ expression }) => {
      try {
        return { expression, result: evaluateExpression(expression) }
      } catch (err) {
        return { expression, error: err instanceof Error ? err.message : 'Invalid expression' }
      }
    },
  }),
}

function isAgentStream(name: string) {
  return name.startsWith(AGENT_PREFIX + '/')
}

function runMeta(run: RunState) {
  const last = run.records[run.records.length - 1]
  return {
    endpoint: ENDPOINT,
    stream: run.stream,
    contentType: CT,
    protocol: 'pico',
    producerId: run.producerId,
    records: run.records.length,
    lastSeq: last?.seq ?? null,
    done: run.done,
  }
}

function eventPreview(event: AgentEvent): string {
  if (event.type === 'step') return event.toolCalls.map((tc) => tc.tool).join(', ') || 'text'
  if (event.type === 'run_start') return preview(event.prompt)
  return `${event.steps} steps`
}

async function loadRunStream(streamName: string) {
  const head = await pico.head(streamName)
  if (!head) return null
  const events: AgentEvent[] = []
  const records: StreamRecordInfo[] = []
  let from = pico.beginning()
  for (;;) {
    const page = await pico.read(streamName, from, 'off', { count: 500 })
    for (const record of page.records) {
      try {
        const event = JSON.parse(dec.decode(record.body)) as AgentEvent
        if (!event?.type) continue
        events.push(event)
        records.push({ seq: record.position, type: event.type, preview: eventPreview(event) })
      } catch {
      }
    }
    if (page.records.length === 0 || page.upToDate || page.next === from) break
    from = page.next
  }
  return {
    events,
    records,
    closed: !!head.closed,
    meta: {
      endpoint: ENDPOINT,
      stream: streamName,
      contentType: CT,
      protocol: 'pico',
      producerId: '-',
      records: records.length,
      lastSeq: records[records.length - 1]?.seq ?? null,
      done: events.some((e) => e.type === 'run_end') || !!head.closed,
    },
  }
}

async function listRecents(): Promise<RecentItem[]> {
  const listing = await pico.list(AGENT_PREFIX + '/', 100)
  const items: RecentItem[] = []
  for (const info of listing.streams) {
    if (!isAgentStream(info.name)) continue
    const loaded = await loadRunStream(info.name)
    if (!loaded) continue
    const start = loaded.events.find((e) => e.type === 'run_start')
    const raw = start && start.type === 'run_start' ? preview(start.prompt).slice(0, 48) : ''
    items.push({
      stream: info.name,
      title: raw ? raw + (raw.length === 48 ? '…' : '') : 'Agent run',
      records: loaded.records.length,
    })
  }
  items.sort((a, b) => b.stream.localeCompare(a.stream))
  return items
}

export function initAgent(client: PicoClient) {
  pico = client
}

export async function handleAgentHistory(url: URL): Promise<Response> {
  const streamName = url.searchParams.get('stream')?.trim()
  if (!streamName) return json({ error: 'Missing stream' }, 400)
  if (!isAgentStream(streamName)) return json({ error: 'Invalid stream' }, 400)
  const loaded = await loadRunStream(streamName)
  if (!loaded) return json({ error: 'Stream not found' }, 404)
  return json({ meta: loaded.meta, events: loaded.events, records: loaded.records })
}

export async function handleAgentRecents(): Promise<Response> {
  return json({ recents: await listRecents() })
}

export async function handleAgentDelete(req: Request): Promise<Response> {
  const streamName = ((await req.json()) as { stream?: string }).stream?.trim()
  if (!streamName || !isAgentStream(streamName)) return json({ error: 'Invalid stream' }, 400)
  await pico.delete(streamName).catch(() => undefined)
  return json({ ok: true, recents: await listRecents() })
}

export async function handleAgentRun(req: Request): Promise<Response> {
  const denied = requireKey()
  if (denied) return denied
  const body = (await req.json()) as { prompt?: string; stream?: string }
  const promptText = body.prompt?.trim()
  if (!promptText) return json({ error: 'empty' }, 400)

  const runId = shortId()
  let streamName = `${AGENT_PREFIX}/run-${runId}`
  const history: ModelMessage[] = []
  const priorRecords: StreamRecordInfo[] = []

  const requested = body.stream?.trim()
  if (requested && isAgentStream(requested)) {
    const loaded = await loadRunStream(requested)
    if (loaded) {
      for (const ev of loaded.events) {
        if (ev.type === 'run_start') history.push({ role: 'user', content: ev.prompt })
        else if (ev.type === 'run_end' && ev.text) {
          history.push({ role: 'assistant', content: ev.text })
        }
      }
      if (!loaded.closed) {
        streamName = requested
        priorRecords.push(...loaded.records)
      }
    }
  }

  const producerId = `ai-sdk-agent-${runId}`
  await ensureStream(pico, streamName)
  const producer = new Producer(pico, streamName, producerId, { lingerMs: 10 })
  const run: RunState = {
    stream: streamName,
    producerId,
    clients: new Set(),
    buffer: [],
    records: priorRecords,
    done: false,
  }
  runs.set(runId, run)

  const emit = (event: string, data: unknown) => {
    run.buffer.push({ event, data })
    broadcastTo(run.clients, event, data)
  }

  const log = async (event: AgentEvent) => {
    const seq = await (await producer.send(enc.encode(JSON.stringify(event)))).durable()
    const rec: StreamRecordInfo = {
      seq: String(seq),
      type: event.type,
      preview: eventPreview(event),
    }
    run.records.push(rec)
    emit('stream-record', rec)
    emit('meta', runMeta(run))
  }

  void (async () => {
    try {
      await log({ type: 'run_start', prompt: promptText, timestamp: new Date().toISOString() })

      let stepIndex = 0
      const result = await generateText({
        model: openai('gpt-4o-mini'),
        tools: agentTools,
        stopWhen: stepCountIs(10),
        messages: [...history, { role: 'user', content: promptText }],
        onStepFinish: async (step) => {
          stepIndex++
          const stepEvent: AgentEvent = {
            type: 'step',
            index: stepIndex,
            text: step.text,
            toolCalls: step.toolCalls.map((tc) => ({
              tool: tc.toolName,
              args: 'input' in tc ? tc.input : undefined,
            })),
            toolResults: step.toolResults.map((tr) => ({
              tool: tr.toolName,
              result: 'output' in tr ? tr.output : undefined,
            })),
            finishReason: step.finishReason,
          }
          await log(stepEvent)
          emit('step', stepEvent)
        },
      })

      const endEvent: AgentEvent = {
        type: 'run_end',
        text: result.text,
        steps: result.steps.length,
        totalTokens: result.usage?.totalTokens ?? 0,
        timestamp: new Date().toISOString(),
      }
      await log(endEvent)
      emit('run-end', endEvent)
      run.done = true
      emit('meta', runMeta(run))
      await producer.close()
    } catch (err) {
      console.error(`[agent] Run ${runId} failed:`, err)
      emit('run-error', { error: err instanceof Error ? err.message : String(err) })
      run.done = true
      emit('meta', runMeta(run))
      await producer.close().catch(() => undefined)
    } finally {
      setTimeout(() => runs.delete(runId), 120_000)
    }
  })()

  return json({ runId, meta: runMeta(run) })
}

export function handleAgentEvents(res: ServerResponse, runId: string) {
  const run = runs.get(runId)
  if (!run) {
    res.writeHead(404, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ error: 'unknown run' }))
    return
  }
  const client = openSse(res)
  for (const { event, data } of run.buffer) client.write(sseFrame(event, data))
  if (run.done) {
    res.end()
    return
  }
  run.clients.add(client)
  res.on('close', () => {
    run.clients.delete(client)
  })
}
