import type { ServerResponse } from 'node:http'
import { openai } from '@ai-sdk/openai'
import { type ModelMessage, generateText } from 'ai'
import { Producer, type PicoClient } from '@picomq/client'
import {
  CT,
  ENDPOINT,
  broadcastTo,
  dec,
  enc,
  json,
  openSse,
  preview,
  requireKey,
  shortId,
  type SseClient,
} from './lib'

export const MULTI_PREFIX = process.env.PICO_MULTI_PREFIX ?? '/examples/ai-sdk/multi'

type AgentDef = { id: string; name: string; system: string }

type BusMessage = { from: string; content: string; turn: number }

type MemoryRecord = {
  type: 'message'
  role: 'system' | 'user' | 'assistant'
  content: string
  busSeq?: string
}

type AgentState = {
  def: AgentDef
  stream: string
  producer: Producer
  instructions: string
  messages: ModelMessage[]
  lastBusSeq: string | null
}

type StreamRecordInfo = { seq: string; stream: string; type: string; preview: string }

type RecentItem = { room: string; title: string; records: number; bus: string }

const AGENTS: AgentDef[] = [
  {
    id: 'ada',
    name: 'Ada',
    system:
      'You are Ada, a staff engineer. You speak in concrete tradeoffs: latency, cost, failure modes. ' +
      'Prefer short, sharp replies (1-3 sentences). Challenge vague claims with a specific question.',
  },
  {
    id: 'remy',
    name: 'Remy',
    system:
      'You are Remy, a product lead. You care about user outcomes and clarity. ' +
      'Translate technical points into what ships and who it helps. Keep replies brief (1-3 sentences).',
  },
  {
    id: 'quill',
    name: 'Quill',
    system:
      'You are Quill, a skeptical reviewer. You poke holes in plans and ask for evidence. ' +
      'Be pointed but not rude. Keep replies brief (1-3 sentences).',
  },
]

const SETTING =
  'You are in a working session with other agents and a human host. ' +
  'Stay in character. Respond to the latest points on the shared bus. Do not narrate stage directions.'

const multiClients = new Set<SseClient>()

let pico!: PicoClient
let activeRoom = ''
let busStream = ''
let busProducer: Producer | undefined
let agents: AgentState[] = []
let started = false
let turnNumber = 0
let agentIndex = 0
let messages: { from: string; content: string }[] = []
let records: StreamRecordInfo[] = []

function broadcast(event: string, data?: unknown) {
  broadcastTo(multiClients, event, data)
}

async function ensureStream(name: string) {
  await pico.create(name, CT).catch(() => undefined)
}

async function readAll(streamName: string): Promise<{ position: string; body: Uint8Array }[]> {
  const out: { position: string; body: Uint8Array }[] = []
  let from = pico.beginning()
  for (;;) {
    const page = await pico.read(streamName, from, 'off', { count: 500 })
    for (const r of page.records) out.push({ position: r.position, body: r.body })
    if (page.records.length === 0 || page.upToDate || page.next === from) break
    from = page.next
  }
  return out
}

function seqGt(a: string | null, b: string | null): boolean {
  if (a == null) return false
  if (b == null) return true
  const na = Number(a)
  const nb = Number(b)
  if (Number.isFinite(na) && Number.isFinite(nb)) return na > nb
  return a > b
}

function roomBusPath(room: string) {
  return room === 'legacy' ? `${MULTI_PREFIX}/bus` : `${MULTI_PREFIX}/${room}/bus`
}

function roomAgentPath(room: string, agentId: string) {
  return room === 'legacy'
    ? `${MULTI_PREFIX}/agent/${agentId}`
    : `${MULTI_PREFIX}/${room}/agent/${agentId}`
}

function roomFromBusPath(bus: string): string | null {
  if (bus === `${MULTI_PREFIX}/bus`) return 'legacy'
  const prefix = `${MULTI_PREFIX}/`
  if (!bus.startsWith(prefix) || !bus.endsWith('/bus')) return null
  const mid = bus.slice(prefix.length, -'/bus'.length)
  if (!mid || mid.includes('/')) return null
  return mid
}

async function closeSession() {
  for (const a of agents) {
    await a.producer.close().catch(() => undefined)
  }
  await busProducer?.close().catch(() => undefined)
  agents = []
  busProducer = undefined
}

async function initAgent(room: string, def: AgentDef): Promise<AgentState> {
  const stream = roomAgentPath(room, def.id)
  await ensureStream(stream)
  const producer = new Producer(pico, stream, `multi-${room}-${def.id}-${Date.now()}`, {
    lingerMs: 10,
  })
  const instructions = `${def.system}\n\n${SETTING}`
  const messages: ModelMessage[] = []
  let lastBusSeq: string | null = null

  for (const record of await readAll(stream)) {
    try {
      const mem = JSON.parse(dec.decode(record.body)) as MemoryRecord
      if (mem.type === 'message' && mem.role !== 'system') {
        messages.push({ role: mem.role, content: mem.content })
      }
      if (mem.busSeq != null && seqGt(mem.busSeq, lastBusSeq)) lastBusSeq = mem.busSeq
    } catch {
    }
  }

  return { def, stream, producer, instructions, messages, lastBusSeq }
}

async function loadRoom(room: string, opts?: { create?: boolean }) {
  await closeSession()
  activeRoom = room
  busStream = roomBusPath(room)
  if (opts?.create) await ensureStream(busStream)
  else {
    const head = await pico.head(busStream)
    if (!head && room !== 'legacy') throw new Error('Room not found')
    if (!head) await ensureStream(busStream)
  }

  busProducer = new Producer(pico, busStream, `multi-bus-${room}-${Date.now()}`, { lingerMs: 10 })
  agents = []
  for (const def of AGENTS) {
    agents.push(await initAgent(room, def))
  }

  messages = []
  records = []
  turnNumber = 0
  agentIndex = 0
  started = false

  const busRecords = await readAll(busStream)
  for (const record of busRecords) {
    try {
      const msg = JSON.parse(dec.decode(record.body)) as BusMessage
      messages.push({ from: msg.from, content: msg.content })
      records.push({
        seq: record.position,
        stream: 'bus',
        type: msg.from,
        preview: preview(msg.content),
      })
      turnNumber = Math.max(turnNumber, (msg.turn ?? 0) + 1)
    } catch {
    }
  }

  if (messages.length > 0) {
    started = true
    agentIndex = turnNumber % agents.length
    for (const state of agents) {
      for (const record of busRecords) {
        if (!seqGt(record.position, state.lastBusSeq)) continue
        try {
          const msg = JSON.parse(dec.decode(record.body)) as BusMessage
          if (msg.from === state.def.name) continue
          const labeled =
            msg.from === 'host' ? `[Host]: ${msg.content}` : `[${msg.from}]: ${msg.content}`
          const already = state.messages.some(
            (m) => m.role === 'user' && typeof m.content === 'string' && m.content === labeled,
          )
          if (!already) {
            state.messages.push({ role: 'user', content: labeled })
            state.lastBusSeq = record.position
          }
        } catch {
        }
      }
    }
  }
}

async function postToBus(msg: BusMessage): Promise<string> {
  const pending = await busProducer!.send(enc.encode(JSON.stringify(msg)))
  return String(await pending.durable())
}

async function saveMemory(
  state: AgentState,
  role: 'user' | 'assistant',
  content: string,
  busSeq?: string,
) {
  const rec: MemoryRecord = { type: 'message', role, content, busSeq }
  const pending = await state.producer.send(enc.encode(JSON.stringify(rec)))
  await pending.durable()
  state.messages.push({ role, content })
  if (busSeq != null && seqGt(busSeq, state.lastBusSeq)) state.lastBusSeq = busSeq
}

function nextAgentName() {
  return agents[agentIndex]?.def.name ?? ''
}

function meta() {
  return {
    endpoint: ENDPOINT,
    room: activeRoom,
    bus: busStream,
    agents: agents.map((a) => ({ name: a.def.name, stream: a.stream })),
    protocol: 'pico',
    contentType: CT,
    started,
    nextAgent: started ? nextAgentName() : null,
    turns: turnNumber,
    records: records.length,
  }
}

function statePayload() {
  return {
    meta: meta(),
    started,
    messages,
    records,
    nextAgent: started ? nextAgentName() : null,
    agents: AGENTS.map((a) => a.name),
  }
}

async function listMultiRecents(): Promise<RecentItem[]> {
  const listing = await pico.list(MULTI_PREFIX + '/', 200)
  const rooms = new Set<string>()
  for (const s of listing.streams) {
    const room = roomFromBusPath(s.name)
    if (room) rooms.add(room)
  }
  const legacy = await pico.head(`${MULTI_PREFIX}/bus`)
  if (legacy) rooms.add('legacy')

  const items: RecentItem[] = []
  for (const room of rooms) {
    const bus = roomBusPath(room)
    const busRecords = await readAll(bus)
    let title = 'New session'
    for (const record of busRecords) {
      try {
        const msg = JSON.parse(dec.decode(record.body)) as BusMessage
        if (msg.from === 'host' && msg.content.trim()) {
          const t = preview(msg.content)
          title = t.slice(0, 48) + (t.length > 48 ? '…' : '')
          break
        }
      } catch {
      }
    }
    items.push({ room, title, records: busRecords.length, bus })
  }
  items.sort((a, b) => b.room.localeCompare(a.room))
  return items
}

export async function initMulti(client: PicoClient) {
  pico = client
  const recents = await listMultiRecents()
  if (recents.length === 0) {
    const room = shortId()
    await loadRoom(room, { create: true })
    console.log(`[multi] fresh room ${room}`)
  } else {
    await loadRoom(recents[0]!.room)
    console.log(
      `[multi] room ${activeRoom} (${messages.length} bus messages${started ? ', started' : ''})`,
    )
  }
}

export function handleMultiEvents(res: ServerResponse) {
  const client = openSse(res)
  multiClients.add(client)
  res.on('close', () => {
    multiClients.delete(client)
  })
}

export function handleMultiState(): Response {
  return json(statePayload())
}

export async function handleMultiRecents(): Promise<Response> {
  return json({ active: activeRoom, recents: await listMultiRecents() })
}

export async function handleMultiNew(): Promise<Response> {
  const room = shortId()
  broadcast('restore-start')
  await loadRoom(room, { create: true })
  broadcast('meta', meta())
  broadcast('restore-end', {
    started: false,
    nextAgent: null,
    meta: meta(),
  })
  broadcast('recents', { active: activeRoom, recents: await listMultiRecents() })
  return json(statePayload())
}

export async function handleMultiDelete(req: Request): Promise<Response> {
  const body = (await req.json()) as { room?: string }
  const room = body.room?.trim()
  if (!room) return json({ error: 'Missing room' }, 400)
  const wasActive = room === activeRoom
  if (wasActive) await closeSession()
  const paths = [roomBusPath(room), ...AGENTS.map((a) => roomAgentPath(room, a.id))]
  for (const p of paths) {
    await pico.delete(p).catch(() => undefined)
  }
  if (wasActive) {
    const remaining = (await listMultiRecents()).filter((r) => r.room !== room)
    if (remaining.length > 0) {
      await loadRoom(remaining[0]!.room)
    } else {
      await loadRoom(shortId(), { create: true })
    }
    broadcast('meta', meta())
  }
  broadcast('recents', { active: activeRoom, recents: await listMultiRecents() })
  return json(statePayload())
}

export async function handleMultiSelect(req: Request): Promise<Response> {
  const body = (await req.json()) as { room?: string }
  const room = body.room?.trim()
  if (!room) return json({ error: 'Missing room' }, 400)
  broadcast('restore-start')
  await new Promise((r) => setTimeout(r, 200))
  await loadRoom(room)
  for (const m of messages) {
    if (m.from === 'host') broadcast('host-message', { content: m.content })
    else broadcast('agent-message', { from: m.from, content: m.content, nextAgent: nextAgentName() })
    await new Promise((r) => setTimeout(r, 40))
  }
  for (const r of records) broadcast('stream-record', r)
  broadcast('restore-end', {
    started,
    nextAgent: started ? nextAgentName() : null,
    meta: meta(),
  })
  broadcast('recents', { active: activeRoom, recents: await listMultiRecents() })
  return json(statePayload())
}

export async function handleMultiStart(req: Request): Promise<Response> {
  const denied = requireKey()
  if (denied) return denied
  if (started) return json({ error: 'Already started. Advance or open a new session' }, 400)
  const body = (await req.json()) as { topic?: string }
  const topic = body.topic?.trim()
  if (!topic) return json({ error: 'empty topic' }, 400)

  const busSeq = await postToBus({ from: 'host', content: topic, turn: turnNumber++ })
  const busRec: StreamRecordInfo = {
    seq: busSeq,
    stream: 'bus',
    type: 'host',
    preview: preview(topic),
  }
  records.push(busRec)
  broadcast('stream-record', busRec)

  for (const state of agents) {
    await saveMemory(state, 'user', `[Host]: ${topic}`, busSeq)
  }

  messages.push({ from: 'host', content: topic })
  started = true
  broadcast('host-message', { content: topic })
  broadcast('started', { nextAgent: nextAgentName(), meta: meta() })
  broadcast('meta', meta())
  broadcast('recents', { active: activeRoom, recents: await listMultiRecents() })
  return json({ ok: true, nextAgent: nextAgentName(), meta: meta() })
}

export async function handleMultiAdvance(): Promise<Response> {
  const denied = requireKey()
  if (denied) return denied
  if (!started) return json({ error: 'Start with a topic first' }, 400)

  const state = agents[agentIndex]!
  broadcast('agent-thinking', { name: state.def.name })

  const result = await generateText({
    model: openai('gpt-4o-mini'),
    instructions: state.instructions,
    messages: state.messages.filter((m) => m.role !== 'system'),
  })
  const text = result.text.trim()
  if (!text) return json({ error: 'Empty model response' }, 500)

  const busSeq = await postToBus({
    from: state.def.name,
    content: text,
    turn: turnNumber++,
  })
  const busRec: StreamRecordInfo = {
    seq: busSeq,
    stream: 'bus',
    type: state.def.name,
    preview: preview(text),
  }
  records.push(busRec)
  broadcast('stream-record', busRec)

  await saveMemory(state, 'assistant', text, busSeq)
  for (const other of agents) {
    if (other === state) continue
    await saveMemory(other, 'user', `[${state.def.name}]: ${text}`, busSeq)
  }

  messages.push({ from: state.def.name, content: text })
  agentIndex = (agentIndex + 1) % agents.length
  const next = nextAgentName()
  broadcast('agent-message', { from: state.def.name, content: text, nextAgent: next })
  broadcast('meta', meta())
  return json({ ok: true, nextAgent: next })
}

export async function handleMultiHost(req: Request): Promise<Response> {
  if (!started) return json({ error: 'Start with a topic first' }, 400)
  const body = (await req.json()) as { message?: string }
  const text = body.message?.trim()
  if (!text) return json({ error: 'empty' }, 400)

  const busSeq = await postToBus({ from: 'host', content: text, turn: turnNumber++ })
  const busRec: StreamRecordInfo = {
    seq: busSeq,
    stream: 'bus',
    type: 'host',
    preview: preview(text),
  }
  records.push(busRec)
  broadcast('stream-record', busRec)

  for (const state of agents) {
    await saveMemory(state, 'user', `[Host]: ${text}`, busSeq)
  }
  messages.push({ from: 'host', content: text })
  broadcast('host-message', { content: text })
  broadcast('meta', meta())
  return json({ ok: true })
}

export async function handleMultiRestart(): Promise<Response> {
  const room = activeRoom
  broadcast('restore-start')
  await new Promise((r) => setTimeout(r, 300))
  await loadRoom(room)
  for (const m of messages) {
    if (m.from === 'host') broadcast('host-message', { content: m.content })
    else broadcast('agent-message', { from: m.from, content: m.content, nextAgent: nextAgentName() })
    await new Promise((r) => setTimeout(r, 50))
  }
  for (const r of records) broadcast('stream-record', r)
  broadcast('restore-end', {
    started,
    nextAgent: started ? nextAgentName() : null,
    meta: meta(),
  })
  return json({ ok: true, meta: meta() })
}
