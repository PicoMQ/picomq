import type { ServerResponse } from 'node:http'
import { openai } from '@ai-sdk/openai'
import { type ModelMessage, streamText } from 'ai'
import { Producer, type PicoClient } from '@picomq/client'
import {
  CT,
  ENDPOINT,
  MAX_CONTEXT,
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

export const CHAT_PREFIX = process.env.PICO_CHAT_PREFIX ?? '/examples/ai-sdk/chat'
const LEGACY_CHAT = process.env.PICO_CHAT_STREAM ?? '/examples/ai-sdk/chat'

const clients = new Set<SseClient>()
const messages: ModelMessage[] = []
const records: StreamRecordInfo[] = []
let replayBuffer: Array<{ event: string; data: unknown }> | null = null
let pico!: PicoClient
let producer: Producer | undefined
let producerId = ''

export let activeChatStream = ''

function textOf(msg: ModelMessage) {
  return typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content)
}

function prune(list: ModelMessage[]) {
  if (!Number.isFinite(MAX_CONTEXT) || MAX_CONTEXT <= 0) return
  const system = list[0]?.role === 'system' ? list[0] : undefined
  const rest = list.slice(system ? 1 : 0)
  if (rest.length <= MAX_CONTEXT) return
  list.splice(0, list.length, ...(system ? [system] : []), ...rest.slice(-MAX_CONTEXT))
}

function emit(event: string, data?: unknown) {
  if (replayBuffer !== null) replayBuffer.push({ event, data })
  broadcastTo(clients, event, data)
}

function isChatStream(name: string) {
  return name === CHAT_PREFIX || name === LEGACY_CHAT || name.startsWith(CHAT_PREFIX + '/')
}

function meta() {
  const last = records[records.length - 1]
  return {
    endpoint: ENDPOINT,
    stream: activeChatStream,
    contentType: CT,
    protocol: 'pico',
    producerId,
    records: records.length,
    lastSeq: last?.seq ?? null,
  }
}

function history() {
  return {
    meta: meta(),
    messages: messages.map((m) => ({ role: m.role, content: textOf(m) })),
    records,
  }
}

async function load(streamName: string) {
  messages.length = 0
  records.length = 0
  let from = pico.beginning()
  for (;;) {
    const page = await pico.read(streamName, from, 'off', { count: 500 })
    for (const record of page.records) {
      try {
        const msg = JSON.parse(dec.decode(record.body)) as ModelMessage
        records.push({ seq: record.position, type: msg.role, preview: preview(textOf(msg)) })
        if (textOf(msg).trim()) messages.push(msg)
      } catch {
      }
    }
    if (page.records.length === 0 || page.upToDate || page.next === from) break
    from = page.next
  }
  prune(messages)
}

async function switchStream(streamName: string, opts?: { create?: boolean }) {
  if (!isChatStream(streamName)) throw new Error('Invalid chat stream')
  if (opts?.create) await ensureStream(pico, streamName)
  else if (!(await pico.head(streamName))) throw new Error('Stream not found')
  await producer?.close().catch(() => undefined)
  activeChatStream = streamName
  producerId = `ai-sdk-chat-${streamName.split('/').pop()}-${Date.now()}`
  producer = new Producer(pico, streamName, producerId, { lingerMs: 10 })
  await load(streamName)
}

async function titleFor(streamName: string): Promise<{ title: string; records: number }> {
  const from = pico.beginning()
  const page = await pico.read(streamName, from, 'off', { count: 20 })
  let title = 'New chat'
  let count = page.records.length
  for (const record of page.records) {
    try {
      const msg = JSON.parse(dec.decode(record.body)) as ModelMessage
      if (msg.role !== 'user') continue
      const text = preview(textOf(msg))
      if (text) {
        title = text.slice(0, 48) + (text.length > 48 ? '…' : '')
        break
      }
    } catch {
    }
  }
  if (page.next !== from && !page.upToDate) {
    const head = await pico.head(streamName)
    if (head) {
      const start = Number(head.start)
      const next = Number(head.next)
      if (Number.isFinite(start) && Number.isFinite(next) && next >= start) {
        count = Math.max(count, next - start)
      }
    }
  }
  return { title, records: count }
}

async function listRecents(): Promise<RecentItem[]> {
  const listing = await pico.list(CHAT_PREFIX, 100)
  const names = new Set(listing.streams.map((s) => s.name))
  if (LEGACY_CHAT !== CHAT_PREFIX && (await pico.head(LEGACY_CHAT))) names.add(LEGACY_CHAT)
  const items: RecentItem[] = []
  for (const stream of names) {
    if (!isChatStream(stream)) continue
    items.push({ stream, ...(await titleFor(stream)) })
  }
  items.sort((a, b) => b.stream.localeCompare(a.stream))
  return items
}

async function emitRecents() {
  emit('recents', { active: activeChatStream, recents: await listRecents() })
}

export async function initChat(client: PicoClient) {
  pico = client
  const recents = await listRecents()
  if (recents[0]) await switchStream(recents[0].stream)
  else await switchStream(`${CHAT_PREFIX}/${shortId()}`, { create: true })
  console.log(`chat active ${activeChatStream} (${messages.length} messages)`)
}

export function handleChatEvents(res: ServerResponse) {
  const client = openSse(res)
  if (replayBuffer) {
    for (const { event, data } of replayBuffer) client.write(sseFrame(event, data))
  }
  clients.add(client)
  res.on('close', () => {
    clients.delete(client)
  })
}

export async function handleChatHistory(): Promise<Response> {
  return json(history())
}

export async function handleChatRecents(): Promise<Response> {
  return json({ active: activeChatStream, recents: await listRecents() })
}

export async function handleChatNew(): Promise<Response> {
  emit('restore-start')
  await switchStream(`${CHAT_PREFIX}/${shortId()}`, { create: true })
  emit('meta', meta())
  emit('restore-end')
  await emitRecents()
  return json(history())
}

export async function handleChatDelete(req: Request): Promise<Response> {
  const streamName = ((await req.json()) as { stream?: string }).stream?.trim()
  if (!streamName || !isChatStream(streamName)) return json({ error: 'Invalid stream' }, 400)
  await pico.delete(streamName).catch(() => undefined)
  if (streamName === activeChatStream) {
    const remaining = (await listRecents()).filter((r) => r.stream !== streamName)
    if (remaining[0]) await switchStream(remaining[0].stream)
    else await switchStream(`${CHAT_PREFIX}/${shortId()}`, { create: true })
    emit('meta', meta())
  }
  await emitRecents()
  return json(history())
}

export async function handleChatSelect(req: Request): Promise<Response> {
  const streamName = ((await req.json()) as { stream?: string }).stream?.trim()
  if (!streamName || !isChatStream(streamName)) return json({ error: 'Invalid stream' }, 400)
  emit('restore-start')
  await new Promise((r) => setTimeout(r, 200))
  await switchStream(streamName)
  for (const m of messages) {
    emit('restore-message', { role: m.role, content: textOf(m) })
    await new Promise((r) => setTimeout(r, 40))
  }
  for (const r of records) emit('restore-record', r)
  emit('restore-end')
  emit('meta', meta())
  await emitRecents()
  return json(history())
}

export async function handleChatSend(req: Request): Promise<Response> {
  const denied = requireKey()
  if (denied) return denied
  if (!producer || !activeChatStream) return json({ error: 'No active chat stream' }, 500)
  const text = ((await req.json()) as { message?: string }).message?.trim()
  if (!text) return json({ error: 'empty' }, 400)

  const userMsg: ModelMessage = { role: 'user', content: text }
  messages.push(userMsg)
  prune(messages)
  const userSeq = await (await producer.send(enc.encode(JSON.stringify(userMsg)))).durable()
  const userRec: StreamRecordInfo = { seq: String(userSeq), type: 'user', preview: preview(text) }
  records.push(userRec)
  emit('stream-record', userRec)
  emit('meta', meta())
  await emitRecents()

  replayBuffer = []
  emit('assistant-start')

  const result = streamText({ model: openai('gpt-4o-mini'), messages })
  let fullText = ''
  for await (const chunk of result.textStream) {
    fullText += chunk
    emit('assistant-chunk', chunk)
  }

  if (fullText.trim()) {
    const assistantMsg: ModelMessage = { role: 'assistant', content: fullText }
    messages.push(assistantMsg)
    prune(messages)
    const seq = await (await producer.send(enc.encode(JSON.stringify(assistantMsg)))).durable()
    const rec: StreamRecordInfo = {
      seq: String(seq),
      type: 'assistant',
      preview: preview(fullText),
    }
    records.push(rec)
    emit('stream-record', rec)
    emit('meta', meta())
  }
  emit('assistant-end')
  replayBuffer = null
  return json({ ok: true })
}

export async function handleChatRestart(): Promise<Response> {
  if (!activeChatStream) return json({ error: 'No active chat stream' }, 500)
  emit('restore-start')
  await new Promise((r) => setTimeout(r, 400))
  await load(activeChatStream)
  for (const m of messages) {
    emit('restore-message', { role: m.role, content: textOf(m) })
    await new Promise((r) => setTimeout(r, 60))
  }
  for (const r of records) emit('restore-record', r)
  emit('restore-end')
  emit('meta', meta())
  return json({ ok: true })
}
