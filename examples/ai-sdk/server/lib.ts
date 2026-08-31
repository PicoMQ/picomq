import type { IncomingMessage, ServerResponse } from 'node:http'
import type { PicoClient } from '@picomq/client'

export const PORT = Number(process.env.PORT ?? 3456)
export const ENDPOINT = process.env.PICO_ENDPOINT ?? 'http://127.0.0.1:4437'
export const CT = 'application/json'
export const MAX_CONTEXT = Number(process.env.AI_SDK_MAX_CONTEXT_MESSAGES ?? 40)

export const enc = new TextEncoder()
export const dec = new TextDecoder()

export type SseClient = { write: (chunk: string) => void; close: () => void }
export type StreamRecordInfo = { seq: string; type: string; preview: string; stream?: string }
export type RecentItem = { stream: string; title: string; records: number }

export function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
    },
  })
}

export function requireKey(): Response | null {
  if (process.env.OPENAI_API_KEY) return null
  return json({ error: 'Set OPENAI_API_KEY' }, 500)
}

export function preview(text: string) {
  return text.replace(/\s+/g, ' ').trim()
}

export function sseFrame(event: string, data?: unknown) {
  const payload = data !== undefined ? JSON.stringify(data) : ''
  return `event: ${event}\ndata: ${payload}\n\n`
}

export function broadcastTo(clients: Set<SseClient>, event: string, data?: unknown) {
  const msg = sseFrame(event, data)
  for (const c of clients) {
    try {
      c.write(msg)
    } catch {
      clients.delete(c)
    }
  }
}

export function openSse(res: ServerResponse): SseClient {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  })
  return {
    write: (chunk) => {
      res.write(chunk)
    },
    close: () => {
      res.end()
    },
  }
}

export async function ensureStream(pico: PicoClient, name: string) {
  await pico.create(name, CT).catch(() => undefined)
}

export async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of req) chunks.push(chunk as Buffer)
  return Buffer.concat(chunks).toString('utf8')
}

export async function asRequest(url: URL, req: IncomingMessage): Promise<Request> {
  return new Request(url, {
    method: req.method,
    headers: { 'Content-Type': 'application/json' },
    body: req.method === 'GET' || req.method === 'HEAD' ? undefined : await readBody(req),
  })
}

export function shortId() {
  return crypto.randomUUID().slice(0, 8)
}
