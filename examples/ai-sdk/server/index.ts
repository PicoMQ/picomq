import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { readFile } from 'node:fs/promises'
import { dirname, extname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { connect } from '@picomq/client'
import { ENDPOINT, PORT, asRequest } from './lib'
import {
  CHAT_PREFIX,
  activeChatStream,
  handleChatDelete,
  handleChatEvents,
  handleChatHistory,
  handleChatNew,
  handleChatRecents,
  handleChatRestart,
  handleChatSelect,
  handleChatSend,
  initChat,
} from './chat'
import {
  AGENT_PREFIX,
  handleAgentDelete,
  handleAgentEvents,
  handleAgentHistory,
  handleAgentRecents,
  handleAgentRun,
  initAgent,
} from './agent'
import {
  MULTI_PREFIX,
  handleMultiAdvance,
  handleMultiDelete,
  handleMultiEvents,
  handleMultiHost,
  handleMultiNew,
  handleMultiRecents,
  handleMultiRestart,
  handleMultiSelect,
  handleMultiStart,
  handleMultiState,
  initMulti,
} from './multi'
import { PAGES, renderDemoPage, type DemoNav } from './shell'

const PUBLIC_DIR = join(dirname(fileURLToPath(import.meta.url)), '..', 'public')

const MIME: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
}

const DEMO_PATHS: Record<string, DemoNav> = {
  '/chat.html': 'chat',
  '/agent.html': 'agent',
  '/multi.html': 'multi',
}

type ApiHandler = (req: Request, url: URL) => Response | Promise<Response>

const GET_ROUTES: Record<string, ApiHandler> = {
  '/api/chat/history': () => handleChatHistory(),
  '/api/chat/recents': () => handleChatRecents(),
  '/api/agent/history': (_req, url) => handleAgentHistory(url),
  '/api/agent/recents': () => handleAgentRecents(),
  '/api/multi/state': () => handleMultiState(),
  '/api/multi/recents': () => handleMultiRecents(),
}

const POST_ROUTES: Record<string, ApiHandler> = {
  '/api/chat/new': () => handleChatNew(),
  '/api/chat/select': (req) => handleChatSelect(req),
  '/api/chat/send': (req) => handleChatSend(req),
  '/api/chat/restart': () => handleChatRestart(),
  '/api/chat/delete': (req) => handleChatDelete(req),
  '/api/agent/run': (req) => handleAgentRun(req),
  '/api/agent/delete': (req) => handleAgentDelete(req),
  '/api/multi/new': () => handleMultiNew(),
  '/api/multi/select': (req) => handleMultiSelect(req),
  '/api/multi/start': (req) => handleMultiStart(req),
  '/api/multi/advance': () => handleMultiAdvance(),
  '/api/multi/host': (req) => handleMultiHost(req),
  '/api/multi/restart': () => handleMultiRestart(),
  '/api/multi/delete': (req) => handleMultiDelete(req),
}

function serveDemoPage(nav: DemoNav): Response {
  return new Response(renderDemoPage(PAGES[nav]), {
    headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' },
  })
}

async function serveStatic(path: string): Promise<Response> {
  const demo = DEMO_PATHS[path]
  if (demo) return serveDemoPage(demo)
  const full = join(PUBLIC_DIR, path === '/' ? '/index.html' : path)
  if (!full.startsWith(PUBLIC_DIR)) return new Response('Not found', { status: 404 })
  try {
    const buf = await readFile(full)
    const type = MIME[extname(full)] ?? 'application/octet-stream'
    return new Response(buf, {
      headers: { 'Content-Type': type, 'Cache-Control': 'no-store' },
    })
  } catch {
    return new Response('Not found', { status: 404 })
  }
}

function handleSse(path: string, url: URL, res: ServerResponse): boolean {
  if (path === '/api/chat/events') handleChatEvents(res)
  else if (path === '/api/agent/events') handleAgentEvents(res, url.searchParams.get('run') ?? '')
  else if (path === '/api/multi/events') handleMultiEvents(res)
  else return false
  return true
}

async function route(req: IncomingMessage, url: URL): Promise<Response> {
  const table = req.method === 'GET' ? GET_ROUTES : req.method === 'POST' ? POST_ROUTES : undefined
  const handler = table?.[url.pathname]
  if (handler) return handler(await asRequest(url, req), url)
  return serveStatic(url.pathname)
}

const pico = connect('pico', ENDPOINT)
await initChat(pico)
initAgent(pico)
await initMulti(pico)
if (!process.env.OPENAI_API_KEY) {
  console.warn('OPENAI_API_KEY is not set. AI routes will fail until it is')
}

createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`)

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    })
    res.end()
    return
  }

  try {
    if (req.method === 'GET' && handleSse(url.pathname, url, res)) return
    const response = await route(req, url)
    res.writeHead(response.status, Object.fromEntries(response.headers))
    res.end(Buffer.from(await response.arrayBuffer()))
  } catch (err) {
    console.error(err)
    res.writeHead(500, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ error: err instanceof Error ? err.message : String(err) }))
  }
}).listen(PORT, () => {
  console.log(`http://localhost:${PORT}`)
  console.log(`pico ${ENDPOINT}`)
  console.log(`chat ${CHAT_PREFIX}/* (active ${activeChatStream})`)
  console.log(`agent ${AGENT_PREFIX}/run-*`)
  console.log(`multi ${MULTI_PREFIX}/{room}/{bus,agent/*}`)
})
