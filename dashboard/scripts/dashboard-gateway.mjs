// Optional same-origin gateway for the embedded dashboard. Node built-ins only.
// This is a separate process; it adds no routes to PicoMQ.
import http from 'node:http'
import https from 'node:https'
import { pathToFileURL } from 'node:url'

const MAX_BODY = 1024 * 1024
const HOP_HEADERS = ['connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te', 'trailer', 'transfer-encoding', 'upgrade']

function origin(value) {
  const url = new URL(value)
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
    throw new Error('Upstreams must be HTTP(S) origins without credentials, paths, queries, or fragments.')
  }
  return url.origin
}

function headersFor(headers) {
  const copy = { ...headers }
  const connection = String(copy.connection || '').split(',').map((name) => name.trim().toLowerCase())
  for (const name of [...HOP_HEADERS, ...connection, 'host']) delete copy[name]
  return copy
}

/** Only explicitly configured protocol origins may receive a redirected token. */
export function createGateway({ admin = 'http://127.0.0.1:9090', stream = 'http://127.0.0.1:4437', owners = [] } = {}) {
  const adminOrigin = origin(admin)
  const streamOrigin = origin(stream)
  const allowedOwners = new Set([streamOrigin, ...owners.map(origin)])
  return http.createServer(async (request, response) => {
    let upstreamRequest
    let upstreamResponse
    const stop = () => { upstreamRequest?.destroy(); upstreamResponse?.destroy() }
    request.on('aborted', stop)
    response.on('close', stop)
    function fail(status, message) {
      if (response.destroyed || response.writableEnded) return
      if (response.headersSent) { response.destroy(); return }
      response.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' })
      response.end(JSON.stringify({ error: message }))
    }
    const path = request.url || '/'
    // Keep the raw path, including percent escapes; do not normalize stream names.
    if (!path.startsWith('/') || path.startsWith('//')) { fail(400, 'Expected an origin-relative path.'); return }
    const protocol = /^\/pico(?:\/|\?|$)/.test(path)
    const forwardedPath = protocol ? path.replace(/^\/pico(?=\/|\?|$)/, '') || '/' : path
    const routedPath = forwardedPath.startsWith('?') ? '/' + forwardedPath : forwardedPath
    const chunks = []
    let size = 0
    try {
      for await (const chunk of request) {
        size += chunk.length
        if (size > MAX_BODY) { fail(413, 'Gateway request limit is 1 MiB.'); return }
        chunks.push(chunk)
      }
    } catch { fail(400, 'Request body was interrupted.'); return }
    if (response.destroyed) return
    const body = Buffer.concat(chunks)
    const requestHeaders = headersFor(request.headers)
    // An explicit zero length preserves Pico close/append classification.
    if (request.headers['content-length'] !== undefined || body.length) requestHeaders['content-length'] = String(body.length)

    function forward(targetOrigin, hops = 0) {
      const upstream = new URL(targetOrigin)
      upstreamRequest = (upstream.protocol === 'https:' ? https : http).request({
        protocol: upstream.protocol, hostname: upstream.hostname.replace(/^\[|\]$/g, ''), port: upstream.port,
        path: routedPath, method: request.method, headers: requestHeaders, agent: false,
      }, (incoming) => {
        upstreamResponse = incoming
        const status = incoming.statusCode || 502
        if (status >= 300 && status < 400) {
          incoming.resume()
          if (!protocol || ![307, 308].includes(status)) { fail(502, 'Upstream redirected. Configure its direct origin.'); return }
          let target
          try { target = new URL(incoming.headers.location, targetOrigin + routedPath) }
          catch { fail(502, 'Owner redirect did not contain a valid destination.'); return }
          if (target.username || target.password || target.hash || !allowedOwners.has(target.origin)
            || target.pathname + target.search !== routedPath || hops >= 3) {
            fail(502, 'Owner redirect is not allowed or exceeded three hops. Configure PICO_GATEWAY_OWNERS with the cluster protocol origins.'); return
          }
          forward(target.origin, hops + 1)
          return
        }
        response.writeHead(status, headersFor(incoming.headers))
        incoming.on('error', () => response.destroy())
        incoming.pipe(response)
      })
      // Exceeds Pico's default 25-second long poll. A write timeout remains an
      // uncertain outcome; never retry it except a pre-operation owner redirect.
      upstreamRequest.setTimeout(40_000, () => upstreamRequest.destroy(new Error('upstream timeout')))
      upstreamRequest.on('error', () => fail(502, 'Upstream request failed; a write may have completed. Check its target before retrying.'))
      upstreamRequest.end(body)
    }
    forward(protocol ? streamOrigin : adminOrigin)
  })
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const host = process.env.PICO_GATEWAY_HOST || '127.0.0.1'
  const port = Number(process.env.PICO_GATEWAY_PORT || 9080)
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('PICO_GATEWAY_PORT must be between 1 and 65535.')
  const server = createGateway({
    admin: process.env.PICO_GATEWAY_ADMIN,
    stream: process.env.PICO_GATEWAY_STREAM,
    owners: (process.env.PICO_GATEWAY_OWNERS || '').split(',').map((entry) => entry.trim()).filter(Boolean),
  })
  server.listen(port, host, () => {
    console.log(`Dashboard gateway listening at http://${host.includes(':') ? `[${host}]` : host}:${port}`)
    console.log('Open Stream connection and use /pico. Admin pages are served by the configured PicoMQ admin listener.')
  })
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => server.close(() => process.exit(0)))
}
