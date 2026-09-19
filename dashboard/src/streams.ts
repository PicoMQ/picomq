export interface Connection {
  endpoint: string
  token: string
}

export interface StreamEntry {
  name: string
  content_type?: string
  closed: boolean
}

export interface StreamInfo {
  contentType: string
  start: string
  next: string
  closed: boolean
  ttl: string | null
  expiresAt: string | null
  schema: string | null
  kafkaTopic: string | null
}

export interface RecordData {
  seq: number
  timestamp?: number
  body?: string
  body_b64?: string
  key?: string
  key_b64?: string
  headers?: Record<string, string>
  headers_b64?: Record<string, string>
  previewTruncated?: boolean
}

export interface ReadPage {
  records: RecordData[]
  next: string
  closed: boolean
}

export class ApiError extends Error {
  constructor(message: string, readonly status = 0, readonly retryable = status === 408 || status === 429 || status >= 500) {
    super(message)
  }
}

const CONNECTION_KEY = 'pico-stream-connection'
export function loadConnection(): Connection {
  try {
    const saved = JSON.parse(sessionStorage.getItem(CONNECTION_KEY) || 'null')
    if (typeof saved?.endpoint === 'string' && typeof saved?.token === 'string') return saved
  } catch { /* Use the default when storage is unavailable. */ }
  const url = new URL(window.location.origin)
  url.port = '4437'
  return { endpoint: import.meta.env.DEV ? '/pico' : url.origin, token: '' }
}

export function saveConnection(connection: Connection) {
  sessionStorage.setItem(CONNECTION_KEY, JSON.stringify(connection))
}

export function normalizeEndpoint(value: string): string {
  const url = new URL(value.trim(), window.location.origin)
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
    throw new Error('Enter an HTTP(S) API URL or a same-origin proxy path, without credentials or a query.')
  }
  return url.origin === window.location.origin
    ? url.pathname.replace(/\/$/, '') || '/'
    : url.href.replace(/\/$/, '')
}

export function streamPath(name: string): string {
  if (!name.startsWith('/') || name.startsWith('//') || name === '/' || /[?#\\\s]/.test(name)) {
    throw new Error('Enter an exact stream path, such as /demo/orders. URL-encode spaces and reserved characters.')
  }
  const url = new URL(name, 'http://picomq.invalid')
  if (url.pathname !== name) throw new Error('Stream paths cannot contain dot segments or unencoded characters.')
  return name
}

export function validateStreamNames(values: string[], limit: number): string[] {
  const names = [...new Set(values.map((name) => name.trim()))]
  if (!names.length || names.includes('')) throw new Error('Enter a stream name in each row.')
  if (names.length > limit) throw new Error(`Choose at most ${limit} simultaneous watches.`)
  return names.map(streamPath)
}

async function request(connection: Connection, path: string, init: RequestInit = {}, timeout = 15_000): Promise<Response> {
  const headers = new Headers(init.headers)
  if (connection.token) headers.set('Authorization', `Bearer ${connection.token}`)
  let response: Response
  const deadline = AbortSignal.timeout(timeout)
  const signal = init.signal ? AbortSignal.any([init.signal, deadline]) : deadline
  try {
    response = await fetch(`${connection.endpoint.replace(/\/$/, '')}${path}`, {
      ...init, signal, headers, redirect: 'manual', cache: 'no-store',
    })
  } catch (error) {
    if (init.signal?.aborted) throw error
    if (deadline.aborted) throw new ApiError(`The stream API request timed out after ${timeout / 1000} seconds.`, 0, true)
    throw new ApiError('Could not reach the stream API. Check the endpoint and its CORS or same-origin proxy configuration.', 0, true)
  }
  if (response.type === 'opaqueredirect' || response.status === 307 || response.status === 308) {
    throw new ApiError('This stream belongs to another node. Connect to its owner or use a proxy that handles ownership redirects.')
  }
  if (!response.ok) {
    let text = ''
    try { text = await readText(response, 4096, true) }
    catch (error) {
      if (init.signal?.aborted) throw error
      // The HTTP status is authoritative even when its optional error body is lost.
    }
    let message = text.slice(0, 500)
    try { const body = JSON.parse(text); message = body.message || body.error || message } catch { /* Plain-text error. */ }
    if (response.status === 401) message = 'A valid stream API token is required.'
    if (response.status === 403) message = 'The token does not allow this operation. Stream requests require the pico audience; schema/config requests require admin.'
    throw new ApiError(`${response.status}: ${message || response.statusText}`, response.status)
  }
  return response
}

async function json<T>(response: Response): Promise<T> {
  if (!response.headers.get('content-type')?.includes('application/json')) {
    throw new ApiError('Expected the Pico JSON API but received another response. Check the stream endpoint and protocol.')
  }
  return JSON.parse(await readText(response, 4 * 1024 * 1024)) as T
}

async function readText(response: Response, maxBytes: number, truncate = false): Promise<string> {
  if (!response.body) return ''
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let size = 0
  let text = ''
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) return text + decoder.decode()
      if (size + value.length > maxBytes) {
        await reader.cancel()
        if (!truncate) throw new ApiError('Response exceeds the 4 MiB preview limit. The server may return an oversized record despite the requested byte limit; this position has not been skipped.')
        return text + decoder.decode(value.subarray(0, maxBytes - size)) + '\n… preview truncated'
      }
      size += value.length
      text += decoder.decode(value, { stream: true })
    }
  } catch (error) {
    if (error instanceof TypeError) throw new ApiError('The connection was interrupted while reading the stream API response.', 0, true)
    throw error
  } finally { reader.releaseLock() }
}

export function previewRecord(record: RecordData): RecordData {
  let truncated = false
  const clip = (value: string | undefined, max: number) => {
    if (value !== undefined && value.length > max) { truncated = true; return value.slice(0, max) }
    return value
  }
  const headers = (values: Record<string, string> | undefined) => {
    if (!values) return undefined
    const entries = Object.entries(values)
    if (entries.length > 20) truncated = true
    return Object.fromEntries(entries.slice(0, 20).map(([key, value]) => [clip(key, 256)!, clip(value, 512)!]))
  }
  const result = {
    ...record, body: clip(record.body, 16_384), body_b64: clip(record.body_b64, 16_384),
    key: clip(record.key, 1024), key_b64: clip(record.key_b64, 1024),
    headers: headers(record.headers), headers_b64: headers(record.headers_b64),
  }
  return { ...result, previewTruncated: truncated || record.previewTruncated }
}

export function formatTimestamp(value: number | string | undefined): string {
  if (value === undefined) return '—'
  const date = new Date(Number(value))
  return Number.isFinite(date.getTime()) ? date.toISOString() : `${value} (timestamp outside date range)`
}

function position(response: Response, header = 'Pico-Next-Seq'): string {
  const value = response.headers.get(header)
  if (!value || !/^\d+$/.test(value)) {
    throw new ApiError(`Missing ${header}. Use a Pico listener and ensure the proxy exposes its response headers.`)
  }
  return value
}

export async function listStreams(connection: Connection, prefix: string, after = '', signal?: AbortSignal) {
  const query = new URLSearchParams({ prefix: prefix || '/', limit: '100' })
  if (after) query.set('start_after', after)
  const page = await json<{ streams: StreamEntry[]; has_more: boolean }>(
    await request(connection, `/?${query}`, { signal }),
  )
  if (!Array.isArray(page.streams) || page.streams.some((s) => typeof s.name !== 'string')) {
    throw new ApiError('Invalid stream listing. This dashboard requires the Pico HTTP protocol.')
  }
  return page
}

export async function inspectStream(connection: Connection, name: string, signal?: AbortSignal): Promise<StreamInfo> {
  const response = await request(connection, streamPath(name), { method: 'HEAD', signal })
  return {
    start: position(response, 'Pico-Start-Seq'), next: position(response),
    contentType: response.headers.get('Content-Type') || 'application/octet-stream',
    closed: response.headers.get('Pico-Closed') === 'true',
    ttl: response.headers.get('Pico-TTL'), expiresAt: response.headers.get('Pico-Expires-At'),
    schema: response.headers.get('Pico-Schema'), kafkaTopic: response.headers.get('Pico-Kafka-Topic'),
  }
}

export async function readStream(connection: Connection, name: string, from: string, signal?: AbortSignal, count = 50, live = false): Promise<ReadPage> {
  const query = new URLSearchParams({ seq: from, count: String(count), bytes: '262144', format: 'json' })
  if (live) query.set('live', 'long-poll')
  const response = await request(connection, `${streamPath(name)}?${query}`, { signal }, live ? 35_000 : 15_000)
  // An idle long poll returns 204 with its resume cursor, without a JSON body.
  if (live && response.status === 204) return { records: [], next: position(response), closed: response.headers.get('Pico-Closed') === 'true' }
  const records = await json<RecordData[]>(response)
  if (!Array.isArray(records) || records.some((record) => !Number.isSafeInteger(record.seq))) {
    throw new ApiError('The response has an invalid sequence or a sequence beyond JavaScript’s safe integer range.')
  }
  return { records: records.map(previewRecord), next: position(response), closed: response.headers.get('Pico-Closed') === 'true' }
}

export async function streamConfig(connection: Connection, name: string, signal?: AbortSignal) {
  return json<{ schema: string | null; schemaValidate: boolean; kafkaTopic: string | null }>(
    await request(connection, `/_streams/${streamPath(name).slice(1).split('/').map(encodeURIComponent).join('/')}`, { signal }),
  )
}

export async function fetchSchema(connection: Connection, name: string, signal?: AbortSignal) {
  const response = await request(connection, `/_schemas/${encodeURIComponent(name)}`, { signal })
  return readText(response, 100_000, true)
}

export async function appendMessage(connection: Connection, name: string, body: string, key: string, headers: Record<string, string>, signal?: AbortSignal) {
  const record: Record<string, unknown> = { body, headers }
  if (key) record.key = key
  // The existing JSON batch format carries one record with an optional key and headers.
  // Never retry an append: an interrupted response does not prove that the write failed.
  const response = await request(connection, streamPath(name), {
    method: 'POST', signal, headers: { 'Content-Type': 'application/vnd.picomq.batch+json' },
    body: JSON.stringify({ records: [record] }),
  })
  return { start: position(response, 'Pico-Start-Seq'), next: position(response), timestamp: response.headers.get('Pico-Timestamp') }
}

export function payload(record: RecordData): string {
  if (record.body_b64 !== undefined) return `[binary · base64]\n${record.body_b64}`
  try { return JSON.stringify(JSON.parse(record.body || ''), null, 2) } catch { return record.body || '' }
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
