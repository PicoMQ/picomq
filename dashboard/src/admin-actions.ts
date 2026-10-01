import { AuthRequired, type NodeInfo } from './api'
import { streamPath, type Connection } from './streams'

export const TOKEN_OPERATIONS = [
  'read', 'head', 'list', 'create', 'append', 'trim', 'close', 'delete',
  'issue_token', 'revoke_token', 'list_tokens', 'cluster_read', 'node_read',
  'stream_inspect', 'transfer_stream', 'update_node_slots',
] as const
export const TOKEN_AUDIENCES = ['pico', 'durable_streams', 'admin'] as const
export type TokenOperation = typeof TOKEN_OPERATIONS[number]
export type TokenAudience = typeof TOKEN_AUDIENCES[number]
export type ResourceMatcher = { exact: string } | { prefix: string }
export interface TokenScope {
  streams?: ResourceMatcher[]
  tokens?: ResourceMatcher[]
  groups?: Partial<Record<'stream' | 'tokens' | 'admin', { read?: boolean; write?: boolean }>>
  ops?: TokenOperation[]
  audiences?: TokenAudience[]
  autoPrefixStreams?: boolean
  expiresAtMs?: number | null
}
export interface TokenRecord {
  id: string
  scope: TokenScope
  createdAtMs: number
  issuedBy: string
}
export interface IssuedToken {
  id: string
  token: string
  scope: TokenScope
  createdAtMs: number
}
export interface TransferResult {
  stream: string
  streamId: number
  toNode: number
  pending: true
}

/** An uncertain write may already have committed. Refresh before retrying. */
export class AdminWriteError extends Error {
  constructor(message: string, readonly uncertain: boolean, readonly status = 0) {
    super(uncertain ? `${message} The outcome is unknown. Refresh the target before deciding to retry.` : message)
  }
}

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}
function integer(value: unknown, min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= min && value <= max
}
function knownKeys(value: unknown, allowed: string[]): value is Record<string, unknown> {
  return object(value) && Object.keys(value).every((key) => allowed.includes(key))
}

/** Mirrors the backend's strict scope JSON shape; authorization stays server-side. */
export function validateTokenScope(value: unknown): TokenScope {
  const fail = () => { throw new Error('Invalid token scope. Check matchers, operations, audiences, and expiry.') }
  if (!knownKeys(value, ['streams', 'tokens', 'groups', 'ops', 'audiences', 'autoPrefixStreams', 'expiresAtMs'])) return fail()
  for (const field of ['streams', 'tokens']) {
    if (!(field in value)) continue
    const entries = value[field]
    if (!Array.isArray(entries) || !entries.every((entry) => object(entry) && Object.keys(entry).length === 1
      && (typeof entry.exact === 'string' || typeof entry.prefix === 'string'))) return fail()
  }
  if ('groups' in value) {
    if (!knownKeys(value.groups, ['stream', 'tokens', 'admin'])) return fail()
    for (const group of Object.values(value.groups)) {
      if (!knownKeys(group, ['read', 'write']) || !Object.values(group).every((flag) => typeof flag === 'boolean')) return fail()
    }
  }
  if ('ops' in value && (!Array.isArray(value.ops) || !value.ops.every((op) => TOKEN_OPERATIONS.includes(op)))) return fail()
  if ('audiences' in value && (!Array.isArray(value.audiences) || !value.audiences.every((audience) => TOKEN_AUDIENCES.includes(audience)))) return fail()
  if ('autoPrefixStreams' in value && typeof value.autoPrefixStreams !== 'boolean') return fail()
  if ('expiresAtMs' in value && value.expiresAtMs !== null && !integer(value.expiresAtMs)) return fail()
  return value as TokenScope
}

export function validateTokenId(id: string): void {
  const bytes = new TextEncoder().encode(id).length
  if (bytes < 1 || bytes > 96) throw new Error('Token IDs must contain 1–96 UTF-8 bytes.')
}

function node(value: unknown): NodeInfo {
  if (!object(value) || !integer(value.nodeId, -2147483648, 2147483647) || !integer(value.nodeEpoch)
    || !(value.advertisedAddress === null || typeof value.advertisedAddress === 'string')
    || !integer(value.slots, 0, 4294967295) || typeof value.local !== 'boolean'
    || !integer(value.openingCount, 0) || !integer(value.placedCount, 0)) {
    throw new Error('Invalid node response from the admin API.')
  }
  return value as unknown as NodeInfo
}

async function readBody(response: Response, maxBytes: number): Promise<string> {
  if (!response.body) return ''
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let bytes = 0
  let text = ''
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) return text + decoder.decode()
      bytes += value.length
      if (bytes > maxBytes) {
        await reader.cancel()
        throw new Error('The admin response exceeds the dashboard size limit.')
      }
      text += decoder.decode(value, { stream: true })
    }
  } finally {
    reader.releaseLock()
  }
}

async function request<T>(connection: Connection, path: string, method: 'GET' | 'POST' | 'DELETE',
  status: number, decode: (value: unknown) => T, body?: unknown, signal?: AbortSignal): Promise<T> {
  const writing = method !== 'GET'
  const payload = body === undefined ? undefined : JSON.stringify(body)
  if (payload && new TextEncoder().encode(payload).length > 256 * 1024) throw new Error('The admin request exceeds 256 KiB.')
  const headers: Record<string, string> = { Accept: 'application/json' }
  if (payload !== undefined) headers['Content-Type'] = 'application/json'
  if (connection.token) headers.Authorization = `Bearer ${connection.token}`
  const deadline = AbortSignal.timeout(15_000)
  const requestSignal = signal ? AbortSignal.any([signal, deadline]) : deadline
  let response: Response
  try {
    response = await fetch(`${connection.endpoint.replace(/\/$/, '')}${path}`, {
      method, headers, body: payload, signal: requestSignal, redirect: 'manual', cache: 'no-store',
    })
  } catch (error) {
    if (signal?.aborted && !writing) throw error
    const message = deadline.aborted ? 'The admin request timed out after 15 seconds.' : 'Could not reach the admin API. Check its endpoint and network connection.'
    throw writing ? new AdminWriteError(message, true) : new Error(message)
  }
  if (response.status === 401 || response.status === 403) throw new AuthRequired(response.status)
  if (response.type === 'opaqueredirect' || (response.status >= 300 && response.status < 400)) {
    const message = 'The admin endpoint redirected. Use its direct URL; credentials were not forwarded.'
    throw writing ? new AdminWriteError(message, true, response.status) : new Error(message)
  }
  if (!response.ok) {
    let detail = ''
    try {
      const text = await readBody(response, 4096)
      const value = JSON.parse(text)
      if (typeof value.error === 'string') detail = `: ${value.error.slice(0, 500)}`
    } catch { /* The status remains authoritative when an optional error body is unavailable. */ }
    const message = `Admin request failed (${response.status})${detail}`
    throw writing ? new AdminWriteError(message, response.status >= 500 || response.status === 408, response.status) : new Error(message)
  }
  try {
    if (response.status !== status) throw new Error(`Unexpected admin response status (${response.status}).`)
    if (status === 204) return decode(undefined)
    if (!response.headers.get('Content-Type')?.includes('application/json')) throw new Error('Expected the PicoMQ admin JSON API.')
    return decode(JSON.parse(await readBody(response, writing ? 64 * 1024 : 1024 * 1024)))
  } catch (error) {
    if (signal?.aborted && !writing) throw error
    const message = deadline.aborted ? 'The admin response timed out after 15 seconds.'
      : error instanceof Error ? error.message : 'Invalid admin response.'
    throw writing ? new AdminWriteError(message, true, response.status) : new Error(message)
  }
}

export function fetchAdminNodes(connection: Connection, signal?: AbortSignal): Promise<{ nodes: NodeInfo[] }> {
  return request(connection, '/admin/nodes', 'GET', 200, (value) => {
    if (!object(value) || !Array.isArray(value.nodes)) throw new Error('Invalid node list from the admin API.')
    return { nodes: value.nodes.map(node) }
  }, undefined, signal)
}

export function transferStream(connection: Connection, name: string, toNode: number): Promise<TransferResult> {
  streamPath(name)
  if (!integer(toNode, -2147483648, 2147483647)) throw new Error('Choose a signed 32-bit destination node ID.')
  return request(connection, '/admin/transfer', 'POST', 202, (value) => {
    if (!object(value) || value.stream !== name || value.toNode !== toNode || value.pending !== true || !integer(value.streamId, 0)) {
      throw new Error('Invalid transfer acknowledgement from the admin API.')
    }
    return value as unknown as TransferResult
  }, { stream: name, toNode })
}

export function updateNodeSlots(connection: Connection, nodeId: number, slots: number): Promise<NodeInfo> {
  if (!integer(nodeId, -2147483648, 2147483647)) throw new Error('Choose a signed 32-bit node ID.')
  if (!integer(slots, 0, 4294967295)) throw new Error('Slots must be an integer between 0 and 4294967295.')
  return request(connection, `/admin/nodes/${nodeId}`, 'POST', 200, (value) => {
    const updated = node(value)
    if (updated.nodeId !== nodeId || updated.slots !== slots) throw new Error('The node acknowledgement does not match the requested update.')
    return updated
  }, { slots })
}

export function listTokens(connection: Connection, signal?: AbortSignal): Promise<{ count: number; tokens: TokenRecord[] }> {
  return request(connection, '/admin/tokens', 'GET', 200, (value) => {
    if (!object(value) || !integer(value.count, 0) || !Array.isArray(value.tokens) || value.count !== value.tokens.length) {
      throw new Error('Invalid token list from the admin API.')
    }
    const tokens = value.tokens.map((entry) => {
      if (!object(entry) || typeof entry.id !== 'string' || !integer(entry.createdAtMs) || typeof entry.issuedBy !== 'string') {
        throw new Error('Invalid token record from the admin API.')
      }
      validateTokenId(entry.id)
      // Pick public fields explicitly; a list response must never expose a secret.
      return { id: entry.id, scope: validateTokenScope(entry.scope), createdAtMs: entry.createdAtMs, issuedBy: entry.issuedBy }
    })
    return { count: value.count, tokens }
  }, undefined, signal)
}

export function issueToken(connection: Connection, id: string, scope: TokenScope): Promise<IssuedToken> {
  validateTokenId(id)
  validateTokenScope(scope)
  if (!scope.audiences?.length || (!scope.ops?.length && !Object.values(scope.groups || {}).some((group) => group.read || group.write))) {
    throw new Error('Choose at least one audience and operation for the token.')
  }
  if (scope.autoPrefixStreams && !(scope.streams?.length === 1 && 'prefix' in scope.streams[0])) {
    throw new Error('Automatic prefixing requires exactly one stream prefix matcher.')
  }
  if (id === 'anonymous' && scope.audiences.includes('admin')) throw new Error('The anonymous grant cannot use the admin audience.')
  return request(connection, '/admin/tokens', 'POST', 201, (value) => {
    if (!object(value) || value.id !== id || typeof value.token !== 'string' || !value.token.length || value.token.length > 4096
      || !integer(value.createdAtMs)) throw new Error('Invalid token issuance acknowledgement from the admin API.')
    return { id, token: value.token, scope: validateTokenScope(value.scope), createdAtMs: value.createdAtMs }
  }, { id, scope })
}

export function revokeToken(connection: Connection, id: string): Promise<void> {
  validateTokenId(id)
  // Browsers normalize these path components, even if their dots are escaped.
  if (id === '.' || id === '..') throw new Error('This token ID cannot be addressed safely by a browser. Revoke it using the CLI.')
  return request(connection, `/admin/tokens/${encodeURIComponent(id)}`, 'DELETE', 204, () => undefined)
}
