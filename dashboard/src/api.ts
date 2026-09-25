import { streamPath, type Connection } from './streams'

export interface PendingTransfer {
  streamId: number
  fromNode: number
  toNode: number
}

export interface ClusterInfo {
  clusterId: string
  nodeId: number
  nodeEpoch: number
  advertisedAddress: string
  registered: boolean
  appliedIndex: number
  streamCount: number
  objectCount: number
  gc: { backlog: number; oldestSeq: number | null; nextSeq: number }
  pendingTransfers: PendingTransfer[]
  leaseHolder: boolean | null
}

export interface NodeInfo {
  nodeId: number
  nodeEpoch: number
  advertisedAddress: string | null
  slots: number
  local: boolean
  openingCount: number
  placedCount: number
}

export interface Readiness {
  ready: boolean
  serving: boolean
  registered: boolean
  appliedIndex: number
  nodeId: number
}

// Session storage: the token survives reloads but not the tab, and is never
// written to disk by the dashboard.
const TOKEN_KEY = 'pico-admin-token'

export const getToken = () => sessionStorage.getItem(TOKEN_KEY)
export const setToken = (token: string) => sessionStorage.setItem(TOKEN_KEY, token)
export const clearToken = () => sessionStorage.removeItem(TOKEN_KEY)

/** The server wants a (different) token. The app answers with the prompt. */
export class AuthRequired extends Error {
  status: number

  constructor(status: number) {
    super(status === 401 ? 'This node requires an access token' : 'The token lacks admin scope')
    this.status = status
  }
}

async function get<T>(path: string): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json' }
  const token = getToken()
  if (token) {
    headers.Authorization = `Bearer ${token}`
  }
  const res = await fetch(path, { headers })

  // Probes are never gated, so auth failures can only come from /admin.
  if (res.status === 401 || res.status === 403) {
    throw new AuthRequired(res.status)
  }

  const body = (await res.json()) as T

  if (!res.ok && path !== '/ready') {
    throw new Error(`GET ${path} failed: ${res.status}`)
  }

  return body
}

export const fetchCluster = () => get<ClusterInfo>('/admin/cluster')
export const fetchNodes = () => get<{ nodes: NodeInfo[] }>('/admin/nodes')
export const fetchReady = () => get<Readiness>('/ready')

export interface StreamOwnership {
  name: string
  ownerNodeId: number
  ownerAdvertisedAddress: string
  // -1 means never opened; null means no engine metadata row is present.
  epoch: number | null
  pendingTransfer?: { fromNode: number; toNode: number } | null
  // These fields share the pending-transfer metadata snapshot. Routing owner
  // can already point at a destination before the handoff finishes.
  streamId?: number
  nodeId?: number | null
  state?: 'opened' | 'closed' | null
}

async function readOwnershipBody(response: Response): Promise<string> {
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
      if (bytes > 1024 * 1024) {
        void reader.cancel().catch(() => {})
        throw new Error('The admin response exceeds the dashboard size limit.')
      }
      text += decoder.decode(value, { stream: true })
    }
  } finally {
    reader.releaseLock()
  }
}

/** Read the explicitly paired admin listener, without reusing a stream token. */
export async function fetchStreamOwnership(connection: Connection, name: string, signal?: AbortSignal): Promise<StreamOwnership> {
  // Admin uses an Axum Path extractor, which decodes the name once. Native
  // stream names preserve escapes, so encode each segment again here.
  const path = streamPath(name).slice(1).split('/').map(encodeURIComponent).join('/')
  const headers: Record<string, string> = { Accept: 'application/json' }
  if (connection.token) headers.Authorization = `Bearer ${connection.token}`
  const deadline = AbortSignal.timeout(15_000)
  const requestSignal = signal ? AbortSignal.any([signal, deadline]) : deadline
  try {
    const response = await fetch(`${connection.endpoint.replace(/\/$/, '')}/admin/streams/${path}`, {
      headers, signal: requestSignal, redirect: 'manual', cache: 'no-store',
    })
    if (response.type === 'opaqueredirect' || (response.status >= 300 && response.status < 400)) {
      throw new Error('The admin endpoint redirected. Enter its direct URL to read ownership details.')
    }
    if (response.status === 401 || response.status === 403) throw new AuthRequired(response.status)
    if (!response.ok) throw new Error(`Ownership lookup failed: ${response.status}${response.status === 404 ? ' (stream not found on this admin endpoint)' : ''}`)
    if (!response.headers.get('Content-Type')?.includes('application/json')) {
      throw new Error('Expected the PicoMQ admin JSON API. Check the paired admin endpoint.')
    }
    const body = JSON.parse(await readOwnershipBody(response)) as StreamOwnership
    if (!body || body.name !== name || !Number.isSafeInteger(body.ownerNodeId)
      || typeof body.ownerAdvertisedAddress !== 'string'
      || (body.epoch !== null && (!Number.isSafeInteger(body.epoch) || body.epoch < -1))) {
      throw new Error('Invalid ownership response. Expected the selected stream and safe integer owner and epoch values.')
    }
    const nodeId = (value: unknown) => typeof value === 'number' && Number.isInteger(value) && value >= -2147483648 && value <= 2147483647
    if (body.pendingTransfer != null && (!nodeId(body.pendingTransfer.fromNode) || !nodeId(body.pendingTransfer.toNode))) {
      throw new Error('Invalid pending transfer in the ownership response.')
    }
    if ((body.streamId !== undefined && (!Number.isSafeInteger(body.streamId) || body.streamId < 0))
      || (body.nodeId !== undefined && body.nodeId !== null && !nodeId(body.nodeId))
      || (body.state !== undefined && body.state !== null && !['opened', 'closed'].includes(body.state))) {
      throw new Error('Invalid persisted stream identity or state in the ownership response.')
    }
    return body
  } catch (error) {
    if (signal?.aborted) throw error
    if (deadline.aborted) throw new Error('The admin ownership request timed out after 15 seconds.')
    if (error instanceof TypeError) throw new Error('Could not read ownership details. Check the admin endpoint and network connection.')
    if (error instanceof SyntaxError) throw new Error('Invalid JSON from the paired admin endpoint.')
    throw error
  }
}
