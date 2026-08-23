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
  destroyedObjectBacklog: number
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
