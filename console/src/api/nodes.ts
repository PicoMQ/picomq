import { admin, request, requestText } from './client'
import { listing, read } from './query'

export interface ClusterNode {
  nodeId: number
  nodeEpoch: number
  advertisedAddress: string | null
  slots: number
  local: boolean
  openingCount: number
  placedCount: number
  fenced: boolean
}

export const nodes = (fenced?: boolean) => listing<ClusterNode>(['nodes'], { fenced })

export const node = (id: number) => read<ClusterNode>(['nodes', id])

export const updateNode = (id: number, slots: number) =>
  request<ClusterNode>('POST', admin('nodes', id), { body: { slots } })

export const nodeMetrics = (id: number, signal?: AbortSignal) =>
  requestText(admin('nodes', id, 'metrics'), { signal })
