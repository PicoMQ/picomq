import { read } from './query'

export interface Cluster {
  clusterId: string
  nodeId: number
  nodeEpoch: number
  advertisedAddress: string
  registered: boolean
  fenced: boolean
  appliedIndex: number
  streamCount: number
  objectCount: number
  streamSetObjectCount: number
  streamObjectCount: number
  preparedObjectCount: number
  gc: {
    backlog: number
    oldestSeq: number | null
    nextSeq: number
  }
  kvEntryCount: number
  kvBytes: number
  pendingTransferCount: number
  leaseHolder: boolean | null
  snapshot: {
    lastAppliedIndex: number
    lastBytes: number
    lastDurationMs: number
    lastAtMs: number
    taken: number
    failed: number
  }
}

export const cluster = () => read<Cluster>(['cluster'])
