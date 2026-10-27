import { admin, request } from './client'
import { listing, read } from './query'

export interface Topic {
  name: string
  id: string
  partitions: number
  contentType: string
  kafkaTopic: string | null
  retentionMs: number | null
  retentionBytes: number | null
  ttlSeconds: number | null
  expiresAtMs: number | null
  closed: boolean
}

export interface TopicDetail extends Topic {
  storedBytes: number
  owners: { nodeId: number | null; partitions: number }[]
}

export interface Partition {
  index: number
  streamId: number | null
  ownerNodeId: number | null
  epoch: number | null
  state: 'opened' | 'closed' | null
  pendingTransfer: { fromNode: number; toNode: number } | null
  startOffset: number | null
  nextOffset: number | null
  current: boolean
  storedBytes: number | null
}

export interface TopicCreate {
  name: string
  partitions?: number
  contentType?: string
  retentionMs?: number
  retentionBytes?: number
  ttlSeconds?: number
}

export interface TopicUpdate {
  partitions?: number
  retentionMs?: number | null
  retentionBytes?: number | null
}

export const topics = (prefix?: string) => listing<Topic>(['topics'], { prefix })

export const topic = (name: string) => read<TopicDetail>(['topics', name])

export const partitions = (name: string, node?: number) =>
  listing<Partition>(['topics', name, 'partitions'], { node })

export const partition = (name: string, index: number) =>
  read<Partition>(['topics', name, 'partitions', index])

export const createTopic = (body: TopicCreate) => request<Topic>('POST', admin('topics'), { body })

export const updateTopic = (name: string, body: TopicUpdate) =>
  request<Topic>('PATCH', admin('topics', name), { body })

export const deleteTopic = (name: string) => request<void>('DELETE', admin('topics', name))

export const trimPartition = (name: string, index: number, before: number) =>
  request<{ startOffset: number }>('POST', admin('topics', name, 'partitions', index, 'trim'), {
    body: { before },
  })
