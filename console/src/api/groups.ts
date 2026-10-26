import { admin, request } from './client'
import { listing, read } from './query'

export type GroupState = 'Empty' | 'PreparingRebalance' | 'CompletingRebalance' | 'Stable'

interface Description {
  state: GroupState
  protocolType: string
  protocolName: string
  generation: number
  members: number
}

export type Group = { groupId: string; coordinatorNodeId: number | null; current: boolean } & (
  | Description
  | { state: null }
)

export type GroupDetail = { groupId: string; coordinatorNodeId: number; committedPartitions: number } & Description

export type Assignment =
  | { streams: string[] }
  | { topics: { topic: string; partitions: number[] }[] }
  | { bytes: number }

export interface Member {
  memberId: string
  instanceId: string | null
  clientId: string
  clientHost: string
  assignment: Assignment
}

export interface GroupOffset {
  topic: string | null
  partition: number
  committedOffset: number
  metadata: string | null
  nextOffset: number | null
  lag: number | null
  current: boolean
}

export type ResetTarget = 'earliest' | 'latest' | { offset: number } | { timestamp: number }

export interface OffsetReset {
  topic: string
  to: ResetTarget
  partitions?: number[]
}

export const groups = (prefix?: string) => listing<Group>(['groups'], { prefix })

export const group = (id: string) => read<GroupDetail>(['groups', id])

export const members = (id: string) => listing<Member>(['groups', id, 'members'])

export const offsets = (id: string, topic?: string) => listing<GroupOffset>(['groups', id, 'offsets'], { topic })

export const resetOffsets = (id: string, body: OffsetReset) =>
  request<{ partitions: { partition: number; offset: number }[] }>(
    'POST',
    admin('groups', id, 'offsets', 'reset'),
    { body },
  )

export const deleteOffsets = (id: string, topic: string) =>
  request<void>('DELETE', admin('groups', id, 'offsets'), { query: { topic } })

export const deleteGroup = (id: string) => request<void>('DELETE', admin('groups', id))
