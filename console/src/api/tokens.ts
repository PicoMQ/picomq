import { admin, request } from './client'
import { listing } from './query'

export type Matcher = { exact: string } | { prefix: string }

export interface Access {
  read: boolean
  write: boolean
}

export const OPERATIONS = [
  'read',
  'head',
  'list',
  'create',
  'append',
  'trim',
  'close',
  'delete',
  'consume',
  'describe_group',
  'delete_group',
  'issue_token',
  'revoke_token',
  'list_tokens',
  'cluster_read',
  'node_read',
  'transfer_stream',
  'update_node_slots',
] as const

export type Operation = (typeof OPERATIONS)[number]

export const AUDIENCES = ['pico', 'durable_streams', 'kafka', 'admin'] as const

export type Audience = (typeof AUDIENCES)[number]

export interface Scope {
  streams: Matcher[]
  tokens: Matcher[]
  groups: Matcher[]
  access: {
    stream: Access
    tokens: Access
    group: Access
    admin: Access
  }
  ops: Operation[]
  audiences: Audience[]
  autoPrefix: boolean
  expiresAtMs: number | null
}

export interface Token {
  id: string
  scope: Scope
  createdAtMs: number
  issuedBy: string
}

export interface IssuedToken extends Token {
  token: string
}

export const tokens = (prefix?: string) => listing<Token>(['tokens'], { prefix })

export const issueToken = (id: string, scope: Partial<Scope>) =>
  request<IssuedToken>('POST', admin('tokens'), { body: { id, scope } })

export const revokeToken = (id: string) => request<void>('DELETE', admin('tokens', id))
