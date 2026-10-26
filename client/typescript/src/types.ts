import type { RetryPolicy } from './retry'
import type { Stream } from './stream'

export type Protocol = 'pico' | 'ds'

export type Live = 'off' | 'long-poll'

export interface ClientConfig {
  http2?: boolean
  token?: string
  retry?: RetryPolicy
}

export interface CallOptions {
  signal?: AbortSignal
}

export interface PartitionOptions extends CallOptions {
  partition?: number
}

export interface AppendOptions extends PartitionOptions {
  contentType?: string
}

export interface SubscribeOptions extends PartitionOptions {
  reconnect?: boolean
  maxReconnectAttempts?: number
  reconnectDelayMs?: number
  maxReconnectDelayMs?: number
}

export interface RecordsOptions extends PartitionOptions {
  live?: boolean
  batch?: ReadLimits
}

export type HeaderValue = string | Uint8Array

export interface Header {
  name: string
  value: HeaderValue | null
}

export type AppendInput =
  | Uint8Array
  | string
  | {
      body: Uint8Array | string | null
      key?: Uint8Array | string
      headers?: Header[]
      timestamp?: number | bigint
    }

export interface StreamInfo {
  name: string
  contentType?: string
  partitions: number
  start?: string
  next?: string
  closed: boolean
  ttlSeconds?: number
  expiresAt?: string
  retentionMs?: number
  retentionBytes?: number
  schema?: string
  kafkaTopic?: string
}

export interface CreateOptions extends CallOptions {
  contentType?: string
  partitions?: number
  ttlSeconds?: number
  expiresAt?: string
  retentionMs?: number
  retentionBytes?: number
  schema?: string
  schemaValidate?: boolean
  closed?: boolean
}

export interface TopicConfig {
  schema?: string
  schemaValidate: boolean
  kafkaTopic?: string
  partitions: number
  retentionMs?: number
  retentionBytes?: number
}

/**
 * A change to a topic's settings. An absent field is left alone and `null`
 * removes the setting. `partitions` grows the topic and never shrinks it.
 */
export interface ConfigUpdate {
  schema?: string | null
  schemaValidate?: boolean
  retentionMs?: number | null
  retentionBytes?: number | null
  partitions?: number
}

export interface PutSchemaOptions extends CallOptions {
  contentType?: string
}

export interface Schema {
  contentType?: string
  body: Uint8Array
}

export interface AppendAck {
  start: string
  next: string
  timestamp?: number
}

export interface StreamRecord {
  position: string
  timestamp?: number
  key?: Uint8Array
  headers: Header[]
  body: Uint8Array | null
}

export interface ReadPage {
  records: StreamRecord[]
  next: string
  upToDate: boolean
  closed: boolean
}

export interface StreamListing {
  streams: StreamInfo[]
  next?: string
}

export interface GroupListing {
  groups: string[]
  next?: string
}

export interface ReadLimits {
  count?: number
  bytes?: number
}

export type SseEvent =
  | { type: 'data'; id?: string; records: StreamRecord[]; raw: Uint8Array }
  | { type: 'control'; id?: string; next: string; upToDate: boolean; closed: boolean }

export interface ProducerRef {
  id: string
  epoch: number
  seq: number
}

export interface ProducerAck {
  applied: boolean
  duplicate: boolean
  ack: AppendAck
}

export interface MemberFence {
  memberId: string
  generation: number
  instanceId?: string
}

export interface CommittedOffset {
  position: number
  metadata?: string
}

export type Offsets = { [stream: string]: CommittedOffset }

export interface JoinOptions extends CallOptions {
  memberId?: string
  instanceId?: string
  clientId?: string
  sessionTimeoutMs?: number
  rebalanceTimeoutMs?: number
}

export interface GroupMembership {
  memberId: string
  generation: number
  assignment: string[]
  members: string[]
}

export interface GroupAssignment {
  generation: number
  assignment: string[]
}

export interface MemberDescription {
  memberId: string
  instanceId?: string
  clientId: string
  subscription?: string[]
  assignment?: string[]
}

export interface GroupDescription {
  group: string
  state: string
  generation: number
  protocolType?: string
  members: MemberDescription[]
}

export interface StreamApi {
  protocol(): Protocol
  beginning(): string
  now(): string
  stream(name: string): Stream
  create(name: string, options?: CreateOptions): Promise<boolean>
  head(name: string, options?: PartitionOptions): Promise<StreamInfo | null>
  append(name: string, records: AppendInput[], options?: AppendOptions): Promise<AppendAck>
  read(
    name: string,
    from: string,
    live: Live,
    limits?: ReadLimits,
    options?: PartitionOptions,
  ): Promise<ReadPage>
  subscribe(name: string, from: string, options?: SubscribeOptions): AsyncIterable<SseEvent>
  list(
    prefix: string,
    limit?: number,
    cursor?: string,
    options?: CallOptions,
  ): Promise<StreamListing>
  close(name: string, options?: CallOptions): Promise<string>
  delete(name: string, options?: CallOptions): Promise<boolean>
  config(name: string, options?: CallOptions): Promise<TopicConfig | null>
  updateConfig(name: string, update: ConfigUpdate, options?: CallOptions): Promise<TopicConfig>
  putSchema(name: string, body: Uint8Array | string, options?: PutSchemaOptions): Promise<void>
  getSchema(name: string, options?: CallOptions): Promise<Schema | null>
  deleteSchema(name: string, options?: CallOptions): Promise<boolean>
  closeTransport(): Promise<void>
}
