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

export interface AppendOptions extends CallOptions {
  contentType?: string
}

export interface SubscribeOptions extends CallOptions {
  reconnect?: boolean
  maxReconnectAttempts?: number
  reconnectDelayMs?: number
  maxReconnectDelayMs?: number
}

export interface RecordsOptions extends CallOptions {
  live?: boolean
  batch?: ReadLimits
}

export type HeaderValue = string | Uint8Array

export type AppendInput =
  | Uint8Array
  | string
  | {
      body: Uint8Array | string
      key?: Uint8Array | string
      headers?: { [key: string]: HeaderValue }
      timestamp?: number | bigint
    }

export interface StreamInfo {
  name: string
  contentType?: string
  start: string
  next: string
  closed: boolean
  ttlSeconds?: number
  expiresAt?: string
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
  headers: { [key: string]: HeaderValue }
  body: Uint8Array
}

export interface ReadPage {
  records: StreamRecord[]
  next: string
  upToDate: boolean
  closed: boolean
}

export interface StreamListing {
  streams: StreamInfo[]
  hasMore: boolean
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

export interface StreamApi {
  protocol(): Protocol
  beginning(): string
  now(): string
  stream(name: string): Stream
  create(
    name: string,
    contentType: string,
    ttlSeconds?: number,
    options?: CallOptions,
  ): Promise<boolean>
  head(name: string, options?: CallOptions): Promise<StreamInfo | null>
  append(name: string, records: AppendInput[], options?: AppendOptions): Promise<AppendAck>
  read(
    name: string,
    from: string,
    live: Live,
    limits?: ReadLimits,
    options?: CallOptions,
  ): Promise<ReadPage>
  subscribe(name: string, from: string, options?: SubscribeOptions): AsyncIterable<SseEvent>
  list(prefix: string, limit?: number, options?: CallOptions): Promise<StreamListing>
  close(name: string, options?: CallOptions): Promise<string>
  delete(name: string, options?: CallOptions): Promise<boolean>
  closeTransport(): Promise<void>
}
