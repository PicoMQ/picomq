import type { RetryPolicy } from './retry'

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

export interface SubscribeOptions extends CallOptions {
  reconnect?: boolean
  maxReconnectAttempts?: number
  reconnectDelayMs?: number
  maxReconnectDelayMs?: number
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
  headers: { [key: string]: string }
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

export interface StreamApi {
  protocol(): Protocol
  beginning(): string
  now(): string
  create(
    name: string,
    contentType: string,
    ttlSeconds?: number,
    options?: CallOptions,
  ): Promise<boolean>
  head(name: string, options?: CallOptions): Promise<StreamInfo | null>
  append(
    name: string,
    records: Uint8Array[],
    contentType: string,
    options?: CallOptions,
  ): Promise<AppendAck>
  read(
    name: string,
    from: string,
    live: Live,
    limits?: ReadLimits,
    options?: CallOptions,
  ): Promise<ReadPage>
  subscribe(
    name: string,
    from: string,
    options?: SubscribeOptions,
  ): AsyncIterable<SseEvent>
  list(prefix: string, limit?: number, options?: CallOptions): Promise<StreamListing>
  close(name: string, options?: CallOptions): Promise<string>
  delete(name: string, options?: CallOptions): Promise<boolean>
}
