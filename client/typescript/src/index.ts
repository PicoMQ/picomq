export type {
  AppendAck,
  CallOptions,
  ClientConfig,
  Live,
  Protocol,
  ReadLimits,
  ReadPage,
  SseEvent,
  StreamApi,
  StreamInfo,
  StreamListing,
  StreamRecord,
  SubscribeOptions,
} from './types'

export { ClientError, isAbortError, type ErrorKind } from './error'
export { DsClient } from './ds'
export { PicoClient, type ProducerAck, type ProducerRef } from './pico'
export { Pending, Producer, type ProducerClient, type ProducerConfig } from './producer'
export { RetryPolicy } from './retry'

import { DsClient } from './ds'
import { PicoClient } from './pico'
import { RetryPolicy } from './retry'
import type { ClientConfig, Protocol, StreamApi } from './types'

export function connect(protocol: 'pico', endpoint: string, config?: ClientConfig): PicoClient
export function connect(protocol: 'ds', endpoint: string, config?: ClientConfig): DsClient
export function connect(protocol: Protocol, endpoint: string, config?: ClientConfig): StreamApi
export function connect(protocol: Protocol, endpoint: string, config: ClientConfig = {}): StreamApi {
  const retry = config.retry ?? RetryPolicy.none()
  const http2 = config.http2 ?? false
  switch (protocol) {
    case 'pico':
      return new PicoClient(endpoint, config.token, http2, retry)
    case 'ds':
      return new DsClient(endpoint, config.token, http2, retry)
  }
}
