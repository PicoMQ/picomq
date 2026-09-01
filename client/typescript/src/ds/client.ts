import { ClientError, type ErrorKind } from '../error'
import { base64Decode, parseOptionalUint, retryableError } from '../util'
import { toEnvelope } from '../record'
import { RetryPolicy } from '../retry'
import { Stream } from '../stream'
import { header, Http, toArrayBuffer, truthy, urlencode } from '../transport/http'
import { subscribeLoop } from '../transport/subscribe'
import {
  H_DS_PRODUCER_EPOCH,
  H_DS_PRODUCER_EXPECTED_SEQ,
  H_DS_PRODUCER_RECEIVED_SEQ,
  H_STREAM_CLOSED,
  H_STREAM_EXPIRES_AT,
  H_STREAM_NEXT_OFFSET,
  H_STREAM_TTL,
  H_STREAM_UP_TO_DATE,
} from './headers'
import type {
  AppendAck,
  AppendInput,
  AppendOptions,
  CallOptions,
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
} from '../types'

const OFFSET_BEGINNING = '-1'
const OFFSET_NOW = 'now'
const DEFAULT_CONTENT_TYPE = 'application/octet-stream'

export class DsClient implements StreamApi {
  private readonly http: Http
  private readonly baseUrl: string
  private readonly retry: RetryPolicy

  constructor(
    endpoint: string,
    token?: string,
    http2 = false,
    retry: RetryPolicy = RetryPolicy.none(),
  ) {
    this.baseUrl = endpoint.replace(/\/+$/, '')
    this.http = new Http(token, http2)
    this.retry = retry
  }

  async closeTransport(): Promise<void> {
    await this.http.close()
  }

  protocol(): Protocol {
    return 'ds'
  }

  beginning(): string {
    return OFFSET_BEGINNING
  }

  now(): string {
    return OFFSET_NOW
  }

  stream(name: string): Stream {
    return new Stream(this, name)
  }

  async create(
    name: string,
    contentType: string,
    ttlSeconds?: number,
    options?: CallOptions,
  ): Promise<boolean> {
    return this.retry.run(
      () => this.createOnce(name, contentType, ttlSeconds, options?.signal),
      retryableError,
      options?.signal,
    )
  }

  async head(name: string, options?: CallOptions): Promise<StreamInfo | null> {
    return this.retry.run(
      () => this.headOnce(name, options?.signal),
      retryableError,
      options?.signal,
    )
  }

  async append(name: string, records: AppendInput[], options?: AppendOptions): Promise<AppendAck> {
    if (records.length !== 1) {
      throw ClientError.unsupported(
        `the Durable Streams protocol appends one message per request, got ${records.length}`,
      )
    }
    const envelope = toEnvelope(records[0]!)
    if (Object.keys(envelope.headers).length > 0) {
      throw ClientError.unsupported('the Durable Streams protocol has no record headers')
    }
    if (envelope.key !== undefined) {
      throw ClientError.unsupported('the Durable Streams protocol has no record keys')
    }
    const response = await expectDs(
      await this.http.send({
        method: 'POST',
        url: this.url(name),
        headers: { 'Content-Type': options?.contentType ?? DEFAULT_CONTENT_TYPE },
        body: toArrayBuffer(envelope.body),
        signal: options?.signal,
      }),
      [200, 204],
    )
    const next = header(response, H_STREAM_NEXT_OFFSET) ?? ''
    return { start: next, next }
  }

  async read(
    name: string,
    from: string,
    live: Live,
    limits: ReadLimits = {},
    options?: CallOptions,
  ): Promise<ReadPage> {
    return this.retry.run(
      () => this.readOnce(name, from, live, limits, options?.signal),
      retryableError,
      options?.signal,
    )
  }

  subscribe(name: string, from: string, options: SubscribeOptions = {}): AsyncIterable<SseEvent> {
    const http = this.http
    const streamUrl = (offset: string) => this.url(name, `?offset=${urlencode(offset)}&live=sse`)
    return subscribeLoop(from, options, {
      open: async (offset, lastEventId, signal) => {
        const headers: { [key: string]: string } = { Accept: 'text/event-stream' }
        if (lastEventId !== undefined) {
          headers['Last-Event-ID'] = lastEventId
        }
        return expectDs(
          await http.send({ method: 'GET', url: streamUrl(offset), headers, signal }),
          [200],
        )
      },
      onData: (raw, ctx): StreamRecord[] => [
        {
          position: '',
          headers: {},
          body: decodeDsData(raw.data, ctx.encoding),
        },
      ],
      onControl: parseDsControl,
      encodingOf: (response) => header(response, 'stream-sse-data-encoding') ?? 'raw',
    })
  }

  async list(_prefix: string, _limit = 0, _options?: CallOptions): Promise<StreamListing> {
    throw ClientError.unsupported(
      'the Durable Streams protocol has no stream listing; use protocol pico',
    )
  }

  async close(name: string, options?: CallOptions): Promise<string> {
    return this.retry.run(
      () => this.closeOnce(name, options?.signal),
      retryableError,
      options?.signal,
    )
  }

  async delete(name: string, options?: CallOptions): Promise<boolean> {
    return this.retry.run(
      () => this.deleteOnce(name, options?.signal),
      retryableError,
      options?.signal,
    )
  }

  private async createOnce(
    name: string,
    contentType: string,
    ttlSeconds: number | undefined,
    signal?: AbortSignal,
  ): Promise<boolean> {
    const headers: { [key: string]: string } = { 'Content-Type': contentType }
    if (ttlSeconds !== undefined) {
      headers[H_STREAM_TTL] = String(ttlSeconds)
    }
    const response = await expectDs(
      await this.http.send({ method: 'PUT', url: this.url(name), headers, signal }),
      [200, 201],
    )
    return response.status === 201
  }

  private async closeOnce(name: string, signal?: AbortSignal): Promise<string> {
    const response = await expectDs(
      await this.http.send({
        method: 'POST',
        url: this.url(name),
        headers: { [H_STREAM_CLOSED]: 'true' },
        signal,
      }),
      [200, 204],
    )
    return header(response, H_STREAM_NEXT_OFFSET) ?? ''
  }

  private async deleteOnce(name: string, signal?: AbortSignal): Promise<boolean> {
    const response = await this.http.send({ method: 'DELETE', url: this.url(name), signal })
    if (response.status === 404) {
      return false
    }
    await expectDs(response, [204])
    return true
  }

  private async headOnce(name: string, signal?: AbortSignal): Promise<StreamInfo | null> {
    const response = await this.http.send({ method: 'HEAD', url: this.url(name), signal })
    if (response.status === 404) {
      return null
    }
    await expectDs(response, [200])
    const info: StreamInfo = {
      name,
      start: OFFSET_BEGINNING,
      next: header(response, H_STREAM_NEXT_OFFSET) ?? '',
      closed: truthy(response, H_STREAM_CLOSED),
    }
    const contentType = header(response, 'content-type')
    if (contentType !== undefined) info.contentType = contentType
    const ttlSeconds = parseOptionalUint(header(response, H_STREAM_TTL))
    if (ttlSeconds !== undefined) info.ttlSeconds = ttlSeconds
    const expiresAt = header(response, H_STREAM_EXPIRES_AT)
    if (expiresAt !== undefined) info.expiresAt = expiresAt
    return info
  }

  private async readOnce(
    name: string,
    from: string,
    live: Live,
    _limits: ReadLimits,
    signal?: AbortSignal,
  ): Promise<ReadPage> {
    let query = `?offset=${urlencode(from)}`
    if (live === 'long-poll') {
      query += '&live=long-poll'
    }

    const response = await expectDs(
      await this.http.send({ method: 'GET', url: this.url(name, query), signal }),
      [200, 204],
    )

    const next = header(response, H_STREAM_NEXT_OFFSET) ?? from
    const upToDate = truthy(response, H_STREAM_UP_TO_DATE)
    const closed = truthy(response, H_STREAM_CLOSED)
    const empty = response.status === 204
    const body = empty ? new Uint8Array() : new Uint8Array(await response.arrayBuffer())

    const records: StreamRecord[] =
      empty || body.length === 0
        ? []
        : [
            {
              position: next,
              headers: {},
              body,
            },
          ]

    return {
      records,
      next,
      upToDate: upToDate || empty,
      closed,
    }
  }

  private url(name: string, query = ''): string {
    const path = name.startsWith('/') ? name : `/${name}`
    return `${this.baseUrl}${path}${query}`
  }
}

function parseDsControl(data: string): { next: string; upToDate: boolean; closed: boolean } {
  let parsed: {
    streamNextOffset?: string
    upToDate?: boolean
    streamClosed?: boolean
  }
  try {
    parsed = JSON.parse(data) as typeof parsed
  } catch {
    throw new ClientError('other', 'invalid sse control json', { code: 'invalid_response' })
  }
  return {
    next: parsed.streamNextOffset ?? '',
    upToDate: parsed.upToDate === true,
    closed: parsed.streamClosed === true,
  }
}

function decodeDsData(data: string, encoding: string): Uint8Array {
  switch (encoding) {
    case 'base64':
      return base64Decode(data)
    case 'json':
    case 'raw':
    default:
      return new TextEncoder().encode(data)
  }
}

async function expectDs(response: Response, expected: number[]): Promise<Response> {
  if (expected.includes(response.status)) {
    return response
  }

  const closed = truthy(response, H_STREAM_CLOSED)
  const epoch = header(response, H_DS_PRODUCER_EPOCH)
  const expectedSeq = header(response, H_DS_PRODUCER_EXPECTED_SEQ)
  const receivedSeq = header(response, H_DS_PRODUCER_RECEIVED_SEQ)
  const next = header(response, H_STREAM_NEXT_OFFSET) ?? null
  const body = await response.text().catch(() => '')

  let kind: ErrorKind
  let code: string
  switch (response.status) {
    case 400:
      kind = 'bad_request'
      code = 'bad_request'
      break
    case 401:
      kind = 'unauthenticated'
      code = 'unauthenticated'
      break
    case 403:
      if (epoch !== undefined) {
        kind = 'stale_epoch'
        code = 'stale_epoch'
      } else {
        kind = 'permission_denied'
        code = 'permission_denied'
      }
      break
    case 404:
      kind = 'not_found'
      code = 'not_found'
      break
    case 409:
      if (closed) {
        kind = 'closed'
        code = 'closed'
      } else if (expectedSeq !== undefined || receivedSeq !== undefined) {
        kind = 'conflict'
        code = 'sequence_conflict'
      } else {
        kind = 'conflict'
        code = 'conflict'
      }
      break
    case 410:
      kind = 'offset_gone'
      code = 'offset_gone'
      break
    default:
      kind = 'other'
      code = 'request_failed'
  }

  let message = body || undefined
  if (kind === 'stale_epoch' && epoch !== undefined) {
    message = `${message ?? 'stale producer epoch'} (current epoch ${epoch})`
  }
  if (expectedSeq !== undefined && receivedSeq !== undefined) {
    message = `${message ?? 'producer sequence gap'} (expected seq ${expectedSeq}, received ${receivedSeq})`
  }

  throw new ClientError(kind, message ?? code, {
    status: response.status,
    code,
    next,
  })
}
