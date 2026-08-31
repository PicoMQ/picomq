import { ClientError, type ErrorKind } from '../error'
import { base64Decode, parseOptionalUint, retryableError } from '../util'
import { toEnvelopes, type RecordEnvelope } from '../record'
import { RetryPolicy } from '../retry'
import { PicoStream } from '../stream'
import { header, Http, toArrayBuffer, truthy, urlencode } from '../transport/http'
import { subscribeLoop } from '../transport/subscribe'
import { CodecError, decodeBatchRead, encodeBatchAppend } from './codec'
import {
  CT_BATCH_BINARY,
  H_CLOSED,
  H_EXPIRES_AT,
  H_NEXT_SEQ,
  H_PRODUCER_EPOCH,
  H_PRODUCER_ID,
  H_PRODUCER_SEQ,
  H_START_SEQ,
  H_TIMESTAMP,
  H_TRIM_SEQ,
  H_TTL,
  H_UP_TO_DATE,
} from './headers'
import type {
  AppendAck,
  AppendInput,
  AppendOptions,
  CallOptions,
  Live,
  ProducerAck,
  ProducerRef,
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

export class PicoClient implements StreamApi {
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
    return 'pico'
  }

  beginning(): string {
    return '0'
  }

  now(): string {
    throw ClientError.unsupported(
      "the Pico protocol has no `now` token; read from the stream's next seq",
    )
  }

  stream(name: string): PicoStream {
    return new PicoStream(this, name)
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
    const response = await this.postBatch(name, toEnvelopes(records), {}, options?.signal)
    return parseAck(response)
  }

  async appendAs(
    name: string,
    records: AppendInput[],
    producer: ProducerRef,
    options?: CallOptions,
  ): Promise<ProducerAck> {
    const response = await this.postBatch(
      name,
      toEnvelopes(records),
      {
        [H_PRODUCER_ID]: producer.id,
        [H_PRODUCER_EPOCH]: String(producer.epoch),
        [H_PRODUCER_SEQ]: String(producer.seq),
      },
      options?.signal,
    )
    const applied = header(response, H_START_SEQ) !== undefined
    return {
      applied,
      duplicate: !applied && records.length > 0,
      ack: parseAck(response),
    }
  }

  async trim(name: string, seq: number, options?: CallOptions): Promise<string> {
    return this.retry.run(
      () => this.trimOnce(name, seq, options?.signal),
      retryableError,
      options?.signal,
    )
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
    const streamUrl = (offset: string) => this.url(name, `?seq=${urlencode(offset)}&live=sse`)
    return subscribeLoop(from, options, {
      open: async (offset, lastEventId, signal) => {
        const headers: { [key: string]: string } = { Accept: 'text/event-stream' }
        if (lastEventId !== undefined) {
          headers['Last-Event-ID'] = lastEventId
        }
        return expectPico(
          await http.send({ method: 'GET', url: streamUrl(offset), headers, signal }),
          [200],
        )
      },
      onData: (raw) => parsePicoData(raw.data),
      onControl: parsePicoControl,
    })
  }

  async list(prefix: string, limit = 0, options?: CallOptions): Promise<StreamListing> {
    return this.retry.run(
      () => this.listOnce(prefix, limit, options?.signal),
      retryableError,
      options?.signal,
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

  private async postBatch(
    name: string,
    envelopes: RecordEnvelope[],
    extraHeaders: { [key: string]: string },
    signal?: AbortSignal,
  ): Promise<Response> {
    const payload = encodeBatchAppend(envelopes)
    return expectPico(
      await this.http.send({
        method: 'POST',
        url: this.url(name),
        headers: { 'Content-Type': CT_BATCH_BINARY, ...extraHeaders },
        body: toArrayBuffer(payload),
        signal,
      }),
      [200],
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
      headers[H_TTL] = String(ttlSeconds)
    }
    const response = await expectPico(
      await this.http.send({ method: 'PUT', url: this.url(name), headers, signal }),
      [200, 201],
    )
    return response.status === 201
  }

  private async headOnce(name: string, signal?: AbortSignal): Promise<StreamInfo | null> {
    const response = await this.http.send({ method: 'HEAD', url: this.url(name), signal })
    if (response.status === 404) {
      return null
    }
    await expectPico(response, [200])
    const info: StreamInfo = {
      name,
      start: header(response, H_START_SEQ) ?? '0',
      next: header(response, H_NEXT_SEQ) ?? '0',
      closed: truthy(response, H_CLOSED),
    }
    const contentType = header(response, 'content-type')
    if (contentType !== undefined) info.contentType = contentType
    const ttlSeconds = parseOptionalUint(header(response, H_TTL))
    if (ttlSeconds !== undefined) info.ttlSeconds = ttlSeconds
    const expiresAt = header(response, H_EXPIRES_AT)
    if (expiresAt !== undefined) info.expiresAt = expiresAt
    return info
  }

  private async trimOnce(name: string, seq: number, signal?: AbortSignal): Promise<string> {
    const response = await expectPico(
      await this.http.send({
        method: 'POST',
        url: this.url(name),
        headers: { [H_TRIM_SEQ]: String(seq) },
        signal,
      }),
      [200],
    )
    return header(response, H_START_SEQ) ?? '0'
  }

  private async closeOnce(name: string, signal?: AbortSignal): Promise<string> {
    const response = await expectPico(
      await this.http.send({
        method: 'POST',
        url: this.url(name),
        headers: { [H_CLOSED]: 'true' },
        signal,
      }),
      [200],
    )
    return header(response, H_NEXT_SEQ) ?? '0'
  }

  private async deleteOnce(name: string, signal?: AbortSignal): Promise<boolean> {
    const response = await this.http.send({ method: 'DELETE', url: this.url(name), signal })
    if (response.status === 404) {
      return false
    }
    await expectPico(response, [204])
    return true
  }

  private async readOnce(
    name: string,
    from: string,
    live: Live,
    limits: ReadLimits,
    signal?: AbortSignal,
  ): Promise<ReadPage> {
    let query = `?format=binary&seq=${urlencode(from)}`
    if (limits.count !== undefined && limits.count > 0) {
      query += `&count=${limits.count}`
    }
    if (limits.bytes !== undefined && limits.bytes > 0) {
      query += `&bytes=${limits.bytes}`
    }
    if (live === 'long-poll') {
      query += '&live=long-poll'
    }

    const response = await this.http.send({ method: 'GET', url: this.url(name, query), signal })
    await expectPico(response, live === 'off' ? [200] : [200, 204])

    const next = header(response, H_NEXT_SEQ) ?? from
    const upToDate = truthy(response, H_UP_TO_DATE)
    const closed = truthy(response, H_CLOSED)
    const empty = response.status === 204
    const body = empty ? new Uint8Array() : new Uint8Array(await response.arrayBuffer())

    let records: StreamRecord[] = []
    if (!empty && body.length > 0) {
      try {
        records = decodeBatchRead(body).map((record) => ({
          position: record.seq.toString(),
          timestamp: Number(record.envelope.timestamp),
          headers: record.envelope.headers,
          body: record.envelope.body,
        }))
      } catch (err) {
        throw new ClientError(
          'other',
          err instanceof CodecError ? err.message : 'invalid_response',
          { code: 'invalid_response' },
        )
      }
    }

    return {
      records,
      next,
      upToDate: upToDate || (empty && live === 'long-poll'),
      closed,
    }
  }

  private async listOnce(
    prefix: string,
    limit: number,
    signal?: AbortSignal,
  ): Promise<StreamListing> {
    let query = `?prefix=${urlencode(prefix)}`
    if (limit > 0) {
      query += `&limit=${limit}`
    }
    const response = await expectPico(
      await this.http.send({ method: 'GET', url: this.url('/', query), signal }),
      [200],
    )
    const body = (await response.json()) as {
      streams?: Array<{
        name?: string
        content_type?: string
        start_seq?: number
        next_seq?: number
        closed?: boolean
        ttl?: number
        expires_at?: string
      }>
      has_more?: boolean
    }

    return {
      streams: (body.streams ?? []).map((node) => {
        const info: StreamInfo = {
          name: node.name ?? '',
          start: String(node.start_seq ?? 0),
          next: String(node.next_seq ?? 0),
          closed: node.closed ?? false,
        }
        if (node.content_type !== undefined) info.contentType = node.content_type
        if (node.ttl !== undefined) info.ttlSeconds = node.ttl
        if (node.expires_at !== undefined) info.expiresAt = node.expires_at
        return info
      }),
      hasMore: body.has_more ?? false,
    }
  }

  private url(name: string, query = ''): string {
    const path = name.startsWith('/') ? name : `/${name}`
    return `${this.baseUrl}${path}${query}`
  }
}

function parseAck(response: Response): AppendAck {
  const next = header(response, H_NEXT_SEQ) ?? '0'
  const ack: AppendAck = {
    start: header(response, H_START_SEQ) ?? next,
    next,
  }
  const timestamp = parseOptionalUint(header(response, H_TIMESTAMP))
  if (timestamp !== undefined) ack.timestamp = timestamp
  return ack
}

async function expectPico(response: Response, expected: number[]): Promise<Response> {
  if (expected.includes(response.status)) {
    return response
  }

  const closed = truthy(response, H_CLOSED)
  const body = await response.text().catch(() => '')
  let parsed: { error?: string; message?: string; next_seq?: number } = {}
  try {
    parsed = JSON.parse(body) as typeof parsed
  } catch {
    parsed = {}
  }

  const code = parsed.error ?? `http_${response.status}`
  const message =
    parsed.message ?? (body && Object.keys(parsed).length === 0 ? body : undefined) ?? code
  const next = parsed.next_seq !== undefined ? String(parsed.next_seq) : null

  throw new ClientError(kind(response.status, code, closed), message, {
    status: response.status,
    code,
    next,
  })
}

function kind(status: number, code: string, closed: boolean): ErrorKind {
  switch (status) {
    case 400:
      return 'bad_request'
    case 401:
      return 'unauthenticated'
    case 403:
      return code === 'permission_denied' ? 'permission_denied' : 'stale_epoch'
    case 404:
      return 'not_found'
    case 409:
      return closed || code === 'closed' ? 'closed' : 'conflict'
    case 410:
      return 'offset_gone'
    case 412:
      return 'conflict'
    default:
      return 'other'
  }
}

function parsePicoData(data: string): StreamRecord[] {
  let parsed: unknown
  try {
    parsed = JSON.parse(data)
  } catch {
    throw new ClientError('other', 'invalid sse data json', { code: 'invalid_response' })
  }
  if (!Array.isArray(parsed)) {
    throw new ClientError('other', 'sse data must be a json array', { code: 'invalid_response' })
  }
  return parsed.map((node) => {
    const row = node as {
      seq?: number | string
      timestamp?: number
      headers?: { [key: string]: string }
      body?: string
      body_b64?: string
    }
    const record: StreamRecord = {
      position: String(row.seq ?? ''),
      headers: row.headers ?? {},
      body:
        row.body_b64 !== undefined
          ? base64Decode(row.body_b64)
          : new TextEncoder().encode(row.body ?? ''),
    }
    if (row.timestamp !== undefined) record.timestamp = row.timestamp
    return record
  })
}

function parsePicoControl(data: string): { next: string; upToDate: boolean; closed: boolean } {
  let parsed: { next_seq?: number | string; up_to_date?: boolean; closed?: boolean }
  try {
    parsed = JSON.parse(data) as typeof parsed
  } catch {
    throw new ClientError('other', 'invalid sse control json', { code: 'invalid_response' })
  }
  return {
    next: String(parsed.next_seq ?? '0'),
    upToDate: parsed.up_to_date === true,
    closed: parsed.closed === true,
  }
}
