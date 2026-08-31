import { CodecError, decodeBatchRead, encodeBatchAppend, type RecordEnvelope } from './codec'
import { ClientError } from './error'
import { expectStatus, header, Http, toArrayBuffer, truthy, urlencode } from './http'
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
import { RetryPolicy } from './retry'
import { subscribeLoop } from './subscribe'
import { base64Decode, parseOptionalUint, retryableError } from './util'
import type {
  AppendAck,
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
} from './types'

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

export class PicoClient implements StreamApi {
  private readonly http: Http
  private readonly baseUrl: string
  private readonly retry: RetryPolicy

  constructor(endpoint: string, token?: string, http2 = false, retry: RetryPolicy = RetryPolicy.none()) {
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

  async create(
    name: string,
    contentType: string,
    ttlSeconds?: number,
    options?: CallOptions,
  ): Promise<boolean> {
    const headers: { [key: string]: string } = { 'Content-Type': contentType }
    if (ttlSeconds !== undefined) {
      headers[H_TTL] = String(ttlSeconds)
    }
    const response = await expectStatus(
      await this.http.send({ method: 'PUT', url: this.url(name), headers, signal: options?.signal }),
      [200, 201],
    )
    return response.status === 201
  }

  async head(name: string, options?: CallOptions): Promise<StreamInfo | null> {
    return this.retry.run(
      () => this.headOnce(name, options?.signal),
      retryableError,
      options?.signal,
    )
  }

  async append(
    name: string,
    records: Uint8Array[],
    _contentType: string,
    options?: CallOptions,
  ): Promise<AppendAck> {
    const envelopes: RecordEnvelope[] = records.map((body) => ({
      timestamp: 0n,
      headers: {},
      body,
    }))
    const payload = encodeBatchAppend(envelopes)
    const response = await expectStatus(
      await this.http.send({
        method: 'POST',
        url: this.url(name),
        headers: { 'Content-Type': CT_BATCH_BINARY },
        body: toArrayBuffer(payload),
        signal: options?.signal,
      }),
      [200],
    )
    const next = header(response, H_NEXT_SEQ) ?? '0'
    const ack: AppendAck = {
      start: header(response, H_START_SEQ) ?? next,
      next,
    }
    const timestamp = parseOptionalUint(header(response, H_TIMESTAMP))
    if (timestamp !== undefined) ack.timestamp = timestamp
    return ack
  }

  async appendAs(
    name: string,
    records: Uint8Array[],
    producer: ProducerRef,
    options?: CallOptions,
  ): Promise<ProducerAck> {
    const envelopes: RecordEnvelope[] = records.map((body) => ({
      timestamp: 0n,
      headers: {},
      body,
    }))
    const payload = encodeBatchAppend(envelopes)
    const response = await expectStatus(
      await this.http.send({
        method: 'POST',
        url: this.url(name),
        headers: {
          'Content-Type': CT_BATCH_BINARY,
          [H_PRODUCER_ID]: producer.id,
          [H_PRODUCER_EPOCH]: String(producer.epoch),
          [H_PRODUCER_SEQ]: String(producer.seq),
        },
        body: toArrayBuffer(payload),
        signal: options?.signal,
      }),
      [200],
    )
    const next = header(response, H_NEXT_SEQ) ?? '0'
    const start = header(response, H_START_SEQ)
    const applied = start !== undefined
    const ack: AppendAck = {
      start: start ?? next,
      next,
    }
    const timestamp = parseOptionalUint(header(response, H_TIMESTAMP))
    if (timestamp !== undefined) ack.timestamp = timestamp
    return {
      applied,
      duplicate: !applied && records.length > 0,
      ack,
    }
  }

  async trim(name: string, seq: number, options?: CallOptions): Promise<string> {
    const response = await expectStatus(
      await this.http.send({
        method: 'POST',
        url: this.url(name),
        headers: { [H_TRIM_SEQ]: String(seq) },
        signal: options?.signal,
      }),
      [200],
    )
    return header(response, H_START_SEQ) ?? '0'
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
        return expectStatus(
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
    const response = await expectStatus(
      await this.http.send({
        method: 'POST',
        url: this.url(name),
        headers: { [H_CLOSED]: 'true' },
        signal: options?.signal,
      }),
      [200],
    )
    return header(response, H_NEXT_SEQ) ?? '0'
  }

  async delete(name: string, options?: CallOptions): Promise<boolean> {
    const response = await this.http.send({
      method: 'DELETE',
      url: this.url(name),
      signal: options?.signal,
    })
    if (response.status === 404) {
      return false
    }
    await expectStatus(response, [204])
    return true
  }

  private async headOnce(name: string, signal?: AbortSignal): Promise<StreamInfo | null> {
    const response = await this.http.send({ method: 'HEAD', url: this.url(name), signal })
    if (response.status === 404) {
      return null
    }
    await expectStatus(response, [200])
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
    await expectStatus(response, live === 'off' ? [200] : [200, 204])

    const next = header(response, H_NEXT_SEQ) ?? from
    const upToDate = truthy(response, H_UP_TO_DATE)
    const closed = truthy(response, H_CLOSED)
    const empty = response.status === 204
    const body = empty ? new Uint8Array() : new Uint8Array(await response.arrayBuffer())

    let records: StreamRecord[] = []
    if (!empty && body.length > 0) {
      try {
        records = decodeBatchRead(body).map((record) => {
          const out: StreamRecord = {
            position: record.seq.toString(),
            headers: record.envelope.headers,
            body: record.envelope.body,
          }
          out.timestamp = Number(record.envelope.timestamp)
          return out
        })
      } catch (err) {
        throw new ClientError('other', err instanceof CodecError ? err.message : 'invalid_response', {
          code: 'invalid_response',
        })
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
    const response = await expectStatus(
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
