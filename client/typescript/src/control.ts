import { ClientError, type ErrorKind } from './error'
import { RetryPolicy } from './retry'
import { header, type Http } from './transport/http'
import { parseOptionalUint, retryableError } from './util'
import type {
  CallOptions,
  ConfigUpdate,
  CreateOptions,
  PutSchemaOptions,
  Schema,
  StreamInfo,
  TopicConfig,
} from './types'

const DEFAULT_CONTENT_TYPE = 'application/octet-stream'

/**
 * The control routes, `/_streams` and `/_schemas`, which are the same on
 * either protocol.
 */
export class Control {
  constructor(
    private readonly http: Http,
    private readonly baseUrl: string,
    private readonly retry: RetryPolicy,
  ) {}

  config(name: string, options?: CallOptions): Promise<TopicConfig | null> {
    return this.retry.run(
      async () => {
        const response = await this.http.send({
          method: 'GET',
          url: this.url('/_streams', name),
          signal: options?.signal,
        })
        if (response.status === 404) return null
        return decodeConfig(await (await expectControl(response, [200])).json())
      },
      retryableError,
      options?.signal,
    )
  }

  async updateConfig(
    name: string,
    update: ConfigUpdate,
    options?: CallOptions,
  ): Promise<TopicConfig> {
    const response = await this.http.send({
      method: 'PATCH',
      url: this.url('/_streams', name),
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(update),
      signal: options?.signal,
    })
    return decodeConfig(await (await expectControl(response, [200])).json())
  }

  async putSchema(name: string, body: Uint8Array | string, options?: PutSchemaOptions): Promise<void> {
    const headers: { [key: string]: string } = {}
    if (options?.contentType !== undefined) headers['Content-Type'] = options.contentType
    const response = await this.http.send({
      method: 'PUT',
      url: this.url('/_schemas', name),
      headers,
      body: body as BodyInit,
      signal: options?.signal,
    })
    await expectControl(response, [204])
  }

  getSchema(name: string, options?: CallOptions): Promise<Schema | null> {
    return this.retry.run(
      async () => {
        const response = await this.http.send({
          method: 'GET',
          url: this.url('/_schemas', name),
          signal: options?.signal,
        })
        if (response.status === 404) return null
        await expectControl(response, [200])
        const schema: Schema = { body: new Uint8Array(await response.arrayBuffer()) }
        const contentType = header(response, 'content-type')
        if (contentType !== undefined) schema.contentType = contentType
        return schema
      },
      retryableError,
      options?.signal,
    )
  }

  async deleteSchema(name: string, options?: CallOptions): Promise<boolean> {
    const response = await this.http.send({
      method: 'DELETE',
      url: this.url('/_schemas', name),
      signal: options?.signal,
    })
    await expectControl(response, [204, 404])
    return response.status === 204
  }

  private url(root: string, name: string): string {
    return `${this.baseUrl}${root}/${name.replace(/^\/+/, '')}`
  }
}

/** A create's settings as headers, named with the protocol's prefix. */
export function createHeaders(prefix: 'Pico' | 'Stream', options?: CreateOptions): {
  [key: string]: string
} {
  const headers: { [key: string]: string } = {
    'Content-Type': options?.contentType ?? DEFAULT_CONTENT_TYPE,
  }
  const set = (name: string, value: number | string | undefined) => {
    if (value !== undefined) headers[`${prefix}-${name}`] = String(value)
  }
  set('Partitions', options?.partitions)
  set('TTL', options?.ttlSeconds)
  set('Expires-At', options?.expiresAt)
  set('Retention-Ms', options?.retentionMs)
  set('Retention-Bytes', options?.retentionBytes)
  set('Schema', options?.schema)
  if (options?.schemaValidate) set('Schema-Validate', 'true')
  if (options?.closed) set('Closed', 'true')
  return headers
}

/** Fills the settings a head reports into `info`. */
export function readSettings(response: Response, prefix: 'Pico' | 'Stream', info: StreamInfo): void {
  const retentionMs = parseOptionalUint(header(response, `${prefix}-Retention-Ms`))
  if (retentionMs !== undefined) info.retentionMs = retentionMs
  const retentionBytes = parseOptionalUint(header(response, `${prefix}-Retention-Bytes`))
  if (retentionBytes !== undefined) info.retentionBytes = retentionBytes
  const schema = header(response, `${prefix}-Schema`)
  if (schema !== undefined) info.schema = schema
}

function decodeConfig(body: unknown): TopicConfig {
  const node = (body ?? {}) as { [key: string]: unknown }
  const config: TopicConfig = {
    schemaValidate: node.schemaValidate === true,
    partitions: typeof node.partitions === 'number' ? node.partitions : 1,
  }
  if (typeof node.schema === 'string') config.schema = node.schema
  if (typeof node.kafkaTopic === 'string') config.kafkaTopic = node.kafkaTopic
  if (typeof node.retentionMs === 'number') config.retentionMs = node.retentionMs
  if (typeof node.retentionBytes === 'number') config.retentionBytes = node.retentionBytes
  return config
}

/** Control route errors are `{"error": "<message>"}` on either protocol. */
async function expectControl(response: Response, expected: number[]): Promise<Response> {
  if (expected.includes(response.status)) return response

  const body = await response.text().catch(() => '')
  let message = body
  try {
    const parsed = JSON.parse(body) as { error?: unknown }
    if (typeof parsed.error === 'string') message = parsed.error
  } catch {
    message = body
  }
  const kinds: { [status: number]: ErrorKind } = {
    400: 'bad_request',
    401: 'unauthenticated',
    403: 'permission_denied',
    404: 'not_found',
    409: 'conflict',
  }
  const code = `http_${response.status}`
  throw new ClientError(kinds[response.status] ?? 'other', message || code, {
    status: response.status,
    code,
  })
}
