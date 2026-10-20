import { ClientError, type ErrorKind } from './error'
import { RetryPolicy } from './retry'
import { decodeConfig } from './settings'
import { header, namePath, type Http } from './transport/http'
import { retryableError } from './util'
import type { CallOptions, ConfigUpdate, PutSchemaOptions, Schema, TopicConfig } from './types'

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

  async putSchema(
    name: string,
    body: Uint8Array | string,
    options?: PutSchemaOptions,
  ): Promise<void> {
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
    return `${this.baseUrl}${root}${namePath(name)}`
  }
}

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
  const kind = kinds[response.status]
  const code = kind ?? `http_${response.status}`
  throw new ClientError(kind ?? 'other', message || code, {
    status: response.status,
    code,
  })
}
