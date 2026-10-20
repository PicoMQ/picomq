import { header } from './transport/http'
import { parseOptionalUint } from './util'
import type { CreateOptions, StreamInfo, TopicConfig } from './types'

const DEFAULT_CONTENT_TYPE = 'application/octet-stream'

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

export function readSettings(
  response: Response,
  prefix: 'Pico' | 'Stream',
  info: StreamInfo,
): void {
  const retentionMs = parseOptionalUint(header(response, `${prefix}-Retention-Ms`))
  if (retentionMs !== undefined) info.retentionMs = retentionMs
  const retentionBytes = parseOptionalUint(header(response, `${prefix}-Retention-Bytes`))
  if (retentionBytes !== undefined) info.retentionBytes = retentionBytes
  const schema = header(response, `${prefix}-Schema`)
  if (schema !== undefined) info.schema = schema
}

export function decodeConfig(body: unknown): TopicConfig {
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
