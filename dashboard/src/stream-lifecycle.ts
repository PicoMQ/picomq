import { streamPath, type Connection } from './streams'

export interface CreateStreamOptions {
  name: string
  contentType: string
  kafkaTopic: string
}

export class StreamMutationError extends Error {
  constructor(message: string, readonly status = 0, readonly uncertain = false) {
    super(message)
  }
}

export function lifecycleStreamPath(name: string): string {
  const path = streamPath(name)
  // Keep lifecycle forms focused on ordinary streams, outside reserved
  // schema/config/group/system namespaces used by the protocol listener.
  if (/^\/_(?:sys|schemas|streams|groups)(?:\/|$)/.test(path)) {
    throw new Error('Choose a stream outside the reserved /_sys, /_schemas, /_streams and /_groups paths.')
  }
  return path
}

export function validateCreateStream(options: CreateStreamOptions): CreateStreamOptions {
  const name = lifecycleStreamPath(options.name.trim())
  const contentType = options.contentType.trim()
  const kafkaTopic = options.kafkaTopic.trim()
  if (!contentType || contentType.length > 1024 || /[^\x20-\x7e]/.test(contentType)
    || !/^[!#$%&'*+.^_`|~\w-]+\/[!#$%&'*+.^_`|~\w-]+(?:\s*;.*)?$/.test(contentType)) {
    throw new Error('Enter a content type such as application/json or text/plain.')
  }
  if (kafkaTopic && (kafkaTopic.length > 249 || !/^[A-Za-z0-9._-]+$/.test(kafkaTopic) || kafkaTopic === '.' || kafkaTopic === '..')) {
    throw new Error('Kafka aliases must be 1–249 letters, digits, dots, underscores or hyphens. A single or double dot is not allowed.')
  }
  return { name, contentType, kafkaTopic }
}

async function conflictDetail(response: Response): Promise<string> {
  const reader = response.body?.getReader()
  if (!reader) return ''
  try {
    let text = ''
    let size = 0
    const decoder = new TextDecoder()
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      size += value.byteLength
      if (size > 4096) return ''
      text += decoder.decode(value, { stream: true })
    }
    const detail = JSON.parse(text + decoder.decode())
    if (detail.code === 'owner_required') {
      const owner = Number.isInteger(detail.ownerNodeId) && detail.ownerNodeId >= -2147483648 && detail.ownerNodeId <= 2147483647
        ? `node ${detail.ownerNodeId}` : 'the stream owner'
      return `Connect Admin connection to ${owner}’s admin listener, then review the request again. The request was not forwarded or retried.`
    }
    if (detail.code === 'transfer_pending') return 'A stream transfer is pending. Wait for it to finish, refresh ownership, and review the request again.'
  } catch { /* An unreadable error body does not change the authoritative 409. */ }
  finally { void reader.cancel().catch(() => {}) }
  return ''
}

async function mutate(connection: Connection, name: string, method: 'PUT' | 'DELETE', headers: Headers, signal?: AbortSignal) {
  if (signal?.aborted) throw signal.reason
  if (connection.token) headers.set('Authorization', `Bearer ${connection.token}`)
  const deadline = AbortSignal.timeout(15_000)
  let response: Response
  try {
    response = await fetch(`${connection.endpoint.replace(/\/$/, '')}/admin/streams/${name.slice(1).split('/').map(encodeURIComponent).join('/')}`, {
      method, headers, redirect: 'manual', cache: 'no-store',
      signal: signal ? AbortSignal.any([signal, deadline]) : deadline,
    })
  } catch {
    throw new StreamMutationError(deadline.aborted
      ? 'The stream request timed out after 15 seconds.'
      : 'The stream request was interrupted. Check the endpoint and its CORS or same-origin proxy configuration.', 0, true)
  }
  // Only structured owner/transfer conflicts need a body; read at most 4 KiB.
  const conflict = response.status === 409 ? await conflictDetail(response) : ''
  if (response.status !== 409) void response.body?.cancel().catch(() => {})
  if (response.type === 'opaqueredirect' || (response.status >= 300 && response.status < 400)) {
    throw new StreamMutationError('The endpoint redirected. Connect Admin connection to the owner’s admin listener; credentials were not forwarded.', response.status, true)
  }
  if (!response.ok) {
    const detail = response.status === 401 ? 'A valid admin API token is required.'
      : response.status === 403 ? 'The token needs the admin audience, matching create or delete permission, and a matching stream scope.'
        : response.status === 404 ? 'The stream was not found at this endpoint.'
          : response.status === 409 ? conflict || 'The stream configuration or Kafka alias conflicts with an existing stream.'
            : 'The admin API rejected the request.'
    throw new StreamMutationError(`${response.status}: ${detail}`, response.status, response.status === 408 || response.status >= 500)
  }
  return response
}

/** Admin PUT reuses native lifecycle semantics, with no records or retry. */
export async function createStream(connection: Connection, options: CreateStreamOptions, signal?: AbortSignal) {
  const draft = validateCreateStream(options)
  const headers = new Headers({ 'Content-Type': draft.contentType })
  if (draft.kafkaTopic) headers.set('Pico-Kafka-Topic', draft.kafkaTopic)
  const response = await mutate(connection, draft.name, 'PUT', headers, signal)
  if (![200, 201].includes(response.status) || !/^\d+$/.test(response.headers.get('Pico-Next-Seq') || '')) {
    throw new StreamMutationError('The endpoint did not return a valid Pico create response. Check the admin endpoint and exposed response headers.', response.status, true)
  }
  return { created: response.status === 201 }
}

/** Admin DELETE. A missing stream is reported as 404, not success. */
export async function deleteStream(connection: Connection, name: string, signal?: AbortSignal) {
  const response = await mutate(connection, lifecycleStreamPath(name), 'DELETE', new Headers(), signal)
  if (response.status !== 204) {
    throw new StreamMutationError('The endpoint did not return the expected Pico delete response.', response.status, true)
  }
}
