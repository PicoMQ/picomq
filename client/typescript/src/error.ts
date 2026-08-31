export type ErrorKind =
  | 'not_found'
  | 'exists'
  | 'closed'
  | 'conflict'
  | 'stale_epoch'
  | 'unauthenticated'
  | 'permission_denied'
  | 'offset_gone'
  | 'bad_request'
  | 'transport'
  | 'unsupported'
  | 'aborted'
  | 'other'

export class ClientError extends Error {
  readonly kind: ErrorKind
  readonly status: number
  readonly code: string
  readonly next: string | null

  constructor(
    kind: ErrorKind,
    message: string,
    options: { status?: number; code?: string; next?: string | null } = {},
  ) {
    super(message)
    this.name = 'ClientError'
    this.kind = kind
    this.status = options.status ?? 0
    this.code = options.code ?? kind
    this.next = options.next ?? null
  }

  static transport(message: string): ClientError {
    return new ClientError('transport', message, { code: 'transport' })
  }

  static unsupported(message: string): ClientError {
    return new ClientError('unsupported', message, { code: 'unsupported' })
  }

  static aborted(message = 'Aborted'): ClientError {
    return new ClientError('aborted', message, { code: 'aborted' })
  }

  retryable(): boolean {
    if (this.kind === 'aborted' || this.kind === 'unsupported') return false
    return (
      this.kind === 'transport' ||
      this.status === 429 ||
      (this.status >= 500 && this.status <= 599)
    )
  }
}

export function isAbortError(error: unknown): boolean {
  if (error instanceof ClientError) return error.kind === 'aborted'
  if (error instanceof Error && error.name === 'AbortError') return true
  return false
}

export function abortError(reason: unknown): ClientError {
  if (reason instanceof ClientError && reason.kind === 'aborted') return reason
  return ClientError.aborted(reason instanceof Error ? reason.message : 'Aborted')
}

export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw abortError(signal.reason)
  }
}

export function asClientError(error: unknown): ClientError {
  if (error instanceof ClientError) return error
  if (isAbortError(error)) {
    return abortError(error)
  }
  return ClientError.transport(error instanceof Error ? error.message : String(error))
}
