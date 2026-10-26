const TOKEN_KEY = 'picomq.token'

type Method = 'GET' | 'POST' | 'PATCH' | 'DELETE'

export type Segments = readonly (string | number)[]

export type Query = Record<string, string | number | boolean | undefined>

interface Options {
  query?: Query
  body?: unknown
  signal?: AbortSignal
}

export class ApiError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

export class AuthRequired extends ApiError {}

export const token = {
  get: (): string | null => sessionStorage.getItem(TOKEN_KEY),
  set: (value: string): void => sessionStorage.setItem(TOKEN_KEY, value),
  clear: (): void => sessionStorage.removeItem(TOKEN_KEY),
}

export function admin(...segments: Segments): string {
  return ['/admin', ...segments.map((segment) => encodeURIComponent(segment))].join('/')
}

export async function request<T>(method: Method, path: string, options: Options = {}): Promise<T> {
  const response = await send(method, path, options)

  if (response.status === 204) {
    return undefined as T
  }
  return (await response.json()) as T
}

export async function requestText(path: string, options: Options = {}): Promise<string> {
  const response = await send('GET', path, options)

  return response.text()
}

async function send(method: Method, path: string, { query, body, signal }: Options): Promise<Response> {
  const headers = new Headers()
  const bearer = token.get()
  if (bearer) {
    headers.set('Authorization', `Bearer ${bearer}`)
  }
  if (body !== undefined) {
    headers.set('Content-Type', 'application/json')
  }

  const response = await fetch(url(path, query), {
    method,
    headers,
    body: body === undefined ? null : JSON.stringify(body),
    signal: signal ?? null,
  })
  if (response.ok) {
    return response
  }

  const message = await failure(response)
  if (response.status === 401) {
    throw new AuthRequired(response.status, message)
  }
  throw new ApiError(response.status, message)
}

function url(path: string, query: Query = {}): string {
  const params = new URLSearchParams()
  for (const [name, value] of Object.entries(query)) {
    if (value !== undefined) {
      params.set(name, String(value))
    }
  }

  const search = params.toString()
  return search ? `${path}?${search}` : path
}

async function failure(response: Response): Promise<string> {
  const body: unknown = await response.json().catch(() => null)

  if (typeof body === 'object' && body !== null && 'error' in body && typeof body.error === 'string') {
    return body.error
  }
  return `${response.status} ${response.statusText}`.trim()
}
