import { ClientError, isAbortError } from '../error'
import { Routes } from './routes'

const MAX_REDIRECT_HOPS = 5

export interface HttpRequest {
  method: string
  url: string
  headers?: HeadersInit
  body?: BodyInit | ArrayBuffer | null
  signal?: AbortSignal | undefined
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

export class Http {
  private fetch: FetchLike | undefined
  private agentClose: (() => Promise<void>) | undefined
  private initPromise: Promise<void> | undefined
  private readonly routes: Routes

  constructor(
    endpoint: string,
    private readonly token?: string,
    http2 = false,
  ) {
    this.routes = new Routes(endpoint)
    if (!http2) {
      this.fetch = (url, init) => globalThis.fetch(url, init as RequestInit)
    }
  }

  async close(): Promise<void> {
    const closer = this.agentClose
    this.agentClose = undefined
    if (closer) await closer()
  }

  async send(req: HttpRequest): Promise<Response> {
    const path = rawPath(req.url)
    if (path.split('/').some((segment) => segment === '.' || segment === '..')) {
      const message = `${path} has a . or .. segment, which a URL cannot carry`
      throw new ClientError('bad_request', message, { code: 'bad_request' })
    }

    await this.ensureFetch()
    const fetch = this.fetch!
    const key = this.routes.key(req.url)
    let url = key === undefined ? req.url : this.routes.resolve(key, req.url)
    const headers = new Headers(req.headers)
    if (this.token && !headers.has('Authorization')) {
      headers.set('Authorization', `Bearer ${this.token}`)
    }

    for (let hop = 0; hop < MAX_REDIRECT_HOPS; hop++) {
      let response: Response
      try {
        const init: RequestInit = {
          method: req.method,
          headers,
          body: (req.body ?? null) as BodyInit | null,
          redirect: 'manual',
        }
        if (req.signal !== undefined) {
          init.signal = req.signal
        }
        response = await fetch(url, init)
      } catch (err) {
        if (isAbortError(err) || req.signal?.aborted) {
          throw ClientError.aborted(err instanceof Error ? err.message : 'Aborted')
        }
        if (key !== undefined) this.routes.forget(key)
        throw ClientError.transport(err instanceof Error ? err.message : String(err))
      }

      if (response.status !== 307 && response.status !== 308) {
        if (key !== undefined && req.method === 'DELETE' && response.ok) {
          this.routes.forget(key)
        }
        return response
      }

      const location = response.headers.get('location')
      if (!location) {
        throw new ClientError('other', 'redirect_without_location', {
          status: response.status,
          code: 'redirect_without_location',
        })
      }
      const next = new URL(location, url).toString()
      if (key !== undefined) this.routes.learn(key, next)
      url = next
    }

    throw new ClientError('other', 'too_many_redirects', { code: 'too_many_redirects' })
  }

  private ensureFetch(): Promise<void> {
    if (this.fetch) return Promise.resolve()
    if (!this.initPromise) {
      this.initPromise = this.initHttp2()
    }
    return this.initPromise
  }

  private async initHttp2(): Promise<void> {
    if (typeof process === 'undefined' || process.versions?.node === undefined) {
      throw ClientError.unsupported('http2 requires Node.js')
    }
    const undici = await import('undici')
    const agent = new undici.Agent({
      connections: 1,
      allowH2: true,
      ...({ useH2c: true } as object),
    })
    this.agentClose = async () => {
      await agent.close()
    }
    this.fetch = (url, init) =>
      undici.fetch(url, {
        method: init?.method,
        headers: init?.headers,
        body: init?.body ?? null,
        signal: init?.signal ?? undefined,
        dispatcher: agent,
        redirect: 'manual',
      } as never) as unknown as Promise<Response>
  }
}

export function header(response: Response, name: string): string | undefined {
  const value = response.headers.get(name)
  return value === null ? undefined : value
}

export function truthy(response: Response, name: string): boolean {
  const value = header(response, name)
  return value !== undefined && value.toLowerCase() === 'true'
}

export function urlencode(value: string): string {
  return encodeURIComponent(value)
}

function rawPath(url: string): string {
  return url.replace(/^[a-z][a-z0-9+.-]*:\/\/[^/?#]*/i, '').split(/[?#]/)[0] ?? ''
}

export function namePath(name: string): string {
  return `/${name.replace(/^\//, '').split('/').map(urlencode).join('/')}`
}

export function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
}
