import { ClientError, isAbortError } from '../error'

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

  constructor(
    private readonly token?: string,
    http2 = false,
  ) {
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
    await this.ensureFetch()
    const fetch = this.fetch!
    let url = req.url
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
        throw ClientError.transport(err instanceof Error ? err.message : String(err))
      }

      if (response.status !== 307 && response.status !== 308) {
        return response
      }

      const location = response.headers.get('location')
      if (!location) {
        throw new ClientError('other', 'redirect_without_location', {
          status: response.status,
          code: 'redirect_without_location',
        })
      }
      url = new URL(location, url).toString()
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

export function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
}
