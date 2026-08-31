import { ClientError, isAbortError, throwIfAborted } from '../error'
import { sleep } from '../util'
import { iterateSse, type RawSseEvent } from './sse'
import type { SseEvent, StreamRecord, SubscribeOptions } from '../types'

export interface SubscribeHooks {
  open(
    offset: string,
    lastEventId: string | undefined,
    signal: AbortSignal | undefined,
  ): Promise<Response>
  onData(raw: RawSseEvent, ctx: { offset: string; encoding: string }): StreamRecord[]
  onControl(data: string): { next: string; upToDate: boolean; closed: boolean }
  encodingOf?(response: Response): string
}

class Reconnect {
  private drops = 0

  constructor(
    private readonly enabled: boolean,
    private readonly maxAttempts: number | undefined,
    private readonly initialDelayMs: number,
    private readonly maxDelayMs: number,
    private readonly signal: AbortSignal | undefined,
  ) {}

  reset(): void {
    this.drops = 0
  }

  async recover(error: unknown): Promise<void> {
    if (isAbortError(error) || this.signal?.aborted) {
      throwIfAborted(this.signal)
      throw error
    }
    if (!this.enabled) throw error
    this.drops += 1
    if (this.exhausted()) throw error
    await this.wait()
  }

  async idle(): Promise<boolean> {
    if (!this.enabled) return false
    this.drops += 1
    if (this.exhausted()) return false
    await this.wait()
    return true
  }

  private exhausted(): boolean {
    return this.maxAttempts !== undefined && this.drops > this.maxAttempts
  }

  private wait(): Promise<void> {
    const delay = Math.min(this.maxDelayMs, this.initialDelayMs * 2 ** Math.max(0, this.drops - 1))
    return sleep(delay, this.signal)
  }
}

export function subscribeLoop(
  from: string,
  options: SubscribeOptions,
  hooks: SubscribeHooks,
): AsyncIterable<SseEvent> {
  const reconnectEnabled = options.reconnect ?? true
  const signal = options.signal

  return {
    async *[Symbol.asyncIterator]() {
      const reconnect = new Reconnect(
        reconnectEnabled,
        options.maxReconnectAttempts,
        options.reconnectDelayMs ?? 1000,
        options.maxReconnectDelayMs ?? 30_000,
        signal,
      )
      let offset = from
      let lastEventId: string | undefined
      for (;;) {
        throwIfAborted(signal)
        let response: Response
        try {
          response = await hooks.open(offset, lastEventId, signal)
        } catch (error) {
          await reconnect.recover(error)
          continue
        }
        if (!response.body) {
          throw new ClientError('other', 'sse response missing body', { code: 'invalid_response' })
        }
        const encoding = hooks.encodingOf?.(response) ?? 'raw'
        let closed = false
        let sawEvent = false
        try {
          for await (const raw of iterateSse(response.body, signal)) {
            sawEvent = true
            if (raw.id !== undefined) lastEventId = raw.id
            if (raw.event === 'control') {
              const control = hooks.onControl(raw.data)
              offset = control.next
              const event: SseEvent = {
                type: 'control',
                next: control.next,
                upToDate: control.upToDate,
                closed: control.closed,
              }
              if (raw.id !== undefined) event.id = raw.id
              yield event
              if (control.closed) {
                closed = true
                break
              }
              continue
            }
            if (raw.event === 'data' || raw.event === 'message') {
              const records = hooks.onData(raw, { offset, encoding })
              const event: SseEvent = {
                type: 'data',
                records,
                raw: new TextEncoder().encode(raw.data),
              }
              if (raw.id !== undefined) event.id = raw.id
              yield event
            }
          }
        } catch (error) {
          await reconnect.recover(error)
          continue
        }

        if (closed || !reconnectEnabled) return
        if (sawEvent) {
          reconnect.reset()
          continue
        }
        if (!(await reconnect.idle())) return
      }
    },
  }
}
