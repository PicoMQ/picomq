import { ClientError, isAbortError, throwIfAborted } from './error'
import { iterateSse, type RawSseEvent } from './sse'
import { sleep } from './util'
import type { SseEvent, StreamRecord, SubscribeOptions } from './types'

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

export function subscribeLoop(
  from: string,
  options: SubscribeOptions,
  hooks: SubscribeHooks,
): AsyncIterable<SseEvent> {
  const reconnect = options.reconnect ?? true
  const maxAttempts = options.maxReconnectAttempts
  const initialDelay = options.reconnectDelayMs ?? 1000
  const maxDelay = options.maxReconnectDelayMs ?? 30_000
  const signal = options.signal

  return {
    async *[Symbol.asyncIterator]() {
      let offset = from
      let lastEventId: string | undefined
      let drops = 0
      for (;;) {
        throwIfAborted(signal)
        let response: Response
        try {
          response = await hooks.open(offset, lastEventId, signal)
        } catch (error) {
          if (isAbortError(error) || signal?.aborted) {
            throwIfAborted(signal)
            throw error
          }
          if (!reconnect) throw error
          drops += 1
          if (maxAttempts !== undefined && drops > maxAttempts) throw error
          await sleep(Math.min(maxDelay, initialDelay * 2 ** Math.max(0, drops - 1)))
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
          if (isAbortError(error) || signal?.aborted) {
            throwIfAborted(signal)
            throw error
          }
          if (!reconnect) throw error
          drops += 1
          if (maxAttempts !== undefined && drops > maxAttempts) throw error
          await sleep(Math.min(maxDelay, initialDelay * 2 ** Math.max(0, drops - 1)))
          continue
        }

        if (closed || !reconnect) return
        if (sawEvent) {
          drops = 0
          continue
        }
        drops += 1
        if (maxAttempts !== undefined && drops > maxAttempts) return
        await sleep(Math.min(maxDelay, initialDelay * 2 ** Math.max(0, drops - 1)))
      }
    },
  }
}
