import { ClientError, isAbortError } from './error'

export interface RawSseEvent {
  event: string
  id?: string
  data: string
}

export async function* iterateSse(
  body: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncGenerator<RawSseEvent> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let event = ''
  let id: string | undefined
  let dataLines: string[] = []

  const onAbort = () => {
    void reader.cancel()
  }
  signal?.addEventListener('abort', onAbort, { once: true })

  try {
    for (;;) {
      if (signal?.aborted) {
        throw ClientError.aborted(
          signal.reason instanceof Error ? signal.reason.message : 'Aborted',
        )
      }
      const { done, value } = await reader.read()
      if (done) {
        if (signal?.aborted) {
          throw ClientError.aborted(
            signal.reason instanceof Error ? signal.reason.message : 'Aborted',
          )
        }
        break
      }
      buffer += decoder.decode(value, { stream: true })
      let newline: number
      while ((newline = buffer.indexOf('\n')) >= 0) {
        let line = buffer.slice(0, newline)
        buffer = buffer.slice(newline + 1)
        if (line.endsWith('\r')) line = line.slice(0, -1)

        if (line === '') {
          if (dataLines.length > 0 || event !== '' || id !== undefined) {
            yield {
              event: event || 'message',
              ...(id !== undefined ? { id } : {}),
              data: dataLines.join('\n'),
            }
          }
          event = ''
          id = undefined
          dataLines = []
          continue
        }
        if (line.startsWith(':')) continue
        const colon = line.indexOf(':')
        const field = colon === -1 ? line : line.slice(0, colon)
        let valuePart = colon === -1 ? '' : line.slice(colon + 1)
        if (valuePart.startsWith(' ')) valuePart = valuePart.slice(1)
        switch (field) {
          case 'event':
            event = valuePart
            break
          case 'id':
            id = valuePart
            break
          case 'data':
            dataLines.push(valuePart)
            break
          default:
            break
        }
      }
    }
  } catch (error) {
    if (isAbortError(error) || signal?.aborted) {
      throw ClientError.aborted(error instanceof Error ? error.message : 'Aborted')
    }
    throw error
  } finally {
    signal?.removeEventListener('abort', onAbort)
    reader.releaseLock()
  }
}
