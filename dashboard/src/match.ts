export function matchNames(pattern: string, names: string[], signal: AbortSignal): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./match.worker.ts', import.meta.url), { type: 'module' })
    let finished = false
    const finish = (error?: Error, matches: string[] = []) => {
      if (finished) return
      finished = true
      clearTimeout(timer)
      signal.removeEventListener('abort', abort)
      worker.terminate()
      if (error) reject(error)
      else resolve(matches)
    }
    const abort = () => finish(new DOMException('Aborted', 'AbortError'))
    let timer = setTimeout(() => finish(new Error('The regex worker could not start. Reload and try again.')), 15_000)
    worker.onmessage = (event: MessageEvent<{ ready?: boolean; error?: string; matches?: string[] }>) => {
      if (event.data.ready) {
        clearTimeout(timer)
        timer = setTimeout(() => finish(new Error('Regex took too long. Use a simpler pattern or a narrower prefix.')), 1000)
        worker.postMessage({ pattern, names })
        return
      }
      finish(event.data.error ? new Error(event.data.error) : undefined, event.data.matches)
    }
    worker.onerror = () => finish(new Error('Could not evaluate the regex.'))
    signal.addEventListener('abort', abort, { once: true })
    if (signal.aborted) abort()
  })
}
