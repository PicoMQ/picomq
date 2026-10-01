self.onmessage = (event: MessageEvent<{ pattern: string; names: string[] }>) => {
  try {
    const regex = new RegExp(event.data.pattern)
    self.postMessage({ matches: event.data.names.filter((name) => regex.test(name)) })
  } catch (error) {
    self.postMessage({ error: error instanceof Error ? error.message : String(error) })
  }
}
self.postMessage({ ready: true })
