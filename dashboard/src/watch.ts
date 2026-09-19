import { matchNames } from './match'
import {
  ApiError, errorMessage, inspectStream, listStreams, readStream,
  type Connection, type RecordData,
} from './streams'

export const MAX_MATCHES = 8
export interface WatchRun { mode: string; query: string; prefix: string; start: string; names: string[] }
export interface WatchStatus { name: string; position: string; state: string }
interface WatchHandlers {
  records: (stream: string, records: RecordData[]) => void
  statuses: (statuses: WatchStatus[]) => void
  discovery: (warning: string) => void
  progress: () => void
  error: (error: unknown) => void
}
interface Subscription {
  name: string
  policy: string
  cursor?: string
  controller: AbortController
  timer?: ReturnType<typeof setTimeout>
  status: WatchStatus
}

// Each stream owns its request, cursor, and timer. Discovery never holds up reads.
export function watchStreams(connection: Connection, run: WatchRun, handlers: WatchHandlers): () => void {
  const controller = new AbortController()
  const subscriptions = new Map<string, Subscription>()
  let discoveryTimer: ReturnType<typeof setTimeout> | undefined
  let initialized = false
  let retryDelay = 5000

  function cancel(subscription: Subscription) {
    subscription.controller.abort()
    clearTimeout(subscription.timer)
  }
  function stop() {
    controller.abort()
    clearTimeout(discoveryTimer)
    subscriptions.forEach(cancel)
  }
  function active(subscription: Subscription) {
    return !controller.signal.aborted && !subscription.controller.signal.aborted
      && subscriptions.get(subscription.name) === subscription
  }
  function emitStatuses() {
    handlers.statuses([...subscriptions.values()].map((subscription) => subscription.status))
  }
  function fail(error: unknown) {
    stop()
    handlers.discovery('')
    handlers.error(error)
  }

  async function poll(subscription: Subscription) {
    if (!active(subscription)) return
    const { name, controller: streamController } = subscription
    try {
      const meta = await inspectStream(connection, name, streamController.signal)
      if (!active(subscription)) return
      if (subscription.cursor === undefined) {
        // Save Now before the first GET so a failed read cannot move its starting point.
        subscription.cursor = subscription.policy === 'now' ? meta.next : meta.start
      }
      const cursor = subscription.cursor
      if (BigInt(cursor) < BigInt(meta.start)) throw new Error('Saved position was trimmed. Stop and restart to choose a new starting point.')
      if (BigInt(cursor) > BigInt(meta.next)) throw new Error('Stream position moved backwards; the name may have been recreated. Stop and restart.')
      const page = await readStream(connection, name, cursor, streamController.signal, 50, true)
      if (!active(subscription)) return
      subscription.cursor = page.next
      subscription.status = { name, position: page.next, state: page.closed ? 'Closed · caught up' : 'Watching' }
      if (page.records.length) handlers.records(name, page.records)
      if (page.closed) {
        emitStatuses(); handlers.progress()
        return
      }
    } catch (error) {
      if (!active(subscription)) return
      subscription.status = { name, position: subscription.cursor ?? '—', state: errorMessage(error) }
    }
    emitStatuses()
    handlers.progress()
    if (active(subscription)) subscription.timer = setTimeout(() => void poll(subscription), 1000)
  }

  function reconcile(names: string[]) {
    if (names.length > MAX_MATCHES) throw new Error(`Selection matches ${names.length} streams. Narrow it to at most ${MAX_MATCHES} simultaneous watches.`)
    for (const [name, subscription] of subscriptions) {
      if (!names.includes(name)) { cancel(subscription); subscriptions.delete(name) }
    }
    const added: Subscription[] = []
    for (const name of names) {
      if (subscriptions.has(name)) continue
      const subscription: Subscription = {
        name, policy: initialized ? 'beginning' : run.start, controller: new AbortController(),
        status: { name, position: '—', state: 'Connecting…' },
      }
      subscriptions.set(name, subscription)
      added.push(subscription)
    }
    initialized = true
    emitStatuses()
    added.forEach((subscription) => void poll(subscription))
  }

  async function discover() {
    try {
      let after = ''
      const candidates: string[] = []
      for (;;) {
        const page = await listStreams(connection, run.prefix, after, controller.signal)
        if (controller.signal.aborted) return
        candidates.push(...page.streams.map((stream) => stream.name))
        if (!page.has_more) break
        if (candidates.length >= 5000 || page.streams.length === 0) throw new Error('Regex discovery reached 5,000 streams. Narrow the name prefix.')
        const next = page.streams[page.streams.length - 1].name
        if (next === after) throw new Error('Stream listing did not advance.')
        after = next
      }
      const names = await matchNames(run.query, candidates, controller.signal)
      if (controller.signal.aborted) return
      reconcile(names)
      handlers.discovery('')
      handlers.progress()
      retryDelay = 5000
      discoveryTimer = setTimeout(() => void discover(), 5000)
    } catch (error) {
      if (controller.signal.aborted) return
      const transient = error instanceof ApiError && error.retryable
        || error instanceof DOMException && ['AbortError', 'TimeoutError'].includes(error.name)
      if (!transient) { fail(error); return }
      handlers.discovery(`Discovery unavailable: ${errorMessage(error)} ${subscriptions.size ? 'Existing watches continue. ' : ''}Retrying in ${retryDelay / 1000} seconds.`)
      discoveryTimer = setTimeout(() => void discover(), retryDelay)
      retryDelay = Math.min(retryDelay * 2, 30_000)
    }
  }

  if (run.mode === 'exact') {
    try { reconcile(run.names) } catch (error) { fail(error) }
  } else void discover()
  return stop
}
