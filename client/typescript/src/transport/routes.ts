/**
 * A stream partition, named by its path relative to the client's endpoint,
 * so it stays the same whichever node a request went to.
 */
export interface RouteKey {
  path: string
  partition: string
}

/**
 * Owners learned from ownership redirects, so later requests for the same
 * stream partition go straight to the node that serves it. An owner is kept
 * as the base URL of its redirect, which may carry a path prefix when nodes
 * sit behind a gateway.
 */
export class Routes {
  private readonly prefix: string
  private readonly owners = new Map<string, Map<string, string>>()

  constructor(endpoint: string) {
    this.prefix = new URL(endpoint).pathname.replace(/\/+$/, '')
  }

  /** The key of a request this client built, or `undefined` for a URL outside its endpoint. */
  key(url: string): RouteKey | undefined {
    const parsed = new URL(url)
    if (!parsed.pathname.startsWith(this.prefix)) return undefined

    return { path: parsed.pathname.slice(this.prefix.length), partition: partition(parsed) }
  }

  resolve(key: RouteKey, url: string): string {
    const owner = this.owners.get(key.path)?.get(key.partition)
    if (owner === undefined) return url

    const routed = new URL(owner)
    routed.pathname = routed.pathname.replace(/\/+$/, '') + key.path
    routed.search = new URL(url).search
    return routed.toString()
  }

  /** A Location that does not end in the stream path has no recognizable base, so it is not cached. */
  learn(key: RouteKey, location: string): void {
    const owner = new URL(location)
    if (!owner.pathname.endsWith(key.path)) return

    owner.pathname = owner.pathname.slice(0, owner.pathname.length - key.path.length)
    owner.search = ''
    let byPartition = this.owners.get(key.path)
    if (byPartition === undefined) {
      byPartition = new Map()
      this.owners.set(key.path, byPartition)
    }
    byPartition.set(key.partition, owner.toString())
  }

  /** Drops every partition of the key's stream. */
  forget(key: RouteKey): void {
    this.owners.delete(key.path)
  }
}

function partition(url: URL): string {
  return url.searchParams.get('partition') ?? ''
}
