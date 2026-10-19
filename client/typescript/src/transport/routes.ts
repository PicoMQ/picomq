/**
 * Owners learned from ownership redirects, so later requests for the same
 * stream partition go straight to the node that serves it.
 */
export class Routes {
  private readonly owners = new Map<string, Map<string, string>>()

  resolve(url: string): string {
    const parsed = new URL(url)
    const owner = this.owners.get(parsed.pathname)?.get(partition(parsed))
    if (owner === undefined) return url

    const routed = new URL(owner)
    routed.pathname = parsed.pathname
    routed.search = parsed.search
    return routed.toString()
  }

  learn(requested: string, location: string): void {
    const from = new URL(requested)
    let byPartition = this.owners.get(from.pathname)
    if (byPartition === undefined) {
      byPartition = new Map()
      this.owners.set(from.pathname, byPartition)
    }
    byPartition.set(partition(from), new URL(location).origin)
  }

  forget(url: string): void {
    this.owners.delete(new URL(url).pathname)
  }
}

function partition(url: URL): string {
  return url.searchParams.get('partition') ?? ''
}
