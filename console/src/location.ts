type Changes = Record<string, string | number | undefined>

export function current(path: string, hrefs: string[]): string | undefined {
  const matching = hrefs.filter((href) => href === '/' || path === href || path.startsWith(`${href}/`))

  return matching.sort((left, right) => right.length - left.length)[0]
}

export function page(...segments: (string | number | undefined)[]): string {
  const present = segments.filter((segment) => segment !== undefined)

  return `/${present.map((segment) => encodeURIComponent(String(segment).replace(/^\//, ''))).join('/')}`
}

export function withQuery(path: string, query: Record<string, string>, changes: Changes): string {
  const params = new URLSearchParams(query)
  for (const [name, value] of Object.entries(changes)) {
    if (value === undefined || value === '') {
      params.delete(name)
    } else {
      params.set(name, String(value))
    }
  }

  const search = params.toString()
  return search ? `${path}?${search}` : path
}
