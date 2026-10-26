export function current(path: string, hrefs: string[]): string | undefined {
  const matching = hrefs.filter((href) => href === '/' || path === href || path.startsWith(`${href}/`))

  return matching.sort((left, right) => right.length - left.length)[0]
}
