export function keep<T>(items: T[], added: T[], max: number): T[] {
  const all = [...items, ...added]

  return all.length > max ? all.slice(all.length - max) : all
}
