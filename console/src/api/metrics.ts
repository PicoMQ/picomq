export interface Sample {
  name: string
  labels: Record<string, string>
  value: number
}

export interface Scrape {
  atMs: number
  samples: Sample[]
}

const LINE = /^([a-zA-Z_:][\w:]*)(?:\{(.*)\})?\s+(\S+)$/
const LABEL = /([a-zA-Z_]\w*)="((?:[^"\\]|\\.)*)"/g

export function parse(text: string, atMs: number): Scrape {
  const samples: Sample[] = []

  for (const line of text.split('\n')) {
    const match = LINE.exec(line.trim())
    if (!match) {
      continue
    }

    const [, name = '', labels = '', value = ''] = match
    samples.push({ name, labels: parseLabels(labels), value: Number(value) })
  }

  return { atMs, samples }
}

export function total(scrape: Scrape, name: string, labels: Record<string, string> = {}): number {
  return scrape.samples
    .filter((sample) => sample.name === name)
    .filter((sample) => Object.entries(labels).every(([key, value]) => sample.labels[key] === value))
    .reduce((sum, sample) => sum + sample.value, 0)
}

export function rate(
  previous: Scrape,
  current: Scrape,
  name: string,
  labels: Record<string, string> = {},
): number | null {
  const seconds = (current.atMs - previous.atMs) / 1000
  const delta = total(current, name, labels) - total(previous, name, labels)

  if (seconds <= 0 || delta < 0) {
    return null
  }
  return delta / seconds
}

function parseLabels(text: string): Record<string, string> {
  const labels: Record<string, string> = {}

  for (const [, key = '', value = ''] of text.matchAll(LABEL)) {
    labels[key] = value.replace(/\\(.)/g, (_, escaped: string) => (escaped === 'n' ? '\n' : escaped))
  }
  return labels
}
