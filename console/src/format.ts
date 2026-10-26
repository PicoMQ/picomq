const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']

const DURATIONS: [string, number][] = [
  ['d', 86_400_000],
  ['h', 3_600_000],
  ['m', 60_000],
  ['s', 1000],
]

const counts = new Intl.NumberFormat('en-US')

const rates = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 })

export function bytes(value: number): string {
  let scaled = value
  let unit = 0
  while (scaled >= 1024 && unit < UNITS.length - 1) {
    scaled /= 1024
    unit += 1
  }

  return unit === 0 ? `${Math.round(value)} B` : `${scaled.toFixed(1)} ${UNITS[unit]}`
}

export function count(value: number | null): string {
  return value === null ? 'unknown' : counts.format(value)
}

export function counted(value: number, noun: string): string {
  return `${count(value)} ${value === 1 ? noun : `${noun}s`}`
}

export function rate(perSecond: number): string {
  return `${rates.format(perSecond)}/s`
}

export function time(ms: number, precision: 'second' | 'millisecond' = 'millisecond'): string {
  const date = new Date(ms)
  const pad = (value: number, width = 2) => String(value).padStart(width, '0')
  const seconds = `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`

  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
    (precision === 'second' ? seconds : `${seconds}.${pad(date.getMilliseconds(), 3)}`)
  )
}

export function clock(ms: number): string {
  return time(ms, 'second').slice(11)
}

export function ago(ms: number, nowMs: number): string {
  const seconds = Math.max(0, Math.floor((nowMs - ms) / 1000))

  if (seconds < 60) {
    return `${seconds}s`
  }
  if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m`
  }
  if (seconds < 86400) {
    return `${Math.floor(seconds / 3600)}h`
  }
  return `${Math.floor(seconds / 86400)}d`
}

export function limit(value: number | null, show: (value: number) => string): string {
  return value === null ? 'unlimited' : show(value)
}

export function duration(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`
  }

  const parts: string[] = []
  let rest = ms
  for (const [unit, size] of DURATIONS) {
    const amount = Math.floor(rest / size)
    rest -= amount * size
    if (amount > 0) {
      parts.push(`${amount}${unit}`)
    }
  }
  return parts.slice(0, 2).join(' ')
}
