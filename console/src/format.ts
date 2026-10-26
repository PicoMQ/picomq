const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']

const counts = new Intl.NumberFormat('en-US')

const rates = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 })

export function bytes(value: number): string {
  let scaled = value
  let unit = 0
  while (scaled >= 1024 && unit < UNITS.length - 1) {
    scaled /= 1024
    unit += 1
  }

  return unit === 0 ? `${value} B` : `${scaled.toFixed(1)} ${UNITS[unit]}`
}

export function count(value: number): string {
  return counts.format(value)
}

export function rate(perSecond: number): string {
  return `${rates.format(perSecond)}/s`
}

export function time(ms: number): string {
  const date = new Date(ms)
  const pad = (value: number, width = 2) => String(value).padStart(width, '0')

  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
    `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)}`
  )
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
