import { describe, expect, it } from 'vitest'

import { month, parse } from '../src/components/datetime'

describe('parse', () => {
  it('reads local date and time with optional seconds', () => {
    expect(parse('2026-10-26 18:24:03')).toBe(new Date(2026, 9, 26, 18, 24, 3).getTime())
    expect(parse('2026-10-26T18:24')).toBe(new Date(2026, 9, 26, 18, 24, 0).getTime())
  })

  it('refuses malformed or impossible dates', () => {
    expect(parse('26/10/2026')).toBeNull()
    expect(parse('2026-02-30 10:00')).toBeNull()
  })
})

describe('month', () => {
  it('fills six Monday-first weeks around the month', () => {
    const days = month(2026, 9)

    expect(days).toHaveLength(42)
    expect(days[0]!.getDay()).toBe(1)
    expect(days.some((day) => day.getMonth() === 9 && day.getDate() === 1)).toBe(true)
  })
})
