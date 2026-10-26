import { describe, expect, it } from 'vitest'

import * as format from '../src/format'

describe('bytes', () => {
  it('uses binary units with one decimal above a kibibyte', () => {
    expect(format.bytes(0)).toBe('0 B')
    expect(format.bytes(1023)).toBe('1023 B')
    expect(format.bytes(1536)).toBe('1.5 KiB')
    expect(format.bytes(5 * 1024 ** 3)).toBe('5.0 GiB')
  })
})

describe('count and rate', () => {
  it('group digits and keep one decimal for rates', () => {
    expect(format.count(1234567)).toBe('1,234,567')
    expect(format.rate(12.345)).toBe('12.3/s')
  })
})

describe('time', () => {
  it('shows local time to the millisecond', () => {
    const ms = new Date(2026, 9, 26, 9, 5, 7, 42).getTime()

    expect(format.time(ms)).toBe('2026-10-26 09:05:07.042')
  })
})

describe('ago', () => {
  it('uses the largest whole unit and never goes negative', () => {
    expect(format.ago(0, 59_999)).toBe('59s')
    expect(format.ago(0, 60_000)).toBe('1m')
    expect(format.ago(0, 3 * 3_600_000)).toBe('3h')
    expect(format.ago(0, 2 * 86_400_000)).toBe('2d')
    expect(format.ago(5000, 0)).toBe('0s')
  })
})

describe('duration', () => {
  it('shows the two largest units', () => {
    expect(format.duration(500)).toBe('500ms')
    expect(format.duration(60_000)).toBe('1m')
    expect(format.duration(5_400_000)).toBe('1h 30m')
    expect(format.duration(7 * 86_400_000)).toBe('7d')
    expect(format.duration(86_400_000 + 61_000)).toBe('1d 1m')
  })
})

describe('clock', () => {
  it('shows the local time of day to the second', () => {
    expect(format.clock(new Date(2026, 9, 26, 9, 5, 7, 42).getTime())).toBe('09:05:07')
  })
})

describe('limit', () => {
  it('reads null as unlimited', () => {
    expect(format.limit(null, format.bytes)).toBe('unlimited')
    expect(format.limit(2048, format.bytes)).toBe('2.0 KiB')
  })
})

describe('counted', () => {
  it('pluralizes regular nouns', () => {
    expect(format.counted(1, 'node')).toBe('1 node')
    expect(format.counted(1200, 'record')).toBe('1,200 records')
  })
})

describe('time to the second', () => {
  it('drops the milliseconds', () => {
    expect(format.time(new Date(2026, 9, 26, 9, 5, 7, 42).getTime(), 'second')).toBe('2026-10-26 09:05:07')
  })
})
