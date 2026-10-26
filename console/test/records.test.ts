import { describe, expect, it } from 'vitest'

import { recordFilter, recordStart } from '../src/api/records'
import type { Partition } from '../src/api/topics'

const partition = (startOffset: number, nextOffset: number): Partition => ({
  index: 0,
  streamId: 1,
  ownerNodeId: 1,
  epoch: 1,
  state: 'opened',
  pendingTransfer: null,
  startOffset,
  nextOffset,
  current: true,
  storedBytes: 0,
})

describe('recordStart', () => {
  it('starts newest at the last window and never before the log start', () => {
    expect(recordStart({}, partition(0, 430), 50)).toEqual({ offset: 380 })
    expect(recordStart({ start: 'newest' }, partition(400, 430), 50)).toEqual({ offset: 400 })
  })

  it('reads oldest, offset and time starts', () => {
    expect(recordStart({ start: 'oldest' }, partition(0, 430), 50)).toBeNull()
    expect(recordStart({ start: 'offset', at: '12' }, partition(0, 430), 50)).toEqual({ offset: 12 })
    expect(recordStart({ start: 'time', at: '1700000000000' }, partition(0, 430), 50)).toEqual({ timestamp: 1700000000000 })
  })

  it('falls back to newest when the position is missing', () => {
    expect(recordStart({ start: 'offset', at: '' }, partition(0, 430), 50)).toEqual({ offset: 380 })
  })
})

describe('recordFilter', () => {
  it('keeps only the filter fields', () => {
    expect(recordFilter({ start: 'oldest', key: 'k', headerValue: 'v' })).toEqual({
      key: 'k',
      value: undefined,
      header: undefined,
      headerValue: 'v',
    })
  })
})
