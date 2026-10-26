import { describe, expect, it } from 'vitest'

import { keep } from '../src/ring'

describe('keep', () => {
  it('appends and drops the oldest past the limit', () => {
    expect(keep([1, 2], [3], 5)).toEqual([1, 2, 3])
    expect(keep([1, 2, 3], [4, 5, 6], 4)).toEqual([3, 4, 5, 6])
    expect(keep([], [1, 2, 3, 4, 5, 6], 2)).toEqual([5, 6])
  })
})
