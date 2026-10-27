import { describe, expect, it } from 'vitest'

import { indexes } from '../src/views/group'

describe('indexes', () => {
  it('reads a comma separated partition list, empty for all', () => {
    expect(indexes('')).toBeUndefined()
    expect(indexes('0, 2,5')).toEqual([0, 2, 5])
  })

  it('refuses anything but whole numbers', () => {
    expect(indexes('0, x')).toBeNull()
    expect(indexes('1.5')).toBeNull()
    expect(indexes('-1')).toBeNull()
  })
})
