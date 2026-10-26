import { describe, expect, it } from 'vitest'

import { current } from '../src/location'

describe('current', () => {
  const sections = ['/', '/topics', '/groups']
  const tabs = ['/topics/orders', '/topics/orders/partitions', '/topics/orders/config']

  it('picks the longest link the path starts with', () => {
    expect(current('/', sections)).toBe('/')
    expect(current('/topics', sections)).toBe('/topics')
    expect(current('/topics/orders/partitions/3/records', sections)).toBe('/topics')
    expect(current('/topics/orders', tabs)).toBe('/topics/orders')
    expect(current('/topics/orders/partitions/3/records', tabs)).toBe('/topics/orders/partitions')
  })

  it('matches whole segments only', () => {
    expect(current('/topicsx', sections)).toBe('/')
    expect(current('/topics/orders2', tabs)).toBeUndefined()
  })
})
