import { describe, expect, it } from 'vitest'

import { current, page, withQuery } from '../src/location'

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

describe('page', () => {
  it('encodes each segment and drops its leading slash', () => {
    expect(page('topics', '/acct/payments', 'partitions', 3)).toBe('/topics/acct%2Fpayments/partitions/3')
    expect(page('groups', 'billing team')).toBe('/groups/billing%20team')
    expect(page('topics', 'orders', undefined)).toBe('/topics/orders')
  })
})

describe('withQuery', () => {
  it('sets, replaces and removes parameters', () => {
    expect(withQuery('/topics', {}, { prefix: 'acct' })).toBe('/topics?prefix=acct')
    expect(withQuery('/topics', { prefix: 'acct' }, { prefix: '' })).toBe('/topics')
    expect(withQuery('/p', { start: 'offset', at: '5' }, { at: 9, record: undefined })).toBe('/p?start=offset&at=9')
  })
})
