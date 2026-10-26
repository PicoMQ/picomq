import { describe, expect, it } from 'vitest'

import { current, page, topicPage, withQuery } from '../src/location'

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
  it('encodes every segment as is and skips missing ones', () => {
    expect(page('groups', '/team a')).toBe('/groups/%2Fteam%20a')
    expect(page('nodes', 3, undefined)).toBe('/nodes/3')
  })
})

describe('topicPage', () => {
  it('drops the leading slash of a topic name the API accepts either way', () => {
    expect(topicPage('/acct/payments', 'partitions')).toBe('/topics/acct%2Fpayments/partitions')
    expect(topicPage('orders', undefined)).toBe('/topics/orders')
  })
})

describe('withQuery', () => {
  it('sets, replaces and removes parameters', () => {
    expect(withQuery('/topics', {}, { prefix: 'acct' })).toBe('/topics?prefix=acct')
    expect(withQuery('/topics', { prefix: 'acct' }, { prefix: '' })).toBe('/topics')
    expect(withQuery('/p', { start: 'offset', at: '5' }, { at: 9, record: undefined })).toBe('/p?start=offset&at=9')
  })
})
