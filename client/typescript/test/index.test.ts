import { describe, expect, it } from 'vitest'
import { connect } from '../src/index'
import { PicoClient } from '../src/pico/client'
import { DsClient } from '../src/ds/client'

describe('connect', () => {
  it('returns a pico client', () => {
    const client = connect('pico', 'http://127.0.0.1:4437')
    expect(client).toBeInstanceOf(PicoClient)
    expect(client.protocol()).toBe('pico')
    expect(client.beginning()).toBe('0')
    expect(() => client.now()).toThrow()
  })

  it('returns a ds client', () => {
    const client = connect('ds', 'http://127.0.0.1:4437')
    expect(client).toBeInstanceOf(DsClient)
    expect(client.protocol()).toBe('ds')
    expect(client.beginning()).toBe('-1')
    expect(client.now()).toBe('now')
  })
})
