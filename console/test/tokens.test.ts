import { describe, expect, it } from 'vitest'

import type { Scope } from '../src/api/tokens'
import { accessSummary } from '../src/views/tokens'

const none = { read: false, write: false }

const scope = (changes: Partial<Scope>): Scope => ({
  streams: [],
  tokens: [],
  groups: [],
  access: { stream: none, tokens: none, group: none, admin: none },
  ops: [],
  audiences: [],
  autoPrefix: false,
  expiresAtMs: null,
  ...changes,
})

describe('accessSummary', () => {
  it('lists granted access and single operations', () => {
    expect(accessSummary(scope({}))).toBe('nothing')
    expect(
      accessSummary(scope({ access: { stream: { read: true, write: true }, group: { read: true, write: false }, tokens: none, admin: none } })),
    ).toBe('stream read and write, group read')
    expect(accessSummary(scope({ ops: ['append'] }))).toBe('1 single operation')
  })
})
