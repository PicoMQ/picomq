import { describe, expect, it } from 'vitest'

import { hex, pretty, shape } from '../src/components/field'

const utf8 = (data: string, truncated = false) => ({
  data,
  encoding: 'utf8' as const,
  size: data.length,
  truncated,
})

const binary = (bytes: string, size = bytes.length) => ({
  data: btoa(bytes),
  encoding: 'base64' as const,
  size,
  truncated: size > bytes.length,
})

describe('shape', () => {
  it('previews text as is, without parsing JSON', () => {
    expect(shape(utf8(''))).toEqual({ kind: 'text', text: '' })
    expect(shape(utf8('{"a":1}'))).toEqual({ kind: 'text', text: '{"a":1}' })
  })

  it('previews binary as hex', () => {
    expect(shape(binary('\x00\xff\x10'))).toEqual({ kind: 'binary', text: '00 ff 10' })
  })
})

describe('pretty', () => {
  it('indents complete JSON and leaves broken or truncated JSON as text', () => {
    expect(pretty(utf8('{"a":1}'))).toEqual({ language: 'json', text: '{\n  "a": 1\n}' })
    expect(pretty(utf8('{"a":'))).toEqual({ language: 'text', text: '{"a":' })
    expect(pretty(utf8('{"a":1}', true))).toEqual({ language: 'text', text: '{"a":1}' })
    expect(pretty(utf8('12'))).toEqual({ language: 'text', text: '12' })
  })

  it('shows every received byte of binary', () => {
    expect(pretty(binary('\x01'.repeat(100))).text.split(' ')).toHaveLength(100)
  })
})

describe('hex', () => {
  it('decodes only up to the limit and marks the rest', () => {
    const long = hex(binary('\x01'.repeat(65)), 64)

    expect(long.split(' ')).toHaveLength(65)
    expect(long.endsWith(' …')).toBe(true)
  })

  it('marks bytes the server did not send', () => {
    expect(hex(binary('\x01\x02', 10), 64)).toBe('01 02 …')
  })
})
