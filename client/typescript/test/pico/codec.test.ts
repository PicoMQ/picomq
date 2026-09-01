import { describe, expect, it } from 'vitest'
import {
  CodecError,
  decodeBatchAppend,
  decodeBatchRead,
  encodeBatchAppend,
  encodeBatchRead,
} from '../../src/pico/codec'

function concat(...parts: (Uint8Array | number[])[]): Uint8Array {
  const arrays = parts.map((p) => (p instanceof Uint8Array ? p : new Uint8Array(p)))
  const total = arrays.reduce((n, a) => n + a.length, 0)
  const out = new Uint8Array(total)
  let o = 0
  for (const a of arrays) {
    out.set(a, o)
    o += a.length
  }
  return out
}

function beI32(n: number): Uint8Array {
  const buf = new Uint8Array(4)
  new DataView(buf.buffer).setInt32(0, n)
  return buf
}

function beU32(n: number): Uint8Array {
  const buf = new Uint8Array(4)
  new DataView(buf.buffer).setUint32(0, n)
  return buf
}

const utf8 = (s: string) => new TextEncoder().encode(s)

describe('codec', () => {
  it('batch append bytes match the server layout', () => {
    const encoded = encodeBatchAppend([
      { timestamp: 0n, key: utf8('k'), headers: { a: 'b' }, body: utf8('xy') },
      { timestamp: 0n, headers: {}, body: new Uint8Array() },
    ])
    const expected = concat(
      [1],
      beU32(2),
      beI32(1),
      utf8('k'),
      beU32(1),
      beU32(1),
      utf8('a'),
      beU32(1),
      utf8('b'),
      beU32(2),
      utf8('xy'),
      beI32(-1),
      beU32(0),
      beU32(0),
    )
    expect(encoded).toEqual(expected)
  })

  it('round trips keys, binary headers, and empty vs absent keys', () => {
    const records = [
      {
        timestamp: 0n,
        key: utf8('k1'),
        headers: { text: 'v', bin: new Uint8Array([0xff, 0x00]) },
        body: utf8('one'),
      },
      { timestamp: 0n, headers: {}, body: utf8('two') },
      { timestamp: 0n, key: new Uint8Array(), headers: {}, body: new Uint8Array([0xfe]) },
    ]
    const decoded = decodeBatchAppend(encodeBatchAppend(records))
    expect(decoded).toEqual(records)
    expect(decoded[1]!.key).toBeUndefined()
    expect(decoded[2]!.key).toEqual(new Uint8Array())

    const sequenced = [
      { seq: 4n, envelope: { timestamp: 9n, key: utf8('k'), headers: { k: 'v' }, body: utf8('one') } },
      { seq: 5n, envelope: { timestamp: 9n, headers: {}, body: new Uint8Array() } },
    ]
    expect(decodeBatchRead(encodeBatchRead(sequenced))).toEqual(sequenced)
  })

  it('rejects bad version, truncation, and trailing bytes', () => {
    const encoded = encodeBatchAppend([{ timestamp: 0n, headers: { a: 'b' }, body: utf8('x') }])
    const bad = new Uint8Array(encoded)
    bad[0] = 9
    expect(() => decodeBatchAppend(bad)).toThrow(CodecError)
    expect(() => decodeBatchAppend(new Uint8Array())).toThrow(CodecError)
    for (let len = 1; len < encoded.length; len++) {
      expect(() => decodeBatchAppend(encoded.subarray(0, len))).toThrow(CodecError)
    }
    expect(() => decodeBatchAppend(concat(encoded, [0]))).toThrow(CodecError)
  })
})
