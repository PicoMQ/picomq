import { describe, expect, it } from 'vitest'
import {
  CodecError,
  decodeBatchAppend,
  decodeBatchRead,
  decodeEnvelope,
  encodeBatchAppend,
  encodeBatchRead,
  encodeEnvelope,
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

function beI64(n: bigint): Uint8Array {
  const buf = new Uint8Array(8)
  new DataView(buf.buffer).setBigInt64(0, n)
  return buf
}

function beU32(n: number): Uint8Array {
  const buf = new Uint8Array(4)
  new DataView(buf.buffer).setUint32(0, n)
  return buf
}

describe('codec', () => {
  it('envelope bytes match java layout', () => {
    const envelope = {
      timestamp: 7n,
      headers: { a: 'b' },
      body: new TextEncoder().encode('xy'),
    }
    const encoded = encodeEnvelope(envelope)
    const expected = concat(
      [1],
      beI64(7n),
      beU32(1),
      beU32(1),
      new TextEncoder().encode('a'),
      beU32(1),
      new TextEncoder().encode('b'),
      new TextEncoder().encode('xy'),
    )
    expect(encoded).toEqual(expected)
    expect(decodeEnvelope(encoded)).toEqual(envelope)
  })

  it('batch append and read round trip', () => {
    const records = [
      { timestamp: 0n, headers: { k: 'v' }, body: new TextEncoder().encode('one') },
      { timestamp: 0n, headers: {}, body: new TextEncoder().encode('two') },
    ]
    expect(decodeBatchAppend(encodeBatchAppend(records))).toEqual(records)

    const sequenced = [
      {
        seq: 4n,
        envelope: { timestamp: 9n, headers: { k: 'v' }, body: new TextEncoder().encode('one') },
      },
      {
        seq: 5n,
        envelope: { timestamp: 9n, headers: {}, body: new Uint8Array() },
      },
    ]
    expect(decodeBatchRead(encodeBatchRead(sequenced))).toEqual(sequenced)
  })

  it('rejects bad version and truncation', () => {
    const encoded = encodeEnvelope({
      timestamp: 1n,
      headers: {},
      body: new TextEncoder().encode('x'),
    })
    const bad = new Uint8Array(encoded)
    bad[0] = 9
    expect(() => decodeEnvelope(bad)).toThrow(CodecError)
    expect(() => decodeEnvelope(new Uint8Array())).toThrow(CodecError)
    expect(() => decodeBatchAppend(new Uint8Array([1, 0, 0]))).toThrow(CodecError)
  })
})
