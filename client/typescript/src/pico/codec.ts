import { toBytes, type RecordEnvelope } from '../record'
import type { HeaderValue } from '../types'

const BATCH_VERSION = 1

const ENC = new TextEncoder()
const STRICT_UTF8 = new TextDecoder('utf-8', { fatal: true })

export class CodecError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'CodecError'
  }
}

export interface SequencedRecord {
  seq: bigint
  envelope: RecordEnvelope
}

interface EncodedRecord {
  size: number
  key: Uint8Array | undefined
  headers: [Uint8Array, Uint8Array][]
  body: Uint8Array
}

function encodeRecord(record: RecordEnvelope): EncodedRecord {
  const headers = Object.entries(record.headers).map(
    ([name, value]) => [ENC.encode(name), toBytes(value)] as [Uint8Array, Uint8Array],
  )
  let size = 4 + (record.key?.length ?? 0) + 4 + 4 + record.body.length
  for (const [name, value] of headers) {
    size += 8 + name.length + value.length
  }
  return { size, key: record.key, headers, body: record.body }
}

class Writer {
  readonly buf: Uint8Array
  private readonly view: DataView
  private offset = 0

  constructor(size: number) {
    this.buf = new Uint8Array(size)
    this.view = new DataView(this.buf.buffer)
  }

  u8(value: number): void {
    this.buf[this.offset++] = value
  }

  i32(value: number): void {
    this.view.setInt32(this.offset, value)
    this.offset += 4
  }

  u32(value: number): void {
    this.view.setUint32(this.offset, value)
    this.offset += 4
  }

  u64(value: bigint): void {
    this.view.setBigUint64(this.offset, value)
    this.offset += 8
  }

  i64(value: bigint): void {
    this.view.setBigInt64(this.offset, value)
    this.offset += 8
  }

  bytes(value: Uint8Array): void {
    this.buf.set(value, this.offset)
    this.offset += value.length
  }

  record(encoded: EncodedRecord): void {
    if (encoded.key === undefined) {
      this.i32(-1)
    } else {
      this.i32(encoded.key.length)
      this.bytes(encoded.key)
    }
    this.u32(encoded.headers.length)
    for (const [name, value] of encoded.headers) {
      this.u32(name.length)
      this.bytes(name)
      this.u32(value.length)
      this.bytes(value)
    }
    this.u32(encoded.body.length)
    this.bytes(encoded.body)
  }
}

class Reader {
  private offset = 0
  constructor(private readonly buf: Uint8Array) {}

  private ensure(n: number): void {
    if (this.offset + n > this.buf.length) {
      throw new CodecError('truncated payload')
    }
  }

  u8(): number {
    this.ensure(1)
    return this.buf[this.offset++]!
  }

  i32(): number {
    this.ensure(4)
    const view = new DataView(this.buf.buffer, this.buf.byteOffset + this.offset, 4)
    const v = view.getInt32(0)
    this.offset += 4
    return v
  }

  u32(): number {
    this.ensure(4)
    const view = new DataView(this.buf.buffer, this.buf.byteOffset + this.offset, 4)
    const v = view.getUint32(0)
    this.offset += 4
    return v
  }

  u64(): bigint {
    this.ensure(8)
    const view = new DataView(this.buf.buffer, this.buf.byteOffset + this.offset, 8)
    const v = view.getBigUint64(0)
    this.offset += 8
    return v
  }

  i64(): bigint {
    this.ensure(8)
    const view = new DataView(this.buf.buffer, this.buf.byteOffset + this.offset, 8)
    const v = view.getBigInt64(0)
    this.offset += 8
    return v
  }

  bytes(len: number): Uint8Array {
    this.ensure(len)
    const out = this.buf.subarray(this.offset, this.offset + len)
    this.offset += len
    return out
  }

  sized(): Uint8Array {
    return this.bytes(this.u32())
  }

  string(): string {
    try {
      return STRICT_UTF8.decode(this.sized())
    } catch {
      throw new CodecError('invalid UTF-8 in header name')
    }
  }

  record(timestamp: bigint): RecordEnvelope {
    const keyLen = this.i32()
    const key = keyLen < 0 ? undefined : this.bytes(keyLen)
    const count = this.u32()
    const headers: { [key: string]: HeaderValue } = {}
    for (let i = 0; i < count; i++) {
      const name = this.string()
      headers[name] = headerValue(this.sized())
    }
    const body = this.sized()
    const envelope: RecordEnvelope = { timestamp, headers, body }
    if (key !== undefined) envelope.key = key
    return envelope
  }

  done(): boolean {
    return this.offset === this.buf.length
  }

  checkVersion(expected: number, what: string): void {
    if (this.buf.length === 0) {
      throw new CodecError(`truncated ${what}`)
    }
    const version = this.u8()
    if (version !== expected) {
      throw new CodecError(`unknown ${what} version ${version}`)
    }
  }
}

export function headerValue(bytes: Uint8Array): HeaderValue {
  try {
    return STRICT_UTF8.decode(bytes)
  } catch {
    return bytes
  }
}

export function encodeBatchAppend(records: RecordEnvelope[]): Uint8Array {
  const encoded = records.map(encodeRecord)
  const w = new Writer(5 + encoded.reduce((n, r) => n + r.size, 0))
  w.u8(BATCH_VERSION)
  w.u32(records.length)
  for (const record of encoded) w.record(record)
  return w.buf
}

export function decodeBatchAppend(payload: Uint8Array): RecordEnvelope[] {
  const r = new Reader(payload)
  r.checkVersion(BATCH_VERSION, 'batch')
  const count = r.u32()
  const records: RecordEnvelope[] = []
  for (let i = 0; i < count; i++) {
    records.push(r.record(0n))
  }
  if (!r.done()) {
    throw new CodecError('trailing bytes after batch')
  }
  return records
}

export function encodeBatchRead(records: SequencedRecord[]): Uint8Array {
  const encoded = records.map((rec) => encodeRecord(rec.envelope))
  const w = new Writer(5 + encoded.reduce((n, r) => n + 16 + r.size, 0))
  w.u8(BATCH_VERSION)
  w.u32(records.length)
  for (let i = 0; i < records.length; i++) {
    w.u64(records[i]!.seq)
    w.i64(records[i]!.envelope.timestamp)
    w.record(encoded[i]!)
  }
  return w.buf
}

export function decodeBatchRead(payload: Uint8Array): SequencedRecord[] {
  const r = new Reader(payload)
  r.checkVersion(BATCH_VERSION, 'batch')
  const count = r.u32()
  const records: SequencedRecord[] = []
  for (let i = 0; i < count; i++) {
    const seq = r.u64()
    const timestamp = r.i64()
    records.push({ seq, envelope: r.record(timestamp) })
  }
  return records
}
