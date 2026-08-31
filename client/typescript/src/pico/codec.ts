import type { RecordEnvelope } from '../record'

const ENVELOPE_VERSION = 1
const BATCH_VERSION = 1

const ENC = new TextEncoder()

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

interface EncodedHeaders {
  size: number
  entries: [Uint8Array, Uint8Array][]
}

function encodeHeaders(headers: { [key: string]: string }): EncodedHeaders {
  const entries = Object.entries(headers)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([name, value]) => [ENC.encode(name), ENC.encode(value)] as [Uint8Array, Uint8Array])
  let size = 4
  for (const [name, value] of entries) {
    size += 8 + name.length + value.length
  }
  return { size, entries }
}

function putU32(view: DataView, offset: number, value: number): number {
  view.setUint32(offset, value)
  return offset + 4
}

function putU64(view: DataView, offset: number, value: bigint): number {
  view.setBigUint64(offset, value)
  return offset + 8
}

function putI64(view: DataView, offset: number, value: bigint): number {
  view.setBigInt64(offset, value)
  return offset + 8
}

function putBytes(buf: Uint8Array, offset: number, bytes: Uint8Array): number {
  buf.set(bytes, offset)
  return offset + bytes.length
}

function putHeaders(buf: Uint8Array, view: DataView, offset: number, encoded: EncodedHeaders): number {
  offset = putU32(view, offset, encoded.entries.length)
  for (const [name, value] of encoded.entries) {
    offset = putU32(view, offset, name.length)
    offset = putBytes(buf, offset, name)
    offset = putU32(view, offset, value.length)
    offset = putBytes(buf, offset, value)
  }
  return offset
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

  rest(): Uint8Array {
    return this.buf.subarray(this.offset)
  }

  string(): string {
    const len = this.u32()
    const bytes = this.bytes(len)
    try {
      return new TextDecoder('utf-8', { fatal: true }).decode(bytes)
    } catch {
      throw new CodecError('invalid UTF-8 in headers')
    }
  }

  headers(): { [key: string]: string } {
    const count = this.u32()
    const headers: { [key: string]: string } = {}
    for (let i = 0; i < count; i++) {
      const name = this.string()
      const value = this.string()
      headers[name] = value
    }
    return headers
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

export function encodeBatchAppend(records: RecordEnvelope[]): Uint8Array {
  const encoded = records.map((r) => encodeHeaders(r.headers))
  let size = 5
  for (let i = 0; i < records.length; i++) {
    size += encoded[i]!.size + 4 + records[i]!.body.length
  }
  const buf = new Uint8Array(size)
  const view = new DataView(buf.buffer)
  let offset = 0
  buf[offset++] = BATCH_VERSION
  offset = putU32(view, offset, records.length)
  for (let i = 0; i < records.length; i++) {
    const r = records[i]!
    offset = putHeaders(buf, view, offset, encoded[i]!)
    offset = putU32(view, offset, r.body.length)
    offset = putBytes(buf, offset, r.body)
  }
  return buf
}

export function decodeBatchAppend(payload: Uint8Array): RecordEnvelope[] {
  const r = new Reader(payload)
  r.checkVersion(BATCH_VERSION, 'batch')
  const count = r.u32()
  const records: RecordEnvelope[] = []
  for (let i = 0; i < count; i++) {
    const headers = r.headers()
    const bodyLen = r.u32()
    const body = r.bytes(bodyLen)
    records.push({ timestamp: 0n, headers, body })
  }
  return records
}

export function encodeBatchRead(records: SequencedRecord[]): Uint8Array {
  const encoded = records.map((rec) => encodeHeaders(rec.envelope.headers))
  let size = 5
  for (let i = 0; i < records.length; i++) {
    size += 16 + encoded[i]!.size + 4 + records[i]!.envelope.body.length
  }
  const buf = new Uint8Array(size)
  const view = new DataView(buf.buffer)
  let offset = 0
  buf[offset++] = BATCH_VERSION
  offset = putU32(view, offset, records.length)
  for (let i = 0; i < records.length; i++) {
    const rec = records[i]!
    offset = putU64(view, offset, rec.seq)
    offset = putI64(view, offset, rec.envelope.timestamp)
    offset = putHeaders(buf, view, offset, encoded[i]!)
    offset = putU32(view, offset, rec.envelope.body.length)
    offset = putBytes(buf, offset, rec.envelope.body)
  }
  return buf
}

export function decodeBatchRead(payload: Uint8Array): SequencedRecord[] {
  const r = new Reader(payload)
  r.checkVersion(BATCH_VERSION, 'batch')
  const count = r.u32()
  const records: SequencedRecord[] = []
  for (let i = 0; i < count; i++) {
    const seq = r.u64()
    const timestamp = r.i64()
    const headers = r.headers()
    const bodyLen = r.u32()
    const body = r.bytes(bodyLen)
    records.push({ seq, envelope: { timestamp, headers, body } })
  }
  return records
}

export function encodeEnvelope(envelope: RecordEnvelope): Uint8Array {
  const encoded = encodeHeaders(envelope.headers)
  const size = 1 + 8 + encoded.size + envelope.body.length
  const buf = new Uint8Array(size)
  const view = new DataView(buf.buffer)
  let offset = 0
  buf[offset++] = ENVELOPE_VERSION
  offset = putI64(view, offset, envelope.timestamp)
  offset = putHeaders(buf, view, offset, encoded)
  putBytes(buf, offset, envelope.body)
  return buf
}

export function decodeEnvelope(payload: Uint8Array): RecordEnvelope {
  const r = new Reader(payload)
  r.checkVersion(ENVELOPE_VERSION, 'record envelope')
  const timestamp = r.i64()
  const headers = r.headers()
  return { timestamp, headers, body: r.rest() }
}
