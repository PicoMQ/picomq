const ENVELOPE_VERSION = 1
const BATCH_VERSION = 1

export class CodecError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'CodecError'
  }
}

export interface RecordEnvelope {
  timestamp: bigint
  headers: { [key: string]: string }
  body: Uint8Array
}

export interface SequencedRecord {
  seq: bigint
  envelope: RecordEnvelope
}

function sortedEntries(headers: { [key: string]: string }): [string, string][] {
  return Object.entries(headers).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
}

function headersSize(headers: { [key: string]: string }): number {
  let size = 4
  for (const [name, value] of sortedEntries(headers)) {
    size += 8 + utf8ByteLength(name) + utf8ByteLength(value)
  }
  return size
}

function utf8ByteLength(s: string): number {
  return new TextEncoder().encode(s).byteLength
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

function putHeaders(buf: Uint8Array, view: DataView, offset: number, headers: { [key: string]: string }): number {
  const entries = sortedEntries(headers)
  offset = putU32(view, offset, entries.length)
  const enc = new TextEncoder()
  for (const [name, value] of entries) {
    const nb = enc.encode(name)
    const vb = enc.encode(value)
    offset = putU32(view, offset, nb.length)
    offset = putBytes(buf, offset, nb)
    offset = putU32(view, offset, vb.length)
    offset = putBytes(buf, offset, vb)
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
  let size = 5
  for (const r of records) {
    size += 4 + headersSize(r.headers) + 4 + r.body.length
  }
  const buf = new Uint8Array(size)
  const view = new DataView(buf.buffer)
  let offset = 0
  buf[offset++] = BATCH_VERSION
  offset = putU32(view, offset, records.length)
  for (const r of records) {
    offset = putHeaders(buf, view, offset, r.headers)
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
  let size = 5
  for (const rec of records) {
    size += 16 + 4 + headersSize(rec.envelope.headers) + 4 + rec.envelope.body.length
  }
  const buf = new Uint8Array(size)
  const view = new DataView(buf.buffer)
  let offset = 0
  buf[offset++] = BATCH_VERSION
  offset = putU32(view, offset, records.length)
  for (const rec of records) {
    offset = putU64(view, offset, rec.seq)
    offset = putI64(view, offset, rec.envelope.timestamp)
    offset = putHeaders(buf, view, offset, rec.envelope.headers)
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
  const size = 1 + 8 + headersSize(envelope.headers) + envelope.body.length
  const buf = new Uint8Array(size)
  const view = new DataView(buf.buffer)
  let offset = 0
  buf[offset++] = ENVELOPE_VERSION
  offset = putI64(view, offset, envelope.timestamp)
  offset = putHeaders(buf, view, offset, envelope.headers)
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
