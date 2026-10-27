import type { AppendInput, Header } from './types'

export interface RecordEnvelope {
  timestamp: bigint
  key?: Uint8Array
  headers: Header[]
  body: Uint8Array | null
}

const ENC = new TextEncoder()

export function toBytes(value: Uint8Array | string): Uint8Array {
  return typeof value === 'string' ? ENC.encode(value) : value
}

export function toEnvelope(input: AppendInput): RecordEnvelope {
  if (input instanceof Uint8Array) {
    return { timestamp: 0n, headers: [], body: input }
  }
  if (typeof input === 'string') {
    return { timestamp: 0n, headers: [], body: ENC.encode(input) }
  }
  const envelope: RecordEnvelope = {
    timestamp: typeof input.timestamp === 'bigint' ? input.timestamp : BigInt(input.timestamp ?? 0),
    headers: input.headers ?? [],
    body: input.body === null ? null : toBytes(input.body),
  }
  if (input.key !== undefined) envelope.key = toBytes(input.key)
  return envelope
}

export function recordBytes(envelope: RecordEnvelope): number {
  let size = (envelope.body?.byteLength ?? 0) + (envelope.key?.byteLength ?? 0)
  for (const { name, value } of envelope.headers) {
    size += ENC.encode(name).byteLength + (value === null ? 0 : toBytes(value).byteLength)
  }
  return Math.max(1, size)
}

export function toEnvelopes(inputs: AppendInput[]): RecordEnvelope[] {
  return inputs.map(toEnvelope)
}
