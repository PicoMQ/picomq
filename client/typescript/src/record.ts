import type { AppendInput, HeaderValue } from './types'

export interface RecordEnvelope {
  timestamp: bigint
  key?: Uint8Array
  headers: { [key: string]: HeaderValue }
  body: Uint8Array
}

const ENC = new TextEncoder()

export function toBytes(value: Uint8Array | string): Uint8Array {
  return typeof value === 'string' ? ENC.encode(value) : value
}

export function toEnvelope(input: AppendInput): RecordEnvelope {
  if (input instanceof Uint8Array) {
    return { timestamp: 0n, headers: {}, body: input }
  }
  if (typeof input === 'string') {
    return { timestamp: 0n, headers: {}, body: ENC.encode(input) }
  }
  const envelope: RecordEnvelope = {
    timestamp: typeof input.timestamp === 'bigint' ? input.timestamp : BigInt(input.timestamp ?? 0),
    headers: input.headers ?? {},
    body: toBytes(input.body),
  }
  if (input.key !== undefined) envelope.key = toBytes(input.key)
  return envelope
}

export function toEnvelopes(inputs: AppendInput[]): RecordEnvelope[] {
  return inputs.map(toEnvelope)
}
