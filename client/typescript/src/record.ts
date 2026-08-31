import type { AppendInput } from './types'

export interface RecordEnvelope {
  timestamp: bigint
  headers: { [key: string]: string }
  body: Uint8Array
}

const ENC = new TextEncoder()

export function toEnvelope(input: AppendInput): RecordEnvelope {
  if (input instanceof Uint8Array) {
    return { timestamp: 0n, headers: {}, body: input }
  }
  if (typeof input === 'string') {
    return { timestamp: 0n, headers: {}, body: ENC.encode(input) }
  }
  return {
    timestamp: typeof input.timestamp === 'bigint' ? input.timestamp : BigInt(input.timestamp ?? 0),
    headers: input.headers ?? {},
    body: typeof input.body === 'string' ? ENC.encode(input.body) : input.body,
  }
}

export function toEnvelopes(inputs: AppendInput[]): RecordEnvelope[] {
  return inputs.map(toEnvelope)
}
