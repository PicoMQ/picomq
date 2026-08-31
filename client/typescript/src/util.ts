import { ClientError } from './error'

export function base64Decode(value: string): Uint8Array {
  const bin = atob(value)
  const out = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
  return out
}

export function parseOptionalUint(value: string | undefined): number | undefined {
  if (value === undefined) return undefined
  const n = Number(value)
  return Number.isFinite(n) ? n : undefined
}

export function retryableError(error: unknown): boolean {
  return error instanceof ClientError && error.retryable()
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export function parseSafeSeq(value: string, label: string): number {
  if (!/^-?\d+$/.test(value)) {
    throw new ClientError('other', `${label} is not an integer: ${value}`, {
      code: 'invalid_response',
    })
  }
  const n = Number(value)
  if (!Number.isSafeInteger(n)) {
    throw new ClientError(
      'other',
      `${label} ${value} exceeds Number.MAX_SAFE_INTEGER`,
      { code: 'invalid_response' },
    )
  }
  return n
}
