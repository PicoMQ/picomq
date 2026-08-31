import { throwIfAborted } from './error'
import { sleep } from './util'

export class RetryPolicy {
  readonly maxAttempts: number
  readonly initialBackoffMs: number
  readonly maxBackoffMs: number
  readonly multiplier: number

  constructor(maxAttempts = 1, initialBackoffMs = 0, maxBackoffMs = 0, multiplier = 1) {
    this.maxAttempts = Math.max(1, maxAttempts)
    this.initialBackoffMs = Math.max(0, initialBackoffMs)
    this.maxBackoffMs = Math.max(0, maxBackoffMs)
    this.multiplier = Math.max(1, multiplier)
  }

  static none(): RetryPolicy {
    return new RetryPolicy(1, 0, 0, 1)
  }

  static attempts(maxAttempts: number): RetryPolicy {
    return new RetryPolicy(Math.max(1, maxAttempts), 100, 30_000, 2)
  }

  delay(attempt: number): number | null {
    if (attempt + 1 >= this.maxAttempts) return null
    if (this.initialBackoffMs === 0) return 0
    const cap = this.maxBackoffMs > 0 ? this.maxBackoffMs : Number.POSITIVE_INFINITY
    return Math.min(this.initialBackoffMs * this.multiplier ** attempt, cap)
  }

  async run<T>(
    operation: () => Promise<T>,
    isRetryable: (error: unknown) => boolean,
    signal?: AbortSignal,
  ): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      throwIfAborted(signal)
      try {
        return await operation()
      } catch (error) {
        const wait = this.delay(attempt)
        if (wait === null || !isRetryable(error)) throw error
        await sleep(jittered(wait), signal)
      }
    }
  }
}

function jittered(ms: number): number {
  if (ms <= 0) return 0
  return ms / 2 + Math.random() * (ms / 2)
}
