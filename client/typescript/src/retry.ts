import {
  ConstantBackoff,
  ExponentialBackoff,
  handleWhen,
  retry,
} from 'cockatiel'

export class RetryPolicy {
  readonly maxAttempts: number
  readonly initialBackoffMs: number
  readonly maxBackoffMs: number
  readonly multiplier: number

  constructor(
    maxAttempts = 1,
    initialBackoffMs = 0,
    maxBackoffMs = 0,
    multiplier = 1,
  ) {
    this.maxAttempts = Math.max(1, maxAttempts)
    this.initialBackoffMs = initialBackoffMs
    this.maxBackoffMs = maxBackoffMs
    this.multiplier = multiplier
  }

  static none(): RetryPolicy {
    return new RetryPolicy(1, 0, 0, 1)
  }

  static attempts(maxAttempts: number): RetryPolicy {
    return new RetryPolicy(Math.max(1, maxAttempts), 100, 30_000, 2)
  }

  delay(attempt: number): number | null {
    if (attempt + 1 >= this.maxAttempts) return null
    return this.backoffMs(attempt + 1)
  }

  async run<T>(
    operation: () => Promise<T>,
    isRetryable: (error: unknown) => boolean,
    signal?: AbortSignal,
  ): Promise<T> {
    if (this.maxAttempts <= 1) {
      return operation()
    }

    const backoff =
      this.initialBackoffMs === 0
        ? new ConstantBackoff(0)
        : new ExponentialBackoff({
            initialDelay: this.initialBackoffMs,
            maxDelay: this.maxBackoffMs,
            exponent: this.multiplier,
          })

    const policy = retry(handleWhen(isRetryable), {
      maxAttempts: this.maxAttempts,
      backoff,
    })

    return policy.execute(() => operation(), signal)
  }

  private backoffMs(attempt: number): number {
    if (this.initialBackoffMs === 0 || attempt === 0) return 0
    const ms = this.initialBackoffMs * this.multiplier ** (attempt - 1)
    return Math.min(ms, this.maxBackoffMs)
  }
}
