import { describe, expect, it } from 'vitest'
import { RetryPolicy } from '../src/retry'

describe('RetryPolicy', () => {
  it('none never delays', () => {
    expect(RetryPolicy.none().delay(0)).toBeNull()
  })

  it('retries retryable failures up to the attempt budget', async () => {
    const policy = new RetryPolicy(3, 0, 0, 2)
    let calls = 0
    const result = await policy.run(
      async () => {
        calls += 1
        if (calls < 3) throw new Error('boom')
        return 'ok'
      },
      () => true,
    )
    expect(result).toBe('ok')
    expect(calls).toBe(3)
  })

  it('does not retry non-retryable failures', async () => {
    const policy = new RetryPolicy(5, 0, 0, 2)
    let calls = 0
    await expect(
      policy.run(
        async () => {
          calls += 1
          throw new Error('nope')
        },
        () => false,
      ),
    ).rejects.toThrow('nope')
    expect(calls).toBe(1)
  })
})
