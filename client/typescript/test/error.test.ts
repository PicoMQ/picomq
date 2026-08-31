import { describe, expect, it } from 'vitest'
import { ClientError, isAbortError } from '../src/error'

describe('ClientError', () => {
  it('does not treat abort as retryable', () => {
    expect(ClientError.aborted().retryable()).toBe(false)
    expect(ClientError.transport('x').retryable()).toBe(true)
  })

  it('detects abort errors', () => {
    expect(isAbortError(ClientError.aborted())).toBe(true)
    expect(isAbortError(Object.assign(new Error('x'), { name: 'AbortError' }))).toBe(true)
    expect(isAbortError(ClientError.transport('x'))).toBe(false)
  })
})
