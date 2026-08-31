import { describe, expect, it } from 'vitest'
import { DsClient } from '../../src/ds/client'
import { ClientError } from '../../src/error'

describe('DsClient', () => {
  const client = new DsClient('http://127.0.0.1:4437')

  it('rejects multi-record append', async () => {
    await expect(
      client.append('/s', [new Uint8Array([1]), new Uint8Array([2])]),
    ).rejects.toBeInstanceOf(ClientError)
  })

  it('rejects record headers', async () => {
    await expect(
      client.append('/s', [{ body: 'x', headers: { a: 'b' } }]),
    ).rejects.toMatchObject({ kind: 'unsupported' })
  })

  it('rejects list', async () => {
    await expect(client.list('/')).rejects.toBeInstanceOf(ClientError)
  })
})
