import { afterEach, describe, expect, it, vi } from 'vitest'

import { listing, read } from '../src/api/query'

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('listing', () => {
  it('keys by path segments and sends the cursor with the same filters', async () => {
    const urls: string[] = []
    vi.stubGlobal('sessionStorage', { getItem: () => null })
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string) => {
        urls.push(url)
        return new Response(JSON.stringify({ items: [1], next: null }))
      }),
    )

    const pages = listing<number>(['topics', '/a', 'partitions'], { node: 2 })
    const page = await pages.fetch('abc', new AbortController().signal)

    expect(pages.key).toEqual(['topics', '/a', 'partitions', { node: 2 }])
    expect(page).toEqual({ items: [1], next: null })
    expect(urls).toEqual(['/admin/topics/%2Fa/partitions?node=2&cursor=abc'])
  })
})

describe('read', () => {
  it('shares the key prefix with listings of the same area', () => {
    expect(read(['topics', '/a']).queryKey).toEqual(['topics', '/a', {}])
  })
})
