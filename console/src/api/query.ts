import { queryOptions, useInfiniteQuery } from '@tanstack/preact-query'
import { useMemo } from 'preact/hooks'

import { admin, request, type Query, type Segments } from './client'

export interface Page<T> {
  items: T[]
  next: string | null
}

export interface Listing<T> {
  key: readonly unknown[]
  fetch: (cursor: string | undefined, signal: AbortSignal) => Promise<Page<T>>
}

export interface Pages<T> {
  items: T[]
  more: boolean
  loading: boolean
  error: Error | null
  next: () => void
}

export function read<T>(segments: Segments, query: Query = {}) {
  return queryOptions({
    queryKey: [...segments, query],
    queryFn: ({ signal }) => request<T>('GET', admin(...segments), { query, signal }),
  })
}

export function listing<T>(segments: Segments, query: Query = {}): Listing<T> {
  return {
    key: [...segments, query],
    fetch: (cursor, signal) =>
      request<Page<T>>('GET', admin(...segments), { query: { ...query, cursor }, signal }),
  }
}

export function usePages<T>(listing: Listing<T>): Pages<T> {
  const query = useInfiniteQuery({
    queryKey: listing.key,
    queryFn: ({ pageParam, signal }) => listing.fetch(pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.next ?? undefined,
  })

  const items = useMemo(() => query.data?.pages.flatMap((page) => page.items) ?? [], [query.data])

  return {
    items,
    more: query.hasNextPage,
    loading: query.isFetching,
    error: query.error,
    next: () => {
      if (query.hasNextPage && !query.isFetchingNextPage) {
        void query.fetchNextPage()
      }
    },
  }
}
