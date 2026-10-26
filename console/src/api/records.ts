import { useEffect, useState } from 'preact/hooks'

import { keep } from '../ring'
import { listing, read, type Listing } from './query'
import type { Partition } from './topics'

export const TAIL_RECORDS = 1000

export const TAIL_WAIT_MS = 5000

export interface Field {
  data: string
  encoding: 'utf8' | 'base64'
  size: number
  truncated: boolean
}

export interface Header {
  name: string
  value: Field | null
}

export interface LogRecord {
  offset: number
  timestamp: number
  key: Field | null
  headers: Header[]
  value: Field | null
}

export interface RecordFilter {
  key?: string
  header?: string
  headerValue?: string
  value?: string
}

export type RecordStart = { offset: number } | { timestamp: number } | null

export interface RecordQuery {
  filter: RecordFilter
  start: RecordStart
  limit: number
  wait?: number
}

export interface Browse {
  start?: string
  at?: string
  key?: string
  value?: string
  header?: string
  headerValue?: string
}

export function recordFilter({ key, value, header, headerValue }: Browse): RecordFilter {
  return { key, value, header, headerValue }
}

export function recordStart({ start, at }: Browse, partition: Partition, limit: number): RecordStart {
  const position = at === undefined || at === '' ? undefined : Number(at)

  if (start === 'earliest') {
    return null
  }
  if (start === 'offset' && position !== undefined) {
    return { offset: position }
  }
  if (start === 'time' && position !== undefined) {
    return { timestamp: position }
  }
  return { offset: Math.max(partition.startOffset ?? 0, (partition.nextOffset ?? 0) - limit) }
}

export const records = (name: string, index: number, { filter, start, limit, wait }: RecordQuery) =>
  listing<LogRecord>(['topics', name, 'partitions', index, 'records'], {
    ...filter,
    ...start,
    limit,
    wait,
  })

export const record = (name: string, index: number, offset: number) =>
  read<LogRecord>(['topics', name, 'partitions', index, 'records', offset])

export function useTail(listing: Listing<LogRecord>): { items: LogRecord[]; error: Error | null } {
  const [items, setItems] = useState<LogRecord[]>([])
  const [error, setError] = useState<Error | null>(null)

  useEffect(() => {
    setItems([])
    setError(null)

    const abort = new AbortController()
    const follow = async () => {
      let cursor: string | undefined
      while (!abort.signal.aborted) {
        const page = await listing.fetch(cursor, abort.signal)
        cursor = page.next ?? cursor
        if (page.items.length > 0) {
          setItems((shown) => keep(shown, page.items, TAIL_RECORDS))
        }
      }
    }

    follow().catch((failure: Error) => {
      if (!abort.signal.aborted) {
        setError(failure)
      }
    })
    return () => abort.abort()
  }, [listing])

  return { items, error }
}
