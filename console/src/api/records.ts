import { listing, read } from './query'

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

export const records = (name: string, index: number, { filter, start, limit, wait }: RecordQuery) =>
  listing<LogRecord>(['topics', name, 'partitions', index, 'records'], {
    ...filter,
    ...start,
    limit,
    wait,
  })

export const record = (name: string, index: number, offset: number) =>
  read<LogRecord>(['topics', name, 'partitions', index, 'records', offset])
