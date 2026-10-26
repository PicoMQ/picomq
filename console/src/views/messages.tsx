import { useQuery, useQueryClient } from '@tanstack/preact-query'
import { ListFilter, Radio, RefreshCw, Send, X } from 'lucide-preact'
import { ErrorBoundary, lazy, useLocation } from 'preact-iso'
import { useMemo, useState } from 'preact/hooks'

import { REFRESH_MS, usePages } from '../api/query'
import {
  TAIL_RECORDS,
  TAIL_WAIT_MS,
  record,
  recordFilter,
  recordStart,
  records,
  useTail,
  type LogRecord,
  type RecordFilter,
} from '../api/records'
import { partition, type Partition, type TopicDetail } from '../api/topics'
import { Button } from '../components/button'
import { Input, Select, numberOf } from '../components/control'
import { DateTime } from '../components/datetime'
import { Dialog } from '../components/dialog'
import { Field, pretty } from '../components/field'
import { Form, Label, useDraft } from '../components/form'
import { List } from '../components/list'
import { PartitionPicker } from '../components/partition'
import { Notice, Pending } from '../components/notice'
import { Table, type Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import * as format from '../format'
import { withQuery } from '../location'
import { Produce } from './produce'

import './messages.css'

const Editor = lazy(() => import('../components/editor'))

const LIMITS = ['50', '100', '500']
const TAIL_PAGE = 100

const COLUMNS: Column<LogRecord>[] = [
  { name: 'offset', header: 'Offset', width: '9rem', kind: 'number', cell: (shown) => format.count(shown.offset) },
  { name: 'timestamp', header: 'Timestamp', width: '14rem', cell: (shown) => format.time(shown.timestamp) },
  { name: 'key', header: 'Key', width: '14rem', cell: (shown) => <Field field={shown.key} /> },
  { name: 'value', header: 'Value', cell: (shown) => <Field field={shown.value} /> },
  { name: 'headers', header: 'Headers', width: '7rem', kind: 'number', cell: (shown) => shown.headers.length },
]

export function Messages({ name, topic }: { name: string; topic: TopicDetail }) {
  const { query } = useLocation()
  const client = useQueryClient()
  const index = Math.min(Math.max(numberOf(query.partition ?? '') ?? 0, 0), topic.partitions - 1)
  const detail = useQuery({ ...partition(name, index), refetchInterval: REFRESH_MS })
  const [generation, setGeneration] = useState(0)

  const refresh = async () => {
    await detail.refetch()
    await client.resetQueries({ queryKey: ['topics', name, 'partitions', index, 'records'] })
    setGeneration(generation + 1)
  }

  return (
    <>
      <Controls name={name} topic={topic} index={index} detail={detail.data} onRefresh={refresh} />
      <Filters />
      {detail.error && <Notice kind="danger">{detail.error.message}</Notice>}
      {detail.data &&
        (query.live === undefined ? (
          <Browse name={name} index={index} detail={detail.data} generation={generation} />
        ) : (
          <Tail name={name} index={index} from={Number(query.live)} />
        ))}
    </>
  )
}

interface ControlsProps {
  name: string
  topic: TopicDetail
  index: number
  detail: Partition | undefined
  onRefresh: () => void
}

function Controls({ name, topic, index, detail, onRefresh }: ControlsProps) {
  const { path, query, route } = useLocation()
  const [filtering, setFiltering] = useState(false)
  const [producing, setProducing] = useState(false)
  const partitions = topic.partitions
  const start = query.start ?? 'latest'
  const live = query.live !== undefined
  const set = (changes: Record<string, string | number | undefined>) =>
    route(withQuery(path, query, { ...changes, record: undefined }))

  return (
    <>
      <Toolbar
        actions={
          <>
            <Button kind="primary" onClick={() => setProducing(true)}>
              <Send size={16} />
              Produce
            </Button>
            <Button onClick={() => setFiltering(true)}>
              <ListFilter size={16} />
              Filter
            </Button>
            <Button kind={live ? 'primary' : 'secondary'} onClick={() => set({ live: live ? undefined : (detail?.nextOffset ?? 0) })}>
              <Radio size={16} />
              {live ? 'Stop live' : 'Live'}
            </Button>
            {!live && (
              <Button onClick={onRefresh}>
                <RefreshCw size={16} />
                Refresh
              </Button>
            )}
          </>
        }
      >
        <PartitionPicker partitions={partitions} value={index} prefix="Partition" onChange={(chosen) => set({ partition: chosen })} />
        <Select prefix="Start" value={start} onChange={(event) => set({ start: event.currentTarget.value, at: undefined })}>
          <option value="latest">Latest</option>
          <option value="earliest">Earliest</option>
          <option value="offset">Offset</option>
          <option value="time">Time</option>
        </Select>
        {start === 'offset' && (
          <Input
            prefix="Offset"
            type="number"
            min={0}
            size={10}
            value={query.at ?? ''}
            onChange={(event) => set({ at: event.currentTarget.value })}
          />
        )}
        {start === 'time' && (
          <DateTime prefix="From" value={query.at === undefined ? null : Number(query.at)} onChange={(ms) => set({ at: ms ?? undefined })} />
        )}
        <Select prefix="Max" value={query.limit ?? LIMITS[0]} onChange={(event) => set({ limit: event.currentTarget.value })}>
          {LIMITS.map((limit) => (
            <option key={limit} value={limit}>
              {limit}
            </option>
          ))}
        </Select>
      </Toolbar>
      <Dialog title="Produce a record" open={producing} onClose={() => setProducing(false)}>
        <Produce
          name={name}
          topic={topic}
          index={index}
          onDone={() => setProducing(false)}
          onShow={(chosen, offset) => {
            setProducing(false)
            route(withQuery(path, query, { partition: chosen, start: 'offset', at: offset, record: offset, live: undefined }))
          }}
        />
      </Dialog>
      <Dialog title="Filter records" open={filtering} onClose={() => setFiltering(false)}>
        <FilterForm filter={recordFilter(query)} onApply={(filter) => set({ ...filter })} onDone={() => setFiltering(false)} />
      </Dialog>
    </>
  )
}

function FilterForm({ filter, onApply, onDone }: { filter: RecordFilter; onApply: (filter: RecordFilter) => void; onDone: () => void }) {
  const { draft, edit } = useDraft({
    key: filter.key ?? '',
    value: filter.value ?? '',
    header: filter.header ?? '',
    headerValue: filter.headerValue ?? '',
  })

  const apply = () => {
    onApply(draft)
    onDone()
  }

  return (
    <Form submit="Apply" onSubmit={apply} onCancel={onDone}>
      <Label text="Key contains">
        <Input value={draft.key} onInput={edit('key')} />
      </Label>
      <Label text="Value contains">
        <Input value={draft.value} onInput={edit('value')} />
      </Label>
      <Label text="Header name">
        <Input value={draft.header} onInput={edit('header')} />
      </Label>
      <Label text="Header value contains">
        <Input value={draft.headerValue} disabled={draft.header === ''} onInput={edit('headerValue')} />
      </Label>
    </Form>
  )
}

function Filters() {
  const { path, query, route } = useLocation()
  const filter = recordFilter(query)
  const remove = (changes: RecordFilter) => route(withQuery(path, query, { ...changes, record: undefined }))

  const active: { label: string; clear: RecordFilter }[] = []
  if (filter.key) {
    active.push({ label: `key contains "${filter.key}"`, clear: { key: undefined } })
  }
  if (filter.value) {
    active.push({ label: `value contains "${filter.value}"`, clear: { value: undefined } })
  }
  if (filter.header) {
    const label = filter.headerValue ? `header ${filter.header} contains "${filter.headerValue}"` : `header ${filter.header}`
    active.push({ label, clear: { header: undefined, headerValue: undefined } })
  }

  if (active.length === 0) {
    return null
  }
  return (
    <div class="messages-filters">
      {active.map((each) => (
        <Tag key={each.label}>
          {each.label}
          <button type="button" class="messages-remove" aria-label={`Remove ${each.label}`} onClick={() => remove(each.clear)}>
            <X size={12} />
          </button>
        </Tag>
      ))}
    </div>
  )
}

function Browse({ name, index, detail, generation }: { name: string; index: number; detail: Partition; generation: number }) {
  const { path, query, route } = useLocation()
  const limit = numberOf(query.limit ?? '') ?? Number(LIMITS[0])
  const listing = useMemo(
    () => records(name, index, { filter: recordFilter(query), start: recordStart(query, detail, limit), limit }),
    [name, index, generation, query.start, query.at, query.limit, query.key, query.value, query.header, query.headerValue],
  )
  const pages = usePages(listing)
  const opened = query.record

  const select = (shown: LogRecord) => {
    const next = opened === String(shown.offset) ? undefined : shown.offset
    route(withQuery(path, query, { record: next }), true)
  }

  return (
    <List
      pages={pages}
      columns={COLUMNS}
      rowKey={(shown) => String(shown.offset)}
      noun="record"
      onSelect={select}
      expanded={opened === undefined ? undefined : { key: opened, content: <FullRecord name={name} index={index} offset={Number(opened)} /> }}
    />
  )
}

function Tail({ name, index, from }: { name: string; index: number; from: number }) {
  const { query } = useLocation()
  const listing = useMemo(
    () => records(name, index, { filter: recordFilter(query), start: { offset: from }, limit: TAIL_PAGE, wait: TAIL_WAIT_MS }),
    [name, index, from, query.key, query.value, query.header, query.headerValue],
  )
  const tail = useTail(listing)

  return (
    <>
      <Table columns={COLUMNS} rows={tail.items} rowKey={(shown) => String(shown.offset)} follow />
      <Notice kind={tail.error ? 'danger' : 'plain'}>
        {tail.error?.message ??
          `Live from offset ${format.count(from)}, ${format.counted(tail.items.length, 'record')}, keeping the last ${format.count(TAIL_RECORDS)}`}
      </Notice>
    </>
  )
}

function FullRecord({ name, index, offset }: { name: string; index: number; offset: number }) {
  const full = useQuery(record(name, index, offset))

  if (!full.data) {
    return <Pending error={full.error} />
  }

  const shown = full.data
  return (
    <div class="messages-detail">
      <Section title="Value" field={shown.value} />
      <Section title="Key" field={shown.key} />
      <div class="messages-section">
        <h3>Headers</h3>
        {shown.headers.length === 0 ? (
          <Notice>No headers</Notice>
        ) : (
          <dl class="messages-headers">
            {shown.headers.map((header, position) => (
              <div key={position} class="messages-header">
                <dt>{header.name}</dt>
                <dd>
                  <Field field={header.value} />
                </dd>
              </div>
            ))}
          </dl>
        )}
      </div>
    </div>
  )
}

function Section({ title, field }: { title: string; field: LogRecord['value'] }) {
  return (
    <div class="messages-section">
      <h3>
        {title}{' '}
        {field && (
          <small>
            {format.bytes(field.size)}
            {field.truncated && ', shown in part'}
          </small>
        )}
      </h3>
      {field === null ? (
        <Notice>null</Notice>
      ) : (
        <ErrorBoundary>
          <Editor {...pretty(field)} />
        </ErrorBoundary>
      )}
    </div>
  )
}
