import { useMutation, useQueryClient } from '@tanstack/preact-query'
import { useLocation } from 'preact-iso'
import { useState } from 'preact/hooks'

import { usePages } from '../api/query'
import { partitions, trimPartition, type Partition, type TopicDetail } from '../api/topics'
import { Button } from '../components/button'
import { Input, Select, numberOf } from '../components/control'
import { Dialog } from '../components/dialog'
import { Form, Label, useDraft } from '../components/form'
import { List } from '../components/list'
import { Notice } from '../components/notice'
import type { Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import * as format from '../format'
import { topicPage, withQuery } from '../location'
import { nodeLink } from './nodes'

export function Partitions({ name, topic }: { name: string; topic: TopicDetail }) {
  const { path, query, route } = useLocation()
  const pages = usePages(partitions(name, numberOf(query.node ?? '')))
  const [trimming, setTrimming] = useState<Partition | null>(null)

  const columns: Column<Partition>[] = [
    {
      name: 'index',
      header: 'Partition',
      width: '8rem',
      kind: 'number',
      cell: (partition) => <a href={withQuery(topicPage(name), {}, { partition: partition.index })}>{partition.index}</a>,
    },
    { name: 'owner', header: 'Owner', width: '9rem', cell: (partition) => nodeLink(partition.ownerNodeId, 'unassigned') },
    { name: 'epoch', header: 'Epoch', width: '7rem', kind: 'number', cell: (partition) => partition.epoch },
    { name: 'start', header: 'Start offset', kind: 'number', cell: (partition) => format.count(partition.startOffset) },
    { name: 'next', header: 'Next offset', kind: 'number', cell: (partition) => format.count(partition.nextOffset) },
    { name: 'records', header: 'Records', kind: 'number', cell: records },
    {
      name: 'stored',
      header: 'Stored',
      kind: 'number',
      cell: (partition) => (partition.storedBytes === null ? 'unknown' : format.bytes(partition.storedBytes)),
    },
    { name: 'state', header: 'State', width: '15rem', cell: state },
    {
      name: 'trim',
      header: '',
      width: '7rem',
      cell: (partition) => <Button onClick={() => setTrimming(partition)}>Trim</Button>,
    },
  ]

  return (
    <>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>}>
        <Select
          prefix="Owner"
          value={query.node ?? ''}
          onChange={(event) => route(withQuery(path, query, { node: event.currentTarget.value }), true)}
        >
          <option value="">All</option>
          {topic.owners
            .filter((entry) => entry.nodeId !== null)
            .map((entry) => (
              <option key={entry.nodeId} value={String(entry.nodeId)}>
                node {entry.nodeId}
              </option>
            ))}
        </Select>
      </Toolbar>
      <List pages={pages} columns={columns} rowKey={(partition) => String(partition.index)} noun="partition" />
      <Dialog title={`Trim partition ${trimming?.index ?? ''}`} open={trimming !== null} onClose={() => setTrimming(null)}>
        {trimming && <Trim name={name} partition={trimming} onDone={() => setTrimming(null)} />}
      </Dialog>
    </>
  )
}

function records(partition: Partition): string {
  return format.count(
    partition.startOffset === null || partition.nextOffset === null ? null : partition.nextOffset - partition.startOffset,
  )
}

function state(partition: Partition) {
  return (
    <>
      {partition.state ?? 'not opened'}{' '}
      {partition.pendingTransfer && <Tag>moving to node {partition.pendingTransfer.toNode}</Tag>}{' '}
      {!partition.current && <Tag kind="danger">offsets stale</Tag>}
    </>
  )
}

function Trim({ name, partition, onDone }: { name: string; partition: Partition; onDone: () => void }) {
  const client = useQueryClient()
  const { draft, edit } = useDraft({ before: '' })
  const before = numberOf(draft.before)
  const mutation = useMutation({
    mutationFn: (offset: number) => trimPartition(name, partition.index, offset),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['topics'] })
      onDone()
    },
  })

  return (
    <Form
      submit="Trim"
      kind="danger"
      pending={mutation.isPending}
      disabled={before === undefined}
      error={mutation.error}
      onSubmit={() => {
        if (before !== undefined) {
          mutation.mutate(before)
        }
      }}
      onCancel={onDone}
    >
      <Notice>
        Every record before the offset is deleted for good. The partition now holds offsets {format.count(partition.startOffset)} to{' '}
        {format.count(partition.nextOffset)}.
      </Notice>
      <Label text="Delete records before offset">
        <Input
          type="number"
          min={partition.startOffset ?? 0}
          max={partition.nextOffset ?? undefined}
          required
          value={draft.before}
          onInput={edit('before')}
        />
      </Label>
    </Form>
  )
}
