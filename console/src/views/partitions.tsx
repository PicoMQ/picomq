import { useLocation } from 'preact-iso'

import { usePages } from '../api/query'
import { partitions, type Partition, type TopicDetail } from '../api/topics'
import { Button } from '../components/button'
import { Select, numberOf } from '../components/control'
import { List } from '../components/list'
import type { Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import * as format from '../format'
import { topicPage, withQuery } from '../location'
import { nodeLink } from './nodes'

export function Partitions({ name, topic }: { name: string; topic: TopicDetail }) {
  const { path, query, route } = useLocation()
  const pages = usePages(partitions(name, numberOf(query.node ?? '')))

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
  ]

  return (
    <>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>}>
        <Select prefix="Owner"
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
