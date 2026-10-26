import { useQuery } from '@tanstack/preact-query'
import { Activity, Archive, GitCommitHorizontal, HardDrive, Layers, Network, Recycle } from 'lucide-preact'

import { cluster } from '../api/cluster'
import { nodes } from '../api/nodes'
import { overview } from '../api/overview'
import { REFRESH_MS, usePages } from '../api/query'
import { Age } from '../components/age'
import { Details } from '../components/details'
import { Heading } from '../components/heading'
import { List } from '../components/list'
import { Pending } from '../components/notice'
import { Table } from '../components/table'
import { Tag } from '../components/tag'
import { View } from '../components/view'
import * as format from '../format'
import { page } from '../location'
import { NODE_COLUMNS } from './nodes'

import './overview.css'

const RANKING_REFRESH_MS = 60_000

export default function Overview() {
  const state = useQuery({ ...cluster(), refetchInterval: REFRESH_MS })
  const ranking = useQuery({ ...overview(), refetchInterval: RANKING_REFRESH_MS })
  const pages = usePages(nodes())

  return (
    <View trail={[{ label: 'Overview' }]}>
      {state.data ? (
        <>
          <Heading
            title={state.data.clusterId}
            icon={Network}
            tags={state.data.fenced ? <Tag kind="danger">fenced</Tag> : <Tag kind="success">running</Tag>}
          />
          <Details
            items={[
              { label: 'Streams', icon: Layers, value: format.count(state.data.streamCount) },
              { label: 'Objects', icon: HardDrive, value: format.count(state.data.objectCount) },
              { label: 'Applied index', icon: GitCommitHorizontal, value: format.count(state.data.appliedIndex) },
              { label: 'Pending transfers', icon: Activity, value: format.count(state.data.pendingTransferCount) },
              { label: 'GC backlog', icon: Recycle, value: format.count(state.data.gc.backlog) },
              {
                label: 'Last snapshot',
                icon: Archive,
                value: state.data.snapshot.lastAtMs === 0 ? 'none yet' : <Age atMs={state.data.snapshot.lastAtMs} />,
              },
            ]}
          />
        </>
      ) : (
        <Pending error={state.error} />
      )}
      <section class="overview-section">
        <h2>Nodes</h2>
        <List pages={pages} columns={NODE_COLUMNS} rowKey={(node) => String(node.nodeId)} noun="nodes" />
      </section>
      <section class="overview-section">
        <div class="overview-title">
          <h2>Topics</h2>
          {ranking.data && (
            <small>
              ranked <Age atMs={ranking.data.computedAtMs} />
            </small>
          )}
        </div>
        {ranking.data ? (
          <div class="overview-rankings">
            <Table
              columns={[
                { name: 'topic', header: 'Largest', cell: (entry) => <a href={page('topics', entry.topic)}>{entry.topic}</a> },
                { name: 'size', header: 'Stored', width: '9rem', kind: 'number', cell: (entry) => format.bytes(entry.storedBytes) },
              ]}
              rows={ranking.data.topicsBySize}
              rowKey={(entry) => entry.topic}
              empty="No stored data yet"
            />
            <Table
              columns={[
                { name: 'topic', header: 'Busiest', cell: (entry) => <a href={page('topics', entry.topic)}>{entry.topic}</a> },
                {
                  name: 'rate',
                  header: 'Writes',
                  width: '9rem',
                  kind: 'number',
                  cell: (entry) => format.rate(entry.recordsPerSecond),
                },
              ]}
              rows={ranking.data.topicsByWriteRate}
              rowKey={(entry) => entry.topic}
              empty="No recent writes"
            />
          </div>
        ) : (
          <Pending error={ranking.error} />
        )}
      </section>
    </View>
  )
}
