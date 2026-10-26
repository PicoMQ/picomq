import { useLocation } from 'preact-iso'

import { nodes, type ClusterNode } from '../api/nodes'
import { usePages } from '../api/query'
import { Button } from '../components/button'
import { Select } from '../components/control'
import { List } from '../components/list'
import type { Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import * as format from '../format'
import { page, withQuery } from '../location'

export const NODE_COLUMNS: Column<ClusterNode>[] = [
  { name: 'node', header: 'Node', width: '8rem', cell: (node) => <a href={page('nodes', node.nodeId)}>node {node.nodeId}</a> },
  { name: 'status', header: 'Status', width: '14rem', cell: status },
  { name: 'address', header: 'Address', cell: (node) => node.advertisedAddress ?? 'none' },
  { name: 'placed', header: 'Logs placed', width: '10rem', kind: 'number', cell: (node) => format.count(node.placedCount) },
  { name: 'opening', header: 'Opening', width: '8rem', kind: 'number', cell: (node) => format.count(node.openingCount) },
  { name: 'slots', header: 'Slots', width: '7rem', kind: 'number', cell: (node) => format.count(node.slots) },
]

export function status(node: ClusterNode) {
  return (
    <>
      {node.fenced ? <Tag kind="danger">fenced</Tag> : <Tag kind="success">running</Tag>}{' '}
      {node.local && <Tag>this node</Tag>}
    </>
  )
}

export default function Nodes() {
  const { path, query, route } = useLocation()
  const fenced = query.fenced === undefined ? undefined : query.fenced === 'true'
  const pages = usePages(nodes(fenced))

  return (
    <View trail={[{ label: 'Nodes' }]}>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>}>
        <Select prefix="Status"
          value={query.fenced ?? ''}
          onChange={(event) => route(withQuery(path, query, { fenced: event.currentTarget.value }), true)}
        >
          <option value="">All</option>
          <option value="false">Running</option>
          <option value="true">Fenced</option>
        </Select>
      </Toolbar>
      <List pages={pages} columns={NODE_COLUMNS} rowKey={(node) => String(node.nodeId)} noun="nodes" />
    </View>
  )
}
