import { useLocation } from 'preact-iso'

import { groups, type Group } from '../api/groups'
import { usePages } from '../api/query'
import { Button } from '../components/button'
import { List } from '../components/list'
import { Search } from '../components/search'
import type { Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import * as format from '../format'
import { page } from '../location'
import { nodeLink } from './nodes'

const COLUMNS: Column<Group>[] = [
  { name: 'group', header: 'Group', cell: (group) => <a href={page('groups', group.groupId)}>{group.groupId}</a> },
  { name: 'state', header: 'State', width: '14rem', cell: (group) => <StateTag state={group.state} /> },
  {
    name: 'members',
    header: 'Members',
    width: '9rem',
    kind: 'number',
    cell: (group) => (group.state === null ? '' : format.count(group.members)),
  },
  { name: 'protocol', header: 'Protocol', width: '14rem', cell: (group) => (group.state === null ? '' : protocol(group)) },
  { name: 'coordinator', header: 'Coordinator', width: '10rem', cell: (group) => nodeLink(group.coordinatorNodeId) },
]

export function StateTag({ state }: { state: Group['state'] }) {
  if (state === null) {
    return <Tag kind="danger">unreachable</Tag>
  }
  return <Tag kind={state === 'Stable' ? 'success' : 'plain'}>{state}</Tag>
}

export function protocol(group: { protocolType: string; protocolName: string }): string {
  return [group.protocolType, group.protocolName].filter((part) => part !== '').join(' / ') || 'none'
}

export default function Groups() {
  const { query } = useLocation()
  const pages = usePages(groups(query.prefix))

  return (
    <View trail={[{ label: 'Groups' }]}>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>}>
        <Search placeholder="Search groups by id prefix" />
      </Toolbar>
      <List pages={pages} columns={COLUMNS} rowKey={(group) => group.groupId} noun="group" />
    </View>
  )
}
