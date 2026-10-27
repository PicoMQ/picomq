import { useMutation, useQuery, useQueryClient } from '@tanstack/preact-query'
import { Users } from 'lucide-preact'
import { useLocation, useRoute } from 'preact-iso'
import { useState } from 'preact/hooks'

import {
  deleteGroup,
  deleteOffsets,
  group,
  members,
  offsets,
  resetOffsets,
  type Assignment,
  type GroupDetail,
  type GroupOffset,
  type Member,
  type ResetTarget,
} from '../api/groups'
import { REFRESH_MS, usePages } from '../api/query'
import { Button } from '../components/button'
import { Confirm } from '../components/confirm'
import { Input, Select } from '../components/control'
import { DateTime } from '../components/datetime'
import { Details } from '../components/details'
import { Dialog } from '../components/dialog'
import { Form, Label, useDraft } from '../components/form'
import { Heading } from '../components/heading'
import { List } from '../components/list'
import { Notice, Pending } from '../components/notice'
import type { Column } from '../components/table'
import { Tabs } from '../components/tabs'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import * as format from '../format'
import { page, topicPage, withQuery } from '../location'
import { StateTag, protocol } from './groups'
import { nodeLink } from './nodes'
import Missing from './missing'

const TABS = [
  { path: undefined, label: 'Offsets', component: Offsets },
  { path: 'members', label: 'Members', component: Members },
]

export default function GroupView() {
  const { params } = useRoute()
  const { route } = useLocation()
  const client = useQueryClient()
  const id = params.id ?? ''
  const tab = TABS.find((each) => each.path === params.tab)
  const detail = useQuery({ ...group(id), refetchInterval: REFRESH_MS })
  const [resetting, setResetting] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const trail = [{ label: 'Groups', href: '/groups' }, { label: id }]

  const remove = async () => {
    await deleteGroup(id)
    await client.invalidateQueries({ queryKey: ['groups'] })
    route('/groups')
  }

  if (!tab) {
    return <Missing />
  }
  if (!detail.data) {
    return (
      <View trail={trail}>
        <Pending error={detail.error} />
      </View>
    )
  }

  const shown = detail.data
  const Content = tab.component
  return (
    <View trail={trail}>
      <Heading
        title={shown.groupId}
        icon={Users}
        tags={<StateTag state={shown.state} />}
        actions={
          <>
            <Button onClick={() => setResetting(true)}>Reset offsets</Button>
            <Button kind="danger" onClick={() => setDeleting(true)}>
              Delete group
            </Button>
          </>
        }
      />
      <Details
        items={[
          { label: 'Members', value: format.count(shown.members) },
          { label: 'Generation', value: format.count(shown.generation) },
          { label: 'Protocol', value: protocol(shown) },
          { label: 'Committed partitions', value: format.count(shown.committedPartitions) },
          { label: 'Coordinator', value: nodeLink(shown.coordinatorNodeId) },
        ]}
      />
      <Tabs tabs={TABS} href={(path) => page('groups', id, path)} />
      <Content id={id} />
      <Dialog title="Reset offsets" open={resetting} onClose={() => setResetting(false)}>
        <Reset group={shown} onDone={() => setResetting(false)} />
      </Dialog>
      <Confirm
        title="Delete group"
        name={shown.groupId}
        action="Delete group"
        open={deleting}
        onClose={() => setDeleting(false)}
        onConfirm={remove}
      >
        This deletes {shown.groupId} and all its committed offsets. Only a group without members can be deleted.
      </Confirm>
    </View>
  )
}

function Offsets({ id }: { id: string }) {
  const { path, query, route } = useLocation()
  const client = useQueryClient()
  const pages = usePages(offsets(id, query.topic))
  const [deleting, setDeleting] = useState(false)

  const columns: Column<GroupOffset>[] = [
    {
      name: 'topic',
      header: 'Topic',
      cell: (offset) => (offset.topic === null ? 'deleted topic' : <a href={topicPage(offset.topic)}>{offset.topic}</a>),
    },
    { name: 'partition', header: 'Partition', width: '9rem', kind: 'number', cell: (offset) => offset.partition },
    { name: 'committed', header: 'Committed', kind: 'number', cell: (offset) => format.count(offset.committedOffset) },
    { name: 'end', header: 'End offset', kind: 'number', cell: (offset) => format.count(offset.nextOffset) },
    { name: 'lag', header: 'Lag', kind: 'number', cell: (offset) => format.count(offset.lag) },
  ]

  const remove = async () => {
    await deleteOffsets(id, query.topic ?? '')
    await client.invalidateQueries({ queryKey: ['groups', id] })
  }

  return (
    <>
      <Toolbar
        actions={
          <>
            {query.topic && (
              <Button kind="danger" onClick={() => setDeleting(true)}>
                Delete offsets
              </Button>
            )}
            <Button onClick={pages.refresh}>Refresh</Button>
          </>
        }
      >
        <Input
          prefix="Topic"
          placeholder="all topics"
          key={query.topic ?? ''}
          defaultValue={query.topic ?? ''}
          onChange={(event) => route(withQuery(path, query, { topic: event.currentTarget.value }), true)}
        />
      </Toolbar>
      <List
        pages={pages}
        columns={columns}
        rowKey={(offset) => `${offset.topic ?? ''}:${offset.partition}`}
        noun="committed partition"
      />
      <Confirm
        title="Delete offsets"
        name={query.topic ?? ''}
        action="Delete offsets"
        open={deleting}
        onClose={() => setDeleting(false)}
        onConfirm={remove}
      >
        This deletes every committed offset of {id} for {query.topic}.
      </Confirm>
    </>
  )
}

function Members({ id }: { id: string }) {
  const pages = usePages(members(id))

  const columns: Column<Member>[] = [
    { name: 'member', header: 'Member', cell: (member) => member.memberId },
    { name: 'client', header: 'Client', width: '12rem', cell: (member) => member.clientId },
    { name: 'host', header: 'Host', width: '12rem', cell: (member) => member.clientHost },
    { name: 'instance', header: 'Instance', width: '10rem', cell: (member) => member.instanceId ?? 'none' },
    { name: 'assignment', header: 'Assignment', cell: (member) => assignment(member.assignment) },
  ]

  return (
    <>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>} />
      <List pages={pages} columns={columns} rowKey={(member) => member.memberId} noun="member" />
    </>
  )
}

function assignment(assigned: Assignment): string {
  if ('topics' in assigned) {
    return assigned.topics.map((each) => `${each.topic}: ${each.partitions.join(', ')}`).join('; ') || 'none'
  }
  if ('streams' in assigned) {
    return assigned.streams.join(', ') || 'none'
  }
  return `${format.bytes(assigned.bytes)} of client data`
}

function Reset({ group: shown, onDone }: { group: GroupDetail; onDone: () => void }) {
  const client = useQueryClient()
  const { draft, edit, set } = useDraft({ topic: '', to: 'earliest', at: '', partitions: '' })
  const chosen = indexes(draft.partitions)
  const positioned = draft.to === 'earliest' || draft.to === 'latest' || draft.at !== ''

  const mutation = useMutation({
    mutationFn: () =>
      resetOffsets(shown.groupId, { topic: draft.topic.trim(), to: target(draft.to, draft.at), partitions: chosen ?? undefined }),
    onSuccess: () => client.invalidateQueries({ queryKey: ['groups', shown.groupId] }),
  })

  return (
    <Form
      submit="Reset"
      pending={mutation.isPending}
      disabled={draft.topic.trim() === '' || !positioned || chosen === null}
      error={mutation.error}
      onSubmit={() => mutation.mutate()}
      onCancel={onDone}
    >
      <Notice>Offsets can only be reset while the group has no members.</Notice>
      <Label text="Topic">
        <Input value={draft.topic} required placeholder="orders" onInput={edit('topic')} />
      </Label>
      <Label text="Reset to">
        <Select value={draft.to} onChange={edit('to')}>
          <option value="earliest">Earliest</option>
          <option value="latest">Latest</option>
          <option value="offset">Offset</option>
          <option value="time">Time</option>
        </Select>
      </Label>
      {draft.to === 'offset' && (
        <Label text="Offset">
          <Input type="number" min={0} required value={draft.at} onInput={edit('at')} />
        </Label>
      )}
      {draft.to === 'time' && (
        <Label text="From">
          <DateTime value={draft.at === '' ? null : Number(draft.at)} onChange={(ms) => set('at', ms === null ? '' : String(ms))} />
        </Label>
      )}
      <Label text="Partitions, comma separated, empty for all">
        <Input value={draft.partitions} placeholder="0, 1, 2" aria-invalid={chosen === null} onInput={edit('partitions')} />
      </Label>
      {mutation.data && <Notice>Reset {format.counted(mutation.data.partitions.length, 'partition')}</Notice>}
    </Form>
  )
}

export function indexes(text: string): number[] | undefined | null {
  if (text.trim() === '') {
    return undefined
  }

  const parts = text.split(',').map((part) => part.trim())
  return parts.every((part) => /^\d+$/.test(part)) ? parts.map(Number) : null
}

function target(to: string, at: string): ResetTarget {
  if (to === 'offset') {
    return { offset: Number(at) }
  }
  if (to === 'time') {
    return { timestamp: Number(at) }
  }
  return to === 'latest' ? 'latest' : 'earliest'
}
