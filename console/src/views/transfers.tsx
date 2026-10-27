import { useMutation, useQueryClient } from '@tanstack/preact-query'
import { ArrowLeftRight } from 'lucide-preact'
import { useState } from 'preact/hooks'

import { nodes } from '../api/nodes'
import { usePages } from '../api/query'
import { createTransfer, transfers, type Log, type Transfer } from '../api/transfers'
import { Button } from '../components/button'
import { Input, Select, numberOf } from '../components/control'
import { Dialog } from '../components/dialog'
import { Form, Label, useDraft } from '../components/form'
import { List } from '../components/list'
import type { Column } from '../components/table'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import { page, topicPage, withQuery } from '../location'
import { nodeLink } from './nodes'

const COLUMNS: Column<Transfer>[] = [
  { name: 'log', header: 'Log', cell: (transfer) => log(transfer.log) },
  { name: 'from', header: 'From', width: '10rem', cell: (transfer) => nodeLink(transfer.fromNode) },
  { name: 'to', header: 'To', width: '10rem', cell: (transfer) => nodeLink(transfer.toNode) },
]

function log(moved: Log) {
  if (moved === null) {
    return 'deleted log'
  }
  if ('group' in moved) {
    return <a href={page('groups', moved.group)}>group {moved.group}</a>
  }
  return <a href={withQuery(topicPage(moved.topic), {}, { partition: moved.partition })}>{`${moved.topic} partition ${moved.partition}`}</a>
}

export default function Transfers() {
  const pages = usePages(transfers())
  const [moving, setMoving] = useState(false)

  return (
    <View trail={[{ label: 'Transfers' }]}>
      <Toolbar
        actions={
          <>
            <Button onClick={pages.refresh}>Refresh</Button>
            <Button kind="primary" onClick={() => setMoving(true)}>
              <ArrowLeftRight size={16} />
              Move partition
            </Button>
          </>
        }
      />
      <List pages={pages} columns={COLUMNS} rowKey={(transfer) => key(transfer)} noun="transfer" />
      <Dialog title="Move partition" open={moving} onClose={() => setMoving(false)}>
        <Move onDone={() => setMoving(false)} />
      </Dialog>
    </View>
  )
}

function key(transfer: Transfer): string {
  if (transfer.log === null) {
    return `deleted:${transfer.fromNode}:${transfer.toNode}`
  }
  return 'group' in transfer.log ? `group:${transfer.log.group}` : `${transfer.log.topic}:${transfer.log.partition}`
}

function Move({ onDone }: { onDone: () => void }) {
  const client = useQueryClient()
  const targets = usePages(nodes(false))
  const { draft, edit } = useDraft({ topic: '', partition: '', toNode: '' })

  const mutation = useMutation({
    mutationFn: () =>
      createTransfer({ topic: draft.topic.trim(), partition: numberOf(draft.partition), toNode: Number(draft.toNode) }),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['transfers'] })
      onDone()
    },
  })

  return (
    <Form
      submit="Move"
      pending={mutation.isPending}
      disabled={draft.topic.trim() === '' || draft.toNode === ''}
      error={mutation.error}
      onSubmit={() => mutation.mutate()}
      onCancel={onDone}
    >
      <Label text="Topic">
        <Input value={draft.topic} required placeholder="orders" onInput={edit('topic')} />
      </Label>
      <Label text="Partition, empty for a single-partition topic">
        <Input type="number" min={0} value={draft.partition} onInput={edit('partition')} />
      </Label>
      <Label text="To node">
        <Select value={draft.toNode} required onChange={edit('toNode')}>
          <option value="">Choose a running node</option>
          {targets.items.map((node) => (
            <option key={node.nodeId} value={String(node.nodeId)}>
              node {node.nodeId}
            </option>
          ))}
        </Select>
      </Label>
    </Form>
  )
}
