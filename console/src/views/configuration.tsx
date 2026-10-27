import { useMutation, useQueryClient } from '@tanstack/preact-query'

import { createTopic, updateTopic, type Topic, type TopicDetail } from '../api/topics'
import { Input, numberOf } from '../components/control'
import { Form, Label, useDraft } from '../components/form'
import { Notice } from '../components/notice'

import './configuration.css'

interface Props {
  topic?: TopicDetail
  onSaved?: (topic: Topic) => void
  onCancel?: () => void
}

export function Configuration({ topic }: { name: string; topic: TopicDetail }) {
  return (
    <div class="configuration">
      <TopicForm topic={topic} />
    </div>
  )
}

export function TopicForm({ topic, onSaved, onCancel }: Props) {
  const client = useQueryClient()
  const creating = topic === undefined
  const { draft, edit } = useDraft({
    name: '',
    partitions: String(topic?.partitions ?? 1),
    contentType: topic?.contentType ?? 'application/octet-stream',
    retentionMs: text(topic?.retentionMs),
    retentionBytes: text(topic?.retentionBytes),
  })

  const mutation = useMutation({
    mutationFn: () =>
      creating
        ? createTopic({
            name: draft.name.trim(),
            partitions: numberOf(draft.partitions),
            contentType: draft.contentType.trim(),
            retentionMs: numberOf(draft.retentionMs),
            retentionBytes: numberOf(draft.retentionBytes),
          })
        : updateTopic(topic.name, {
            partitions: numberOf(draft.partitions),
            retentionMs: numberOf(draft.retentionMs) ?? null,
            retentionBytes: numberOf(draft.retentionBytes) ?? null,
          }),
    onSuccess: (saved) => {
      void client.invalidateQueries({ queryKey: ['topics'] })
      onSaved?.(saved)
    },
  })

  return (
    <Form
      submit={creating ? 'Create' : 'Save changes'}
      pending={mutation.isPending}
      disabled={creating && draft.name.trim() === ''}
      error={mutation.error}
      onSubmit={() => mutation.mutate()}
      onCancel={onCancel}
    >
      {creating && (
        <Label text="Name">
          <Input value={draft.name} required placeholder="orders" onInput={edit('name')} />
        </Label>
      )}
      <Label text={creating ? 'Partitions' : 'Partitions, can only grow'}>
        <Input type="number" min={topic?.partitions ?? 1} required value={draft.partitions} onInput={edit('partitions')} />
      </Label>
      <Label text={creating ? 'Content type' : 'Content type, fixed at creation'}>
        <Input value={draft.contentType} required disabled={!creating} onInput={edit('contentType')} />
      </Label>
      <Label text="Retention in milliseconds, empty for unlimited">
        <Input type="number" min={1} value={draft.retentionMs} onInput={edit('retentionMs')} />
      </Label>
      <Label text="Retention in bytes, empty for unlimited">
        <Input type="number" min={1} value={draft.retentionBytes} onInput={edit('retentionBytes')} />
      </Label>
      {mutation.isSuccess && !creating && <Notice>Saved</Notice>}
    </Form>
  )
}

function text(value: number | null | undefined): string {
  return value === null || value === undefined ? '' : String(value)
}
