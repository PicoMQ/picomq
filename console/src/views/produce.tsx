import { useMutation, useQueryClient } from '@tanstack/preact-query'
import { ErrorBoundary, lazy } from 'preact-iso'
import { useState } from 'preact/hooks'

import { produce, type FieldWrite } from '../api/records'
import type { TopicDetail } from '../api/topics'
import { Button } from '../components/button'
import { Checkbox, Input, Select } from '../components/control'
import { indent } from '../components/field'
import { Form, Label } from '../components/form'
import { Notice } from '../components/notice'
import { PartitionPicker } from '../components/partition'
import { Rows } from '../components/rows'
import * as format from '../format'

import './produce.css'

const Editor = lazy(() => import('../components/editor'))

interface HeaderRow {
  name: string
  value: string
}

interface Props {
  name: string
  topic: TopicDetail
  index: number
  onDone: () => void
  onShow: (partition: number, offset: number) => void
}

export function Produce({ name, topic, index, onDone, onShow }: Props) {
  const client = useQueryClient()
  const [partition, setPartition] = useState(index)
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [encoding, setEncoding] = useState<FieldWrite['encoding']>('utf8')
  const [tombstone, setTombstone] = useState(false)
  const [headers, setHeaders] = useState<HeaderRow[]>([])
  const json = topic.contentType.includes('json') && encoding === 'utf8'

  const mutation = useMutation({
    mutationFn: () =>
      produce(name, partition, {
        key: key === '' ? null : { data: key, encoding: 'utf8' },
        value: tombstone ? null : { data: value, encoding },
        headers: headers.map((header) => ({ name: header.name, value: { data: header.value, encoding: 'utf8' } })),
      }),
    onSuccess: () => client.invalidateQueries({ queryKey: ['topics', name] }),
  })

  const formatted = json ? indent(value) : null

  return (
    <Form
      submit="Produce"
      pending={mutation.isPending}
      disabled={headers.some((header) => header.name.trim() === '')}
      error={mutation.error}
      onSubmit={() => mutation.mutate()}
      onCancel={onDone}
    >
      <Label text="Partition">
        <PartitionPicker partitions={topic.partitions} value={partition} onChange={setPartition} />
      </Label>
      <Label text="Key, empty for none">
        <Input value={key} onInput={(event) => setKey(event.currentTarget.value)} />
      </Label>
      <div class="produce-value">
        <div class="produce-heading">
          <small>Value</small>
          <Select value={encoding} disabled={tombstone} onChange={(event) => setEncoding(event.currentTarget.value as FieldWrite['encoding'])}>
            <option value="utf8">Text</option>
            <option value="base64">Base64</option>
          </Select>
          {formatted !== null && formatted !== value && <Button onClick={() => setValue(formatted)}>Format</Button>}
        </div>
        {!tombstone && (
          <ErrorBoundary>
            <Editor text={value} language={json ? 'json' : 'text'} onChange={setValue} />
          </ErrorBoundary>
        )}
        <Checkbox label="Null value, a tombstone" checked={tombstone} onChange={() => setTombstone(!tombstone)} />
      </div>
      <Rows label="Headers" items={headers} blank={{ name: '', value: '' }} onChange={setHeaders}>
        {(header, update) => (
          <>
            <Input
              placeholder="name"
              value={header.name}
              onInput={(event) => {
                const changed = event.currentTarget.value
                update((each) => ({ ...each, name: changed }))
              }}
            />
            <Input
              placeholder="value"
              value={header.value}
              onInput={(event) => {
                const changed = event.currentTarget.value
                update((each) => ({ ...each, value: changed }))
              }}
            />
          </>
        )}
      </Rows>
      {mutation.data && (
        <div class="produce-result">
          <Notice>Produced at offset {format.count(mutation.data.offset)}</Notice>
          <Button onClick={() => onShow(partition, mutation.data.offset)}>Show</Button>
        </div>
      )}
    </Form>
  )
}
