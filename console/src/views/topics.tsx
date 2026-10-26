import { Plus } from 'lucide-preact'
import { useLocation } from 'preact-iso'
import { useState } from 'preact/hooks'

import { usePages } from '../api/query'
import { topics, type Topic } from '../api/topics'
import { Button } from '../components/button'
import { Dialog } from '../components/dialog'
import { List } from '../components/list'
import { Search } from '../components/search'
import type { Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import * as format from '../format'
import { page } from '../location'
import { TopicForm } from './configuration'

const COLUMNS: Column<Topic>[] = [
  {
    name: 'name',
    header: 'Name',
    cell: (topic) => (
      <>
        <a href={page('topics', topic.name)}>{topic.name}</a> {topic.closed && <Tag>closed</Tag>}
      </>
    ),
  },
  { name: 'partitions', header: 'Partitions', width: '9rem', kind: 'number', cell: (topic) => format.count(topic.partitions) },
  { name: 'retention', header: 'Retention', width: '13rem', cell: retention },
  { name: 'contentType', header: 'Content type', width: '15rem', cell: (topic) => topic.contentType },
  { name: 'kafkaTopic', header: 'Kafka topic', width: '15rem', cell: (topic) => topic.kafkaTopic },
]

function retention(topic: Topic): string {
  const limits = [
    topic.retentionMs === null ? null : format.duration(topic.retentionMs),
    topic.retentionBytes === null ? null : format.bytes(topic.retentionBytes),
  ].filter((limit) => limit !== null)

  return limits.length > 0 ? limits.join(' or ') : 'unlimited'
}

export default function Topics() {
  const { query, route } = useLocation()
  const pages = usePages(topics(query.prefix))
  const [creating, setCreating] = useState(false)

  return (
    <View trail={[{ label: 'Topics' }]}>
      <Toolbar
        actions={
          <>
            <Button onClick={pages.refresh}>Refresh</Button>
            <Button kind="primary" onClick={() => setCreating(true)}>
              <Plus size={16} />
              Create topic
            </Button>
          </>
        }
      >
        <Search placeholder="Search topics by name prefix" />
      </Toolbar>
      <List pages={pages} columns={COLUMNS} rowKey={(topic) => topic.id} noun="topics" />
      <Dialog title="Create topic" open={creating} onClose={() => setCreating(false)}>
        <TopicForm onCancel={() => setCreating(false)} onSaved={(topic) => route(page('topics', topic.name))} />
      </Dialog>
    </View>
  )
}
