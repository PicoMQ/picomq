import { useQuery, useQueryClient } from '@tanstack/preact-query'
import { Layers } from 'lucide-preact'
import { useLocation, useRoute } from 'preact-iso'
import { useState } from 'preact/hooks'

import { REFRESH_MS } from '../api/query'
import { deleteTopic, topic, type TopicDetail } from '../api/topics'
import { Button } from '../components/button'
import { Confirm } from '../components/confirm'
import { Details } from '../components/details'
import { Heading } from '../components/heading'
import { Pending } from '../components/notice'
import { Tabs } from '../components/tabs'
import { Tag } from '../components/tag'
import { View } from '../components/view'
import * as format from '../format'
import { topicPage } from '../location'
import { Configuration } from './configuration'
import { Messages } from './messages'
import Missing from './missing'
import { Partitions } from './partitions'

const TABS = [
  { path: undefined, label: 'Messages', component: Messages },
  { path: 'partitions', label: 'Partitions', component: Partitions },
  { path: 'config', label: 'Configuration', component: Configuration },
]

export default function TopicView() {
  const { params } = useRoute()
  const { route } = useLocation()
  const client = useQueryClient()
  const name = params.name ?? ''
  const tab = TABS.find((each) => each.path === params.tab)
  const detail = useQuery({ ...topic(name), refetchInterval: REFRESH_MS })
  const [deleting, setDeleting] = useState(false)
  const trail = [{ label: 'Topics', href: '/topics' }, { label: name }]

  const remove = async () => {
    await deleteTopic(name)
    await client.invalidateQueries({ queryKey: ['topics'] })
    route('/topics')
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
        title={shown.name}
        icon={Layers}
        tags={shown.closed && <Tag>closed</Tag>}
        actions={
          <Button kind="danger" onClick={() => setDeleting(true)}>
            Delete topic
          </Button>
        }
      />
      <Details
        items={[
          { label: 'Partitions', value: format.count(shown.partitions) },
          { label: 'Stored', value: format.bytes(shown.storedBytes) },
          { label: 'Retention time', value: format.limit(shown.retentionMs, format.duration) },
          { label: 'Retention size', value: format.limit(shown.retentionBytes, format.bytes) },
          { label: 'Kafka topic', value: shown.kafkaTopic ?? 'none' },
          { label: 'Owners', value: owners(shown) },
        ]}
      />
      <Tabs tabs={TABS} href={(path) => topicPage(name, path)} />
      <Content name={name} topic={shown} />
      <Confirm
        title="Delete topic"
        name={shown.name}
        action="Delete topic"
        open={deleting}
        onClose={() => setDeleting(false)}
        onConfirm={remove}
      >
        This deletes every partition of {shown.name} and all its records.
      </Confirm>
    </View>
  )
}

function owners(topic: TopicDetail): string {
  return topic.owners
    .map((owner) => `${owner.nodeId === null ? 'unassigned' : `node ${owner.nodeId}`}: ${owner.partitions}`)
    .join(', ')
}
