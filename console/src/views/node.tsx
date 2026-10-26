import { useMutation, useQuery, useQueryClient } from '@tanstack/preact-query'
import { Server } from 'lucide-preact'
import { ErrorBoundary, lazy, useLocation, useRoute } from 'preact-iso'
import { useState } from 'preact/hooks'

import * as metrics from '../api/metrics'
import { REFRESH_MS } from '../api/query'
import { node, updateNode, type ClusterNode } from '../api/nodes'
import { Button } from '../components/button'
import { Input, numberOf } from '../components/control'
import { Details } from '../components/details'
import { Dialog } from '../components/dialog'
import { Form, Label } from '../components/form'
import { Heading } from '../components/heading'
import { Pending } from '../components/notice'
import { View } from '../components/view'
import * as format from '../format'
import { withQuery } from '../location'
import { status } from './nodes'

import './node.css'

const Chart = lazy(() => import('../components/chart'))

const SCRAPE_MS = 2000
const SCRAPES = 300

interface Metric {
  key: string
  label: string
  kind: 'rate' | 'gauge'
  show: (value: number) => string
  series: { label: string; color: string; name: string; labels?: Record<string, string> }[]
}

const METRICS: Metric[] = [
  {
    key: 'records',
    label: 'Records',
    kind: 'rate',
    show: format.rate,
    series: [
      { label: 'in', color: '--color-series-1', name: 'pico_records_in_total' },
      { label: 'out', color: '--color-series-2', name: 'pico_records_out_total' },
    ],
  },
  {
    key: 'bytes',
    label: 'Bytes',
    kind: 'rate',
    show: (value) => `${format.bytes(value)}/s`,
    series: [
      { label: 'in', color: '--color-series-1', name: 'pico_bytes_in_total' },
      { label: 'out', color: '--color-series-2', name: 'pico_bytes_out_total' },
    ],
  },
  {
    key: 'requests',
    label: 'Requests',
    kind: 'rate',
    show: format.rate,
    series: [
      { label: '2xx', color: '--color-success', name: 'pico_requests_total', labels: { status: '2xx' } },
      { label: '4xx', color: '--color-series-2', name: 'pico_requests_total', labels: { status: '4xx' } },
      { label: '5xx', color: '--color-danger', name: 'pico_requests_total', labels: { status: '5xx' } },
    ],
  },
  {
    key: 'connections',
    label: 'Connections',
    kind: 'gauge',
    show: format.count,
    series: [
      { label: 'stream', color: '--color-series-1', name: 'pico_connections', labels: { listener: 'stream' } },
      { label: 'kafka', color: '--color-series-2', name: 'pico_connections', labels: { listener: 'kafka' } },
      { label: 'admin', color: '--color-series-3', name: 'pico_connections', labels: { listener: 'admin' } },
    ],
  },
]

export default function NodeView() {
  const { params } = useRoute()
  const id = Number(params.id)
  const detail = useQuery({ ...node(id), refetchInterval: REFRESH_MS })
  const [editing, setEditing] = useState(false)
  const trail = [{ label: 'Nodes', href: '/nodes' }, { label: `node ${id}` }]

  if (!detail.data) {
    return (
      <View trail={trail}>
        <Pending error={detail.error} />
      </View>
    )
  }

  const shown = detail.data

  return (
    <View trail={trail}>
      <Heading
        title={`node ${shown.nodeId}`}
        icon={Server}
        tags={status(shown)}
        actions={<Button onClick={() => setEditing(true)}>Edit slots</Button>}
      />
      <Details
        items={[
          { label: 'Logs placed', value: format.count(shown.placedCount) },
          { label: 'Opening', value: format.count(shown.openingCount) },
          { label: 'Slots', value: format.count(shown.slots) },
          { label: 'Epoch', value: shown.nodeEpoch },
          { label: 'Address', value: shown.advertisedAddress ?? 'none', wide: true },
        ]}
      />
      <Live id={shown.nodeId} />
      <Dialog title="Edit slots" open={editing} onClose={() => setEditing(false)}>
        <Slots clusterNode={shown} onDone={() => setEditing(false)} />
      </Dialog>
    </View>
  )
}

function Live({ id }: { id: number }) {
  const { path, query } = useLocation()
  const { scrapes, error } = metrics.useScrapes(id, SCRAPE_MS, SCRAPES)
  const selected = METRICS.find((metric) => metric.key === query.metric) ?? METRICS[0]!

  const plotted = (metric: Metric) => ({
    times: (metric.kind === 'rate' ? scrapes.slice(1) : scrapes).map((scrape) => scrape.atMs),
    series: metric.series.map((each) => ({
      label: each.label,
      color: each.color,
      values:
        metric.kind === 'rate'
          ? metrics.series(scrapes, each.name, each.labels)
          : metrics.gauge(scrapes, each.name, each.labels),
    })),
  })
  const chart = plotted(selected)

  return (
    <section class="node-live">
      <nav class="node-metrics">
        {METRICS.map((metric) => (
          <a
            key={metric.key}
            class="node-metric"
            href={withQuery(path, query, { metric: metric.key })}
            aria-current={metric === selected ? 'page' : undefined}
          >
            <span>{metric.label}</span>
            <small>{range(plotted(metric).series.flatMap((each) => each.values), metric.show)}</small>
          </a>
        ))}
      </nav>
      <div class="node-chart">
        {error && <Pending error={error} />}
        <ErrorBoundary>
          <Chart key={selected.key} times={chart.times} series={chart.series} show={selected.show} />
        </ErrorBoundary>
        <small class="node-note">
          every {SCRAPE_MS / 1000}s since this page opened, last {format.duration(SCRAPE_MS * SCRAPES)}
        </small>
      </div>
    </section>
  )
}

function range(values: (number | null)[], show: (value: number) => string): string {
  const known = values.filter((value) => value !== null)

  return known.length === 0 ? 'waiting' : `${show(Math.min(...known))} - ${show(Math.max(...known))}`
}

function Slots({ clusterNode, onDone }: { clusterNode: ClusterNode; onDone: () => void }) {
  const client = useQueryClient()
  const [slots, setSlots] = useState(String(clusterNode.slots))
  const mutation = useMutation({
    mutationFn: () => updateNode(clusterNode.nodeId, numberOf(slots) ?? clusterNode.slots),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['nodes'] })
      onDone()
    },
  })

  return (
    <Form submit="Save" pending={mutation.isPending} error={mutation.error} onSubmit={() => mutation.mutate()} onCancel={onDone}>
      <Label text="Slots, its weight when new logs are placed">
        <Input type="number" min={0} required value={slots} onInput={(event) => setSlots(event.currentTarget.value)} />
      </Label>
    </Form>
  )
}
