import { describe, expect, it } from 'vitest'

import * as metrics from '../src/api/metrics'

const TEXT = `# TYPE pico_requests_total counter
pico_requests_total{protocol="pico",operation="append",status="2xx"} 10
pico_requests_total{protocol="kafka",operation="Produce",status="ok"} 5
pico_records_in_total 100
pico_request_duration_seconds_bucket{protocol="pico",operation="append",le="+Inf"} 10
`

describe('parse', () => {
  it('reads names, labels and values and skips comments', () => {
    const scrape = metrics.parse(TEXT, 1000)

    expect(scrape.samples).toHaveLength(4)
    expect(scrape.samples[0]).toEqual({
      name: 'pico_requests_total',
      labels: { protocol: 'pico', operation: 'append', status: '2xx' },
      value: 10,
    })
    expect(scrape.samples[3]?.labels.le).toBe('+Inf')
  })

  it('unescapes label values', () => {
    const scrape = metrics.parse('m{name="a\\"b\\\\c\\nd"} 1', 0)

    expect(scrape.samples[0]?.labels.name).toBe('a"b\\c\nd')
  })
})

describe('total', () => {
  it('sums the samples that match every given label', () => {
    const scrape = metrics.parse(TEXT, 0)

    expect(metrics.total(scrape, 'pico_requests_total')).toBe(15)
    expect(metrics.total(scrape, 'pico_requests_total', { protocol: 'kafka' })).toBe(5)
    expect(metrics.total(scrape, 'absent')).toBe(0)
  })
})

describe('rate', () => {
  it('divides the counter delta by the elapsed seconds', () => {
    const previous = metrics.parse('pico_records_in_total 100', 1000)
    const current = metrics.parse('pico_records_in_total 160', 4000)

    expect(metrics.rate(previous, current, 'pico_records_in_total')).toBe(20)
  })

  it('has no rate across a counter reset or without elapsed time', () => {
    const before = metrics.parse('pico_records_in_total 100', 1000)

    expect(metrics.rate(before, metrics.parse('pico_records_in_total 5', 2000), 'pico_records_in_total')).toBeNull()
    expect(metrics.rate(before, metrics.parse('pico_records_in_total 200', 1000), 'pico_records_in_total')).toBeNull()
  })
})
