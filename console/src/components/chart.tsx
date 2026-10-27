import { useEffect, useRef } from 'preact/hooks'
import uPlot from 'uplot'

import * as format from '../format'

import 'uplot/dist/uPlot.min.css'
import './chart.css'

const HEIGHT = 280

interface Series {
  label: string
  color: string
  values: (number | null)[]
}

interface Props {
  times: number[]
  series: Series[]
  show: (value: number) => string
}

export default function Chart({ times, series, show }: Props) {
  const parent = useRef<HTMLDivElement>(null)
  const plot = useRef<uPlot | null>(null)

  useEffect(() => {
    const element = parent.current
    if (!element) {
      return
    }

    const created = new uPlot(options(element.clientWidth, series, show), [[], ...series.map(() => [])], element)
    plot.current = created
    const resize = new ResizeObserver(() => created.setSize({ width: element.clientWidth, height: HEIGHT }))
    resize.observe(element)

    return () => {
      resize.disconnect()
      created.destroy()
      plot.current = null
    }
  }, [])

  useEffect(() => {
    plot.current?.setData([times.map((ms) => ms / 1000), ...series.map((each) => each.values)])
  }, [times, series])

  return (
    <div class="chart">
      <div class="chart-legend">
        {series.map((each) => (
          <span key={each.label} class="chart-value">
            <span class="chart-dot" style={{ background: `var(${each.color})` }} />
            {each.label}: <span class="chart-number">{latest(each.values, show)}</span>
          </span>
        ))}
      </div>
      <div class="chart-plot" ref={parent} />
    </div>
  )
}

function latest(values: (number | null)[], show: (value: number) => string): string {
  const value = values.findLast((each) => each !== null)

  return value === undefined || value === null ? 'none' : show(value)
}

function options(width: number, series: Series[], show: (value: number) => string): uPlot.Options {
  const style = getComputedStyle(document.documentElement)
  const token = (name: string) => style.getPropertyValue(name).trim()
  const remPx = Number.parseFloat(style.fontSize)
  const fontPx = Number.parseFloat(token('--text-sm')) * remPx
  const font = `${fontPx}px ${token('--font-mono')}`
  const axis = {
    font,
    stroke: token('--color-text-3'),
    grid: { stroke: token('--color-border'), width: 1, dash: [4, 4] },
    ticks: { show: false },
  }

  return {
    width,
    height: HEIGHT,
    legend: { show: false },
    cursor: { drag: { x: false, y: false } },
    axes: [
      { ...axis, space: fontPx * 8, values: (_, ticks) => ticks.map((seconds) => format.clock(seconds * 1000)) },
      {
        ...axis,
        values: (_, ticks) => ticks.map(show),
        size: (_, values) => Math.max(0, ...(values ?? []).map((value) => value.length)) * fontPx * 0.62 + fontPx * 2,
      },
    ],
    series: [
      {},
      ...series.map(({ label, color }) => ({
        label,
        stroke: token(color),
        width: 2,
        points: { show: false },
      })),
    ],
  }
}
