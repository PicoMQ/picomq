import { useEffect, useRef } from 'preact/hooks'
import uPlot from 'uplot'

import 'uplot/dist/uPlot.min.css'
import './chart.css'

const HEIGHT = 160

export interface Series {
  label: string
  values: (number | null)[]
}

interface Props {
  times: number[]
  series: Series[]
}

export default function Chart({ times, series }: Props) {
  const parent = useRef<HTMLDivElement>(null)
  const plot = useRef<uPlot | null>(null)

  useEffect(() => {
    const element = parent.current
    if (!element) {
      return
    }

    const created = new uPlot(options(element.clientWidth, series), [[], ...series.map(() => [])], element)
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

  return <div class="chart" ref={parent} />
}

function options(width: number, series: Series[]): uPlot.Options {
  const style = getComputedStyle(document.documentElement)
  const token = (name: string) => style.getPropertyValue(name).trim()
  const remPx = Number.parseFloat(style.fontSize)
  const font = `${Number.parseFloat(token('--text-xs')) * remPx}px ${token('--font-mono')}`
  const strokes = [token('--color-text-1'), token('--color-text-3'), token('--color-text-2')]
  const axis = {
    font,
    stroke: token('--color-text-3'),
    grid: { stroke: token('--color-border'), width: 1 },
    ticks: { stroke: token('--color-border'), width: 1 },
  }

  return {
    width,
    height: HEIGHT,
    legend: { show: false },
    cursor: { drag: { x: false, y: false } },
    axes: [axis, axis],
    series: [
      {},
      ...series.map(({ label }, index) => ({
        label,
        stroke: strokes[index % strokes.length] ?? '',
        width: 1.5,
        points: { show: false },
      })),
    ],
  }
}
