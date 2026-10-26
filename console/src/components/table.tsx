import { elementScroll, observeElementOffset, observeElementRect, Virtualizer } from '@tanstack/virtual-core'
import type { ComponentChildren } from 'preact'
import { useEffect, useLayoutEffect, useRef, useState } from 'preact/hooks'

import './table.css'

const ROW_ESTIMATE = 32
const OVERSCAN = 10

export interface Column<T> {
  name: string
  header: string
  width?: string
  kind?: 'number'
  cell: (row: T) => ComponentChildren
}

interface Props<T> {
  columns: Column<T>[]
  rows: T[]
  rowKey: (row: T) => string
  onEnd?: () => void
}

export function Table<T>({ columns, rows, rowKey, onEnd }: Props<T>) {
  const scroller = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer(scroller, rows.length, (index) => rowKey(rows[index]!))

  const items = virtualizer.getVirtualItems()
  const first = items[0]
  const last = items.at(-1)
  const above = first?.start ?? 0
  const below = last ? virtualizer.getTotalSize() - last.end : 0

  useEffect(() => {
    if (last && last.index >= rows.length - OVERSCAN) {
      onEnd?.()
    }
  })

  return (
    <div class="table" ref={scroller}>
      <table class="table-grid">
        <colgroup>
          {columns.map((column) => (
            <col key={column.name} style={column.width ? { width: column.width } : undefined} />
          ))}
        </colgroup>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.name} class="table-header" data-kind={column.kind}>
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {above > 0 && <tr aria-hidden="true" style={{ height: `${above}px` }} />}
          {items.map((item) => {
            const row = rows[item.index]!

            return (
              <tr key={item.key} class="table-row" data-index={item.index} ref={virtualizer.measureElement}>
                {columns.map((column) => (
                  <td key={column.name} class="table-cell" data-kind={column.kind}>
                    {column.cell(row)}
                  </td>
                ))}
              </tr>
            )
          })}
          {below > 0 && <tr aria-hidden="true" style={{ height: `${below}px` }} />}
        </tbody>
      </table>
    </div>
  )
}

function useVirtualizer(
  scroller: { current: HTMLDivElement | null },
  count: number,
  getItemKey: (index: number) => string,
) {
  const [, setTick] = useState(0)

  const options = {
    count,
    getItemKey,
    getScrollElement: () => scroller.current,
    estimateSize: () => ROW_ESTIMATE,
    overscan: OVERSCAN,
    scrollToFn: elementScroll,
    observeElementRect,
    observeElementOffset,
    onChange: () => setTick((tick) => tick + 1),
  }

  const [virtualizer] = useState(() => new Virtualizer<HTMLDivElement, HTMLTableRowElement>(options))
  virtualizer.setOptions(options)

  useLayoutEffect(() => virtualizer._didMount(), [])
  useLayoutEffect(() => virtualizer._willUpdate())

  return virtualizer
}
