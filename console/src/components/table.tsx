import { elementScroll, observeElementOffset, observeElementRect, Virtualizer } from '@tanstack/virtual-core'
import type { ComponentChildren } from 'preact'
import { useEffect, useLayoutEffect, useRef, useState } from 'preact/hooks'

import './table.css'

const ROW_ESTIMATE = 44
const OVERSCAN = 10

export interface Column<T> {
  name: string
  header: string
  width?: string
  kind?: 'number'
  cell: (row: T) => ComponentChildren
}

export interface Expanded {
  key: string
  content: ComponentChildren
}

interface Props<T> {
  columns: Column<T>[]
  rows: T[]
  rowKey: (row: T) => string
  onEnd?: () => void
  onSelect?: (row: T) => void
  expanded?: Expanded
  follow?: boolean
  empty?: string
}

export function Table<T>({ columns, rows, rowKey, onEnd, onSelect, expanded, follow = false, empty }: Props<T>) {
  const scroller = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer(scroller, rows.length, (index) => rowKey(rows[index]!), follow)

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
        {rows.length === 0 && empty && (
          <tbody>
            <tr>
              <td class="table-empty" colSpan={columns.length}>
                {empty}
              </td>
            </tr>
          </tbody>
        )}
        {above > 0 && <Spacer height={above} columns={columns.length} />}
        {items.map((item) => {
          const row = rows[item.index]!
          const open = expanded?.key === item.key

          return (
            <tbody key={item.key} data-index={item.index} ref={virtualizer.measureElement}>
              <tr
                class="table-row"
                data-kind={onSelect ? 'selectable' : undefined}
                aria-expanded={onSelect ? open : undefined}
                onClick={onSelect && (() => onSelect(row))}
              >
                {columns.map((column) => (
                  <td key={column.name} class="table-cell" data-kind={column.kind}>
                    {column.cell(row)}
                  </td>
                ))}
              </tr>
              {open && (
                <tr class="table-expanded">
                  <td colSpan={columns.length}>{expanded.content}</td>
                </tr>
              )}
            </tbody>
          )
        })}
        {below > 0 && <Spacer height={below} columns={columns.length} />}
      </table>
    </div>
  )
}

function Spacer({ height, columns }: { height: number; columns: number }) {
  return (
    <tbody aria-hidden="true">
      <tr style={{ height: `${height}px` }}>
        <td colSpan={columns} />
      </tr>
    </tbody>
  )
}

function useVirtualizer(
  scroller: { current: HTMLDivElement | null },
  count: number,
  getItemKey: (index: number) => string,
  follow: boolean,
) {
  const [, setTick] = useState(0)

  const options = {
    count,
    getItemKey,
    getScrollElement: () => scroller.current,
    estimateSize: () => ROW_ESTIMATE,
    overscan: OVERSCAN,
    followOnAppend: follow,
    scrollToFn: elementScroll,
    observeElementRect,
    observeElementOffset,
    onChange: () => setTick((tick) => tick + 1),
  }

  const [virtualizer] = useState(() => new Virtualizer<HTMLDivElement, HTMLTableSectionElement>(options))
  virtualizer.setOptions(options)

  useLayoutEffect(() => virtualizer._didMount(), [])
  useLayoutEffect(() => virtualizer._willUpdate())

  return virtualizer
}
