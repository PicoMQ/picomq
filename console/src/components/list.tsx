import type { Pages } from '../api/query'
import * as format from '../format'
import { Notice } from './notice'
import { Table, type Column, type Expanded } from './table'

interface Props<T> {
  pages: Pages<T>
  columns: Column<T>[]
  rowKey: (row: T) => string
  noun: string
  onSelect?: (row: T) => void
  expanded?: Expanded
}

export function List<T>({ pages, columns, rowKey, noun, onSelect, expanded }: Props<T>) {
  return (
    <>
      <Table
        columns={columns}
        rows={pages.items}
        rowKey={rowKey}
        onEnd={pages.next}
        onSelect={onSelect}
        expanded={expanded}
      />
      <Status pages={pages} noun={noun} />
    </>
  )
}

function Status<T>({ pages, noun }: { pages: Pages<T>; noun: string }) {
  const shown = `${format.count(pages.items.length)} ${noun}`

  if (pages.error) {
    return <Notice kind="danger">{pages.error.message}</Notice>
  }
  if (pages.items.length === 0) {
    return <Notice>{pages.loading ? 'Loading' : `No ${noun}`}</Notice>
  }
  if (pages.more) {
    return <Notice>{pages.loading ? `${shown}, loading more` : `${shown} so far, scroll for more`}</Notice>
  }
  return <Notice>{shown}</Notice>
}
