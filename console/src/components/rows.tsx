import { Plus, X } from 'lucide-preact'
import type { ComponentChildren } from 'preact'

import { Actions } from './actions'
import { Button } from './button'
import { Fieldset } from './fieldset'

import './rows.css'

type Update<T> = (change: (current: T) => T) => void

interface Props<T> {
  label: string
  items: T[]
  blank: T
  onChange: Update<T[]>
  children: (item: T, update: Update<T>) => ComponentChildren
}

export function Rows<T>({ label, items, blank, onChange, children }: Props<T>) {
  const update = (index: number): Update<T> => (change) =>
    onChange((current) => current.map((each, at) => (at === index ? change(each) : each)))

  return (
    <Fieldset label={label}>
      {items.map((item, index) => (
        <div key={index} class="rows-row">
          {children(item, update(index))}
          <Button aria-label={`Remove from ${label}`} onClick={() => onChange((current) => current.filter((_, at) => at !== index))}>
            <X size={14} />
          </Button>
        </div>
      ))}
      <Actions>
        <Button onClick={() => onChange((current) => [...current, blank])}>
          <Plus size={14} />
          Add
        </Button>
      </Actions>
    </Fieldset>
  )
}
