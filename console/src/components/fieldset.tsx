import type { ComponentChildren } from 'preact'

import './fieldset.css'

interface Props {
  label: string
  kind?: 'stack' | 'wrap'
  children: ComponentChildren
}

export function Fieldset({ label, kind = 'stack', children }: Props) {
  return (
    <fieldset class="fieldset" data-kind={kind}>
      <legend>
        <small>{label}</small>
      </legend>
      {children}
    </fieldset>
  )
}
