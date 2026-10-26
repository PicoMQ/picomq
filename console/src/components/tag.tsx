import type { ComponentChildren } from 'preact'

import './tag.css'

interface Props {
  kind?: 'plain' | 'success' | 'danger'
  children: ComponentChildren
}

export function Tag({ kind = 'plain', children }: Props) {
  return (
    <small class="tag" data-kind={kind}>
      {children}
    </small>
  )
}
