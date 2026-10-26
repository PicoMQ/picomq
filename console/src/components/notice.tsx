import type { ComponentChildren } from 'preact'

import './notice.css'

interface Props {
  kind?: 'plain' | 'danger'
  children: ComponentChildren
}

export function Notice({ kind = 'plain', children }: Props) {
  return (
    <p class="notice" data-kind={kind} role={kind === 'danger' ? 'alert' : undefined}>
      {children}
    </p>
  )
}

export function Pending({ error }: { error: Error | null }) {
  return error ? <Notice kind="danger">{error.message}</Notice> : <Notice>Loading</Notice>
}
