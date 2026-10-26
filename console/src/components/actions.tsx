import type { ComponentChildren } from 'preact'

import './actions.css'

export function Actions({ children }: { children: ComponentChildren }) {
  return <div class="actions">{children}</div>
}
