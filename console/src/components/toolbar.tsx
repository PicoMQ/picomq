import type { ComponentChildren } from 'preact'

import { Actions } from './actions'

import './toolbar.css'

export function Toolbar({ children, actions }: { children?: ComponentChildren; actions?: ComponentChildren }) {
  return (
    <div class="toolbar">
      <div class="toolbar-filters">{children}</div>
      {actions && <Actions>{actions}</Actions>}
    </div>
  )
}
