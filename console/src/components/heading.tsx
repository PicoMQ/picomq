import type { LucideIcon } from 'lucide-preact'
import type { ComponentChildren } from 'preact'

import { Actions } from './actions'

import './heading.css'

interface Props {
  title: ComponentChildren
  icon?: LucideIcon
  tags?: ComponentChildren
  actions?: ComponentChildren
}

export function Heading({ title, icon: Icon, tags, actions }: Props) {
  return (
    <header class="heading">
      <div class="heading-title">
        {Icon && <Icon size={28} />}
        <h1>{title}</h1>
        {tags}
      </div>
      {actions && <Actions>{actions}</Actions>}
    </header>
  )
}
