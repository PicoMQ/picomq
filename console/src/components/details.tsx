import type { LucideIcon } from 'lucide-preact'
import type { ComponentChildren } from 'preact'

import './details.css'

interface Detail {
  label: string
  value: ComponentChildren
  icon?: LucideIcon
  wide?: boolean
}

export function Details({ items }: { items: Detail[] }) {
  return (
    <dl class="details">
      {items.map(({ label, value, icon: Icon, wide }) => (
        <div key={label} class="details-item" data-kind={wide ? 'wide' : undefined}>
          <dt class="details-label">
            {Icon && <Icon size={16} />}
            {label}
          </dt>
          <dd class="details-value">{value}</dd>
        </div>
      ))}
    </dl>
  )
}
