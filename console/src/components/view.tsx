import { ChevronRight } from 'lucide-preact'
import type { ComponentChildren } from 'preact'

import './view.css'

interface Crumb {
  label: string
  href?: string
}

interface Props {
  trail: Crumb[]
  children: ComponentChildren
}

export function View({ trail, children }: Props) {
  return (
    <>
      <div class="view-bar">
        <nav class="view-trail" aria-label="Breadcrumb">
          {trail.map((crumb, index) => (
            <span key={crumb.label} class="view-crumb">
              {index > 0 && <ChevronRight size={16} />}
              {crumb.href ? <a href={crumb.href}>{crumb.label}</a> : <span aria-current="page">{crumb.label}</span>}
            </span>
          ))}
        </nav>
      </div>
      <div class="view-body">{children}</div>
    </>
  )
}
