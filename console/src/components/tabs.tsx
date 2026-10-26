import { useLocation } from 'preact-iso'

import { current } from '../location'

import './tabs.css'

export interface Tab {
  href: string
  label: string
}

export function Tabs({ tabs }: { tabs: Tab[] }) {
  const { path } = useLocation()
  const active = current(path, tabs.map((tab) => tab.href))

  return (
    <nav class="tabs">
      {tabs.map((tab) => (
        <a key={tab.href} class="tabs-item" href={tab.href} aria-current={tab.href === active ? 'page' : undefined}>
          {tab.label}
        </a>
      ))}
    </nav>
  )
}
