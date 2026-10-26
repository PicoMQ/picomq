import { MutationCache, QueryCache, QueryClient, QueryClientProvider, useQuery } from '@tanstack/preact-query'
import { LocationProvider, useLocation } from 'preact-iso'
import { useState } from 'preact/hooks'

import { ApiError, AuthRequired, token } from './api/client'
import { cluster } from './api/cluster'
import { Button } from './components/button'
import { Login } from './components/login'
import { Logo } from './components/logo'
import { Tag } from './components/tag'
import { current } from './location'

import './app.css'

const SECTIONS = [
  { href: '/', label: 'Overview' },
  { href: '/topics', label: 'Topics' },
  { href: '/groups', label: 'Groups' },
  { href: '/nodes', label: 'Nodes' },
  { href: '/transfers', label: 'Transfers' },
  { href: '/tokens', label: 'Tokens' },
]

export function App() {
  const [locked, setLocked] = useState(false)
  const [client] = useState(() => {
    const onError = (error: Error) => {
      if (error instanceof AuthRequired) {
        setLocked(true)
      }
    }

    return new QueryClient({
      queryCache: new QueryCache({ onError }),
      mutationCache: new MutationCache({ onError }),
      defaultOptions: {
        queries: {
          refetchOnWindowFocus: false,
          retry: (failures, error) => !(error instanceof ApiError) && failures < 2,
        },
      },
    })
  })

  const unlock = () => {
    client.clear()
    setLocked(false)
  }

  const signOut = () => {
    token.clear()
    client.clear()
    setLocked(true)
  }

  if (locked) {
    return <Login onSignIn={unlock} />
  }
  return (
    <QueryClientProvider client={client}>
      <LocationProvider>
        <div class="app">
          <a class="app-brand" href="/">
            <Logo />
            PicoMQ
          </a>
          <Navigation />
          <Header onSignOut={signOut} />
          <main class="app-main">
            <Missing />
          </main>
        </div>
      </LocationProvider>
    </QueryClientProvider>
  )
}

function Navigation() {
  const { path } = useLocation()
  const active = current(path, SECTIONS.map((section) => section.href))

  return (
    <nav class="app-nav">
      {SECTIONS.map((section) => (
        <a
          key={section.href}
          class="app-section"
          href={section.href}
          aria-current={section.href === active ? 'page' : undefined}
        >
          {section.label}
        </a>
      ))}
    </nav>
  )
}

function Header({ onSignOut }: { onSignOut: () => void }) {
  const { data } = useQuery(cluster())

  return (
    <header class="app-header">
      {data && (
        <>
          <code>{data.clusterId}</code>
          <span class="app-node">node {data.nodeId}</span>
          {data.fenced && <Tag kind="danger">fenced</Tag>}
          {data.leaseHolder && <Tag>lease holder</Tag>}
        </>
      )}
      {token.get() && (
        <span class="app-account">
          <Button onClick={onSignOut}>Sign out</Button>
        </span>
      )}
    </header>
  )
}

function Missing() {
  return <h1>No such page</h1>
}
