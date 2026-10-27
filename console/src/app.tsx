import { MutationCache, QueryCache, QueryClient, QueryClientProvider, useQuery } from '@tanstack/preact-query'
import { ArrowLeftRight, BookOpen, KeyRound, Layers, LayoutDashboard, LogOut, Server, Users } from 'lucide-preact'
import { LocationProvider, Route, Router, lazy, useLocation } from 'preact-iso'
import { useState } from 'preact/hooks'

import { ApiError, AuthRequired, token } from './api/client'
import { cluster } from './api/cluster'
import { REFRESH_MS } from './api/query'
import { Button } from './components/button'
import { Login } from './components/login'
import { Logo } from './components/logo'
import { Tag } from './components/tag'
import { current } from './location'

import './app.css'

const DOCS = 'https://picomq.com/docs/'

const SECTIONS = [
  { href: '/', label: 'Overview', icon: LayoutDashboard },
  { href: '/topics', label: 'Topics', icon: Layers },
  { href: '/groups', label: 'Groups', icon: Users },
  { href: '/nodes', label: 'Nodes', icon: Server },
  { href: '/transfers', label: 'Transfers', icon: ArrowLeftRight },
  { href: '/tokens', label: 'Tokens', icon: KeyRound },
]

const Overview = lazy(() => import('./views/overview'))
const Topics = lazy(() => import('./views/topics'))
const TopicView = lazy(() => import('./views/topic'))
const Groups = lazy(() => import('./views/groups'))
const GroupView = lazy(() => import('./views/group'))
const Nodes = lazy(() => import('./views/nodes'))
const NodeView = lazy(() => import('./views/node'))
const Transfers = lazy(() => import('./views/transfers'))
const Tokens = lazy(() => import('./views/tokens'))
const Missing = lazy(() => import('./views/missing'))

export function App() {
  const [locked, setLocked] = useState(false)
  const [refused, setRefused] = useState(false)
  const [client] = useState(() => {
    const onError = (error: Error) => {
      if (error instanceof AuthRequired) {
        if (token.get() !== null) {
          token.clear()
          setRefused(true)
        }
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
    setRefused(false)
    setLocked(false)
  }

  const signOut = () => {
    token.clear()
    client.clear()
    setRefused(false)
    setLocked(true)
  }

  if (locked) {
    return <Login refused={refused} onSignIn={unlock} />
  }
  return (
    <QueryClientProvider client={client}>
      <LocationProvider>
        <div class="app">
          <Header onSignOut={signOut} />
          <Navigation />
          <main class="app-main">
            <Router>
              <Route path="/" component={Overview} />
              <Route path="/topics" component={Topics} />
              <Route path="/topics/:name" component={TopicView} />
              <Route path="/topics/:name/:tab" component={TopicView} />
              <Route path="/groups" component={Groups} />
              <Route path="/groups/:id" component={GroupView} />
              <Route path="/groups/:id/:tab" component={GroupView} />
              <Route path="/nodes" component={Nodes} />
              <Route path="/nodes/:id" component={NodeView} />
              <Route path="/transfers" component={Transfers} />
              <Route path="/tokens" component={Tokens} />
              <Route default component={Missing} />
            </Router>
          </main>
        </div>
      </LocationProvider>
    </QueryClientProvider>
  )
}

function Header({ onSignOut }: { onSignOut: () => void }) {
  const { data } = useQuery({ ...cluster(), refetchInterval: REFRESH_MS })

  return (
    <header class="app-header">
      <a class="app-brand" href="/">
        <Logo />
        {data?.clusterId ?? 'PicoMQ'}
      </a>
      {data && (
        <>
          <span class="app-separator">/</span>
          <span class="app-node">node {data.nodeId}</span>
          {data.leaseHolder && <Tag>lease holder</Tag>}
          {data.fenced && <Tag kind="danger">fenced</Tag>}
        </>
      )}
      {token.get() && (
        <span class="app-account">
          <Button onClick={onSignOut}>
            <LogOut size={16} />
            Sign out
          </Button>
        </span>
      )}
    </header>
  )
}

function Navigation() {
  const { path } = useLocation()
  const active = current(path, SECTIONS.map((section) => section.href))

  return (
    <nav class="app-nav" aria-label="Sections">
      {SECTIONS.map(({ href, label, icon: Icon }) => (
        <a key={href} class="app-section" href={href} aria-current={href === active ? 'page' : undefined}>
          <Icon size={18} />
          {label}
        </a>
      ))}
      <div class="app-footer">
        <a class="app-section" href={DOCS} target="_blank" rel="noreferrer">
          <BookOpen size={18} />
          Documentation
        </a>
      </div>
    </nav>
  )
}
