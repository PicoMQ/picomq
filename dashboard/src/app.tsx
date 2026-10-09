import { useCallback, useEffect, useRef, useState } from 'preact/hooks'
import { ConnectionSettings, Discovery, Publish, Watch } from './stream-tabs'
import { loadConnection, saveConnection, type Connection } from './streams'
import { NodeSlots } from './admin-tools'
import { TokenTools } from './token-tools'
import {
  AuthRequired,
  fetchCluster,
  fetchNodes,
  fetchReady,
  setToken,
  type ClusterInfo,
  type NodeInfo,
  type Readiness,
} from './api'

const POLL_INTERVAL_MS = 2000

function Pill({ state, label }: { state: 'ok' | 'warn' | 'err'; label: string }) {
  return (
    <span class={`ss-pill ${state}`}>
      <span class="dot" />
      {label}
    </span>
  )
}

function Stat({ label, value, small }: { label: string; value: unknown; small?: boolean }) {
  return (
    <div class="ss-card">
      <div class="label">{label}</div>
      <div class={small ? 'value small' : 'value'}>{String(value ?? '—')}</div>
    </div>
  )
}

function leaseLabel(cluster: ClusterInfo | null): string {
  if (!cluster || cluster.leaseHolder === null) {
    return '—'
  }
  return cluster.leaseHolder ? 'holder' : 'standby'
}

function TokenGate({ message, onSubmit }: { message: string; onSubmit: (token: string) => void }) {
  const [draft, setDraft] = useState('')

  return (
    <section class="ss-section ss-token-gate">
      <h2>Access token</h2>
      <p>{message}. Paste a token with the admin audience to continue.</p>
      <form
        onSubmit={(e) => {
          e.preventDefault()
          const token = draft.trim()
          if (token) {
            onSubmit(token)
          }
        }}
      >
        <input
          aria-label="Admin access token"
          type="password"
          class="mono"
          placeholder="Bearer token"
          value={draft}
          onInput={(e) => setDraft((e.target as HTMLInputElement).value)}
        />
        <button type="submit">Unlock</button>
      </form>
    </section>
  )
}

export function App() {
  const [tab, setTab] = useState('Overview')
  const [connection, setConnection] = useState(loadConnection)
  const appliedConnection = useRef(connection)
  const isConnectionCurrent = useCallback((value: Connection) => appliedConnection.current === value, [])
  const [publishing, setPublishing] = useState(false)
  const publishPending = useRef(false)
  const [streamWriting, setStreamWriting] = useState(false)
  const streamWritePending = useRef(false)
  const onStreamPending = useCallback((pending: boolean) => {
    streamWritePending.current = pending
    setStreamWriting(pending)
  }, [])
  const onPublishPending = useCallback((pending: boolean) => {
    publishPending.current = pending
    setPublishing(pending)
  }, [])
  function applyConnection(value: Connection) {
    if (publishPending.current || streamWritePending.current) throw new Error('Wait for the pending write to finish before applying a connection.')
    saveConnection(value)
    const current = appliedConnection.current
    if (current.endpoint !== value.endpoint || current.token !== value.token) {
      appliedConnection.current = value
      setConnection(value)
    }
  }
  const [watchSelection, setWatchSelection] = useState({ name: '', revision: 0 })
  const [publishSelection, setPublishSelection] = useState({ name: '', revision: 0 })
  const [cluster, setCluster] = useState<ClusterInfo | null>(null)
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [ready, setReady] = useState<Readiness | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [authNeeded, setAuthNeeded] = useState<string | null>(null)
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null)
  // Bumped when a token is submitted, so polling restarts immediately.
  const [attempt, setAttempt] = useState(0)

  useEffect(() => {
    let alive = true

    async function poll() {
      try {
        const [c, n, r] = await Promise.all([fetchCluster(), fetchNodes(), fetchReady()])

        if (!alive) {
          return
        }

        setCluster(c)
        setNodes(n.nodes)
        setReady(r)
        setError(null)
        setAuthNeeded(null)
        setUpdatedAt(new Date())
      } catch (e) {
        if (!alive) {
          return
        }
        if (e instanceof AuthRequired) {
          setAuthNeeded(e.message)
          setError(null)
        } else {
          setError(e instanceof Error ? e.message : String(e))
        }
      }
    }

    poll()
    const timer = setInterval(poll, POLL_INTERVAL_MS)

    return () => {
      alive = false
      clearInterval(timer)
    }
  }, [attempt])

  const readyState = ready?.ready ? 'ok' : ready ? 'warn' : 'err'
  const readyLabel = ready?.ready ? 'ready' : ready ? 'not ready' : 'unknown'
  const transfers = cluster?.pendingTransfers ?? []

  return (
    <div class="ss-app">
      <header class="ss-topbar">
        <span class="name">PicoMQ</span>
        <span class="tag">admin</span>
        <span class="spacer" />
        {authNeeded ? <Pill state="warn" label="admin locked" /> : error ? <Pill state="err" label="unreachable" /> : <Pill state={readyState} label={readyLabel} />}
      </header>

      <nav class="ss-tabs" aria-label="Dashboard sections">
        {['Overview', 'Discovery', 'Watch', 'Publish', 'Tokens'].map((name) => <button
          key={name} class={tab === name ? 'active' : ''}
          aria-current={tab === name ? 'page' : undefined}
          onClick={() => setTab(name)}
        >{name}</button>)}
      </nav>
      {['Discovery', 'Watch', 'Publish'].includes(tab) && <ConnectionSettings connection={connection} onChange={applyConnection} publishing={publishing || streamWriting} />}
      <div hidden={tab !== 'Overview'}>
      {authNeeded && <TokenGate message={authNeeded} onSubmit={(token) => {
        setToken(token)
        setAttempt((n) => n + 1)
      }} />}
      <div hidden={!!authNeeded}>
      <div class="ss-grid">
        <Stat label="Cluster" value={cluster?.clusterId} small />
        <Stat label="Node" value={cluster?.nodeId} />
        <Stat label="Applied index" value={cluster?.appliedIndex} />
        <Stat label="Streams" value={cluster?.streamCount} />
        <Stat label="Objects" value={cluster?.objectCount} />
        <Stat label="Maintenance lease" value={leaseLabel(cluster)} small />
      </div>

      <section class="ss-section">
        <h2>This node</h2>
        <div class="ss-table-scroll">
        <table>
          <tbody>
            <tr>
              <th>Advertised address</th>
              <td class="mono">{cluster?.advertisedAddress ?? '—'}</td>
            </tr>
            <tr>
              <th>Node epoch</th>
              <td class="mono">{cluster?.nodeEpoch ?? '—'}</td>
            </tr>
            <tr>
              <th>Registered</th>
              <td class="mono">{cluster ? String(cluster.registered) : '—'}</td>
            </tr>
            <tr>
              <th>GC backlog</th>
              <td class="mono">{cluster?.gc?.backlog ?? '—'}</td>
            </tr>
          </tbody>
        </table>
        </div>
      </section>

      <section class="ss-section">
        <h2>Nodes</h2>
        {nodes.length === 0 ? (
          <div class="ss-empty">No registered nodes</div>
        ) : (
          <div class="ss-table-scroll">
          <table>
            <thead>
              <tr>
                <th>Node</th>
                <th>Advertised address</th>
                <th>Epoch</th>
                <th>Slots</th>
                <th>Opening</th>
                <th>Placed</th>
                <th></th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((n) => (
                <tr key={n.nodeId}>
                  <td class="mono">{n.nodeId}</td>
                  <td class="mono">{n.advertisedAddress ?? '—'}</td>
                  <td class="mono">{n.nodeEpoch}</td>
                  <td class="mono">{n.slots}</td>
                  <td class="mono">{n.openingCount}</td>
                  <td class="mono">{n.placedCount}</td>
                  <td>{n.local ? <Pill state="ok" label="this node" /> : null}</td>
                  <td><NodeSlots node={n} adminRevision={attempt} onChanged={() => setAttempt((n) => n + 1)} /></td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
        )}
      </section>

      <section class="ss-section">
        <h2>Pending transfers</h2>
        {transfers.length === 0 ? (
          <div class="ss-empty">No transfers in flight</div>
        ) : (
          <div class="ss-table-scroll">
          <table>
            <thead>
              <tr>
                <th>Stream</th>
                <th>From node</th>
                <th>To node</th>
              </tr>
            </thead>
            <tbody>
              {transfers.map((t) => (
                <tr key={t.streamId}>
                  <td class="mono">{t.streamId}</td>
                  <td class="mono">{t.fromNode}</td>
                  <td class="mono">{t.toNode}</td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
        )}
      </section>

      </div>
      </div>
      <div hidden={tab !== 'Discovery'}>
        <Discovery connection={connection} isConnectionCurrent={isConnectionCurrent} active={tab === 'Discovery'} adminRevision={attempt}
          onPending={onStreamPending} writing={publishing || streamWriting}
          onWatch={(name) => { setWatchSelection((old) => ({ name, revision: old.revision + 1 })); setTab('Watch') }}
          onPublish={(name) => { setPublishSelection((old) => ({ name, revision: old.revision + 1 })); setTab('Publish') }}
        />
      </div>
      <div hidden={tab !== 'Watch'}><Watch connection={connection} selection={watchSelection} /></div>
      <div hidden={tab !== 'Publish'}><Publish connection={connection} selection={publishSelection} onPendingChange={onPublishPending} /></div>
      <div hidden={tab !== 'Tokens'}><TokenTools adminRevision={attempt} /></div>
      <footer class="ss-footer">
        {authNeeded
          ? 'Overview requires admin access. Stream tabs use their own connection.'
          : error
          ? `Last error: ${error}`
          : updatedAt
            ? `Updated ${updatedAt.toLocaleTimeString()} · polling every ${POLL_INTERVAL_MS / 1000}s`
            : 'Loading…'}
      </footer>
    </div>
  )
}
