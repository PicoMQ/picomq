import { useEffect, useRef, useState } from 'preact/hooks'
import { fetchStreamOwnership, getToken, type NodeInfo, type StreamOwnership } from './api'
import { fetchAdminNodes, transferStream, updateNodeSlots } from './admin-actions'
import { errorMessage, type Connection } from './streams'

export function NodeSlots({ node, adminRevision, onChanged }: { node: NodeInfo; adminRevision: number; onChanged: () => void }) {
  const [open, setOpen] = useState(false)
  const [slots, setSlots] = useState(String(node.slots))
  const [confirmed, setConfirmed] = useState<{ slots: number; connection: Connection; revision: number } | null>(null)
  const [busy, setBusy] = useState(false)
  const pending = useRef(false)
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')
  const revision = useRef(adminRevision)
  revision.current = adminRevision
  useEffect(() => { if (!pending.current) setConfirmed(null) }, [adminRevision])

  async function save() {
    if (pending.current || confirmed === null) return
    if (confirmed.revision !== revision.current || confirmed.connection.token !== (getToken() || '')) {
      setConfirmed(null); setError('The admin token changed. Review the slot update again before confirming.'); return
    }
    pending.current = true; setBusy(true); setError(''); setMessage('')
    const requested = confirmed.slots
    try {
      await updateNodeSlots(confirmed.connection, node.nodeId, requested)
      setMessage(`Node ${node.nodeId}: slots updated to ${requested}.`)
      setConfirmed(null); setOpen(false); onChanged()
    } catch (error) { setError(errorMessage(error)); setConfirmed(null) }
    finally { pending.current = false; setBusy(false) }
  }

  return <>
    <button class="ss-button" disabled={busy} aria-expanded={open} onClick={() => {
      setOpen(!open); setSlots(String(node.slots)); setConfirmed(null); setError('')
    }}>Edit slots</button>
    {open && <form class="ss-form ss-node-slots" onSubmit={(event) => {
      event.preventDefault()
      if (pending.current) return
      const value = Number(slots)
      if (!/^\d+$/.test(slots) || !Number.isSafeInteger(value) || value > 4294967295) {
        setError('Slots must be a whole number from 0 to 4294967295.'); return
      }
      setError(''); setConfirmed({ slots: value, connection: { endpoint: window.location.origin, token: getToken() || '' }, revision: adminRevision })
    }}>
      <label>Slots for node {node.nodeId}<input type="number" min="0" max="4294967295" step="1" required value={slots} disabled={busy}
        onInput={(event) => { setSlots(event.currentTarget.value); setConfirmed(null) }} /></label>
      {confirmed === null ? <button class="ss-button" type="submit" disabled={busy}>Review slots</button> : <div class="ss-card">
        <p>Change node <span class="mono">{node.nodeId}</span> from {node.slots} to {confirmed.slots} slots? This changes its placement weight.</p>
        <p class="ss-hint">Admin API: {confirmed.connection.endpoint}</p>
        <div class="ss-toolbar"><button class="ss-button primary" type="button" disabled={busy} onClick={() => void save()}>{busy ? 'Updating…' : 'Confirm slots'}</button>
          <button class="ss-button" type="button" disabled={busy} onClick={() => setConfirmed(null)}>Cancel</button></div>
      </div>}
    </form>}
    {error && <p class="ss-notice" role="alert">Node {node.nodeId}: {error}</p>}
    {message && <p class="ss-success" role="status">{message}</p>}
  </>
}

export function StreamTransfer({ connection, name, owner, disabled, onPending, onTransferred }: {
  connection: Connection; name: string; owner: StreamOwnership; disabled: boolean
  onPending: (pending: boolean) => void; onTransferred: () => void
}) {
  const [open, setOpen] = useState(false)
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [destination, setDestination] = useState('')
  const [loading, setLoading] = useState(false)
  const [confirmed, setConfirmed] = useState<{ connection: Connection; name: string; node: number } | null>(null)
  const [busy, setBusy] = useState(false)
  const pending = useRef(false)
  const [error, setError] = useState('')
  const [receipt, setReceipt] = useState<{ connection: Connection; name: string; node: number; streamId: number } | null>(null)
  const [status, setStatus] = useState('')
  const sourceNode = owner.nodeId ?? owner.ownerNodeId
  const unopened = owner.state !== undefined && owner.state !== 'opened'

  useEffect(() => {
    setConfirmed(null); setNodes([]); setDestination(''); setError('')
    if (!open) return
    const controller = new AbortController()
    setLoading(true)
    void fetchAdminNodes(connection, controller.signal).then((result) => {
      if (!controller.signal.aborted) setNodes(result.nodes)
    }).catch((error) => { if (!controller.signal.aborted) setError(errorMessage(error)) })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [open, connection.endpoint, connection.token, name])

  useEffect(() => {
    if (!receipt) return
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout>
    async function check() {
      try {
        const info = await fetchStreamOwnership(receipt!.connection, receipt!.name, controller.signal)
        if (controller.signal.aborted) return
        if (info.streamId !== undefined && info.streamId !== receipt!.streamId) {
          setStatus('This stream name now refers to a different stream. Transfer monitoring stopped.'); return
        }
        if (info.pendingTransfer === null && info.nodeId === receipt!.node && info.streamId === receipt!.streamId) {
          setStatus(`Transfer complete: ownership handed to node ${info.nodeId}. Routing owner currently reports node ${info.ownerNodeId}.`)
          onTransferred()
          return
        }
        setStatus(info.pendingTransfer
          ? `Transfer pending: node ${info.pendingTransfer.fromNode} → ${info.pendingTransfer.toNode}.`
          : `Transfer requested. Waiting for ownership handoff to node ${receipt!.node}; routing owner currently reports node ${info.ownerNodeId}.`)
      } catch (error) {
        if (controller.signal.aborted) return
        setStatus(`Transfer status unavailable: ${errorMessage(error)} Read-only status checks will continue.`)
      }
      timer = setTimeout(() => void check(), 2000)
    }
    void check()
    return () => { controller.abort(); clearTimeout(timer) }
  }, [receipt])

  async function transfer() {
    if (!confirmed || pending.current || disabled || unopened || owner.pendingTransfer || confirmed.node === sourceNode) return
    if (confirmed.name !== name || confirmed.connection.endpoint !== connection.endpoint || confirmed.connection.token !== connection.token) {
      setConfirmed(null); setError('The admin connection changed. Review the transfer again before confirming.'); return
    }
    const target = confirmed
    pending.current = true; setBusy(true); onPending(true); setError(''); setReceipt(null); setStatus('')
    try {
      const accepted = await transferStream(target.connection, target.name, target.node)
      setStatus('Transfer accepted. Checking ownership…'); setReceipt({ ...target, streamId: accepted.streamId }); setOpen(false)
      onTransferred()
    } catch (error) { setError(errorMessage(error)) }
    finally { pending.current = false; setBusy(false); setConfirmed(null); onPending(false) }
  }

  return <div class="ss-section">
    <div class="ss-toolbar"><button class="ss-button" disabled={busy || disabled || unopened || !!owner.pendingTransfer}
      aria-expanded={open} onClick={() => setOpen(!open)}>Transfer stream</button>
      {owner.pendingTransfer && <span class="ss-hint">Pending: node {owner.pendingTransfer.fromNode} → {owner.pendingTransfer.toNode}</span>}</div>
    {unopened && <p class="ss-hint">The stream must be opened by a client before it can transfer. Refresh details after opening it.</p>}
    {open && <form class="ss-form" onSubmit={(event) => {
      event.preventDefault()
      if (pending.current || disabled || unopened || owner.pendingTransfer || !destination || !nodes.some((node) => node.slots > 0 && node.nodeId !== sourceNode && String(node.nodeId) === destination)) return
      setConfirmed({ connection: { ...connection }, name, node: Number(destination) }); setError('')
    }}>
      <label>Destination node<select required disabled={busy || disabled || loading} value={destination}
        onChange={(event) => { setDestination(event.currentTarget.value); setConfirmed(null) }}>
        <option value="">{loading ? 'Loading nodes…' : 'Choose a node'}</option>
        {nodes.filter((node) => node.slots > 0 && node.nodeId !== sourceNode).map((node) => <option key={node.nodeId} value={node.nodeId}>Node {node.nodeId} · {node.advertisedAddress || 'address unavailable'} · {node.slots} slots</option>)}
      </select></label>
      {!loading && nodes.length > 0 && !nodes.some((node) => node.slots > 0 && node.nodeId !== sourceNode) && <p class="ss-hint">A second registered node with positive slots is needed to transfer this stream.</p>}
      {confirmed ? <div class="ss-card">
        <p>Transfer <span class="mono">{confirmed.name}</span> from node {sourceNode} to node {confirmed.node}?</p>
        <p class="ss-hint">Admin API: {confirmed.connection.endpoint}. Active clients may need to reconnect to the new owner.</p>
        <div class="ss-toolbar"><button class="ss-button primary" type="button" disabled={busy || disabled || unopened || !!owner.pendingTransfer || confirmed.node === sourceNode} onClick={() => void transfer()}>{busy ? 'Requesting…' : 'Confirm transfer'}</button>
          <button class="ss-button" type="button" disabled={busy} onClick={() => setConfirmed(null)}>Cancel</button></div>
      </div> : <div class="ss-toolbar"><button class="ss-button" type="submit" disabled={busy || disabled || !destination}>Review transfer</button></div>}
    </form>}
    {error && <p class="ss-notice" role="alert">Transfer of {name} at {connection.endpoint}: {error}</p>}
    {receipt && <p class="ss-success" role="status">{receipt.name} → node {receipt.node} · {receipt.connection.endpoint}. {status}</p>}
  </div>
}
