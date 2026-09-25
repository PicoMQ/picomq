import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'preact/hooks'
import { MAX_MATCHES, watchStreams, type WatchRun, type WatchStatus } from './watch'
import { inferShape } from './shape'
import { fetchStreamOwnership, getToken, type StreamOwnership as OwnershipInfo } from './api'
import { StreamTransfer } from './admin-tools'
import { CreateStream, DeleteStream } from './stream-lifecycle-ui'
import {
  ApiError, appendMessage, errorMessage, fetchSchema, formatTimestamp, inspectStream, listStreams,
  normalizeEndpoint, validateStreamNames, payload, readStream, streamConfig, streamPath,
  type Connection, type RecordData, type StreamEntry, type StreamInfo,
} from './streams'

export function ConnectionSettings({ connection, onChange, publishing }: {
  connection: Connection; onChange: (value: Connection) => void; publishing: boolean
}) {
  const [endpoint, setEndpoint] = useState(connection.endpoint)
  const [token, setToken] = useState(connection.token)
  const [error, setError] = useState('')
  const [saved, setSaved] = useState(false)
  return (
    <details class="ss-connection">
      <summary>Stream connection <span class="mono">{connection.endpoint}</span></summary>
      <form class="ss-form" onSubmit={(event) => {
        event.preventDefault()
        if (publishing) return
        try {
          const value = { endpoint: normalizeEndpoint(endpoint), token: token.trim() }
          onChange(value)
          setError(''); setSaved(true)
        } catch (error) { setError(errorMessage(error)) }
      }}>
        <label>Stream API endpoint<input value={endpoint} onInput={(e) => { setEndpoint(e.currentTarget.value); setSaved(false) }} required /></label>
        <label>Stream API token<input type="password" autoComplete="off" value={token} onInput={(e) => { setToken(e.currentTarget.value); setSaved(false) }} placeholder="Optional when authentication is off" /></label>
        <p class="ss-hint">Connection and token stay in this tab’s session. Reading and publishing need the pico audience; schema details also need admin. Changing the connection stops any active watch.</p>
        <div class="ss-toolbar"><button class="ss-button primary" type="submit" disabled={publishing}>Apply connection</button>{publishing ? <span class="ss-hint" role="status">Wait for the pending write to finish before applying a connection.</span> : saved && <span role="status">Connection applied</span>}</div>
        {error && <Notice message={error} />}
      </form>
    </details>
  )
}

function Notice({ message }: { message: string }) {
  return <p class="ss-notice" role="alert">{message}</p>
}

function Message({ record }: { record: RecordData }) {
  return (
    <details class="ss-record">
      <summary><span class="mono">#{record.seq}</span><span>{record.body_b64 !== undefined ? 'Binary payload' : (record.body || '(empty)').slice(0, 120)}</span></summary>
      <dl class="ss-metadata">
        <dt>Timestamp</dt><dd>{formatTimestamp(record.timestamp)}</dd>
        <dt>Key</dt><dd class="mono">{record.key ?? (record.key_b64 === undefined ? '—' : `${record.key_b64} (base64)`)}</dd>
      </dl>
      {record.previewTruncated && <p class="ss-hint">Preview truncated: payload limited to 16,384 characters; key and header previews are also bounded. The original message is unchanged. This record is excluded from shape inference.</p>}
      {(record.headers || record.headers_b64) && <pre>{JSON.stringify({ headers: record.headers, headers_b64: record.headers_b64 }, null, 2)}</pre>}
      <pre>{payload(record)}</pre>
    </details>
  )
}

interface OwnershipConnection extends Connection {
  streamEndpoint: string
  dashboard: boolean
}

export function Discovery({ connection, isConnectionCurrent, active, adminRevision, onWatch, onPublish, onPending, writing }: {
  connection: Connection; isConnectionCurrent: (value: Connection) => boolean; active: boolean; adminRevision: number; onWatch: (name: string) => void; onPublish: (name: string) => void
  onPending: (pending: boolean) => void; writing: boolean
}) {
  const [draft, setDraft] = useState('/')
  const [prefix, setPrefix] = useState('/')
  const [revision, setRevision] = useState(0)
  const [streams, setStreams] = useState<StreamEntry[]>([])
  const [more, setMore] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [selected, setSelected] = useState('')
  const selectedStream = useRef('')
  const selectStream = (name: string) => { selectedStream.current = name; setSelected(name) }
  const adminPairing = useRef<OwnershipConnection | null>(null)
  const [ownershipConnection, setOwnershipConnection] = useState<OwnershipConnection | null>(null)
  const listAbort = useRef<AbortController>()
  const writes = useRef(new Set<string>())
  const markPending = useCallback((kind: string, value: boolean) => {
    if (value) writes.current.add(kind)
    else writes.current.delete(kind)
    onPending(writes.current.size > 0)
  }, [onPending])
  const creating = useCallback((value: boolean) => markPending('create', value), [markPending])
  const deleting = useCallback((value: boolean) => markPending('delete', value), [markPending])
  const transferring = useCallback((value: boolean) => markPending('transfer', value), [markPending])

  async function load(after = '') {
    listAbort.current?.abort()
    const controller = new AbortController()
    listAbort.current = controller
    setBusy(true); setError('')
    try {
      const page = await listStreams(connection, prefix, after, controller.signal)
      if (controller.signal.aborted) return
      setStreams((current) => after ? [...current, ...page.streams.filter((s) => !current.some((old) => old.name === s.name))] : page.streams)
      setMore(page.has_more)
    } catch (error) { if (!controller.signal.aborted) setError(errorMessage(error)) }
    finally { if (!controller.signal.aborted) setBusy(false) }
  }

  useEffect(() => {
    if (!active) return
    setStreams([]); setMore(false)
    void load()
    return () => listAbort.current?.abort()
  }, [connection, prefix, revision, active])
  useLayoutEffect(() => selectStream(''), [connection])
  useLayoutEffect(() => { adminPairing.current = null; setOwnershipConnection(null) }, [connection.endpoint])
  const paired = ownershipConnection?.streamEndpoint === connection.endpoint ? ownershipConnection : null
  const tokenRevision = paired?.dashboard ? adminRevision : 0
  const adminConnection = useMemo(() => paired ? {
    endpoint: paired.endpoint, token: paired.dashboard ? getToken() || '' : paired.token, dashboard: paired.dashboard,
    isCurrent: () => adminPairing.current === paired && isConnectionCurrent(connection),
  } : null, [paired, tokenRevision, connection, isConnectionCurrent])
  const connectAdmin = (value: OwnershipConnection) => {
    if (!writes.current.size && !writing && isConnectionCurrent(connection)) {
      adminPairing.current = value
      setOwnershipConnection(value)
    }
  }

  return <>
    <AdminConnectionSettings streamEndpoint={connection.endpoint} connection={paired} onConnection={connectAdmin} writing={writing} />
    <CreateStream connection={adminConnection} disabled={writing} onPending={creating} onChanged={(name) => {
      selectStream(name); setRevision((n) => n + 1)
    }} />
    <section class="ss-section">
      <h2>Discover streams</h2>
      <form class="ss-toolbar ss-search" onSubmit={(event) => { event.preventDefault(); setPrefix(draft); setRevision((n) => n + 1) }}>
        <label>Name prefix<input class="mono" value={draft} onInput={(e) => setDraft(e.currentTarget.value)} placeholder="/demo/" /></label>
        <button class="ss-button" type="submit" disabled={busy}>Search</button>
      </form>
      {error && <Notice message={error} />}
      {streams.length > 0 ? <div class="ss-table-scroll"><table>
        <thead><tr><th>Name</th><th>Content type</th><th>Status</th><th>Actions</th></tr></thead>
        <tbody>{streams.map((stream) => <tr key={stream.name}>
          <td class="mono"><button class="ss-link" disabled={writing} onClick={() => { if (!writes.current.size) selectStream(stream.name) }} aria-pressed={selected === stream.name}>{stream.name}</button></td>
          <td>{stream.content_type || '—'}</td><td>{stream.closed ? 'Closed' : 'Open'}</td>
          <td><div class="ss-actions"><button class="ss-button" onClick={() => onWatch(stream.name)}>Watch</button><button class="ss-button" disabled={stream.closed} onClick={() => onPublish(stream.name)}>Publish</button></div></td>
        </tr>)}</tbody>
      </table></div> : <div class="ss-empty">{busy ? 'Loading streams…' : error ? 'Streams could not be loaded.' : 'No streams match this prefix.'}</div>}
      <div class="ss-toolbar ss-list-footer"><span class="ss-hint" role="status">{streams.length} streams loaded{busy ? ' · loading…' : ''}</span>{more && <button class="ss-button" disabled={busy} onClick={() => void load(streams[streams.length - 1]?.name)}>Load more</button>}</div>
    </section>
    {selected && <StreamDetails key={`${connection.endpoint}:${selected}`} connection={connection} name={selected}
      ownershipConnection={paired} adminRevision={adminRevision}
      onPending={transferring} writing={writing}
    />}
    <DeleteStream connection={adminConnection} name={selected} isSelectionCurrent={() => selectedStream.current === selected} disabled={writing} onPending={deleting} onDeleted={() => {
      selectStream(''); setRevision((n) => n + 1)
    }} />
  </>
}

function StreamDetails({ connection, name, ownershipConnection, adminRevision, onPending, writing }: {
  connection: Connection; name: string; ownershipConnection: OwnershipConnection | null
  adminRevision: number
  onPending: (pending: boolean) => void; writing: boolean
}) {
  const heading = useRef<HTMLHeadingElement>(null)
  const [info, setInfo] = useState<StreamInfo | null>(null)
  const [records, setRecords] = useState<RecordData[]>([])
  const [schema, setSchema] = useState('')
  const [schemaState, setSchemaState] = useState('Loading schema details…')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [revision, setRevision] = useState(0)
  useEffect(() => heading.current?.focus(), [name])
  useEffect(() => {
    const controller = new AbortController()
    setInfo(null); setRecords([]); setSchema(''); setError(''); setBusy(true)
    setSchemaState('Loading schema details…')
    async function load() {
      try {
        const meta = await inspectStream(connection, name, controller.signal)
        if (controller.signal.aborted) return
        setInfo(meta)
        const from = BigInt(meta.next) - 20n > BigInt(meta.start) ? String(BigInt(meta.next) - 20n) : meta.start
        const page = await readStream(connection, name, from, controller.signal, 20)
        if (!controller.signal.aborted) setRecords(page.records)
      } catch (error) { if (!controller.signal.aborted) setError(errorMessage(error)) }
      finally { if (!controller.signal.aborted) setBusy(false) }
    }
    async function loadSchema() {
      try {
        const config = await streamConfig(connection, name, controller.signal)
        if (controller.signal.aborted) return
        setSchemaState(config.schema ? `${config.schema} · validation ${config.schemaValidate ? 'enabled' : 'disabled'}` : 'No declared schema bound to this stream.')
        if (config.schema) {
          const text = await fetchSchema(connection, config.schema, controller.signal)
          if (!controller.signal.aborted) setSchema(text)
        }
      } catch (error) { if (!controller.signal.aborted) setSchemaState(`Schema details unavailable: ${errorMessage(error)}`) }
    }
    void load(); void loadSchema()
    return () => controller.abort()
  }, [connection, name, revision])
  const shape = inferShape(records)
  return <section class="ss-section">
    <div class="ss-section-heading"><h2 ref={heading} tabIndex={-1}>Stream details</h2><button class="ss-button" disabled={busy} onClick={() => setRevision((n) => n + 1)}>Refresh details</button></div>
    <p class="mono ss-stream-name">{name}</p>
    {error && <Notice message={error} />}
    {info && <dl class="ss-metadata ss-card">
      <dt>Content type</dt><dd>{info.contentType}</dd><dt>Status</dt><dd>{info.closed ? 'Closed' : 'Open'}</dd>
      <dt>Earliest position</dt><dd class="mono">{info.start}</dd><dt>Next position</dt><dd class="mono">{info.next}</dd>
      <dt>Kafka topic</dt><dd class="mono">{info.kafkaTopic || '—'}</dd><dt>TTL (seconds)</dt><dd>{info.ttl || '—'}</dd><dt>Expires at</dt><dd>{info.expiresAt || '—'}</dd>
    </dl>}
    <StreamOwnership name={name} connection={ownershipConnection} revision={revision} adminRevision={adminRevision}
      onPending={onPending} writing={writing} onTransferred={() => setRevision((n) => n + 1)}
    />
    <h2>Declared schema</h2><p class="ss-hint">{schemaState}</p>{schema && <pre class="ss-schema">{schema}</pre>}
    <h2>Observed JSON shape</h2>
    <p class="ss-hint">{busy ? 'Reading sample…' : `${shape.parsed} complete JSON payloads in a sample of ${records.length} records. Requests up to 20 recent records / 256 KiB; oversized previews are truncated. This is an observation, not a schema guarantee.`}{shape.limited ? ' Shape depth or field limit reached.' : ''}</p>
    {shape.fields.length > 0 && <div class="ss-table-scroll"><table><thead><tr><th>Field</th><th>Observed types</th><th>Present in sample</th></tr></thead>
      <tbody>{shape.fields.map((field) => <tr key={field.path}><td class="mono">{field.path}</td><td>{field.types.join(', ')}</td><td>{field.samples} / {shape.parsed}</td></tr>)}</tbody>
    </table></div>}
    <h2 class="ss-spaced">Sample messages</h2>
    {records.map((record) => <Message key={record.seq} record={record} />)}
    {!busy && records.length === 0 && <div class="ss-empty">No messages available in this sample.</div>}
  </section>
}

function StreamOwnership({ name, connection, revision, adminRevision, onPending, writing, onTransferred }: {
  name: string; connection: OwnershipConnection | null
  revision: number; adminRevision: number
  onPending: (pending: boolean) => void; writing: boolean; onTransferred: () => void
}) {
  const tokenRevision = connection?.dashboard ? adminRevision : 0
  const adminConnection = useMemo(() => connection ? {
    endpoint: connection.endpoint, token: connection.dashboard ? getToken() || '' : connection.token,
  } : null, [connection, tokenRevision])
  const [result, setResult] = useState<{
    connection: OwnershipConnection; revision: number; tokenRevision: number
    info: OwnershipInfo | null; error: string
  } | null>(null)
  const [lastOwner, setLastOwner] = useState<{ connection: OwnershipConnection; info: OwnershipInfo } | null>(null)
  const current = result?.connection === connection && result?.revision === revision && result?.tokenRevision === tokenRevision ? result : null

  useEffect(() => {
    if (!connection) return
    const controller = new AbortController()
    const source = connection
    async function load() {
      try {
        const info = await fetchStreamOwnership({ endpoint: source.endpoint, token: source.dashboard ? getToken() || '' : source.token }, name, controller.signal)
        if (!controller.signal.aborted) {
          setResult({ connection: source, revision, tokenRevision, info, error: '' })
          setLastOwner({ connection: source, info })
        }
      } catch (error) {
        if (!controller.signal.aborted) setResult({ connection: source, revision, tokenRevision, info: null, error: errorMessage(error) })
      }
    }
    void load()
    return () => controller.abort()
  }, [connection, name, revision, tokenRevision])

  return <>
    <h2>Ownership</h2>
    {connection ? <>
      <p class="ss-hint">Admin API: <span class="mono">{connection.endpoint}</span></p>
      {!current && <p class="ss-hint" role="status">Loading ownership…</p>}
      {current?.error && <Notice message={`Ownership unavailable: ${current.error}${connection.dashboard ? ' Check admin access in Overview, then refresh details.' : ''}`} />}
      {current?.info && <dl class="ss-metadata ss-card">
        <dt>Routing owner</dt><dd class="mono">{current.info.ownerNodeId === -1 && !current.info.ownerAdvertisedAddress ? 'Unassigned' : current.info.ownerNodeId}</dd>
        <dt>Owner address</dt><dd class="mono">{current.info.ownerAdvertisedAddress || 'Not available'}</dd>
        <dt>Stream epoch</dt><dd class="mono">{current.info.epoch === null ? 'Not available' : current.info.epoch === -1 ? 'Not opened (−1)' : current.info.epoch}</dd>
      </dl>}
    </> : <p class="ss-hint">Connect an admin API from the same cluster to inspect this stream’s owner and epoch.</p>}
    {connection && adminConnection && lastOwner?.connection === connection && <StreamTransfer
      connection={adminConnection} name={name} owner={lastOwner.info} disabled={writing || !current?.info}
      onPending={onPending} onTransferred={onTransferred} />}
  </>
}

function AdminConnectionSettings({ streamEndpoint, connection, onConnection, writing }: {
  streamEndpoint: string; connection: OwnershipConnection | null
  onConnection: (value: OwnershipConnection) => void; writing: boolean
}) {
  const [endpoint, setEndpoint] = useState('')
  const [token, setToken] = useState('')
  const [formError, setFormError] = useState('')
  useEffect(() => { setEndpoint(''); setToken(''); setFormError('') }, [streamEndpoint])
  return (
    <details class="ss-connection">
      <summary>Admin connection</summary>
      {connection && <p class="ss-hint">Connected admin API: <span class="mono">{connection.endpoint}</span></p>}
      <p class="ss-hint">Choose the admin API for the stream connection above. The dashboard cannot verify that they belong to the same cluster. Changing the stream endpoint clears this pairing.</p>
      <div class="ss-toolbar"><button class="ss-button" disabled={writing} onClick={() => {
        if (writing) return
        onConnection({ streamEndpoint, endpoint: window.location.origin, token: '', dashboard: true })
        setFormError('')
      }}>Use dashboard admin</button><span class="ss-hint">Uses the admin token from Overview.</span></div>
      <form class="ss-form" onSubmit={(event) => {
        event.preventDefault()
        if (writing) return
        try {
          if (!endpoint.trim()) throw new Error('Enter the admin API endpoint for this stream’s cluster.')
          onConnection({ streamEndpoint, endpoint: normalizeEndpoint(endpoint), token: token.trim(), dashboard: false })
          setFormError('')
        } catch (error) { setFormError(errorMessage(error)) }
      }}>
        <label>Admin API endpoint<input value={endpoint} onInput={(event) => setEndpoint(event.currentTarget.value)} placeholder="http://node:9090" required /></label>
        <label>Admin API token<input type="password" autoComplete="off" value={token} onInput={(event) => setToken(event.currentTarget.value)} placeholder="Optional when authentication is off" /></label>
        <p class="ss-hint">Requires the admin audience and permissions for the operation: stream_inspect, create, delete or transfer_stream. The admin permission group alone does not grant create or delete. This connection stays in memory for this page.</p>
        <div class="ss-toolbar"><button class="ss-button" type="submit" disabled={writing}>Connect admin API</button></div>
        {formError && <Notice message={formError} />}
      </form>
    </details>
  )
}

interface WatchRow { stream: string; record: RecordData }

function keepRows(rows: WatchRow[]): WatchRow[] {
  let size = 0
  let index = rows.length
  while (index > 0 && rows.length - index < 250) {
    const record = rows[index - 1].record
    const length = JSON.stringify(record).length
    if (size + length > 2_000_000) break
    size += length; index--
  }
  return rows.slice(index)
}

export function Watch({ connection, selection }: { connection: Connection; selection: { name: string; revision: number } }) {
  const heading = useRef<HTMLHeadingElement>(null)
  const [mode, setMode] = useState('exact')
  const [query, setQuery] = useState('^/demo/')
  const [streamInputs, setStreamInputs] = useState([{ id: 0, name: selection.name || '/demo/orders' }])
  const nextInputId = useRef(1)
  const inputRefs = useRef(new Map<number, HTMLInputElement>())
  const focusInput = useRef<number>()
  const [prefix, setPrefix] = useState('/')
  const [start, setStart] = useState('now')
  const [run, setRun] = useState<(WatchRun & { connection: Connection }) | null>(null)
  const [rows, setRows] = useState<WatchRow[]>([])
  const [statuses, setStatuses] = useState<WatchStatus[]>([])
  const [error, setError] = useState('')
  const [ticks, setTicks] = useState(0)
  const [discoveryWarning, setDiscoveryWarning] = useState('')
  useEffect(() => { setRun(null); setRows([]); setStatuses([]); setError(''); setDiscoveryWarning('') }, [connection])
  useLayoutEffect(() => {
    if (focusInput.current === undefined) return
    const input = inputRefs.current.get(focusInput.current)
    if (!input) return
    input.focus()
    focusInput.current = undefined
  }, [streamInputs])
  useEffect(() => {
    if (selection.name) {
      setRun(null); setDiscoveryWarning(''); setMode('exact')
      setStreamInputs([{ id: nextInputId.current++, name: selection.name }])
      heading.current?.focus()
    }
  }, [selection])

  useEffect(() => {
    if (!run || run.connection !== connection) return
    return watchStreams(connection, run, {
      records: (stream, records) => setRows((current) => keepRows([...current, ...records.map((record) => ({ stream, record }))])),
      statuses: setStatuses,
      discovery: setDiscoveryWarning,
      progress: () => setTicks((n) => n + 1),
      error: (error) => { setError(errorMessage(error)); setRun(null) },
    })
  }, [connection, run])

  return <section class="ss-section">
    <h2 ref={heading} tabIndex={-1}>Watch streams</h2>
    <form class="ss-form" onSubmit={(event) => {
      event.preventDefault()
      try {
        const names = mode === 'exact' ? validateStreamNames(streamInputs.map((stream) => stream.name), MAX_MATCHES) : []
        if (mode === 'regex' && !query.trim()) throw new Error('Enter a name pattern.')
        setRows([]); setStatuses([]); setError(''); setDiscoveryWarning(''); setTicks(0)
        setRun({ mode, query, prefix, start, names, connection })
      } catch (error) { setError(errorMessage(error)) }
    }}>
      <div class="ss-fields">
        <label>Match<select disabled={!!run} value={mode} onChange={(e) => setMode(e.currentTarget.value)}><option value="exact">Exact names</option><option value="regex">Regex</option></select></label>
        {mode === 'regex' && <label>Name pattern<input class="mono" disabled={!!run} value={query} onInput={(e) => setQuery(e.currentTarget.value)} placeholder="^/demo/(orders|events)$" required /></label>}
        {mode === 'regex' && <label>Discovery prefix<input class="mono" disabled={!!run} value={prefix} onInput={(e) => setPrefix(e.currentTarget.value)} /></label>}
        <label>Start at<select disabled={!!run} value={start} onChange={(e) => setStart(e.currentTarget.value)}><option value="now">Now</option><option value="beginning">Earliest retained</option></select></label>
      </div>
      {mode === 'exact' && <fieldset class="ss-watch-streams" disabled={!!run}>
        <legend>Streams</legend>
        {streamInputs.map((stream, index) => <div class="ss-stream-input-row" key={stream.id}>
          <label>{`Stream ${index + 1}`}<input class="mono" value={stream.name} placeholder="/demo/orders" spellcheck={false} autoComplete="off"
            ref={(input) => { if (input) inputRefs.current.set(stream.id, input); else inputRefs.current.delete(stream.id) }}
            onInput={(event) => {
              const name = event.currentTarget.value
              setStreamInputs((current) => current.map((item) => item.id === stream.id ? { ...item, name } : item))
            }} required /></label>
          <button class="ss-button" type="button" aria-label={`Remove stream ${index + 1}`} disabled={streamInputs.length === 1} onClick={() => {
            if (run) return
            setStreamInputs((current) => {
              const position = current.findIndex((item) => item.id === stream.id)
              if (current.length === 1 || position < 0) return current
              const remaining = current.filter((item) => item.id !== stream.id)
              focusInput.current = remaining[Math.min(position, remaining.length - 1)].id
              return remaining
            })
          }}>Remove</button>
        </div>)}
        <div class="ss-toolbar">
          <button class="ss-button" type="button" disabled={streamInputs.length >= MAX_MATCHES} onClick={() => {
            if (run) return
            const id = nextInputId.current++
            setStreamInputs((current) => {
              if (current.length >= MAX_MATCHES) return current
              focusInput.current = id
              return [...current, { id, name: '' }]
            })
          }}>Add stream</button>
          <span class="ss-hint" role="status">{streamInputs.length} / {MAX_MATCHES} streams{run ? ' · Stop watching to edit' : ''}</span>
        </div>
      </fieldset>}
      <div class="ss-toolbar"><button class="ss-button primary" type="submit" disabled={!!run}>Start watching</button><button class="ss-button" type="button" disabled={!run} onClick={() => { setRun(null); setDiscoveryWarning('') }}>Stop</button><button class="ss-button" type="button" onClick={() => setRows([])} disabled={!rows.length}>Clear displayed messages</button><span class="ss-hint" role="status">{run ? ticks ? 'Watching · streams poll independently' : discoveryWarning ? 'Retrying discovery…' : 'Connecting…' : 'Stopped'} · {rows.length} displayed</span></div>
    </form>
    <p class="ss-hint">Keeps up to 250 records / 2 million characters. Each stream retains its own order; the combined view uses arrival order. {mode === 'regex' && 'Regex matches stream names. Discovery refreshes every 5 seconds; new matches start at the earliest retained position. Short-lived streams can be missed.'}</p>
    {error && <Notice message={error} />}
    {discoveryWarning && <Notice message={discoveryWarning} />}
    {statuses.length > 0 && <div class="ss-table-scroll"><table><thead><tr><th>Stream</th><th>Next position</th><th>Status</th></tr></thead><tbody>{statuses.map((status) => <tr key={status.name}><td class="mono">{status.name}</td><td class="mono">{status.position}</td><td>{!run && ['Watching', 'Connecting…'].includes(status.state) ? 'Stopped' : status.state}</td></tr>)}</tbody></table></div>}
    {run && ticks > 0 && statuses.length === 0 && <div class="ss-empty">No matching streams yet. Discovery is still running.</div>}
    <div class="ss-spaced">{rows.map((row) => <div class="ss-watch-record" key={`${row.stream}:${row.record.seq}`}><div class="mono ss-record-stream">{row.stream}</div><Message record={row.record} /></div>)}</div>
    {rows.length === 0 && <div class="ss-empty">{run ? 'Waiting for messages…' : 'Choose stream names or a pattern to start watching.'}</div>}
  </section>
}

export function Publish({ connection, selection, onPendingChange }: {
  connection: Connection; selection: { name: string; revision: number }; onPendingChange: (pending: boolean) => void
}) {
  const heading = useRef<HTMLHeadingElement>(null)
  const [name, setName] = useState(selection.name || '/demo/orders')
  const [body, setBody] = useState('')
  const [format, setFormat] = useState('json')
  const [key, setKey] = useState('')
  const [headers, setHeaders] = useState('{}')
  const [error, setError] = useState('')
  const [result, setResult] = useState('')
  const [busy, setBusy] = useState(false)
  const pending = useRef(false)
  const controller = useRef<AbortController>()
  useEffect(() => { if (selection.name) { setName(selection.name); heading.current?.focus() } setResult(''); setError('') }, [selection])
  useEffect(() => {
    setBusy(false); setResult(''); setError('')
    return () => controller.current?.abort()
  }, [connection])
  async function send(event: Event) {
    event.preventDefault()
    if (pending.current) return
    setError(''); setResult('')
    let phase = 'inspect'
    const current = new AbortController()
    controller.current = current
    try {
      streamPath(name)
      if (!body.length) throw new Error('Enter a message body.')
      if (new TextEncoder().encode(body).length > 262144) throw new Error('Message preview limit is 256 KiB.')
      if (format === 'json') JSON.parse(body)
      const parsed: unknown = JSON.parse(headers)
      if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object' || Object.values(parsed).some((value) => typeof value !== 'string')) throw new Error('Headers must be a JSON object with string values.')
      setBusy(true)
      pending.current = true
      onPendingChange(true)
      const meta = await inspectStream(connection, name, current.signal)
      if (meta.closed) throw new Error('This stream is closed. Choose an open stream.')
      phase = 'append'
      const ack = await appendMessage(connection, name, body, key, parsed as Record<string, string>, current.signal)
      if (!current.signal.aborted) setResult(`Sent one message to ${name} via ${connection.endpoint} · sequence ${ack.start} · next ${ack.next}${ack.timestamp ? ` · ${formatTimestamp(ack.timestamp)}` : ''}`)
    } catch (error) {
      if (!current.signal.aborted) setError(`${errorMessage(error)}${phase === 'append' && (!(error instanceof ApiError) || error.status === 0 || error.status >= 500) ? ` Delivery to ${name} via ${connection.endpoint} is unknown; inspect the stream before sending again. This request was not retried.` : ''}`)
    } finally {
      pending.current = false
      onPendingChange(false)
      if (!current.signal.aborted) setBusy(false)
    }
  }
  return <section class="ss-section">
    <h2 ref={heading} tabIndex={-1}>Publish a message</h2>
    <p class="ss-hint">Send one message to an existing stream. The payload is sent as entered; JSON mode checks syntax. The server applies any enabled schema validation.</p>
    <form class="ss-form" onSubmit={(event) => void send(event)}>
      <div class="ss-fields"><label>Stream name<input class="mono" required disabled={busy} value={name} onInput={(e) => { setName(e.currentTarget.value); setResult('') }} /></label><label>Payload format<select disabled={busy} value={format} onChange={(e) => setFormat(e.currentTarget.value)}><option value="json">JSON</option><option value="text">Text</option></select></label><label>Key (optional)<input class="mono" disabled={busy} value={key} onInput={(e) => setKey(e.currentTarget.value)} /></label></div>
      <label>Message body<textarea class="mono" rows={10} required disabled={busy} spellcheck={false} value={body} onInput={(e) => { setBody(e.currentTarget.value); setResult('') }} placeholder={'{"message": "Hello PicoMQ"}'} /></label>
      <details class="ss-advanced"><summary>Record headers (optional)</summary><label>Headers as JSON<textarea class="mono" rows={3} disabled={busy} spellcheck={false} value={headers} onInput={(e) => setHeaders(e.currentTarget.value)} /></label></details>
      <div class="ss-toolbar"><button class="ss-button primary" type="submit" disabled={busy}>{busy ? 'Sending…' : 'Send message'}</button><span class="ss-hint">Appends are never automatically retried.</span></div>
      {error && <Notice message={error} />}{result && <p class="ss-success" role="status">{result}</p>}
    </form>
  </section>
}
