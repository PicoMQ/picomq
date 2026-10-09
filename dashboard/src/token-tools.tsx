import { useEffect, useRef, useState } from 'preact/hooks'
import { getToken } from './api'
import {
  AdminWriteError, issueToken, listTokens, revokeToken, validateTokenId,
  TOKEN_AUDIENCES, TOKEN_OPERATIONS,
  type IssuedToken, type ResourceMatcher, type TokenAudience, type TokenOperation, type TokenRecord, type TokenScope,
} from './admin-actions'
import { buildTokenScope, type MatcherDraft } from './token-form'
import type { Connection } from './streams'

const audienceLabels: Record<TokenAudience, string> = {
  pico: 'Pico protocol', durable_streams: 'Durable Streams protocol', admin: 'Admin API',
}
const operationLabels: Record<TokenOperation, string> = {
  read: 'Read messages', head: 'Read stream metadata', list: 'List streams', create: 'Create streams',
  append: 'Append messages', trim: 'Trim streams', close: 'Close streams', delete: 'Delete streams',
  issue_token: 'Issue tokens', revoke_token: 'Revoke tokens', list_tokens: 'List tokens',
  cluster_read: 'Read cluster', node_read: 'Read nodes', stream_inspect: 'Inspect stream ownership',
  transfer_stream: 'Transfer streams', update_node_slots: 'Update node slots',
}
type Choice<T extends string> = { id: number; value: T }
type Confirmation = {
  connection: Connection
  revision: number
} & ({ kind: 'issue'; id: string; scope: TokenScope } | { kind: 'revoke'; id: string })

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
function dateLabel(value: number | null | undefined): string {
  if (value === null || value === undefined) return 'No expiry'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString()
}
function matcherLabel(matcher: ResourceMatcher): string {
  if ('exact' in matcher) return `Exact: ${matcher.exact}`
  return matcher.prefix === '' ? 'All' : `Prefix: ${matcher.prefix}`
}

function ScopeSummary({ scope }: { scope: TokenScope }) {
  const permissions = [...(scope.ops ?? []).map((op) => operationLabels[op])]
  for (const group of ['stream', 'tokens', 'admin'] as const) {
    if (scope.groups?.[group]?.read) permissions.push(`${group}: all read operations`)
    if (scope.groups?.[group]?.write) permissions.push(`${group}: all write operations`)
  }
  return <dl class="ss-metadata">
    <dt>Audiences</dt><dd>{scope.audiences?.map((audience) => audienceLabels[audience]).join(', ') || 'None'}</dd>
    <dt>Permissions</dt><dd>{permissions.join(', ') || 'None'}
      {(scope.groups?.admin?.read || scope.groups?.admin?.write || scope.ops?.some((op) => ['cluster_read', 'node_read', 'stream_inspect', 'transfer_stream', 'update_node_slots'].includes(op)))
        && <p class="ss-hint">Admin permissions apply across the cluster. Stream rules do not restrict ownership inspection or transfer.</p>}
    </dd>
    <dt>Streams</dt><dd>{scope.streams?.map(matcherLabel).join('; ') || 'None'}</dd>
    <dt>Token IDs</dt><dd>{scope.tokens?.map(matcherLabel).join('; ') || 'None'}</dd>
    <dt>Automatic stream prefix</dt><dd>{scope.autoPrefixStreams ? 'Enabled' : 'Disabled'}</dd>
    <dt>Expires</dt><dd>{dateLabel(scope.expiresAtMs)}</dd>
  </dl>
}

function MatcherRows({ label, rows, disabled, onChange, nextId }: {
  label: string; rows: MatcherDraft[]; disabled: boolean; onChange: (rows: MatcherDraft[]) => void; nextId: () => number
}) {
  return <fieldset class="ss-watch-streams" disabled={disabled}>
    <legend>{label}</legend>
    {rows.length === 0 && <span class="ss-hint">No {label.toLowerCase()} allowed.</span>}
    {rows.map((row, index) => <div class="ss-fields" key={row.id}>
      <label>{label} rule {index + 1}
        <select value={row.kind} onChange={(event) => onChange(rows.map((item) => item.id === row.id
          ? { ...item, kind: event.currentTarget.value as MatcherDraft['kind'] } : item))}>
          <option value="exact">Exact</option><option value="prefix">Prefix</option><option value="all">All</option>
        </select>
      </label>
      {row.kind !== 'all' && <label>{label} value {index + 1}
        <input class="mono" value={row.value} placeholder={label === 'Streams' ? '/team/orders' : 'team/reader'}
          onInput={(event) => onChange(rows.map((item) => item.id === row.id ? { ...item, value: event.currentTarget.value } : item))} />
      </label>}
      <button class="ss-button" type="button" aria-label={`Remove ${label.toLowerCase()} rule ${index + 1}`}
        onClick={() => onChange(rows.filter((item) => item.id !== row.id))}>Remove</button>
    </div>)}
    <div><button class="ss-button" type="button" disabled={disabled || rows.length >= 32}
      onClick={() => onChange([...rows, { id: nextId(), kind: 'exact', value: '' }])}>Add {label.toLowerCase()} rule</button></div>
  </fieldset>
}

function ChoiceRows<T extends string>({ label, rows, options, labels, disabled, onChange, nextId }: {
  label: string; rows: Choice<T>[]; options: readonly T[]; labels: Record<T, string>; disabled: boolean
  onChange: (rows: Choice<T>[]) => void; nextId: () => number
}) {
  return <fieldset class="ss-watch-streams" disabled={disabled}>
    <legend>{label}</legend>
    {rows.map((row, index) => <div class="ss-stream-input-row" key={row.id}>
      <label>{label} {index + 1}
        <select value={row.value} onChange={(event) => onChange(rows.map((item) => item.id === row.id
          ? { ...item, value: event.currentTarget.value as T } : item))}>
          {options.map((value) => <option key={value} value={value} disabled={rows.some((item) => item.id !== row.id && item.value === value)}>{labels[value]}</option>)}
        </select>
      </label>
      <button class="ss-button" type="button" aria-label={`Remove ${label.toLowerCase()} ${index + 1}`}
        onClick={() => onChange(rows.filter((item) => item.id !== row.id))}>Remove</button>
    </div>)}
    <div><button class="ss-button" type="button" disabled={disabled || rows.length >= options.length} onClick={() => {
      const value = options.find((option) => !rows.some((row) => row.value === option))
      if (value) onChange([...rows, { id: nextId(), value }])
    }}>Add {label.toLowerCase()}</button></div>
  </fieldset>
}

/** Kept mounted by App so changing tabs cannot discard a pending issue result. */
export function TokenTools({ adminRevision }: { adminRevision: number }) {
  const [records, setRecords] = useState<{ revision: number; tokens: TokenRecord[] } | null>(null)
  const [readError, setReadError] = useState('')
  const [reading, setReading] = useState(false)
  const [refresh, setRefresh] = useState(0)
  const [id, setId] = useState('')
  const [streams, setStreams] = useState<MatcherDraft[]>([{ id: 1, kind: 'prefix', value: '' }])
  const [tokens, setTokens] = useState<MatcherDraft[]>([])
  const [audiences, setAudiences] = useState<Choice<TokenAudience>[]>([{ id: 2, value: 'pico' }])
  const [ops, setOps] = useState<Choice<TokenOperation>[]>([
    { id: 3, value: 'read' }, { id: 4, value: 'head' }, { id: 5, value: 'list' },
  ])
  const [autoPrefixStreams, setAutoPrefixStreams] = useState(false)
  const [expiry, setExpiry] = useState('')
  const rowId = useRef(6)
  const nextId = () => rowId.current++
  const [confirmation, setConfirmation] = useState<Confirmation | null>(null)
  const [pending, setPending] = useState(false)
  const pendingRef = useRef(false)
  const currentRevision = useRef(adminRevision)
  currentRevision.current = adminRevision
  const [writeError, setWriteError] = useState('')
  const [success, setSuccess] = useState('')
  const [issued, setIssued] = useState<IssuedToken | null>(null)
  const [copyStatus, setCopyStatus] = useState('')
  const confirmHeading = useRef<HTMLHeadingElement>(null)
  const secretHeading = useRef<HTMLHeadingElement>(null)
  const locked = pending || confirmation !== null || issued !== null

  useEffect(() => {
    const controller = new AbortController()
    let alive = true
    setReading(true)
    setReadError('')
    const connection = { endpoint: window.location.origin, token: getToken() || '' }
    listTokens(connection, controller.signal).then((result) => {
      if (alive) setRecords({ revision: adminRevision, tokens: result.tokens })
    }).catch((error) => {
      if (alive) {
        setRecords(null)
        setReadError(`${errorText(error)} Token management uses the dashboard admin connection; update its token in Overview if needed.`)
      }
    }).finally(() => { if (alive) setReading(false) })
    return () => { alive = false; controller.abort() }
  }, [adminRevision, refresh])

  useEffect(() => { setConfirmation(null) }, [adminRevision])
  useEffect(() => {
    if (confirmation && confirmHeading.current?.getClientRects().length) confirmHeading.current.focus()
  }, [confirmation])
  useEffect(() => {
    if (issued && secretHeading.current?.getClientRects().length) secretHeading.current.focus()
  }, [issued])

  function reviewIssue() {
    if (pendingRef.current || issued) return
    setWriteError('')
    setSuccess('')
    try {
      validateTokenId(id)
      if (id === '.' || id === '..') throw new Error('Use an ID other than . or ..; browsers cannot address those IDs for revocation.')
      const scope = buildTokenScope({ streams, tokens, ops: ops.map((row) => row.value), audiences: audiences.map((row) => row.value), autoPrefixStreams, expiry })
      if (id === 'anonymous' && scope.audiences?.includes('admin')) {
        throw new Error('The anonymous grant cannot have the admin audience.')
      }
      setConfirmation({ kind: 'issue', id, scope, revision: adminRevision, connection: { endpoint: window.location.origin, token: getToken() || '' } })
    } catch (error) { setWriteError(errorText(error)) }
  }

  function reviewRevoke(tokenId: string) {
    if (pendingRef.current || confirmation) return
    setWriteError('')
    setSuccess('')
    setConfirmation({ kind: 'revoke', id: tokenId, revision: adminRevision, connection: { endpoint: window.location.origin, token: getToken() || '' } })
  }

  async function confirmWrite() {
    const action = confirmation
    if (!action || pendingRef.current) return
    if (action.revision !== currentRevision.current || action.connection.token !== (getToken() || '')) {
      setConfirmation(null)
      setWriteError('The admin token changed. Review the action again before confirming.')
      return
    }
    if (action.kind === 'issue' && action.scope.expiresAtMs != null && action.scope.expiresAtMs <= Date.now()) {
      setConfirmation(null)
      setWriteError('The selected expiry has passed. Choose a future expiry and review the token again.')
      return
    }
    pendingRef.current = true
    setPending(true)
    setWriteError('')
    try {
      if (action.kind === 'issue') {
        const result = await issueToken(action.connection, action.id, action.scope)
        setIssued(result)
        setCopyStatus('')
        setSuccess(`Issued token ${action.id}. Save its secret before dismissing it.`)
      } else {
        await revokeToken(action.connection, action.id)
        if (issued?.id === action.id) setIssued(null)
        setSuccess(`Revoked token ${action.id}.`)
      }
      setRefresh((value) => value + 1)
    } catch (error) {
      let message = errorText(error)
      if (error instanceof AdminWriteError && error.uncertain) {
        message += action.kind === 'issue'
          ? ' Refresh the token list to check this ID. If it was issued, its secret cannot be recovered; revoke it before issuing a replacement.'
          : ' Refresh the token list to check whether this ID remains before attempting another revoke.'
      }
      setWriteError(`${action.kind === 'issue' ? 'Issue' : 'Revoke'} ${action.id}: ${message}`)
    } finally {
      pendingRef.current = false
      setPending(false)
      setConfirmation(null)
    }
  }

  async function copySecret() {
    if (!issued) return
    try {
      await navigator.clipboard.writeText(issued.token)
      setCopyStatus('Copied.')
    } catch { setCopyStatus('Copy is unavailable here. Select the secret and copy it manually.') }
  }

  const visibleRecords = records?.revision === adminRevision ? records.tokens : null
  return <>
    <section class="ss-section">
      <div class="ss-section-heading">
        <h2>Tokens</h2>
        <button class="ss-button" type="button" disabled={pending || reading || confirmation !== null} onClick={() => setRefresh((value) => value + 1)}>{reading ? 'Refreshing…' : 'Refresh tokens'}</button>
      </div>
      <p class="ss-hint ss-spaced">Admin API: {window.location.origin}. Only token IDs visible to your admin scope are listed. Listing never includes secrets.</p>
      {readError && <p class="ss-notice" role="alert">{readError}</p>}
      {visibleRecords && (visibleRecords.length ? <div class="ss-table-scroll">
        <table><thead><tr><th>ID</th><th>Scope</th><th>Created</th><th>Issued by</th><th>Action</th></tr></thead><tbody>
          {visibleRecords.map((record) => <tr key={record.id}>
            <td class="mono">{record.id}</td>
            <td><details><summary>View scope</summary><ScopeSummary scope={record.scope} /></details></td>
            <td>{dateLabel(record.createdAtMs)}</td><td class="mono">{record.issuedBy || 'Uncredentialed admin'}</td>
            <td><button class="ss-button" type="button" disabled={pending || confirmation !== null}
              aria-label={`Revoke token ${record.id}`} onClick={() => reviewRevoke(record.id)}>Revoke</button></td>
          </tr>)}
        </tbody></table>
      </div> : <div class="ss-empty">No visible tokens</div>)}
    </section>

    {success && <p class="ss-success" role="status">{success}</p>}
    {writeError && <p class="ss-notice" role="alert">{writeError}</p>}
    {issued && <section class="ss-section ss-card ss-form" aria-label="New token secret">
      <h2 ref={secretHeading} tabIndex={-1}>Save your new token</h2>
      <p class="ss-hint">Token {issued.id}. This secret is shown only for this issuance. It stays in this page's memory until dismissed or the page closes; it cannot be retrieved later.</p>
      <label>New token secret<textarea class="mono" rows={3} readOnly value={issued.token} spellcheck={false} /></label>
      <div class="ss-toolbar">
        <button class="ss-button" type="button" onClick={copySecret}>Copy secret</button>
        <button class="ss-button" type="button" disabled={pending} onClick={() => { setIssued(null); setCopyStatus(''); setSuccess('') }}>Dismiss secret</button>
        {copyStatus && <span class="ss-hint" role="status">{copyStatus}</span>}
      </div>
    </section>}

    {confirmation && <section class="ss-section ss-card" aria-label="Confirm token action">
      <h2 ref={confirmHeading} tabIndex={-1}>{confirmation.kind === 'issue' ? 'Confirm token issuance' : 'Confirm token revocation'}</h2>
      <dl class="ss-metadata"><dt>Token ID</dt><dd class="mono">{confirmation.id}</dd><dt>Admin API</dt><dd>{confirmation.connection.endpoint}</dd></dl>
      {confirmation.kind === 'issue' ? <>
        <ScopeSummary scope={confirmation.scope} />
        {confirmation.id === 'anonymous' && <p class="ss-notice">The reserved ID anonymous grants these permissions to clients that send no token. They do not need the generated secret.</p>}
        <p class="ss-hint">The server requires this scope to fit within your own permissions. Its secret will be returned once.</p>
      </> : <p class="ss-hint">This token will stop authorizing requests. Revoking the token you are using can lock your dashboard admin connection.</p>}
      <div class="ss-toolbar">
        <button class="ss-button primary" type="button" disabled={pending} onClick={confirmWrite}>{pending ? 'Submitting…' : confirmation.kind === 'issue' ? 'Confirm issue token' : 'Confirm revoke token'}</button>
        <button class="ss-button" type="button" disabled={pending} onClick={() => setConfirmation(null)}>Cancel</button>
      </div>
    </section>}

    <section class="ss-section ss-spaced">
      <h2>Issue token</h2>
      <p class="ss-hint">Choose the listeners, permissions and resources this token can use. The server checks that the requested scope is no broader than your admin token.</p>
      <form class="ss-form" onSubmit={(event) => { event.preventDefault(); reviewIssue() }}>
        <div class="ss-fields">
          <label>New token ID<input class="mono" value={id} disabled={locked} placeholder="team/reader" onInput={(event) => setId(event.currentTarget.value)} /></label>
          <label>Expires at (local time, optional)<input type="datetime-local" value={expiry} disabled={locked} onInput={(event) => setExpiry(event.currentTarget.value)} /></label>
        </div>
        <p class="ss-hint">Token IDs contain 1–96 UTF-8 bytes. The ID anonymous grants access without a token.</p>
        <ChoiceRows label="Audience" rows={audiences} options={TOKEN_AUDIENCES} labels={audienceLabels} disabled={locked} onChange={setAudiences} nextId={nextId} />
        <ChoiceRows label="Permission" rows={ops} options={TOKEN_OPERATIONS} labels={operationLabels} disabled={locked} onChange={setOps} nextId={nextId} />
        <MatcherRows label="Streams" rows={streams} disabled={locked} onChange={setStreams} nextId={nextId} />
        <MatcherRows label="Token IDs" rows={tokens} disabled={locked} onChange={setTokens} nextId={nextId} />
        <label>Automatic stream prefixing<select value={autoPrefixStreams ? 'yes' : 'no'} disabled={locked} onChange={(event) => setAutoPrefixStreams(event.currentTarget.value === 'yes')}>
          <option value="no">Disabled</option><option value="yes">Use the single stream prefix for relative names</option>
        </select></label>
        <p class="ss-hint">Stream rules apply to protocol stream operations; token ID rules apply to token management. Admin permissions apply across the cluster, including ownership inspection and transfer. An empty rule list grants no matching resources. Prefix matching uses a literal string prefix.</p>
        <div><button class="ss-button primary" type="submit" disabled={locked}>Review token</button></div>
      </form>
    </section>
  </>
}
