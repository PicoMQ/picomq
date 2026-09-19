import { useEffect, useLayoutEffect, useRef, useState } from 'preact/hooks'
import { errorMessage, type Connection } from './streams'
import { getToken } from './api'

import {
  createStream, deleteStream, lifecycleStreamPath, StreamMutationError,
  validateCreateStream, type CreateStreamOptions,
} from './stream-lifecycle'

interface AdminConnection extends Connection { dashboard: boolean; isCurrent: () => boolean }

function failure(error: unknown, name: string, endpoint: string) {
  const unknown = error instanceof StreamMutationError && error.uncertain
  return `${errorMessage(error)} ${unknown ? 'The outcome is unknown.' : 'Request failed.'} Target: ${name} via ${endpoint}.${unknown ? ' Refresh Discovery before deciding to try again. This request was not retried.' : ''}`
}

export function CreateStream({ connection, onChanged, onPending, disabled = false }: {
  connection: AdminConnection | null; onChanged: (name: string) => void; onPending: (pending: boolean) => void; disabled?: boolean
}) {
  const [name, setName] = useState('')
  const [contentType, setContentType] = useState('application/json')
  const [kafkaTopic, setKafkaTopic] = useState('')
  const [review, setReview] = useState<(CreateStreamOptions & { connection: AdminConnection }) | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState('')
  const pending = useRef(false)
  const mounted = useRef(true)
  useEffect(() => { mounted.current = true; return () => { mounted.current = false } }, [])
  useLayoutEffect(() => {
    if (pending.current) return
    setName(''); setContentType('application/json'); setKafkaTopic(''); setReview(null); setError(''); setResult('')
  }, [connection])

  async function confirm() {
    if (!review || !connection || review.connection !== connection || pending.current || disabled) return
    if (!connection.isCurrent()) {
      setReview(null); setError('The connection changed. Review this request again before submitting.'); return
    }
    if (connection.dashboard && connection.token !== (getToken() || '')) {
      setReview(null); setError('The admin token changed. Pair the admin connection again and review this request before submitting.'); return
    }
    const target = { ...connection }
    const draft = review
    pending.current = true; setBusy(true); onPending(true); setError(''); setResult('')
    try {
      const response = await createStream(target, draft)
      if (mounted.current) {
        setResult(`${response.created ? 'Created' : 'Already exists with matching configuration:'} ${draft.name} via ${target.endpoint}.`)
        setReview(null)
        onChanged(draft.name)
      }
    } catch (error) {
      if (mounted.current) { setError(failure(error, draft.name, target.endpoint)); setReview(null) }
    } finally {
      pending.current = false; onPending(false)
      if (mounted.current) setBusy(false)
    }
  }

  return <details class="ss-connection">
    <summary>Create stream</summary>
    {!connection && <p class="ss-hint">Choose an Admin connection above before creating a stream.</p>}
    <form class="ss-form" onSubmit={(event) => {
      event.preventDefault()
      if (!connection || pending.current || disabled) return
      try { setReview({ ...validateCreateStream({ name, contentType, kafkaTopic }), connection }); setError(''); setResult('') }
      catch (error) { setReview(null); setError(errorMessage(error)) }
    }}>
      <div class="ss-fields">
        <label>New stream name<input class="mono" required value={name} disabled={busy || disabled || !connection} placeholder="/demo/new-stream" onInput={(event) => { setName(event.currentTarget.value); setReview(null) }} /></label>
        <label>Content type<input required value={contentType} disabled={busy || disabled || !connection} onInput={(event) => { setContentType(event.currentTarget.value); setReview(null) }} /></label>
        <label>Kafka alias (optional)<input class="mono" value={kafkaTopic} disabled={busy || disabled || !connection} onInput={(event) => { setKafkaTopic(event.currentTarget.value); setReview(null) }} /></label>
      </div>
      <p class="ss-hint">Uses the paired admin connection with admin audience, create permission and a matching stream scope. Use the full stored stream name; automatic token prefixes are not applied. A blank Kafka alias lets the server derive one when available. No initial messages are sent.</p>
      {!review && <div class="ss-toolbar"><button type="submit" class="ss-button" disabled={busy || disabled || !connection}>Review create</button></div>}
      {review && review.connection === connection && <div class="ss-card">
        <p class="ss-hint">Create this stream at <span class="mono">{review.connection.endpoint}</span>. An existing stream must have matching configuration.</p>
        <dl class="ss-metadata"><dt>Stream</dt><dd class="mono">{review.name}</dd><dt>Content type</dt><dd>{review.contentType}</dd><dt>Kafka alias</dt><dd class="mono">{review.kafkaTopic || 'Server default'}</dd></dl>
        <div class="ss-toolbar"><button type="button" class="ss-button primary" disabled={busy || disabled || !connection} onClick={() => void confirm()}>{busy ? 'Creating…' : 'Confirm create stream'}</button><button type="button" class="ss-button" disabled={busy} onClick={() => setReview(null)}>Cancel</button></div>
      </div>}
      {error && <p class="ss-notice" role="alert">{error}</p>}
      {result && <p class="ss-success" role="status">{result}</p>}
    </form>
  </details>
}

export function DeleteStream({ connection, name, isSelectionCurrent, onDeleted, onPending, disabled = false }: {
  connection: AdminConnection | null; name: string; isSelectionCurrent: () => boolean; onDeleted: () => void; onPending: (pending: boolean) => void; disabled?: boolean
}) {
  const [confirmation, setConfirmation] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState('')
  const pending = useRef(false)
  const mounted = useRef(true)
  const priorConnection = useRef(connection)
  useEffect(() => { mounted.current = true; return () => { mounted.current = false } }, [])
  useLayoutEffect(() => {
    if (pending.current) return
    setConfirmation(''); setError('')
    // Discovery clears its selection after deletion; keep that receipt visible
    // until the user selects another stream or changes the connection.
    if (name || priorConnection.current !== connection) setResult('')
    priorConnection.current = connection
  }, [connection, name])

  async function remove(event: Event) {
    event.preventDefault()
    if (!connection || pending.current || disabled || priorConnection.current !== connection || !name || confirmation !== name) return
    if (!connection.isCurrent() || !isSelectionCurrent()) {
      setConfirmation(''); setError('The stream or connection changed. Confirm the current stream again before submitting.'); return
    }
    if (connection.dashboard && connection.token !== (getToken() || '')) {
      setConfirmation(''); setError('The admin token changed. Pair the admin connection again and confirm the stream name before submitting.'); return
    }
    try { lifecycleStreamPath(name) }
    catch (error) { setError(errorMessage(error)); return }
    const target = { ...connection }
    const targetName = name
    pending.current = true; setBusy(true); onPending(true); setError(''); setResult('')
    try {
      await deleteStream(target, targetName)
      if (mounted.current) {
        setResult(`Deleted ${targetName} via ${target.endpoint}.`)
        setConfirmation('')
        onDeleted()
      }
    } catch (error) {
      if (mounted.current) { setError(failure(error, targetName, target.endpoint)); setConfirmation('') }
    } finally {
      pending.current = false; onPending(false)
      if (mounted.current) setBusy(false)
    }
  }

  return <details class="ss-connection">
    <summary>Delete stream</summary>
    {!name && <p class="ss-hint">Select a stream in Discovery to delete it.</p>}
    {name && !connection && <p class="ss-hint">Choose an Admin connection above before deleting this stream.</p>}
    {name && connection && <form class="ss-form" onSubmit={(event) => void remove(event)}>
      <p class="ss-hint">Permanently delete <span class="mono">{name}</span> and its messages from <span class="mono">{connection.endpoint}</span>. This cannot be undone. Requires admin audience, delete permission and a matching stream scope.</p>
      <label>Type the exact stream name to confirm<input class="mono" autoComplete="off" required value={confirmation} disabled={busy || disabled || !!result} onInput={(event) => setConfirmation(event.currentTarget.value)} /></label>
      <div class="ss-toolbar"><button type="submit" class="ss-button" disabled={busy || disabled || confirmation !== name || !!result}>{busy ? 'Deleting…' : 'Confirm delete stream'}</button></div>
      {error && <p class="ss-notice" role="alert">{error}</p>}
    </form>}
    {result && <p class="ss-success" role="status">{result}</p>}
  </details>
}
