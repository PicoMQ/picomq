import { useMutation, useQueryClient } from '@tanstack/preact-query'
import { Copy, KeyRound, Plus, X } from 'lucide-preact'
import { useLocation } from 'preact-iso'
import { useState } from 'preact/hooks'

import { usePages } from '../api/query'
import {
  AUDIENCES,
  OPERATIONS,
  issueToken,
  revokeToken,
  tokens,
  type Access,
  type Audience,
  type IssuedToken,
  type Matcher,
  type Scope,
  type Token,
} from '../api/tokens'
import { Actions } from '../components/actions'
import { Button } from '../components/button'
import { Confirm } from '../components/confirm'
import { Checkbox, Input, Select } from '../components/control'
import { DateTime } from '../components/datetime'
import { Dialog } from '../components/dialog'
import { Form, Label } from '../components/form'
import { List } from '../components/list'
import { Search } from '../components/search'
import type { Column } from '../components/table'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import * as format from '../format'

import './tokens.css'

const AUDIENCE_LABELS: Record<Audience, string> = {
  pico: 'Pico',
  durable_streams: 'Durable Streams',
  kafka: 'Kafka',
  admin: 'Admin',
}

const ACCESS: { key: keyof Scope['access']; label: string }[] = [
  { key: 'stream', label: 'Streams' },
  { key: 'group', label: 'Groups' },
  { key: 'tokens', label: 'Tokens' },
  { key: 'admin', label: 'Admin' },
]

const NONE: Access = { read: false, write: false }

const EMPTY: Scope = {
  streams: [],
  tokens: [],
  groups: [],
  access: { stream: NONE, tokens: NONE, group: NONE, admin: NONE },
  ops: [],
  audiences: [],
  autoPrefix: false,
  expiresAtMs: null,
}

export function accessSummary(scope: Scope): string {
  const granted = ACCESS.flatMap(({ key }) => {
    const flags = [scope.access[key].read && 'read', scope.access[key].write && 'write'].filter((flag) => flag !== false)
    return flags.length === 0 ? [] : [`${key} ${flags.join(' and ')}`]
  })
  const single = scope.ops.length > 0 ? [format.counted(scope.ops.length, 'single operation')] : []

  return [...granted, ...single].join(', ') || 'nothing'
}

export default function Tokens() {
  const { query } = useLocation()
  const client = useQueryClient()
  const pages = usePages(tokens(query.prefix))
  const [issuing, setIssuing] = useState(false)
  const [revoking, setRevoking] = useState<string | null>(null)

  const columns: Column<Token>[] = [
    { name: 'id', header: 'Token', cell: (token) => token.id },
    { name: 'audiences', header: 'Audiences', width: '16rem', cell: (token) => token.scope.audiences.join(', ') || 'none' },
    { name: 'access', header: 'Access', cell: (token) => accessSummary(token.scope) },
    { name: 'issuer', header: 'Issued by', width: '10rem', cell: (token) => token.issuedBy || 'none' },
    { name: 'created', header: 'Created', width: '14rem', cell: (token) => format.time(token.createdAtMs) },
    {
      name: 'expires',
      header: 'Expires',
      width: '14rem',
      cell: (token) => (token.scope.expiresAtMs === null ? 'never' : format.time(token.scope.expiresAtMs)),
    },
    {
      name: 'revoke',
      header: '',
      width: '9rem',
      cell: (token) => (
        <Button kind="danger" onClick={() => setRevoking(token.id)}>
          Revoke
        </Button>
      ),
    },
  ]

  const revoke = async () => {
    await revokeToken(revoking ?? '')
    await client.invalidateQueries({ queryKey: ['tokens'] })
  }

  return (
    <View trail={[{ label: 'Tokens' }]}>
      <Toolbar
        actions={
          <>
            <Button onClick={pages.refresh}>Refresh</Button>
            <Button kind="primary" onClick={() => setIssuing(true)}>
              <Plus size={16} />
              Issue token
            </Button>
          </>
        }
      >
        <Search placeholder="Search tokens by id prefix" />
      </Toolbar>
      <List pages={pages} columns={columns} rowKey={(token) => token.id} noun="token" />
      <Dialog title="Issue token" open={issuing} onClose={() => setIssuing(false)}>
        <Issue onDone={() => setIssuing(false)} />
      </Dialog>
      <Confirm
        title="Revoke token"
        name={revoking ?? ''}
        action="Revoke token"
        open={revoking !== null}
        onClose={() => setRevoking(null)}
        onConfirm={revoke}
      >
        Every client using {revoking} loses access on its next request, on every node.
      </Confirm>
    </View>
  )
}

function Issue({ onDone }: { onDone: () => void }) {
  const client = useQueryClient()
  const [id, setId] = useState('')
  const [scope, setScope] = useState<Scope>(EMPTY)
  const mutation = useMutation({
    mutationFn: () => issueToken(id.trim(), scope),
    onSuccess: () => client.invalidateQueries({ queryKey: ['tokens'] }),
  })

  if (mutation.data) {
    return <Issued issued={mutation.data} onDone={onDone} />
  }

  const toggle = <T,>(list: T[], value: T) => (list.includes(value) ? list.filter((each) => each !== value) : [...list, value])

  return (
    <Form
      submit="Issue"
      pending={mutation.isPending}
      disabled={id.trim() === ''}
      error={mutation.error}
      onSubmit={() => mutation.mutate()}
      onCancel={onDone}
    >
      <Label text="Token id">
        <Input value={id} required placeholder="svc/billing" onInput={(event) => setId(event.currentTarget.value)} />
      </Label>
      <fieldset class="tokens-group">
        <legend>
          <small>Audiences</small>
        </legend>
        {AUDIENCES.map((audience) => (
          <Checkbox
            key={audience}
            label={AUDIENCE_LABELS[audience]}
            checked={scope.audiences.includes(audience)}
            onChange={() => setScope((current) => ({ ...current, audiences: toggle(current.audiences, audience) }))}
          />
        ))}
      </fieldset>
      <fieldset class="tokens-access">
        <legend>
          <small>Access</small>
        </legend>
        {ACCESS.map(({ key, label }) => (
          <div key={key} class="tokens-row">
            <span>{label}</span>
            {(['read', 'write'] as const).map((flag) => (
              <Checkbox
                key={flag}
                label={flag}
                checked={scope.access[key][flag]}
                onChange={() =>
                  setScope((current) => ({
                    ...current,
                    access: { ...current.access, [key]: { ...current.access[key], [flag]: !current.access[key][flag] } },
                  }))
                }
              />
            ))}
          </div>
        ))}
      </fieldset>
      <Matchers
        label="Streams"
        matchers={scope.streams}
        onChange={(update) => setScope((current) => ({ ...current, streams: update(current.streams) }))}
      />
      <Matchers
        label="Groups"
        matchers={scope.groups}
        onChange={(update) => setScope((current) => ({ ...current, groups: update(current.groups) }))}
      />
      <Matchers
        label="Token ids it may issue"
        matchers={scope.tokens}
        onChange={(update) => setScope((current) => ({ ...current, tokens: update(current.tokens) }))}
      />
      <fieldset class="tokens-group">
        <legend>
          <small>Single operations</small>
        </legend>
        {OPERATIONS.map((operation) => (
          <Checkbox
            key={operation}
            label={operation.replaceAll('_', ' ')}
            checked={scope.ops.includes(operation)}
            onChange={() => setScope((current) => ({ ...current, ops: toggle(current.ops, operation) }))}
          />
        ))}
      </fieldset>
      <Checkbox
        label="Auto prefix: names are relative to its one stream prefix and one group prefix"
        checked={scope.autoPrefix}
        onChange={() => setScope((current) => ({ ...current, autoPrefix: !current.autoPrefix }))}
      />
      <Label text="Expires, empty for never">
        <DateTime
          value={scope.expiresAtMs}
          placeholder="never"
          clearable
          onChange={(ms) => setScope((current) => ({ ...current, expiresAtMs: ms }))}
        />
      </Label>
    </Form>
  )
}

interface MatchersProps {
  label: string
  matchers: Matcher[]
  onChange: (update: (current: Matcher[]) => Matcher[]) => void
}

function Matchers({ label, matchers, onChange }: MatchersProps) {
  const kind = (matcher: Matcher) => ('exact' in matcher ? 'exact' : 'prefix')
  const text = (matcher: Matcher) => ('exact' in matcher ? matcher.exact : matcher.prefix)
  const make = (chosen: string, value: string): Matcher => (chosen === 'exact' ? { exact: value } : { prefix: value })
  const replace = (index: number, change: (matcher: Matcher) => Matcher) =>
    onChange((current) => current.map((each, at) => (at === index ? change(each) : each)))

  return (
    <fieldset class="tokens-matchers">
      <legend>
        <small>{label}</small>
      </legend>
      {matchers.map((matcher, index) => (
        <div key={index} class="tokens-matcher">
          <Select
            value={kind(matcher)}
            onChange={(event) => {
              const chosen = event.currentTarget.value
              replace(index, (each) => make(chosen, text(each)))
            }}
          >
            <option value="prefix">Prefix</option>
            <option value="exact">Exact</option>
          </Select>
          <Input
            value={text(matcher)}
            onInput={(event) => {
              const value = event.currentTarget.value
              replace(index, (each) => make(kind(each), value))
            }}
          />
          <Button aria-label={`Remove ${label} matcher`} onClick={() => onChange((current) => current.filter((_, at) => at !== index))}>
            <X size={14} />
          </Button>
        </div>
      ))}
      <Actions>
        <Button onClick={() => onChange((current) => [...current, { prefix: '' }])}>
          <Plus size={14} />
          Add
        </Button>
      </Actions>
    </fieldset>
  )
}

function Issued({ issued, onDone }: { issued: IssuedToken; onDone: () => void }) {
  const [copied, setCopied] = useState(false)

  const copy = async () => {
    await navigator.clipboard.writeText(issued.token)
    setCopied(true)
  }

  return (
    <div class="tokens-issued">
      <p>
        <KeyRound size={16} /> {issued.id} is ready. Copy the token now, it is not shown again.
      </p>
      <Input value={issued.token} readOnly onFocus={(event) => event.currentTarget.select()} />
      <Actions>
        <Button onClick={() => void copy()}>
          <Copy size={14} />
          {copied ? 'Copied' : 'Copy'}
        </Button>
        <Button kind="primary" onClick={onDone}>
          Done
        </Button>
      </Actions>
    </div>
  )
}
