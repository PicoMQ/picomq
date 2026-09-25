import type { ResourceMatcher, TokenAudience, TokenOperation, TokenScope } from './admin-actions'

export interface MatcherDraft {
  id: number
  kind: 'exact' | 'prefix' | 'all'
  value: string
}

export interface TokenScopeDraft {
  streams: MatcherDraft[]
  tokens: MatcherDraft[]
  ops: TokenOperation[]
  audiences: TokenAudience[]
  autoPrefixStreams: boolean
  expiry: string
}

function matchers(rows: MatcherDraft[], label: string): ResourceMatcher[] {
  return rows.map((row) => {
    if (row.kind === 'all') return { prefix: '' }
    // An empty prefix grants every resource. Make that an explicit choice in
    // the form, rather than turning an unfinished row into a broad grant.
    if (!row.value) throw new Error(`Enter a ${label} ${row.kind}, remove the empty row, or choose All.`)
    return row.kind === 'exact' ? { exact: row.value } : { prefix: row.value }
  })
}

export function buildTokenScope(draft: TokenScopeDraft, now = Date.now()): TokenScope {
  const streams = matchers(draft.streams, 'stream')
  const tokens = matchers(draft.tokens, 'token ID')
  const audiences = [...new Set(draft.audiences)]
  const ops = [...new Set(draft.ops)]
  if (!audiences.length) throw new Error('Choose at least one audience.')
  if (!ops.length) throw new Error('Choose at least one permission.')
  if (draft.autoPrefixStreams && (streams.length !== 1 || !('prefix' in streams[0]))) {
    throw new Error('Automatic stream prefixing requires exactly one stream prefix matcher.')
  }
  const expiresAtMs = draft.expiry ? new Date(draft.expiry).getTime() : null
  if (expiresAtMs !== null && (!Number.isSafeInteger(expiresAtMs) || expiresAtMs <= now)) {
    throw new Error('Choose an expiry in the future, or leave it blank for no expiry.')
  }
  return { streams, tokens, ops, audiences, autoPrefixStreams: draft.autoPrefixStreams, expiresAtMs }
}
