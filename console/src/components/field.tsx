import type * as records from '../api/records'
import * as format from '../format'
import { Tag } from './tag'
import type { Language } from './viewer'

import './field.css'

const HEX_PREVIEW = 64

export interface Shape {
  kind: 'text' | 'binary'
  text: string
}

export function shape(field: records.Field): Shape {
  if (field.encoding === 'base64') {
    return { kind: 'binary', text: hex(field, HEX_PREVIEW) }
  }
  return { kind: 'text', text: field.data }
}

export function pretty(field: records.Field): { language: Language; text: string } {
  if (field.encoding === 'base64') {
    return { language: 'text', text: hex(field, field.size) }
  }

  const json = !field.truncated && /^\s*[[{]/.test(field.data) ? indent(field.data) : null
  return json === null ? { language: 'text', text: field.data } : { language: 'json', text: json }
}

export function hex(field: records.Field, limit: number): string {
  const decoded = atob(field.data.slice(0, Math.ceil(limit / 3) * 4))
  const shown = Math.min(decoded.length, limit)
  const pairs = Array.from({ length: shown }, (_, index) =>
    decoded.charCodeAt(index).toString(16).padStart(2, '0'),
  )

  return field.size > shown ? `${pairs.join(' ')} …` : pairs.join(' ')
}

export function Field({ field }: { field: records.Field | null }) {
  if (field === null) {
    return <Tag>null</Tag>
  }

  const shaped = shape(field)
  return (
    <span class="field" data-kind={shaped.kind}>
      <span class="field-text">{shaped.text}</span>
      {shaped.kind === 'binary' && <Tag>binary</Tag>}
      {field.truncated && <span class="field-size">{format.bytes(field.size)}</span>}
    </span>
  )
}

function indent(text: string): string | null {
  try {
    return JSON.stringify(JSON.parse(text), null, 2)
  } catch {
    return null
  }
}
