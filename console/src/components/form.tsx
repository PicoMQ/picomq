import type { ComponentChildren } from 'preact'
import { useState } from 'preact/hooks'

import { Actions } from './actions'
import { Button } from './button'
import { Notice } from './notice'

import './form.css'

interface Props {
  submit: string
  kind?: 'primary' | 'danger'
  pending?: boolean
  disabled?: boolean
  error?: Error | null
  onSubmit: () => void
  onCancel?: () => void
  children: ComponentChildren
}

export function Form({ submit, kind = 'primary', pending, disabled, error, onSubmit, onCancel, children }: Props) {
  const send = (event: Event) => {
    event.preventDefault()
    onSubmit()
  }

  return (
    <form class="form" onSubmit={send}>
      {children}
      {error && <Notice kind="danger">{error.message}</Notice>}
      <Actions>
        {onCancel && <Button onClick={onCancel}>Cancel</Button>}
        <Button kind={kind} type="submit" disabled={pending || disabled}>
          {submit}
        </Button>
      </Actions>
    </form>
  )
}

export function useDraft<T extends Record<string, string>>(initial: T) {
  const [draft, setDraft] = useState(initial)

  const set = (field: keyof T, value: string) => setDraft((current) => ({ ...current, [field]: value }))
  const edit = (field: keyof T) => (event: { currentTarget: { value: string } }) => set(field, event.currentTarget.value)

  return { draft, edit, set }
}

export function Label({ text, children }: { text: string; children: ComponentChildren }) {
  return (
    <label class="form-label">
      <small>{text}</small>
      {children}
    </label>
  )
}
