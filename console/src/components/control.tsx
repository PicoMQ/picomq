import { Check, ChevronDown } from 'lucide-preact'
import type { ComponentChildren, InputHTMLAttributes, SelectHTMLAttributes } from 'preact'
import { useId } from 'preact/hooks'

import './control.css'

type Prefixed<T> = T & {
  prefix?: string
}

export function Input({ prefix, ...props }: Prefixed<InputHTMLAttributes<HTMLInputElement>>) {
  const id = useId()

  return (
    <Prefix text={prefix} id={id}>
      <input {...props} id={id} class="control" />
    </Prefix>
  )
}

export function Select({ prefix, ...props }: Prefixed<SelectHTMLAttributes<HTMLSelectElement>>) {
  const id = useId()

  return (
    <Prefix text={prefix} id={id}>
      <span class="control-select">
        <select {...props} id={id} class="control" />
        <ChevronDown size={14} />
      </span>
    </Prefix>
  )
}

interface CheckboxProps {
  label: string
  checked: boolean
  onChange: () => void
}

export function Checkbox({ label, checked, onChange }: CheckboxProps) {
  return (
    <label class="control-check">
      <input type="checkbox" class="control-checkbox" checked={checked} onChange={onChange} />
      <span class="control-box" aria-hidden="true">
        {checked && <Check size={12} strokeWidth={3} />}
      </span>
      {label}
    </label>
  )
}

export function numberOf(text: string): number | undefined {
  return text.trim() === '' ? undefined : Number(text)
}

function Prefix({ text, id, children }: { text: string | undefined; id: string; children: ComponentChildren }) {
  if (text === undefined) {
    return <>{children}</>
  }
  return (
    <div class="control-prefixed">
      <label class="control-prefix" for={id}>
        {text}
      </label>
      {children}
    </div>
  )
}
