import { ChevronDown } from 'lucide-preact'
import type { ComponentChildren, InputHTMLAttributes, SelectHTMLAttributes } from 'preact'

import './control.css'

type Prefixed<T> = T & {
  prefix?: string
}

export function Input({ prefix, ...props }: Prefixed<InputHTMLAttributes<HTMLInputElement>>) {
  return (
    <Prefix text={prefix}>
      <input {...props} class="control" />
    </Prefix>
  )
}

export function Select({ prefix, ...props }: Prefixed<SelectHTMLAttributes<HTMLSelectElement>>) {
  return (
    <Prefix text={prefix}>
      <span class="control-select">
        <select {...props} class="control" />
        <ChevronDown size={14} />
      </span>
    </Prefix>
  )
}

export function numberOf(text: string): number | undefined {
  return text.trim() === '' ? undefined : Number(text)
}

function Prefix({ text, children }: { text: string | undefined; children: ComponentChildren }) {
  if (text === undefined) {
    return <>{children}</>
  }
  return (
    <label class="control-prefixed">
      <span class="control-prefix">{text}</span>
      {children}
    </label>
  )
}
