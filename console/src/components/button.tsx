import type { ButtonHTMLAttributes } from 'preact'

import './button.css'

type Props = ButtonHTMLAttributes<HTMLButtonElement> & {
  kind?: 'primary' | 'secondary' | 'danger'
}

export function Button({ kind = 'secondary', type = 'button', ...props }: Props) {
  return <button {...props} type={type} class="button" data-kind={kind} />
}
