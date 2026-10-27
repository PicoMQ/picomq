import type { ComponentChildren } from 'preact'
import { useLayoutEffect, useRef } from 'preact/hooks'

import './dialog.css'

interface Props {
  title: string
  open: boolean
  onClose: () => void
  children: ComponentChildren
}

export function Dialog({ title, open, onClose, children }: Props) {
  const dialog = useRef<HTMLDialogElement>(null)

  useLayoutEffect(() => {
    const element = dialog.current
    if (open && !element?.open) {
      element?.showModal()
    }
    if (!open && element?.open) {
      element.close()
    }
  }, [open])

  return (
    <dialog class="dialog" ref={dialog} onClose={onClose}>
      <h2>{title}</h2>
      {open && children}
    </dialog>
  )
}
