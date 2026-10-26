import { useMutation } from '@tanstack/preact-query'
import type { ComponentChildren } from 'preact'
import { useState } from 'preact/hooks'

import { Input } from './control'
import { Dialog } from './dialog'
import { Form, Label } from './form'

interface Props {
  title: string
  name: string
  action: string
  open: boolean
  onClose: () => void
  onConfirm: () => Promise<unknown>
  children: ComponentChildren
}

export function Confirm({ title, name, action, open, onClose, onConfirm, children }: Props) {
  return (
    <Dialog title={title} open={open} onClose={onClose}>
      <Check name={name} action={action} onClose={onClose} onConfirm={onConfirm}>
        {children}
      </Check>
    </Dialog>
  )
}

function Check({ name, action, onClose, onConfirm, children }: Omit<Props, 'title' | 'open'>) {
  const [typed, setTyped] = useState('')
  const mutation = useMutation({ mutationFn: onConfirm, onSuccess: onClose })

  return (
    <Form
      submit={action}
      kind="danger"
      pending={mutation.isPending}
      disabled={typed !== name}
      error={mutation.error}
      onSubmit={() => mutation.mutate()}
      onCancel={onClose}
    >
      <p>{children}</p>
      <Label text={`Type ${name} to confirm`}>
        <Input value={typed} autocomplete="off" onInput={(event) => setTyped(event.currentTarget.value)} />
      </Label>
    </Form>
  )
}
