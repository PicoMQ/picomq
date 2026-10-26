import { useState } from 'preact/hooks'

import { token } from '../api/client'
import { Button } from './button'
import { Logo } from './logo'

import './login.css'

export function Login({ onSignIn }: { onSignIn: () => void }) {
  const [value, setValue] = useState('')

  const submit = (event: Event) => {
    event.preventDefault()
    token.set(value.trim())
    onSignIn()
  }

  return (
    <main class="login">
      <form class="login-form" onSubmit={submit}>
        <Logo />
        <h1>PicoMQ Console</h1>
        <p class="login-note">This cluster requires a token with the admin audience.</p>
        <input
          class="login-input"
          type="password"
          autocomplete="off"
          aria-label="Token"
          placeholder="token"
          value={value}
          onInput={(event) => setValue(event.currentTarget.value)}
        />
        <Button kind="primary" type="submit" disabled={value.trim() === ''}>
          Sign in
        </Button>
      </form>
    </main>
  )
}
