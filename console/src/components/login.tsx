import { useState } from 'preact/hooks'

import { token } from '../api/client'
import { Input } from './control'
import { Form } from './form'
import { Logo } from './logo'

import './login.css'

export function Login({ onSignIn }: { onSignIn: () => void }) {
  const [value, setValue] = useState('')

  const submit = () => {
    token.set(value.trim())
    onSignIn()
  }

  return (
    <main class="login">
      <div class="login-card">
        <Logo />
        <h1>PicoMQ Console</h1>
        <p class="login-note">This cluster requires a token with the admin audience.</p>
        <Form submit="Sign in" disabled={value.trim() === ''} onSubmit={submit}>
          <Input
            type="password"
            autocomplete="off"
            aria-label="Token"
            placeholder="token"
            value={value}
            onInput={(event) => setValue(event.currentTarget.value)}
          />
        </Form>
      </div>
    </main>
  )
}
