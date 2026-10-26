import { useLocation } from 'preact-iso'
import { useEffect, useState } from 'preact/hooks'

import { withQuery } from '../location'
import { Input } from './control'

const DELAY_MS = 250

export function Search({ placeholder }: { placeholder: string }) {
  const { path, query, route } = useLocation()
  const applied = query.prefix ?? ''
  const [value, setValue] = useState(applied)

  useEffect(() => {
    if (value === applied) {
      return
    }

    const timer = setTimeout(() => route(withQuery(path, query, { prefix: value }), true), DELAY_MS)
    return () => clearTimeout(timer)
  }, [value])

  return (
    <Input
      type="search"
      aria-label={placeholder}
      placeholder={placeholder}
      value={value}
      onInput={(event) => setValue(event.currentTarget.value)}
    />
  )
}
