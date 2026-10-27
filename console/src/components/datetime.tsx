import { ChevronLeft, ChevronRight } from 'lucide-preact'
import { useEffect, useRef, useState } from 'preact/hooks'

import * as format from '../format'
import { Actions } from './actions'
import { Button } from './button'
import { Input } from './control'

import './datetime.css'

const WEEKDAYS = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su']
const PATTERN = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/

interface Props {
  value: number | null
  onChange: (ms: number | null) => void
  prefix?: string
  placeholder?: string
  clearable?: boolean
}

export function parse(text: string): number | null {
  const match = PATTERN.exec(text.trim())
  if (!match) {
    return null
  }

  const [year = 0, month = 0, day, hour, minute, second] = match.slice(1).map((part) => Number(part ?? 0))
  const date = new Date(year, month - 1, day, hour, minute, second)
  return date.getMonth() === month - 1 ? date.getTime() : null
}

export function month(year: number, index: number): Date[] {
  const first = new Date(year, index, 1)
  const offset = (first.getDay() + 6) % 7

  return Array.from({ length: 42 }, (_, day) => new Date(year, index, day + 1 - offset))
}

function monthOf(value: number | null): { year: number; month: number } {
  const date = new Date(value ?? Date.now())

  return { year: date.getFullYear(), month: date.getMonth() }
}

export function DateTime({ value, onChange, prefix, placeholder = 'YYYY-MM-DD HH:MM:SS', clearable = false }: Props) {
  const root = useRef<HTMLDivElement>(null)
  const [open, setOpen] = useState(false)
  const [text, setText] = useState(value === null ? '' : format.time(value, 'second'))
  const [view, setView] = useState(monthOf(value))

  useEffect(() => setText(value === null ? '' : format.time(value, 'second')), [value])

  useEffect(() => {
    if (!open) {
      return
    }
    const close = (event: Event) => {
      if (event instanceof KeyboardEvent ? event.key === 'Escape' : !root.current?.contains(event.target as Node)) {
        setOpen(false)
      }
    }

    addEventListener('mousedown', close)
    addEventListener('keydown', close)
    return () => {
      removeEventListener('mousedown', close)
      removeEventListener('keydown', close)
    }
  }, [open])

  const show = () => {
    if (!open) {
      setView(monthOf(value))
      setOpen(true)
    }
  }

  const commit = () => {
    if (text.trim() === '') {
      if (clearable) {
        onChange(null)
      }
      return
    }
    const parsed = parse(text)
    if (parsed !== null) {
      onChange(parsed)
    }
  }

  const pick = (day: Date) => {
    const time = new Date(value ?? Date.now())
    onChange(new Date(day.getFullYear(), day.getMonth(), day.getDate(), time.getHours(), time.getMinutes(), time.getSeconds()).getTime())
  }

  const setPart = (part: 'hours' | 'minutes' | 'seconds', amount: string) => {
    const time = new Date(value ?? Date.now())
    const number = Math.max(0, Number(amount) || 0)
    if (part === 'hours') {
      time.setHours(Math.min(number, 23))
    } else if (part === 'minutes') {
      time.setMinutes(Math.min(number, 59))
    } else {
      time.setSeconds(Math.min(number, 59))
    }
    onChange(time.getTime())
  }

  const step = (by: number) => {
    const next = new Date(view.year, view.month + by, 1)
    setView({ year: next.getFullYear(), month: next.getMonth() })
  }

  const selected = value === null ? null : new Date(value)
  const today = new Date()
  const same = (left: Date, right: Date | null) => right !== null && left.toDateString() === right.toDateString()

  return (
    <div class="datetime" ref={root}>
      <Input
        prefix={prefix}
        value={text}
        placeholder={placeholder}
        aria-invalid={text.trim() !== '' && parse(text) === null}
        onFocus={show}
        onClick={show}
        onInput={(event) => setText(event.currentTarget.value)}
        onChange={commit}
      />
      {open && (
        <div class="datetime-popup" role="dialog" aria-label="Choose a date and time">
          <div class="datetime-month">
            <Button aria-label="Previous month" onClick={() => step(-1)}>
              <ChevronLeft size={14} />
            </Button>
            <span>{new Date(view.year, view.month).toLocaleString('en-US', { month: 'long', year: 'numeric' })}</span>
            <Button aria-label="Next month" onClick={() => step(1)}>
              <ChevronRight size={14} />
            </Button>
          </div>
          <div class="datetime-days">
            {WEEKDAYS.map((weekday) => (
              <small key={weekday} class="datetime-weekday">
                {weekday}
              </small>
            ))}
            {month(view.year, view.month).map((day) => (
              <button
                key={day.getTime()}
                type="button"
                class="datetime-day"
                data-kind={day.getMonth() === view.month ? undefined : 'outside'}
                aria-pressed={same(day, selected)}
                aria-current={same(day, today) ? 'date' : undefined}
                onClick={() => pick(day)}
              >
                {day.getDate()}
              </button>
            ))}
          </div>
          <div class="datetime-time">
            <Input prefix="H" type="number" min={0} max={23} value={selected ? selected.getHours() : ''} onChange={(event) => setPart('hours', event.currentTarget.value)} />
            <Input prefix="M" type="number" min={0} max={59} value={selected ? selected.getMinutes() : ''} onChange={(event) => setPart('minutes', event.currentTarget.value)} />
            <Input prefix="S" type="number" min={0} max={59} value={selected ? selected.getSeconds() : ''} onChange={(event) => setPart('seconds', event.currentTarget.value)} />
          </div>
          <Actions>
            {clearable && <Button onClick={() => onChange(null)}>Clear</Button>}
            <Button onClick={() => onChange(Date.now())}>Now</Button>
            <Button kind="primary" onClick={() => setOpen(false)}>
              Done
            </Button>
          </Actions>
        </div>
      )}
    </div>
  )
}
