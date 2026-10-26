import { json } from '@codemirror/lang-json'
import { HighlightStyle, foldGutter, syntaxHighlighting } from '@codemirror/language'
import { EditorState } from '@codemirror/state'
import { EditorView, lineNumbers } from '@codemirror/view'
import { tags } from '@lezer/highlight'
import { useEffect, useRef } from 'preact/hooks'

import './viewer.css'

const theme = EditorView.theme(
  {
    '&': {
      color: 'var(--color-text-1)',
      backgroundColor: 'var(--color-surface-1)',
      fontSize: 'var(--text-sm)',
    },
    '.cm-scroller': {
      fontFamily: 'var(--font-mono)',
    },
    '.cm-gutters': {
      color: 'var(--color-text-3)',
      backgroundColor: 'var(--color-surface-1)',
      border: 'none',
    },
    '&.cm-focused': {
      outline: 'none',
    },
    '.cm-selectionBackground, &.cm-focused .cm-selectionBackground': {
      backgroundColor: 'var(--color-surface-3)',
    },
  },
  { dark: true },
)

const highlight = HighlightStyle.define([
  { tag: tags.propertyName, color: 'var(--color-text-2)' },
  { tag: tags.string, color: 'var(--color-text-1)' },
  { tag: [tags.number, tags.bool, tags.null], color: 'var(--color-text-1)', fontWeight: '600' },
  { tag: [tags.punctuation, tags.bracket], color: 'var(--color-text-3)' },
])

export type Language = 'json' | 'text'

interface Props {
  text: string
  language: Language
}

export default function Viewer({ text, language }: Props) {
  const parent = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const view = new EditorView({
      parent: parent.current ?? undefined,
      state: EditorState.create({
        doc: text,
        extensions: [
          EditorState.readOnly.of(true),
          EditorView.lineWrapping,
          lineNumbers(),
          theme,
          ...(language === 'json' ? [json(), foldGutter(), syntaxHighlighting(highlight)] : []),
        ],
      }),
    })

    return () => view.destroy()
  }, [text, language])

  return <div class="viewer" ref={parent} />
}
