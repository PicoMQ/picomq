import { json } from '@codemirror/lang-json'
import { HighlightStyle, foldGutter, syntaxHighlighting } from '@codemirror/language'
import { EditorState } from '@codemirror/state'
import { EditorView, lineNumbers } from '@codemirror/view'
import { tags } from '@lezer/highlight'
import { useEffect, useRef } from 'preact/hooks'

import './editor.css'

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
)

const editing = EditorView.theme({
  '.cm-content, .cm-gutter': {
    minHeight: '10rem',
  },
})

const highlight = HighlightStyle.define([
  { tag: tags.propertyName, color: 'var(--color-text-2)' },
  { tag: tags.string, color: 'var(--color-text-1)' },
  { tag: [tags.punctuation, tags.bracket], color: 'var(--color-text-3)' },
])

export type Language = 'json' | 'text'

interface Props {
  text: string
  language: Language
  onChange?: (text: string) => void
}

export default function Editor({ text, language, onChange }: Props) {
  const parent = useRef<HTMLDivElement>(null)
  const view = useRef<EditorView | null>(null)
  const changed = useRef(onChange)
  changed.current = onChange
  const editable = onChange !== undefined

  useEffect(() => {
    const created = new EditorView({
      parent: parent.current ?? undefined,
      state: EditorState.create({
        doc: text,
        extensions: [
          EditorState.readOnly.of(!editable),
          EditorView.lineWrapping,
          EditorView.updateListener.of((update) => {
            if (update.docChanged) {
              changed.current?.(update.state.doc.toString())
            }
          }),
          lineNumbers(),
          theme,
          ...(editable ? [editing] : []),
          ...(language === 'json' ? [json(), foldGutter(), syntaxHighlighting(highlight)] : []),
        ],
      }),
    })
    view.current = created

    return () => {
      created.destroy()
      view.current = null
    }
  }, [language, editable])

  useEffect(() => {
    const current = view.current
    if (current && current.state.doc.toString() !== text) {
      current.dispatch({ changes: { from: 0, to: current.state.doc.length, insert: text } })
    }
  }, [text])

  return <div class="editor" ref={parent} />
}
