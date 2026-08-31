import {
  addChatMessage,
  addStreamRecord,
  connectSSE,
  createRecents,
  mountShell,
  qs,
  renderMeta,
  scrollBottom,
} from '/lib.js'

const shell = mountShell()
const messagesEl = qs('messages')
const msgInput = qs('msg-input')
const sendBtn = qs('send-btn')

let metaState = null
let sending = false
let es = null
let bubble = null

const recents = createRecents({
  listEl: shell.recentsList,
  storageKey: 'picomq-chat-last-stream',
  idKey: 'stream',
  defaultTitle: 'New chat',
  onSelect: (stream) => selectChat(stream),
  onDelete: (stream) => deleteChat(stream),
})

async function deleteChat(stream) {
  if (sending) return
  if (!confirm('Delete this chat and its Pico stream?')) return
  try {
    const res = await fetch('/api/chat/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stream }),
    })
    const payload = await res.json()
    if (!res.ok) throw new Error(payload.error || res.statusText)
    if (stream === recents.saved()) recents.forget()
    applyHistory(payload)
    await refreshRecents()
  } catch (err) {
    alert(err.message || err)
  }
}

function setMeta(meta) {
  if (!meta) return
  metaState = { ...metaState, ...meta }
  if (meta.stream) recents.remember(meta.stream)
  const m = metaState
  renderMeta(shell.streamInfo, [
    ['endpoint', m.endpoint],
    ['stream', m.stream],
    ['protocol', m.protocol],
    ['content-type', m.contentType],
    ['producer', m.producerId],
    ['records', String(m.records ?? 0)],
    ['last-seq', m.lastSeq == null ? '-' : String(m.lastSeq)],
  ])
}

function setInputEnabled(on) {
  msgInput.disabled = !on
  sendBtn.disabled = !on
  shell.newBtn.disabled = !on
}

function clearView() {
  messagesEl.innerHTML = ''
  shell.streamRecords.innerHTML = ''
}

function applyHistory(payload) {
  clearView()
  setMeta(payload.meta)
  for (const m of payload.messages || []) {
    addChatMessage(messagesEl, shell.mainBody, {
      role: m.role,
      label: m.role === 'user' ? 'You' : 'Assistant',
      content: m.content,
    })
  }
  for (const r of payload.records || []) {
    addStreamRecord(shell.streamRecords, shell.streamBody, r)
  }
}

async function refreshRecents() {
  recents.render(await fetch('/api/chat/recents').then((r) => r.json()))
}

async function selectChat(stream) {
  if (sending || stream === recents.active) return
  setInputEnabled(false)
  try {
    const res = await fetch('/api/chat/select', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stream }),
    })
    if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText)
  } catch (err) {
    shell.statusBar.classList.add('hidden')
    setInputEnabled(true)
    alert(err.message || err)
  }
}

async function newChat() {
  if (sending) return
  setInputEnabled(false)
  try {
    const res = await fetch('/api/chat/new', { method: 'POST' })
    const payload = await res.json()
    if (!res.ok) throw new Error(payload.error || res.statusText)
    applyHistory(payload)
    await refreshRecents()
    setInputEnabled(true)
    msgInput.focus()
  } catch (err) {
    setInputEnabled(true)
    alert(err.message || err)
  }
}

async function sendMessage() {
  const text = msgInput.value.trim()
  if (!text || sending) return
  msgInput.value = ''
  addChatMessage(messagesEl, shell.mainBody, {
    role: 'user',
    label: 'You',
    content: text,
  })
  try {
    const res = await fetch('/api/chat/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text }),
    })
    if (!res.ok) {
      const err = await res.json().catch(() => ({}))
      throw new Error(err.error || res.statusText)
    }
  } catch (err) {
    addChatMessage(messagesEl, shell.mainBody, {
      role: 'assistant',
      label: 'Assistant',
      content: `Error: ${err.message || err}`,
    })
    setInputEnabled(true)
    sending = false
  }
}

function connect() {
  if (es) es.close()
  es = connectSSE('/api/chat/events', {
    'assistant-start': () => {
      sending = true
      setInputEnabled(false)
      bubble = addChatMessage(messagesEl, shell.mainBody, {
        role: 'assistant',
        label: 'Assistant',
        content: '',
        streaming: true,
      })
    },
    'assistant-chunk': (chunk) => {
      if (bubble) {
        bubble.textContent += chunk
        scrollBottom(shell.mainBody)
      }
    },
    'assistant-end': () => {
      if (bubble) {
        bubble.classList.remove('streaming')
        if (!bubble.textContent.trim()) bubble.parentElement?.remove()
        bubble = null
      }
      sending = false
      setInputEnabled(true)
      msgInput.focus()
      refreshRecents()
    },
    'stream-record': (r) => addStreamRecord(shell.streamRecords, shell.streamBody, r),
    meta: (m) => setMeta(m),
    recents: (p) => recents.render(p),
    'restore-start': () => {
      shell.statusBar.classList.remove('hidden')
      clearView()
      setInputEnabled(false)
    },
    'restore-message': (m) =>
      addChatMessage(messagesEl, shell.mainBody, {
        role: m.role,
        label: m.role === 'user' ? 'You' : 'Assistant',
        content: m.content,
      }),
    'restore-record': (r) => addStreamRecord(shell.streamRecords, shell.streamBody, r),
    'restore-end': () => {
      shell.statusBar.classList.add('hidden')
      setInputEnabled(true)
      msgInput.focus()
      refreshRecents()
    },
  })
}

sendBtn.addEventListener('click', sendMessage)
shell.newBtn.addEventListener('click', newChat)
shell.restartBtn.addEventListener('click', async () => {
  shell.restartBtn.disabled = true
  try {
    await fetch('/api/chat/restart', { method: 'POST' })
  } catch {}
  shell.restartBtn.disabled = false
})
msgInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault()
    sendMessage()
  }
})

connect()
const saved = recents.saved()
const list = await fetch('/api/chat/recents').then((r) => r.json())
recents.render(list)
if (saved && (list.recents || []).some((r) => r.stream === saved) && saved !== list.active) {
  await selectChat(saved)
} else {
  applyHistory(await fetch('/api/chat/history').then((r) => r.json()))
}
