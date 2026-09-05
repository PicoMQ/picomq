import {
  addChatMessage,
  addStreamRecord,
  connectSSE,
  createRecents,
  mountShell,
  qs,
  renderMeta,
} from '/lib.js'

const shell = mountShell()
const messagesEl = qs('messages')
const nextLabel = qs('next-label')
const topicInput = qs('topic-input')
const startBtn = qs('start-btn')
const advanceBtn = qs('advance-btn')
const hostInput = qs('host-input')
const hostBtn = qs('host-btn')

let metaState = null
let started = false
let busy = false
let es = null

const recents = createRecents({
  listEl: shell.recentsList,
  storageKey: 'picomq-multi-last-room',
  idKey: 'room',
  defaultTitle: 'New session',
  subtitle: (item) => item.room,
  onSelect: (room) => selectRoom(room),
  onDelete: (room) => deleteRoom(room),
})

async function deleteRoom(room) {
  if (busy) return
  if (!confirm('Delete this session and its Pico streams?')) return
  setBusy(true)
  try {
    const res = await fetch('/api/multi/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ room }),
    })
    const body = await res.json()
    if (!res.ok) throw new Error(body.error || res.statusText)
    if (room === recents.saved()) recents.forget()
    applyState(body)
    await refreshRecents()
  } catch (err) {
    alert(err.message || err)
  }
  setBusy(false)
}

function setStarted(on, next) {
  started = on
  startBtn.disabled = on || busy
  topicInput.disabled = on || busy
  advanceBtn.disabled = !on || busy
  hostInput.disabled = !on || busy
  hostBtn.disabled = !on || busy
  shell.newBtn.disabled = busy
  nextLabel.textContent = on && next ? `next · ${next}` : ''
}

function setBusy(on) {
  busy = on
  setStarted(started, metaState?.nextAgent)
  advanceBtn.textContent = on ? 'Thinking…' : 'Next turn'
}

function setMeta(meta) {
  if (!meta) return
  metaState = { ...metaState, ...meta }
  if (meta.room) recents.remember(meta.room)
  const m = metaState
  const agentLines = (m.agents || []).map((a) => `${a.name}: ${a.stream}`).join(' · ')
  renderMeta(shell.streamInfo, [
    ['endpoint', m.endpoint],
    ['room', m.room],
    ['bus', m.bus],
    ['agents', agentLines || '-'],
    ['protocol', m.protocol],
    ['turns', String(m.turns ?? 0)],
    ['records', String(m.records ?? 0)],
    ['next', m.nextAgent || '-'],
  ])
  if (m.nextAgent) nextLabel.textContent = `next · ${m.nextAgent}`
}

function addMessage(from, content) {
  const isHost = from === 'host'
  addChatMessage(messagesEl, shell.mainBody, {
    role: isHost ? 'user' : 'assistant',
    label: isHost ? 'Host' : from,
    content,
    agent: isHost ? undefined : from.toLowerCase(),
  })
}

function addBusRecord(r) {
  if (r.stream !== 'bus') return
  addStreamRecord(shell.streamRecords, shell.streamBody, r)
}

function clearView() {
  messagesEl.innerHTML = ''
  shell.streamRecords.innerHTML = ''
  shell.streamInfo.innerHTML = ''
}

function applyState(s) {
  clearView()
  setMeta(s.meta)
  for (const m of s.messages || []) addMessage(m.from, m.content)
  for (const r of s.records || []) addBusRecord(r)
  setStarted(!!s.started, s.nextAgent)
}

async function refreshRecents() {
  recents.render(await fetch('/api/multi/recents').then((r) => r.json()))
}

async function selectRoom(room) {
  if (busy || room === recents.active) return
  setBusy(true)
  try {
    const res = await fetch('/api/multi/select', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ room }),
    })
    const body = await res.json()
    if (!res.ok) throw new Error(body.error || res.statusText)
    applyState(body)
    await refreshRecents()
  } catch (err) {
    alert(err.message || err)
  }
  setBusy(false)
}

async function newSession() {
  if (busy) return
  setBusy(true)
  try {
    const res = await fetch('/api/multi/new', { method: 'POST' })
    const body = await res.json()
    if (!res.ok) throw new Error(body.error || res.statusText)
    applyState(body)
    await refreshRecents()
    topicInput.focus()
  } catch (err) {
    alert(err.message || err)
  }
  setBusy(false)
}

async function start() {
  const topic = topicInput.value.trim()
  if (!topic || busy || started) return
  setBusy(true)
  try {
    const res = await fetch('/api/multi/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic }),
    })
    const body = await res.json()
    if (!res.ok) throw new Error(body.error || res.statusText)
    topicInput.value = ''
    setStarted(true, body.nextAgent)
    if (body.meta) setMeta(body.meta)
    await refreshRecents()
  } catch (err) {
    alert(err.message || err)
  }
  setBusy(false)
}

async function advance() {
  if (!started || busy) return
  setBusy(true)
  try {
    const res = await fetch('/api/multi/advance', { method: 'POST' })
    const body = await res.json()
    if (!res.ok) throw new Error(body.error || res.statusText)
    if (body.nextAgent) nextLabel.textContent = `next · ${body.nextAgent}`
  } catch (err) {
    alert(err.message || err)
  }
  setBusy(false)
}

async function hostSend() {
  const text = hostInput.value.trim()
  if (!text || !started || busy) return
  setBusy(true)
  try {
    const res = await fetch('/api/multi/host', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text }),
    })
    const body = await res.json()
    if (!res.ok) throw new Error(body.error || res.statusText)
    hostInput.value = ''
  } catch (err) {
    alert(err.message || err)
  }
  setBusy(false)
}

function connect() {
  if (es) es.close()
  es = connectSSE('/api/multi/events', {
    'host-message': (d) => addMessage('host', d.content),
    'agent-message': (d) => {
      addMessage(d.from, d.content)
      if (d.nextAgent) nextLabel.textContent = `next · ${d.nextAgent}`
    },
    'agent-thinking': (d) => {
      nextLabel.textContent = `thinking · ${d.name}`
    },
    'stream-record': (r) => addBusRecord(r),
    meta: (m) => setMeta(m),
    recents: (p) => recents.render(p),
    started: (d) => {
      setStarted(true, d.nextAgent)
      if (d.meta) setMeta(d.meta)
    },
    'restore-start': () => {
      shell.statusBar.classList.remove('hidden')
      clearView()
      setBusy(true)
    },
    'restore-end': (d) => {
      shell.statusBar.classList.add('hidden')
      if (d?.meta) setMeta(d.meta)
      setBusy(false)
      setStarted(!!d?.started, d?.nextAgent)
    },
  })
}

startBtn.addEventListener('click', start)
advanceBtn.addEventListener('click', advance)
hostBtn.addEventListener('click', hostSend)
shell.newBtn.addEventListener('click', newSession)
shell.restartBtn.addEventListener('click', async () => {
  shell.restartBtn.disabled = true
  try {
    await fetch('/api/multi/restart', { method: 'POST' })
  } catch {}
  shell.restartBtn.disabled = false
})
topicInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault()
    start()
  }
})
hostInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault()
    hostSend()
  }
})

connect()
const list = await fetch('/api/multi/recents').then((r) => r.json())
recents.render(list)
const saved = recents.saved()
if (saved && (list.recents || []).some((r) => r.room === saved) && saved !== list.active) {
  await selectRoom(saved)
} else {
  applyState(await fetch('/api/multi/state').then((r) => r.json()))
}
