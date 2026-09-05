import {
  addChatMessage,
  addStreamRecord,
  connectSSE,
  createRecents,
  esc,
  mountShell,
  qs,
  renderMeta,
  scrollBottom,
} from '/lib.js'

const shell = mountShell()
const stepsEl = qs('steps')
const summaryEl = qs('summary')
const promptIn = qs('prompt-input')
const runBtn = qs('run-btn')

let metaState = null
let es = null
let running = false

const recents = createRecents({
  listEl: shell.recentsList,
  storageKey: 'picomq-agent-last-stream',
  idKey: 'stream',
  defaultTitle: 'Agent run',
  onSelect: (stream) => selectRun(stream),
  onDelete: (stream) => deleteRun(stream),
})

async function deleteRun(stream) {
  if (running) return
  if (!confirm('Delete this run and its Pico stream?')) return
  try {
    const res = await fetch('/api/agent/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stream }),
    })
    const payload = await res.json()
    if (!res.ok) throw new Error(payload.error || res.statusText)
    if (stream === recents.active || stream === recents.saved()) {
      recents.forget()
      clearView()
    }
    recents.render(payload.recents)
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
    ['done', m.done ? 'true' : 'false'],
  ])
}

function setRunning(on) {
  running = on
  runBtn.disabled = on
  runBtn.textContent = on ? 'Running…' : 'Run'
  promptIn.disabled = on
  shell.newBtn.disabled = on
}

function clearView() {
  stepsEl.innerHTML = ''
  shell.streamRecords.innerHTML = ''
  summaryEl.className = 'run-summary hidden'
  shell.streamInfo.innerHTML = ''
  metaState = null
}

function addStep(data) {
  const hasCalls = data.toolCalls && data.toolCalls.length > 0
  const kind = hasCalls ? 'tool_use' : 'text'
  const step = document.createElement('div')
  step.className = 'step'
  let html =
    `<div class="step-head"><span class="step-index">Step ${data.index}</span>` +
    `<span class="step-kind">${kind}</span></div>`
  if (hasCalls) {
    for (let i = 0; i < data.toolCalls.length; i++) {
      const tc = data.toolCalls[i]
      const tr = data.toolResults?.[i]
      html += `<div class="tool-call">`
      html += `<div class="tool-name">${esc(tc.tool)}</div>`
      html += `<div class="tool-args">${esc(JSON.stringify(tc.args))}</div>`
      if (tr) html += `<div class="tool-result">${esc(JSON.stringify(tr.result))}</div>`
      html += `</div>`
    }
  }
  if (data.text) html += `<div class="step-text">${esc(data.text)}</div>`
  step.innerHTML = html
  stepsEl.appendChild(step)
  scrollBottom(shell.mainBody)
}

function markDone() {
  stepsEl.querySelectorAll('.step').forEach((s) => s.classList.add('done'))
}

function showSummary(data) {
  if (data.text) {
    addChatMessage(stepsEl, shell.mainBody, {
      role: 'assistant',
      label: 'Assistant',
      content: data.text,
    })
  }
  summaryEl.className = 'run-summary'
  summaryEl.textContent = `${data.steps} steps · ${(data.totalTokens ?? 0).toLocaleString()} tokens`
  scrollBottom(shell.mainBody)
}

async function applyEvents(events, paced) {
  for (const ev of events) {
    if (ev.type === 'run_start') {
      addChatMessage(stepsEl, shell.mainBody, {
        role: 'user',
        label: 'You',
        content: ev.prompt,
      })
    } else if (ev.type === 'step') {
      addStep(ev)
      if (paced) await new Promise((r) => setTimeout(r, 60))
    } else if (ev.type === 'run_end') {
      markDone()
      showSummary(ev)
    }
  }
}

async function loadStream(stream, paced) {
  shell.statusBar.classList.remove('hidden')
  try {
    const res = await fetch(`/api/agent/history?stream=${encodeURIComponent(stream)}`)
    if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText)
    const payload = await res.json()
    recents.remember(stream)
    clearView()
    setMeta(payload.meta)
    for (const r of payload.records) addStreamRecord(shell.streamRecords, shell.streamBody, r)
    await applyEvents(payload.events, paced)
  } finally {
    shell.statusBar.classList.add('hidden')
  }
}

async function refreshRecents() {
  const data = await fetch('/api/agent/recents').then((r) => r.json())
  recents.render(data.recents)
}

async function selectRun(stream) {
  if (running || stream === recents.active) return
  if (es) {
    es.close()
    es = null
  }
  try {
    await loadStream(stream, false)
    await refreshRecents()
  } catch (err) {
    summaryEl.className = 'run-summary error'
    summaryEl.textContent = err.message || String(err)
  }
}

function newRun() {
  if (running) return
  if (es) {
    es.close()
    es = null
  }
  recents.forget()
  clearView()
  promptIn.focus()
}

async function restart() {
  if (running) return
  const stream = recents.active || recents.saved()
  if (!stream) return
  shell.restartBtn.disabled = true
  if (es) {
    es.close()
    es = null
  }
  try {
    await loadStream(stream, true)
  } catch (err) {
    summaryEl.className = 'run-summary error'
    summaryEl.textContent = err.message || String(err)
  }
  shell.restartBtn.disabled = false
}

async function runAgent() {
  const prompt = promptIn.value.trim()
  if (!prompt || running) return

  const continueStream = recents.active
  setRunning(true)
  if (!continueStream) clearView()
  summaryEl.className = 'run-summary hidden'
  addChatMessage(stepsEl, shell.mainBody, {
    role: 'user',
    label: 'You',
    content: prompt,
  })
  promptIn.value = ''

  try {
    const res = await fetch('/api/agent/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt, stream: continueStream || undefined }),
    })
    const body = await res.json()
    if (!res.ok) throw new Error(body.error || res.statusText)

    if (body.meta?.stream) recents.remember(body.meta.stream)
    setMeta(body.meta)
    await refreshRecents()
    if (es) es.close()
    es = connectSSE(`/api/agent/events?run=${body.runId}`, {
      step: (d) => addStep(d),
      'stream-record': (r) => addStreamRecord(shell.streamRecords, shell.streamBody, r),
      meta: (m) => setMeta(m),
      'run-end': (d) => {
        markDone()
        showSummary(d)
        setRunning(false)
        es.close()
        refreshRecents()
      },
      'run-error': (err) => {
        summaryEl.className = 'run-summary error'
        summaryEl.textContent = err.error || 'Run failed'
        setRunning(false)
        es.close()
      },
    })
    es.addEventListener('error', () => {
      if (running) {
        setRunning(false)
        summaryEl.className = 'run-summary error'
        summaryEl.textContent = 'Connection lost'
      }
    })
  } catch (err) {
    summaryEl.className = 'run-summary error'
    summaryEl.textContent = err.message || String(err)
    setRunning(false)
  }
}

runBtn.addEventListener('click', runAgent)
shell.newBtn.addEventListener('click', newRun)
shell.restartBtn.addEventListener('click', restart)
promptIn.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault()
    runAgent()
  }
})

await refreshRecents()
const saved = recents.saved()
if (saved) {
  try {
    await loadStream(saved, false)
  } catch {}
  await refreshRecents()
}
