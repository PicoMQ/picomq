export function qs(id) {
  return document.getElementById(id)
}

export function esc(s) {
  const d = document.createElement('div')
  d.textContent = String(s ?? '')
  return d.innerHTML
}

export function scrollBottom(el) {
  if (el) el.scrollTop = el.scrollHeight
}

export function bindSplitter(layout, splitter, opts = {}) {
  const streamMin = opts.streamMin ?? 192
  const chatMin = opts.chatMin ?? 280

  function setStreamWidth(px) {
    const max = Math.max(streamMin, layout.clientWidth - chatMin - 1)
    const w = Math.min(max, Math.max(streamMin, px))
    layout.style.setProperty('--stream-width', `${w}px`)
  }

  let dragging = false
  const onMove = (e) => {
    if (!dragging) return
    setStreamWidth(layout.getBoundingClientRect().right - e.clientX)
  }
  const onUp = () => {
    if (!dragging) return
    dragging = false
    document.body.classList.remove('resizing')
  }
  splitter.addEventListener('pointerdown', (e) => {
    if (window.matchMedia('(max-width: 900px)').matches) return
    dragging = true
    document.body.classList.add('resizing')
    splitter.setPointerCapture(e.pointerId)
    e.preventDefault()
  })
  splitter.addEventListener('pointermove', onMove)
  splitter.addEventListener('pointerup', onUp)
  splitter.addEventListener('pointercancel', onUp)
}

export function renderMeta(el, rows) {
  if (!el) return
  el.innerHTML = rows
    .map(
      ([k, v]) =>
        `<div class="meta-row"><span class="meta-key">${esc(k)}</span><span class="meta-val" title="${esc(v)}">${esc(v)}</span></div>`,
    )
    .join('')
}

export function addStreamRecord(recordsEl, bodyEl, { seq, type, preview }) {
  const row = document.createElement('div')
  row.className = 'stream-record'
  row.innerHTML =
    `<span class="stream-seq">${esc(seq)}</span>` +
    `<span class="stream-meta">${esc(type)}${preview ? ' · ' + esc(preview) : ''}</span>`
  recordsEl.appendChild(row)
  scrollBottom(bodyEl)
}

export function addChatMessage(container, scrollEl, opts) {
  const { role, label, content, agent, streaming } = opts
  if (!streaming && !String(content ?? '').trim()) return null
  const wrap = document.createElement('div')
  wrap.className = `message ${role}`
  if (agent) {
    wrap.classList.add('multi-msg')
    wrap.dataset.agent = agent
  }
  const lab = document.createElement('div')
  lab.className = 'message-label'
  lab.textContent = label
  const bubble = document.createElement('div')
  bubble.className = 'message-bubble'
  if (streaming) bubble.classList.add('streaming')
  bubble.textContent = content ?? ''
  wrap.appendChild(lab)
  wrap.appendChild(bubble)
  container.appendChild(wrap)
  scrollBottom(scrollEl)
  return bubble
}

export function createRecents(opts) {
  const {
    listEl,
    storageKey,
    idKey = 'stream',
    defaultTitle = 'Untitled',
    subtitle = (item) => String(item[idKey] ?? '').split('/').pop() || '',
    onSelect,
    onDelete,
  } = opts

  let active = null

  function remember(id) {
    if (!id) return
    active = id
    localStorage.setItem(storageKey, id)
    highlight()
  }

  function forget() {
    active = null
    localStorage.removeItem(storageKey)
    highlight()
  }

  function highlight() {
    for (const el of listEl.querySelectorAll('.recent-item')) {
      el.classList.toggle('active', el.dataset.id === active)
    }
  }

  function render(payload) {
    const items = Array.isArray(payload) ? payload : payload?.recents || []
    const act = Array.isArray(payload) ? undefined : payload?.active
    if (act) active = act
    listEl.innerHTML = ''
    for (const item of items) {
      const id = item[idKey]
      const btn = document.createElement('button')
      btn.type = 'button'
      btn.className = 'recent-item' + (id === active ? ' active' : '')
      btn.dataset.id = id
      btn.innerHTML = `<span class="recent-title"></span><span class="recent-meta"></span>`
      btn.querySelector('.recent-title').textContent = item.title || defaultTitle
      btn.querySelector('.recent-meta').textContent = subtitle(item)
      btn.addEventListener('click', () => onSelect(id, item))
      if (onDelete) {
        const del = document.createElement('span')
        del.className = 'recent-delete'
        del.title = 'Delete'
        del.setAttribute('role', 'button')
        del.innerHTML =
          '<svg viewBox="0 0 16 16" width="12" height="12" fill="none" stroke="currentColor" stroke-width="1.25" stroke-linecap="round">' +
          '<path d="M2.5 4h11M6.5 4V2.75c0-.4.35-.75.75-.75h1.5c.4 0 .75.35.75.75V4M5.5 6.5v6M8 6.5v6M10.5 6.5v6" />' +
          '<path d="M3.5 4l.6 9.2c.04.45.42.8.87.8h6.06c.45 0 .83-.35.87-.8L12.5 4" />' +
          '</svg>'
        del.addEventListener('click', (e) => {
          e.stopPropagation()
          onDelete(id, item)
        })
        btn.appendChild(del)
      }
      listEl.appendChild(btn)
    }
  }

  function saved() {
    return localStorage.getItem(storageKey)
  }

  return {
    get active() {
      return active
    },
    set active(v) {
      active = v
    },
    remember,
    forget,
    highlight,
    render,
    saved,
  }
}

export function connectSSE(url, handlers) {
  const es = new EventSource(url)
  for (const [name, fn] of Object.entries(handlers)) {
    es.addEventListener(name, (e) => {
      let data
      try {
        data = e.data ? JSON.parse(e.data) : undefined
      } catch {
        data = e.data
      }
      fn(data, e)
    })
  }
  return es
}

export function mountShell() {
  const layout = qs('layout')
  const splitter = qs('splitter')
  bindSplitter(layout, splitter)
  return {
    layout,
    splitter,
    mainBody: qs('main-body'),
    streamBody: qs('stream-body'),
    streamInfo: qs('stream-info'),
    streamRecords: qs('stream-records'),
    recentsList: qs('recents-list'),
    statusBar: qs('status-bar'),
    restartBtn: qs('restart-btn'),
    newBtn: qs('new-btn'),
  }
}
