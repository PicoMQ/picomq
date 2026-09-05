export type DemoNav = 'chat' | 'agent' | 'multi'

export type DemoPage = {
  nav: DemoNav
  title: string
  brand: string
  newTitle: string
  mainHead: string
  mainBody: string
  composer: string
  streamHead: string
  script: string
}

const NAV: { id: DemoNav; href: string; label: string }[] = [
  { id: 'chat', href: '/chat.html', label: 'Chat' },
  { id: 'agent', href: '/agent.html', label: 'Agent' },
  { id: 'multi', href: '/multi.html', label: 'Multi' },
]

export function renderDemoPage(page: DemoPage): string {
  const nav = NAV.map(
    (n) =>
      `<a${n.id === page.nav ? ' class="active"' : ''} href="${n.href}">${n.label}</a>`,
  ).join('\n        ')

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${page.title}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link
      href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&family=Marcellus&display=swap"
      rel="stylesheet"
    />
    <link rel="stylesheet" href="/styles.css" />
  </head>
  <body>
    <header class="topbar">
      <div class="brand">
        <a class="brand-name" href="/">PicoMQ</a>
        <span class="brand-sep">/</span>
        <span class="brand-title">${page.brand}</span>
      </div>
      <nav class="nav">
        ${nav}
        <button type="button" class="btn btn-ghost" id="restart-btn">Restart</button>
      </nav>
    </header>

    <div class="shell">
      <aside class="recents" id="recents-rail">
        <div class="recents-head">
          <span>Recents</span>
          <button type="button" class="btn-icon" id="new-btn" title="${page.newTitle}" aria-label="${page.newTitle}">+</button>
        </div>
        <div class="recents-list" id="recents-list"></div>
      </aside>

      <div class="layout" id="layout">
        <section class="panel panel-chat" id="main-panel">
          <div class="panel-head">${page.mainHead}</div>
          <div id="status-bar" class="status hidden">Restoring from Pico…</div>
          <div class="panel-body" id="main-body">
            ${page.mainBody}
          </div>
          <div class="composer${page.nav === 'multi' ? ' multi-composer' : ''}">
            ${page.composer}
          </div>
        </section>

        <div
          class="splitter"
          id="splitter"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize panels"
          tabindex="0"
        ></div>

        <aside class="panel panel-stream" id="stream-panel">
          <div class="panel-head">${page.streamHead}</div>
          <div class="stream-info" id="stream-info"></div>
          <div class="panel-body" id="stream-body">
            <div class="stream-records" id="stream-records"></div>
          </div>
        </aside>
      </div>
    </div>

    <script type="module" src="${page.script}"></script>
  </body>
</html>
`
}

export const PAGES: Record<DemoNav, DemoPage> = {
  chat: {
    nav: 'chat',
    title: 'Chat Persistence',
    brand: 'Chat Persistence',
    newTitle: 'New chat',
    mainHead: 'Conversation',
    mainBody: '<div class="messages" id="messages"></div>',
    composer: `<textarea id="msg-input" rows="1" placeholder="Message"></textarea>
            <button type="button" class="btn" id="send-btn">Send</button>`,
    streamHead: 'Pico Stream',
    script: '/pages/chat.js',
  },
  agent: {
    nav: 'agent',
    title: 'Agent Audit Trail',
    brand: 'Agent Audit Trail',
    newTitle: 'New run',
    mainHead: 'Agent Steps',
    mainBody: `<div class="steps" id="steps"></div>
            <div id="summary" class="run-summary hidden"></div>`,
    composer: `<input
              id="prompt-input"
              type="text"
              placeholder="Message"
            />
            <button type="button" class="btn" id="run-btn">Run</button>`,
    streamHead: 'Pico Stream',
    script: '/pages/agent.js',
  },
  multi: {
    nav: 'multi',
    title: 'Multi-agent Persistence',
    brand: 'Multi-agent Persistence',
    newTitle: 'New session',
    mainHead: `Shared bus
          <span id="next-label" class="panel-head-note"></span>`,
    mainBody: '<div class="messages" id="messages"></div>',
    composer: `<input id="topic-input" type="text" placeholder="Topic to discuss" />
            <button type="button" class="btn" id="start-btn">Start</button>
            <button type="button" class="btn btn-ghost" id="advance-btn" disabled>Next turn</button>
            <input id="host-input" type="text" placeholder="Host interjection" disabled />
            <button type="button" class="btn btn-ghost" id="host-btn" disabled>Send</button>`,
    streamHead: 'Bus stream',
    script: '/pages/multi.js',
  },
}
