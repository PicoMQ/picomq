import { defineConfig } from 'vitepress';

const docsSidebar = [
  {
    text: 'Getting started',
    items: [
      { text: 'Introduction', link: '/docs/' },
      { text: 'Quick start', link: '/docs/quick-start' },
      { text: 'Playground', link: '/docs/playground' },
    ],
  },
  {
    text: 'Design',
    items: [
      { text: 'Overview', link: '/docs/design/overview' },
      { text: 'Metadata', link: '/docs/design/metadata' },
      { text: 'Streams', link: '/docs/design/streams' },
      { text: 'Writes', link: '/docs/design/writes' },
      { text: 'Reads', link: '/docs/design/reads' },
      { text: 'Ownership & routing', link: '/docs/design/ownership' },
      { text: 'Transfers', link: '/docs/design/transfers' },
      { text: 'Leases', link: '/docs/design/leases' },
      { text: 'Garbage collection', link: '/docs/design/gc' },
      { text: 'Protocols', link: '/docs/design/protocols' },
      { text: 'Authorization', link: '/docs/design/auth' },
    ],
  },
  {
    text: 'Operations',
    items: [
      { text: 'CLI', link: '/docs/operations/cli' },
      { text: 'Configuration', link: '/docs/operations/configuration' },
      { text: 'Authentication', link: '/docs/operations/auth' },
      {
        text: 'Deployment',
        items: [
          { text: 'Docker', link: '/docs/operations/deployment/docker' },
          { text: 'Fly', link: '/docs/operations/deployment/fly' },
        ],
      },
      { text: 'Admin API & dashboard', link: '/docs/operations/admin' },
      { text: 'Tuning', link: '/docs/operations/tuning' },
    ],
  },
  {
    text: 'API reference',
    items: [{ text: 'HTTP API', link: '/docs/api' }],
  },
];

export default defineConfig({
  title: 'PicoMQ',
  description:
    'PicoMQ is durable, real-time streams over HTTP, built on S3-compatible object storage.',
  base: process.env.BASE_PATH ?? '/',
  cleanUrls: true,
  appearance: false,
  srcDir: 'pages',
  vite: {
    publicDir: 'assets',
    server: {
      fs: {
        allow: ['..'],
      },
    },
  },
  markdown: {
    theme: 'github-light',
  },
  head: [
    ['link', { rel: 'icon', href: '/images/favicon.ico', sizes: 'any' }],
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/images/logo.svg' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    [
      'link',
      { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
    ],
    [
      'link',
      {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&family=Marcellus&family=Playfair+Display:wght@400;500;600&display=swap',
      },
    ],
  ],
  themeConfig: {
    siteTitle: false,
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Docs', link: '/docs' },
      { text: 'GitHub', link: 'https://github.com/picomq/picomq' },
    ],
    sidebar: docsSidebar,
    search: {
      provider: 'local',
    },
    outline: {
      level: [1, 3],
      label: 'On this page',
    },
    footer: {
      copyright: '© 2026 PicoMQ. Apache 2.0.',
    },
  },
});
