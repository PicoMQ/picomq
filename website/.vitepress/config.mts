import { defineConfig } from 'vitepress';
import { tabsMarkdownPlugin } from 'vitepress-plugin-tabs';

const docsSidebar = [
  {
    text: 'Getting started',
    collapsed: false,
    items: [
      { text: 'Introduction', link: '/docs/' },
      { text: 'Quick start', link: '/docs/quick-start' },
      { text: 'Playground', link: '/docs/playground' },
    ],
  },
  {
    text: 'FAQ',
    collapsed: true,
    items: [{ text: 'Why not Kafka?', link: '/docs/faq/why-not-kafka' }],
  },
  {
    text: 'Design',
    collapsed: true,
    items: [
      { text: 'Overview', link: '/docs/design/overview' },
      { text: 'Interactive cluster', link: '/docs/design/simulator' },
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
    text: 'Connectors',
    collapsed: true,
    items: [
      { text: 'Overview', link: '/docs/connectors/' },
      { text: 'First connector', link: '/docs/connectors/first-connector' },
      { text: 'Sources', link: '/docs/connectors/sources' },
      { text: 'Sinks', link: '/docs/connectors/sinks' },
      { text: 'Routing & templating', link: '/docs/connectors/routing' },
      { text: 'Delivery guarantees', link: '/docs/connectors/delivery' },
      { text: 'Transforms', link: '/docs/connectors/transforms' },
      { text: 'Writing a plugin', link: '/docs/connectors/plugin-sdk' },
      {
        text: 'Source catalog',
        collapsed: true,
        items: [
          { text: 'Postgres', link: '/docs/connectors/sources/postgres' },
          { text: 'Elasticsearch', link: '/docs/connectors/sources/elasticsearch' },
          { text: 'InfluxDB', link: '/docs/connectors/sources/influxdb' },
          { text: 'Random', link: '/docs/connectors/sources/random' },
        ],
      },
      {
        text: 'Sink catalog',
        collapsed: true,
        items: [
          { text: 'Postgres', link: '/docs/connectors/sinks/postgres' },
          { text: 'ClickHouse', link: '/docs/connectors/sinks/clickhouse' },
          { text: 'Elasticsearch', link: '/docs/connectors/sinks/elasticsearch' },
          { text: 'Quickwit', link: '/docs/connectors/sinks/quickwit' },
          { text: 'MongoDB', link: '/docs/connectors/sinks/mongodb' },
          { text: 'Meilisearch', link: '/docs/connectors/sinks/meilisearch' },
          { text: 'SurrealDB', link: '/docs/connectors/sinks/surrealdb' },
          { text: 'InfluxDB', link: '/docs/connectors/sinks/influxdb' },
          { text: 'S3', link: '/docs/connectors/sinks/s3' },
          { text: 'HTTP', link: '/docs/connectors/sinks/http' },
          { text: 'stdout', link: '/docs/connectors/sinks/stdout' },
          { text: 'Doris', link: '/docs/connectors/sinks/doris' },
          { text: 'Iceberg', link: '/docs/connectors/sinks/iceberg' },
          { text: 'Delta Lake', link: '/docs/connectors/sinks/delta' },
          { text: 'Redshift', link: '/docs/connectors/sinks/redshift' },
        ],
      },
    ],
  },
  {
    text: 'Operations',
    collapsed: true,
    items: [
      { text: 'CLI', link: '/docs/operations/cli' },
      { text: 'Configuration', link: '/docs/operations/configuration' },
      { text: 'Authentication', link: '/docs/operations/auth' },
      {
        text: 'Deployment',
        collapsed: true,
        items: [
          { text: 'Docker', link: '/docs/operations/deployment/docker' },
          { text: 'Fly', link: '/docs/operations/deployment/fly' },
          { text: 'AWS', link: '/docs/operations/deployment/aws' },
        ],
      },
      { text: 'Admin API & dashboard', link: '/docs/operations/admin' },
      { text: 'Connectors runtime', link: '/docs/operations/connectors' },
      { text: 'Tuning', link: '/docs/operations/tuning' },
    ],
  },
  {
    text: 'API reference',
    collapsed: true,
    items: [
      { text: 'HTTP API', link: '/docs/api' },
      { text: 'Kafka protocol', link: '/docs/kafka' },
      { text: 'Schemas', link: '/docs/schemas' },
      { text: 'Protocol facades', link: '/docs/extending' },
      {
        text: 'Clients',
        collapsed: true,
        items: [
          { text: 'Go', link: '/docs/client/go' },
          { text: 'Rust', link: '/docs/client/rust' },
          { text: 'TypeScript', link: '/docs/client/typescript' },
        ],
      },
    ],
  },
  {
    text: 'Community',
    collapsed: true,
    items: [
      { text: 'Contribute', link: '/docs/contribute' },
      { text: 'Discord', link: 'https://discord.gg/qsMy5sSpYX' },
      {
        text: 'Hacker News',
        link: 'https://news.ycombinator.com/item?id=49421806',
      },
    ],
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
    config(md) {
      md.use(tabsMarkdownPlugin);
    },
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
      { text: 'Docs', link: '/docs' },
      { text: 'Contribute', link: '/docs/contribute' },
      { text: 'Discord', link: 'https://discord.gg/qsMy5sSpYX' },
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
