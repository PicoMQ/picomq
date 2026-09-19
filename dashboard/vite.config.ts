import { defineConfig } from 'vite'
import preact from '@preact/preset-vite'

export default defineConfig({
  plugins: [preact()],
  base: './',
  build: {
    outDir: '../picomq/pico-http/_dashboard',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      // Development-only forwarding to the existing protocol listener.
      // No routes are added to the PicoMQ server.
      '^/pico(?:/|$)': {
        target: process.env.PICO_DASHBOARD_STREAM_TARGET || 'http://127.0.0.1:4437',
        rewrite: (path) => path.replace(/^\/pico/, ''),
      },
      '/admin': 'http://127.0.0.1:9090',
      '/ready': 'http://127.0.0.1:9090',
      '/health': 'http://127.0.0.1:9090',
    },
  },
})
