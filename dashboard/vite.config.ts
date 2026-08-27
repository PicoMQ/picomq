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
      '/admin': 'http://127.0.0.1:9090',
      '/ready': 'http://127.0.0.1:9090',
      '/health': 'http://127.0.0.1:9090',
    },
  },
})
