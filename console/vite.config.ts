import preact from '@preact/preset-vite'
import { defineConfig } from 'vitest/config'

import { budget } from './build/budget.ts'
import { gzip } from './build/gzip.ts'

export default defineConfig({
  plugins: [preact(), gzip(), budget()],
  build: {
    outDir: '../picomq/pico-http/_console',
    emptyOutDir: true,
    reportCompressedSize: false,
  },
  server: {
    proxy: {
      '/admin': 'http://127.0.0.1:9090',
    },
  },
  test: {
    include: ['test/**/*.test.ts'],
  },
})
