import { defineConfig } from '@playwright/test'

const PORT = 4173

export default defineConfig({
  testDir: 'scale',
  workers: 1,
  timeout: 120_000,
  reporter: 'list',
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
  },
  webServer: {
    command: `npx vite preview --host 127.0.0.1 --port ${PORT} --strictPort`,
    url: `http://127.0.0.1:${PORT}`,
    cwd: '..',
    reuseExistingServer: true,
  },
})
