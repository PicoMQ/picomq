import { defineConfig } from '@playwright/test'

import { ADMIN } from './live/cluster'

export default defineConfig({
  testDir: 'live',
  workers: 1,
  timeout: 60_000,
  reporter: 'list',
  use: {
    baseURL: ADMIN,
  },
})
