import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['runner.test.js'],
    fileParallelism: false,
    testTimeout: 120000,
    hookTimeout: 120000,
  },
})
