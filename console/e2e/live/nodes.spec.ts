import { expect, test } from '@playwright/test'

import { createTopic, open, produce, request, unique } from './cluster'

test('a node page charts the records written while it is open', async ({ page }) => {
  const topic = `/${unique('e2e-rates')}`
  await createTopic(topic)
  const nodes = await request<{ items: { nodeId: number; local: boolean }[] }>('GET', '/admin/nodes')
  const local = nodes.items.find((node) => node.local)!
  await open(page, `/nodes/${local.nodeId}`)
  await expect(page.getByRole('heading', { name: `node ${local.nodeId}` })).toBeVisible()

  const until = Date.now() + 8_000
  while (Date.now() < until) {
    await produce(topic, 0, 'x')
  }

  await expect(page.getByText(/^in: [1-9]/)).toBeVisible({ timeout: 10_000 })
})
