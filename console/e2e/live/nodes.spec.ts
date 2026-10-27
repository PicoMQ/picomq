import { expect, test } from '@playwright/test'

import { createTopic, open, produce, request, unique } from './cluster'

test('a node page charts the records written while it is open', async ({ page }) => {
  const topic = `/${unique('e2e-rates')}`
  await createTopic(topic)
  await produce(topic, 0, 'x')
  const owner = await request<{ ownerNodeId: number }>('GET', `/admin/topics/${encodeURIComponent(topic)}/partitions/0`)
  await open(page, `/nodes/${owner.ownerNodeId}`)
  await expect(page.getByRole('heading', { name: `node ${owner.ownerNodeId}` })).toBeVisible()

  const until = Date.now() + 8_000
  while (Date.now() < until) {
    await produce(topic, 0, 'x')
  }

  await expect(page.getByText(/^in: [1-9]/)).toBeVisible({ timeout: 10_000 })
})
