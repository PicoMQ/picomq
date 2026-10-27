import { expect, test } from '@playwright/test'

import { clusterOnly, createTopic, open, produce, request, unique } from './cluster'

test.beforeEach(clusterOnly)

test('a partition is moved to another node', async ({ page }) => {
  const topic = `/${unique('e2e-move')}`
  await createTopic(topic)
  await produce(topic, 0, 'x')
  const path = `/admin/topics/${encodeURIComponent(topic)}/partitions/0`
  const before = await request<{ ownerNodeId: number }>('GET', path)
  const target = before.ownerNodeId === 1 ? 2 : 1
  await open(page, '/transfers')

  await page.getByRole('button', { name: 'Move partition' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel('Topic').fill(topic)
  await dialog.getByLabel('Partition, empty for a single-partition topic').fill('0')
  await dialog.getByLabel('To node').selectOption(String(target))
  await dialog.getByRole('button', { name: 'Move' }).click()

  await expect.poll(async () => (await request<{ ownerNodeId: number }>('GET', path)).ownerNodeId, { timeout: 30_000 }).toBe(target)
  await open(page, `/topics/${topic.slice(1)}/partitions`)
  await expect(page.getByRole('link', { name: `node ${target}` })).toBeVisible()
})
