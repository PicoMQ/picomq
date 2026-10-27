import { expect, test } from '@playwright/test'

import { createTopic, open, produce, unique } from './cluster'

test('a topic is created, found, changed, trimmed and deleted', async ({ page }) => {
  const name = unique('e2e-topic')
  await open(page, '/topics')

  await page.getByRole('button', { name: 'Create topic' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel('Name').fill(name)
  await dialog.getByLabel('Partitions').fill('2')
  await dialog.getByRole('button', { name: 'Create' }).click()
  await expect(page.getByRole('heading', { name: `/${name}` })).toBeVisible()

  await open(page, '/topics')
  await page.getByRole('searchbox').fill(name)
  await expect(page.getByRole('link', { name: `/${name}` })).toBeVisible()
  await expect(page.getByText('1 topic', { exact: true })).toBeVisible()

  await page.getByRole('link', { name: `/${name}` }).click()
  await page.getByRole('link', { name: 'Configuration' }).click()
  await page.getByLabel('Partitions, can only grow').fill('3')
  await page.getByRole('button', { name: 'Save changes' }).click()
  await expect(page.getByText('Saved')).toBeVisible()

  for (const value of ['a', 'b', 'c', 'd', 'e']) {
    await produce(`/${name}`, 0, value)
  }
  await page.getByRole('link', { name: 'Partitions' }).click()
  await expect(page.getByText('3 partitions')).toBeVisible()
  const first = page.getByRole('row').filter({ has: page.getByRole('link', { name: '0', exact: true }) })
  await first.getByRole('button', { name: 'Trim' }).click()
  await page.getByRole('dialog').getByLabel('Delete records before offset').fill('3')
  await page.getByRole('dialog').getByRole('button', { name: 'Trim' }).click()
  await expect(first.getByRole('cell').nth(3)).toHaveText('3')

  await page.getByRole('button', { name: 'Delete topic' }).click()
  await page.getByRole('dialog').getByRole('textbox').fill(`/${name}`)
  await page.getByRole('dialog').getByRole('button', { name: 'Delete topic' }).click()
  await expect(page).toHaveURL(/\/topics$/)
  await page.getByRole('searchbox').fill(name)
  await expect(page.getByText('No topics')).toBeVisible()
})

test('creating a topic that exists with other settings shows the error', async ({ page }) => {
  const name = unique('e2e-twice')
  await createTopic(`/${name}`, 1)
  await open(page, '/topics')

  await page.getByRole('button', { name: 'Create topic' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel('Name').fill(name)
  await dialog.getByLabel('Partitions').fill('4')
  await dialog.getByRole('button', { name: 'Create' }).click()

  await expect(dialog.getByRole('alert')).toBeVisible()
})
