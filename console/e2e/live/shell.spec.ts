import { expect, test } from '@playwright/test'

import { open } from './cluster'

test('the overview shows the cluster and every section loads', async ({ page }) => {
  await open(page, '/')
  await expect(page.getByText('Applied index')).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Nodes' })).toBeVisible()

  const sections = page.getByRole('navigation', { name: 'Sections' })
  for (const section of ['Topics', 'Groups', 'Nodes', 'Transfers', 'Tokens']) {
    await sections.getByRole('link', { name: section, exact: true }).click()
    await expect(page.getByRole('navigation', { name: 'Breadcrumb' })).toHaveText(section)
  }
})

test('a deep link survives a reload', async ({ page }) => {
  await open(page, '/topics?prefix=deep-link')
  await page.reload()

  await expect(page.getByRole('searchbox')).toHaveValue('deep-link')
})

test('the documentation link opens the docs', async ({ page }) => {
  await open(page, '/')

  await expect(page.getByRole('link', { name: 'Documentation' })).toHaveAttribute('href', 'https://picomq.com/docs/')
})
