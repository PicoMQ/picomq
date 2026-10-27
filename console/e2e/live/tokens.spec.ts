import { expect, test } from '@playwright/test'

import { open, unique } from './cluster'

test('a token is issued with a scope, shown once, listed and revoked', async ({ page }) => {
  const id = unique('svc/e2e')
  await open(page, '/tokens')

  await page.getByRole('button', { name: 'Issue token' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel('Token id').fill(id)
  await dialog.getByText('Pico', { exact: true }).click()
  await dialog.getByRole('group', { name: 'Access' }).getByText('read').first().click()
  await dialog.getByRole('group', { name: 'Streams' }).getByRole('button', { name: 'Add' }).click()
  await dialog.getByRole('group', { name: 'Streams' }).getByRole('textbox').fill('/orders')
  await dialog.getByRole('button', { name: 'Issue', exact: true }).click()
  await expect(dialog.getByText(`${id} is ready`)).toBeVisible()
  await expect(dialog.getByRole('textbox')).toHaveValue(/\S{20,}/)
  await dialog.getByRole('button', { name: 'Done' }).click()

  await page.getByRole('searchbox').fill(id)
  const row = page.getByRole('row').filter({ hasText: id })
  await expect(row).toContainText('stream read')
  await row.getByRole('button', { name: 'Revoke' }).click()
  await page.getByRole('dialog').getByRole('textbox').fill(id)
  await page.getByRole('dialog').getByRole('button', { name: 'Revoke token' }).click()
  await expect(page.getByText('No tokens')).toBeVisible()
})
