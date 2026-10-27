import { expect, test } from '@playwright/test'

import { rows } from '../table'
import { createTopic, open, produce, request, unique } from './cluster'

test('a record is produced from the console and shown', async ({ page }) => {
  const name = unique('e2e-produce')
  await createTopic(`/${name}`)
  await open(page, `/topics/${name}`)

  await page.getByRole('button', { name: 'Produce' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel('Key, empty for none').fill('cust-1')
  await dialog.locator('.cm-content').click()
  await page.keyboard.insertText('{"amount":7}')
  await dialog.getByRole('button', { name: 'Add' }).click()
  await dialog.getByPlaceholder('name').fill('trace-id')
  await dialog.getByPlaceholder('value').fill('abc')
  await dialog.getByRole('button', { name: 'Produce' }).click()
  await expect(dialog.getByText('Produced at offset 0')).toBeVisible()

  await dialog.getByRole('button', { name: 'Show' }).click()
  await expect(page.getByRole('heading', { name: /Value/ })).toBeVisible()
  await expect(page.getByText('"amount": 7')).toBeVisible()
  await expect(page.getByRole('term').filter({ hasText: 'trace-id' })).toBeVisible()
})

test('records are filtered and started at an offset or a time', async ({ page }) => {
  const name = unique('e2e-browse')
  await createTopic(`/${name}`)
  await produce(`/${name}`, 0, '1', 'a')
  await produce(`/${name}`, 0, '2', 'b')
  await page.waitForTimeout(1_100)
  const later = await produce(`/${name}`, 0, '3', 'a')
  await produce(`/${name}`, 0, '4', 'b')
  const third = await request<{ timestamp: number }>(
    'GET',
    `/admin/topics/${encodeURIComponent(`/${name}`)}/partitions/0/records/${later.offset}`,
  )
  await open(page, `/topics/${name}`)
  await expect(rows(page)).toHaveCount(4)

  await page.getByRole('button', { name: 'Filter' }).click()
  await page.getByRole('dialog').getByLabel('Key contains').fill('a')
  await page.getByRole('dialog').getByRole('button', { name: 'Apply' }).click()
  await expect(rows(page)).toHaveCount(2)
  await page.getByRole('button', { name: /Remove key contains/ }).click()
  await expect(rows(page)).toHaveCount(4)

  await page.getByLabel('Start').selectOption('offset')
  await page.getByLabel('Offset').fill('1')
  await page.getByLabel('Offset').press('Enter')
  await expect(rows(page)).toHaveCount(3)
  await expect(rows(page).first().getByRole('cell').first()).toHaveText('1')

  await page.getByLabel('Start').selectOption('time')
  await page.getByLabel('From').fill(second(third.timestamp))
  await page.getByLabel('From').press('Enter')
  await expect(rows(page)).toHaveCount(2)
  await expect(rows(page).first().getByRole('cell').first()).toHaveText('2')
})

function second(ms: number): string {
  const at = new Date(ms)
  const pad = (value: number) => String(value).padStart(2, '0')

  return `${at.getFullYear()}-${pad(at.getMonth() + 1)}-${pad(at.getDate())} ${pad(at.getHours())}:${pad(at.getMinutes())}:${pad(at.getSeconds())}`
}

test('live mode shows records as they arrive', async ({ page }) => {
  const name = unique('e2e-live')
  await createTopic(`/${name}`)
  await open(page, `/topics/${name}`)

  await page.getByRole('button', { name: 'Live' }).click()
  await expect(page.getByText(/Live from offset 0/)).toBeVisible()
  await produce(`/${name}`, 0, '{"arrived":true}')

  await expect(page.getByText('{"arrived":true}')).toBeVisible({ timeout: 15_000 })
})
