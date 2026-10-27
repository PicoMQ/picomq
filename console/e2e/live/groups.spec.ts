import { GroupMember, connect } from '@picomq/client'
import { expect, test } from '@playwright/test'

import { AUTH, ENDPOINT, TOKEN, createTopic, open, produce, unique } from './cluster'

test('a group shows lag, refuses a reset while it has members, then resets and is deleted', async ({ page }) => {
  const topic = `/${unique('e2e-lag')}`
  const id = unique('e2e-group')
  await createTopic(topic)
  for (const value of ['1', '2', '3', '4', '5']) {
    await produce(topic, 0, value)
  }
  const pico = connect('pico', ENDPOINT, AUTH ? { token: TOKEN } : {})
  const member = await GroupMember.join(pico, id, [topic])
  await member.commit({ [topic]: { position: 2 } })

  await open(page, `/groups/${encodeURIComponent(id)}`)
  const row = page.getByRole('row').filter({ has: page.getByRole('link', { name: topic }) })
  await expect(row.getByRole('cell').nth(2)).toHaveText('2')
  await expect(row.getByRole('cell').nth(4)).toHaveText('3')

  await page.getByRole('button', { name: 'Reset offsets' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel('Topic').fill(topic)
  await dialog.getByRole('button', { name: 'Reset' }).click()
  await expect(dialog.getByRole('alert')).toContainText('active members')

  await member.leave()
  await dialog.getByRole('button', { name: 'Reset' }).click()
  await expect(dialog.getByText('Reset 1 partition')).toBeVisible()
  await dialog.getByRole('button', { name: 'Cancel' }).click()
  await page.getByRole('button', { name: 'Refresh' }).click()
  await expect(row.getByRole('cell').nth(4)).toHaveText('5')

  await page.getByLabel('Topic').fill(topic)
  await page.waitForTimeout(11_000)
  await page.getByLabel('Topic').press('Enter')
  await page.getByRole('button', { name: 'Delete offsets' }).click()
  await page.getByRole('dialog').getByRole('textbox').fill(topic)
  await page.getByRole('dialog').getByRole('button', { name: 'Delete offsets' }).click()
  await expect(page.getByText('No committed partitions')).toBeVisible()

  await page.getByRole('button', { name: 'Delete group' }).click()
  await page.getByRole('dialog').getByRole('textbox').fill(id)
  await page.getByRole('dialog').getByRole('button', { name: 'Delete group' }).click()
  await expect(page).toHaveURL(/\/groups$/)
})
