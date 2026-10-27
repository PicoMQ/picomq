import { expect, test } from '@playwright/test'

import { TOKEN, authOnly, request, unique } from './cluster'

test.beforeEach(authOnly)

test('a token is asked for, a wrong one is refused, and sign out locks again', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('heading', { name: 'PicoMQ Console' })).toBeVisible()

  await page.getByLabel('Token').fill('svc/nobody.not-a-secret')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('alert')).toContainText('That token was not accepted')

  await page.getByLabel('Token').fill(TOKEN)
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByText('Applied index')).toBeVisible()

  await page.getByRole('button', { name: 'Sign out' }).click()
  await expect(page.getByRole('heading', { name: 'PicoMQ Console' })).toBeVisible()
})

test('a page the token may not read shows why in place', async ({ page }) => {
  const id = unique('svc/console-lister')
  const issued = await request<{ token: string }>('POST', '/admin/tokens', {
    id,
    scope: { streams: [{ prefix: '' }], ops: ['list'], audiences: ['admin'] },
  })

  await page.goto('/')
  await page.getByLabel('Token').fill(issued.token)
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByRole('alert').first()).toBeVisible()
  await expect(page.getByRole('navigation', { name: 'Sections' })).toBeVisible()

  await request('DELETE', `/admin/tokens/${encodeURIComponent(id)}`)
})
