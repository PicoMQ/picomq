import { expect, test, type Page } from '@playwright/test'

import { rows } from '../table'
import { SCALE, fakeAdmin } from './fake'

const MOST_ROWS = 60
const MOST_HEAP_BYTES = 256 * 1024 * 1024

async function scrollToEnd(page: Page, total: number, noun: string) {
  const table = page.locator('.table').first()
  const done = `${total.toLocaleString('en-US')} ${noun}s`
  const status = page.getByText(new RegExp(`^${total.toLocaleString('en-US')} ${noun}s$|so far|loading more`))

  for (;;) {
    await table.evaluate((element) => {
      element.scrollTop = element.scrollHeight
    })
    if ((await page.getByText(done, { exact: true }).count()) > 0) {
      return
    }
    await expect(status).toBeVisible()
  }
}

async function heap(page: Page): Promise<number> {
  return page.evaluate(() => (performance as Performance & { memory: { usedJSHeapSize: number } }).memory.usedJSHeapSize)
}

function cursorsInOrder(cursors: (string | null)[]) {
  expect(cursors[0]).toBeNull()
  const numbers = cursors.slice(1).map(Number)
  expect(numbers).toEqual([...numbers].sort((left, right) => left - right))
  expect(new Set(numbers).size).toBe(numbers.length)
}

for (const [area, path, total, noun] of [
  ['topics', '/topics', SCALE.topics, 'topic'],
  ['groups', '/groups', SCALE.groups, 'group'],
  ['tokens', '/tokens', SCALE.tokens, 'token'],
] as const) {
  test(`all ${total.toLocaleString('en-US')} ${area} page through with a bounded table`, async ({ page }) => {
    const fake = await fakeAdmin(page)
    await page.goto(path)
    const started = Date.now()

    await scrollToEnd(page, total, noun)

    expect(Date.now() - started).toBeLessThan(60_000)
    expect(await rows(page).count()).toBeLessThanOrEqual(MOST_ROWS)
    expect(await heap(page)).toBeLessThan(MOST_HEAP_BYTES)
    const cursors = fake.cursors(`/admin/${area}`)
    expect(cursors).toHaveLength(total / 100)
    cursorsInOrder(cursors)
  })
}

test('search asks the server with a prefix and shows its answer as is', async ({ page }) => {
  const fake = await fakeAdmin(page)
  await page.goto('/topics')

  await page.getByRole('searchbox').fill('topic-0001')
  await expect(rows(page).first()).toContainText('/topic-000100')
  await scrollToEnd(page, 100, 'topic')

  expect(fake.prefixes('/admin/topics')).toContain('topic-0001')
})

test('ten thousand partitions of one topic page through', async ({ page }) => {
  const fake = await fakeAdmin(page)
  await page.goto('/topics/big/partitions')

  await scrollToEnd(page, SCALE.partitions, 'partition')

  expect(await rows(page).count()).toBeLessThanOrEqual(MOST_ROWS)
  cursorsInOrder(fake.cursors('/admin/topics/big/partitions'))
})

test('ten thousand members and committed partitions of one group page through', async ({ page }) => {
  await fakeAdmin(page)
  await page.goto('/groups/big')
  await scrollToEnd(page, SCALE.offsets, 'committed partition')
  expect(await rows(page).count()).toBeLessThanOrEqual(MOST_ROWS)

  await page.goto('/groups/big/members')
  await scrollToEnd(page, SCALE.members, 'member')
  expect(await rows(page).count()).toBeLessThanOrEqual(MOST_ROWS)
})

test('a hundred nodes list in full', async ({ page }) => {
  await fakeAdmin(page)
  await page.goto('/nodes')

  await scrollToEnd(page, SCALE.nodes, 'node')
  expect(await rows(page).count()).toBeLessThanOrEqual(MOST_ROWS)
})

test('a million record partition opens on its latest window and pages forward from an offset', async ({ page }) => {
  await fakeAdmin(page)
  await page.goto('/topics/big?partition=0')
  await expect(rows(page).first()).toContainText((SCALE.records - 50).toLocaleString('en-US'))

  await page.goto('/topics/big?partition=0&start=offset&at=0&limit=500')
  const table = page.locator('.table').first()
  for (let round = 0; round < 5; round += 1) {
    await table.evaluate((element) => {
      element.scrollTop = element.scrollHeight
    })
    await page.waitForTimeout(100)
  }
  await expect(page.getByText(/so far|loading more/)).toBeVisible()
  expect(await rows(page).count()).toBeLessThanOrEqual(MOST_ROWS)
})
