import type { Page } from '@playwright/test'

export const rows = (page: Page) => page.getByRole('row').filter({ has: page.getByRole('cell') })
