import { test, type Page } from '@playwright/test'

export const ADMIN = process.env.PICO_ADMIN ?? 'http://127.0.0.1:9090'
export const ADMIN_2 = process.env.PICO_ADMIN_2
export const ENDPOINT = process.env.PICO_ENDPOINT ?? 'http://127.0.0.1:4437'
export const AUTH = process.env.PICO_AUTH === 'required'
export const TOKEN = process.env.PICO_TOKEN ?? 'ZGV2L3Jvb3Q.BwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSY'

export function unique(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`
}

export async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const response = await fetch(`${ADMIN}${path}`, {
    method,
    headers: {
      ...(AUTH ? { Authorization: `Bearer ${TOKEN}` } : {}),
      ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!response.ok) {
    throw new Error(`${method} ${path}: ${response.status} ${await response.text()}`)
  }
  return (response.status === 204 ? undefined : await response.json()) as T
}

export async function createTopic(name: string, partitions = 1, contentType = 'application/json') {
  await request('POST', '/admin/topics', { name, partitions, contentType })
}

export async function produce(topic: string, partition: number, value: string, key?: string) {
  return request<{ offset: number }>('POST', `/admin/topics/${encodeURIComponent(topic)}/partitions/${partition}/records`, {
    key: key === undefined ? null : { data: key, encoding: 'utf8' },
    value: { data: value, encoding: 'utf8' },
  })
}

export async function open(page: Page, path: string) {
  if (AUTH) {
    await page.addInitScript((token) => sessionStorage.setItem('picomq.token', token), TOKEN)
  }
  await page.goto(path)
}

export const clusterOnly = () => test.skip(ADMIN_2 === undefined, 'needs a second node')

export const authOnly = () => test.skip(!AUTH, 'needs auth required')
