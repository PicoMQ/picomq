import assert from 'node:assert/strict'
import { test } from 'node:test'
import { readFile } from 'node:fs/promises'
import ts from 'typescript'

// Pure form checks use the dashboard's existing compiler and Node runner.
const source = await readFile(new URL('../src/token-form.ts', import.meta.url), 'utf8')
const { outputText } = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
})
const { buildTokenScope } = await import(`data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}`)
const draft = (overrides = {}) => ({
  streams: [{ id: 1, kind: 'prefix', value: '/orders/' }], tokens: [],
  ops: ['read', 'head', 'list'], audiences: ['pico'], autoPrefixStreams: false, expiry: '',
  ...overrides,
})

test('scope editor preserves exact and prefix resources without adding operations or audiences', () => {
  assert.deepEqual(buildTokenScope(draft({ streams: [
    { id: 1, kind: 'prefix', value: '/orders/' }, { id: 2, kind: 'exact', value: '/one%20two' },
  ], tokens: [{ id: 3, kind: 'exact', value: 'service/reader' }] })), {
    streams: [{ prefix: '/orders/' }, { exact: '/one%20two' }], tokens: [{ exact: 'service/reader' }],
    ops: ['read', 'head', 'list'], audiences: ['pico'], autoPrefixStreams: false, expiresAtMs: null,
  })
})

test('an unfinished matcher cannot accidentally become a grant for all resources', () => {
  for (const kind of ['prefix', 'exact']) {
    assert.throws(() => buildTokenScope(draft({ streams: [{ id: 1, kind, value: '' }] })), /Enter a stream/)
    assert.throws(() => buildTokenScope(draft({ tokens: [{ id: 1, kind, value: '' }] })), /Enter a token ID/)
  }
  const scope = buildTokenScope(draft({ streams: [{ id: 1, kind: 'all', value: 'old draft' }] }))
  assert.deepEqual(scope.streams, [{ prefix: '' }])
  assert.deepEqual(buildTokenScope(draft({ streams: [] })).streams, [])
})

test('auto-prefix requires one prefix and permission sets cannot be empty', () => {
  for (const streams of [[], [{ id: 1, kind: 'exact', value: '/one' }], [
    { id: 1, kind: 'prefix', value: '/a/' }, { id: 2, kind: 'prefix', value: '/b/' },
  ]]) {
    assert.throws(() => buildTokenScope(draft({ streams, autoPrefixStreams: true })), /exactly one/)
  }
  assert.equal(buildTokenScope(draft({ autoPrefixStreams: true })).autoPrefixStreams, true)
  assert.throws(() => buildTokenScope(draft({ audiences: [] })), /audience/)
  assert.throws(() => buildTokenScope(draft({ ops: [] })), /permission/)
})

test('expiry is explicit and future; duplicate selections do not expand grants', () => {
  const now = Date.parse('2030-01-01T00:00:00Z')
  for (const expiry of ['invalid', '2029-12-31T23:59:00Z', '2030-01-01T00:00:00Z']) {
    assert.throws(() => buildTokenScope(draft({ expiry }), now), /future/)
  }
  const scope = buildTokenScope(draft({ expiry: '2030-01-02T00:00:00Z', ops: ['read', 'read'], audiences: ['admin', 'admin'] }), now)
  assert.equal(scope.expiresAtMs, Date.parse('2030-01-02T00:00:00Z'))
  assert.deepEqual(scope.ops, ['read'])
  assert.deepEqual(scope.audiences, ['admin'])
})
