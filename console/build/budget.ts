import type { Plugin, Rollup } from 'vite'

import { compress, source } from './gzip.ts'

const KIB = 1024

const LIMITS: Record<string, number> = {
  initial: 90,
  lazy: 40,
  editor: 160,
  chart: 30,
  total: 400,
}

interface Measured {
  name: string
  limit: string
  bytes: number
}

export function budget(): Plugin {
  return {
    name: 'budget',
    apply: 'build',
    enforce: 'post',
    writeBundle(_, bundle) {
      const chunks = Object.values(bundle).filter(
        (output): output is Rollup.OutputChunk => output.type === 'chunk',
      )
      const initial = initialChunks(chunks)
      const lazy = chunks.filter((chunk) => !initial.includes(chunk))

      const measured: Measured[] = [
        { name: 'initial', limit: 'initial', bytes: loaded(bundle, initial) },
        ...lazy.map((chunk) => ({
          name: chunk.fileName,
          limit: chunk.name in LIMITS ? chunk.name : 'lazy',
          bytes: loaded(bundle, [chunk]),
        })),
        { name: 'total', limit: 'total', bytes: loaded(bundle, chunks) },
      ]

      for (const { name, limit, bytes } of measured) {
        const allowed = (LIMITS[limit] ?? 0) * KIB
        const shown = `${(bytes / KIB).toFixed(1)} KiB of ${allowed / KIB} KiB`
        if (bytes > allowed) {
          this.error(`budget: ${name} is ${shown}`)
        }
        this.info(`budget: ${name} ${shown}`)
      }
    },
  }
}

function initialChunks(chunks: Rollup.OutputChunk[]): Rollup.OutputChunk[] {
  const byFile = new Map(chunks.map((chunk) => [chunk.fileName, chunk]))
  const found = new Set<Rollup.OutputChunk>()
  const pending = chunks.filter((chunk) => chunk.isEntry)

  for (let chunk = pending.pop(); chunk; chunk = pending.pop()) {
    if (found.has(chunk)) {
      continue
    }
    found.add(chunk)
    pending.push(...chunk.imports.flatMap((file) => byFile.get(file) ?? []))
  }

  return [...found]
}

function loaded(bundle: Rollup.OutputBundle, chunks: Rollup.OutputChunk[]): number {
  const styles = new Set(chunks.flatMap((chunk) => [...(chunk.viteMetadata?.importedCss ?? [])]))
  const files = [...chunks, ...[...styles].flatMap((file) => bundle[file] ?? [])]

  return files.reduce((sum, output) => sum + compress(source(output)).length, 0)
}
