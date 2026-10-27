import { writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { gzipSync } from 'node:zlib'
import type { Plugin, Rollup } from 'vite'

const COMPRESSIBLE = /\.(js|css|html|svg|json)$/

export function source(output: Rollup.OutputChunk | Rollup.OutputAsset): string | Uint8Array {
  return output.type === 'chunk' ? output.code : output.source
}

export function compress(data: string | Uint8Array): Uint8Array {
  return gzipSync(data, { level: 9 })
}

export function gzip(): Plugin {
  return {
    name: 'gzip',
    apply: 'build',
    enforce: 'post',
    writeBundle({ dir = '.' }, bundle) {
      const outputs = Object.values(bundle).filter((output) => COMPRESSIBLE.test(output.fileName))

      for (const output of outputs) {
        const compressed = compress(source(output))
        if (compressed.length < Buffer.byteLength(source(output))) {
          writeFileSync(join(dir, `${output.fileName}.gz`), compressed)
        }
      }
    },
  }
}
