import type { RecordData } from './streams'

export interface FieldShape { path: string; types: string[]; samples: number }

// A bounded description of observed payloads, not a generated schema contract.
export function inferShape(records: RecordData[]): { fields: FieldShape[]; parsed: number; limited: boolean } {
  const fields = new Map<string, { types: Set<string>; samples: Set<number> }>()
  let parsed = 0
  let limited = false
  function visit(value: unknown, path: string, sample: number, depth: number) {
    if (depth > 6 || (!fields.has(path) && fields.size >= 200)) { limited = true; return }
    const type = value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value
    const field = fields.get(path) || { types: new Set<string>(), samples: new Set<number>() }
    field.types.add(type)
    field.samples.add(sample)
    fields.set(path, field)
    if (Array.isArray(value)) {
      if (value.length > 20) limited = true
      value.slice(0, 20).forEach((item) => visit(item, `${path}[]`, sample, depth + 1))
    } else if (value !== null && typeof value === 'object') {
      for (const [key, child] of Object.entries(value)) {
        visit(child, `${path}[${JSON.stringify(key)}]`, sample, depth + 1)
        if (fields.size >= 200) { limited = true; break }
      }
    }
  }
  records.forEach((record, index) => {
    if (record.body === undefined || record.previewTruncated) return
    try { const value: unknown = JSON.parse(record.body); parsed++; visit(value, '$', index, 0) } catch { /* Non-JSON payload. */ }
  })
  return { fields: [...fields].map(([path, field]) => ({ path, types: [...field.types].sort(), samples: field.samples.size })), parsed, limited }
}
