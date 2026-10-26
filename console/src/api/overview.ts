import { read } from './query'

export interface Overview {
  computedAtMs: number
  topicsBySize: { topic: string; storedBytes: number }[]
  topicsByWriteRate: { topic: string; recordsPerSecond: number }[]
}

export const overview = () => read<Overview>(['overview'])
