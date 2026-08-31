import { asClientError, ClientError } from './error'
import { parseSafeSeq, sleep } from './util'
import { toEnvelope, type RecordEnvelope } from './record'
import { RetryPolicy } from './retry'
import type { AppendInput, CallOptions, ProducerAck, ProducerRef } from './types'

export interface ProducerConfig {
  epoch?: number
  lingerMs?: number
  maxBatchRecords?: number
  maxBatchBytes?: number
  maxInflight?: number
  maxBufferedBytes?: number
  retry?: RetryPolicy
}

export interface ProducerClient {
  appendAs(
    name: string,
    records: AppendInput[],
    producer: ProducerRef,
    options?: CallOptions,
  ): Promise<ProducerAck>
}

interface ResolvedConfig {
  epoch: number
  lingerMs: number
  maxBatchRecords: number
  maxBatchBytes: number
  maxInflight: number
  maxBufferedBytes: number
  retry: RetryPolicy
}

function resolveConfig(config: ProducerConfig = {}): ResolvedConfig {
  return {
    epoch: config.epoch ?? 0,
    lingerMs: config.lingerMs ?? 5,
    maxBatchRecords: config.maxBatchRecords ?? 500,
    maxBatchBytes: config.maxBatchBytes ?? 1024 * 1024,
    maxInflight: Math.max(1, config.maxInflight ?? 1),
    maxBufferedBytes: config.maxBufferedBytes ?? 32 * 1024 * 1024,
    retry: config.retry ?? new RetryPolicy(12, 1, 100, 2),
  }
}

interface Item {
  envelope: RecordEnvelope
  resolve: (seq: number) => void
  reject: (error: ClientError) => void
  release: () => void
}

export class Pending {
  constructor(private readonly promise: Promise<number>) {
    promise.catch(() => undefined)
  }

  durable(): Promise<number> {
    return this.promise
  }
}

export class Producer {
  private readonly items: Item[] = []
  private readonly waiters: Array<(item: Item | null) => void> = []
  private readonly budget: Semaphore
  private readonly config: ResolvedConfig
  private readonly runner: Promise<void>
  private closed = false
  private poisoned = false

  constructor(client: ProducerClient, name: string, id: string, config: ProducerConfig = {}) {
    this.config = resolveConfig(config)
    this.budget = new Semaphore(this.config.maxBufferedBytes)
    this.runner = this.run(client, name, id)
  }

  async send(record: AppendInput): Promise<Pending> {
    const envelope = toEnvelope(record)
    if (envelope.body.byteLength > this.config.maxBufferedBytes) {
      throw new ClientError(
        'bad_request',
        `record of ${envelope.body.byteLength} bytes exceeds the session's ${this.config.maxBufferedBytes} byte budget`,
        { code: 'record_too_large' },
      )
    }
    this.ensureOpen()
    const release = await this.budget.acquire(Math.max(1, envelope.body.byteLength))
    let settle!: (seq: number) => void
    let fail!: (error: ClientError) => void
    const promise = new Promise<number>((resolve, reject) => {
      settle = resolve
      fail = reject
    })
    this.push({
      envelope,
      resolve: settle,
      reject: fail,
      release,
    })
    return new Pending(promise)
  }

  async sendDurable(record: AppendInput): Promise<number> {
    return (await this.send(record)).durable()
  }

  async flush(): Promise<void> {
    const release = await this.budget.acquire(this.config.maxBufferedBytes)
    release()
    this.ensureNotPoisoned()
  }

  async close(): Promise<void> {
    this.closed = true
    try {
      const release = await this.budget.acquire(this.config.maxBufferedBytes)
      release()
    } finally {
      while (this.waiters.length > 0) {
        this.waiters.shift()?.(null)
      }
      await this.runner
    }
    this.ensureNotPoisoned()
  }

  private ensureOpen(): void {
    this.ensureNotPoisoned()
    if (this.closed) {
      throw stopped('producer is closed')
    }
  }

  private ensureNotPoisoned(): void {
    if (this.poisoned) {
      throw new ClientError(
        'conflict',
        'producer session failed and cannot continue its sequence; open a new session (a higher epoch restarts at sequence 0)',
        { code: 'producer_poisoned' },
      )
    }
  }

  private push(item: Item): void {
    const waiter = this.waiters.shift()
    if (waiter) {
      waiter(item)
      return
    }
    this.items.push(item)
  }

  private recv(timeoutMs: number | null): Promise<Item | null> {
    const next = this.items.shift()
    if (next) return Promise.resolve(next)
    if (this.closed) return Promise.resolve(null)
    return new Promise((resolve) => {
      let done = false
      const finish = (item: Item | null) => {
        if (done) return
        done = true
        resolve(item)
      }
      this.waiters.push(finish)
      if (timeoutMs !== null) {
        setTimeout(() => {
          const idx = this.waiters.indexOf(finish)
          if (idx >= 0) this.waiters.splice(idx, 1)
          finish(null)
        }, Math.max(0, timeoutMs))
      }
    })
  }

  private tryRecv(): Item | null {
    return this.items.shift() ?? null
  }

  private async run(client: ProducerClient, name: string, id: string): Promise<void> {
    const inflight = new Semaphore(this.config.maxInflight)
    let seq = 0
    for (;;) {
      const first = await this.recv(null)
      if (first === null) return
      const batch = await this.collect(first)
      const batchSeq = seq
      seq += 1
      if (batchSeq === 0) {
        await this.sendBatch(client, name, id, batch, batchSeq)
        continue
      }
      const release = await inflight.acquire(1)
      void this.sendBatch(client, name, id, batch, batchSeq).finally(release)
    }
  }

  private async collect(first: Item): Promise<Item[]> {
    let bytes = first.envelope.body.byteLength
    const batch = [first]
    if (this.config.lingerMs === 0) {
      while (batch.length < this.config.maxBatchRecords && bytes < this.config.maxBatchBytes) {
        const item = this.tryRecv()
        if (!item) break
        bytes += item.envelope.body.byteLength
        batch.push(item)
      }
      return batch
    }
    const deadline = Date.now() + this.config.lingerMs
    while (batch.length < this.config.maxBatchRecords && bytes < this.config.maxBatchBytes) {
      const remaining = deadline - Date.now()
      if (remaining <= 0) break
      const item = await this.recv(remaining)
      if (!item) break
      bytes += item.envelope.body.byteLength
      batch.push(item)
    }
    return batch
  }

  private async sendBatch(
    client: ProducerClient,
    name: string,
    id: string,
    batch: Item[],
    seq: number,
  ): Promise<void> {
    try {
      const start = await this.appendWithRetries(
        client,
        name,
        batch.map((item) => item.envelope),
        id,
        seq,
      )
      for (let i = 0; i < batch.length; i++) {
        const item = batch[i]!
        item.resolve(start + i)
        item.release()
      }
    } catch (error) {
      this.poisoned = true
      const err = asClientError(error)
      for (const item of batch) {
        item.reject(err)
        item.release()
      }
    }
  }

  private async appendWithRetries(
    client: ProducerClient,
    name: string,
    records: RecordEnvelope[],
    id: string,
    seq: number,
  ): Promise<number> {
    let attempt = 0
    for (;;) {
      try {
        const ack = await client.appendAs(name, records, {
          id,
          epoch: this.config.epoch,
          seq,
        })
        if (ack.duplicate) {
          return parseSafeSeq(ack.ack.next, 'next') - records.length
        }
        return parseSafeSeq(ack.ack.start, 'start')
      } catch (error) {
        const err = asClientError(error)
        const delay = this.config.retry.delay(attempt)
        if (delay !== null && (err.code === 'sequence_gap' || err.retryable())) {
          await sleep(delay)
          attempt += 1
          continue
        }
        throw err
      }
    }
  }
}

class Semaphore {
  private available: number
  private readonly queue: Array<{ n: number; resolve: () => void }> = []

  constructor(private readonly capacity: number) {
    this.available = capacity
  }

  async acquire(n: number): Promise<() => void> {
    if (n > this.capacity) {
      throw new ClientError('bad_request', 'acquire exceeds semaphore capacity', {
        code: 'bad_request',
      })
    }
    await new Promise<void>((resolve) => {
      const tryTake = () => {
        if (this.available >= n) {
          this.available -= n
          resolve()
          return true
        }
        return false
      }
      if (!tryTake()) {
        this.queue.push({
          n,
          resolve: () => {
            this.available -= n
            resolve()
          },
        })
      }
    })
    return () => {
      this.available += n
      this.pump()
    }
  }

  private pump(): void {
    while (this.queue.length > 0) {
      const next = this.queue[0]!
      if (this.available < next.n) break
      this.queue.shift()
      next.resolve()
    }
  }
}

function stopped(message: string): ClientError {
  return new ClientError('other', message, { code: 'producer_stopped' })
}
