import { asClientError, ClientError, throwIfAborted } from './error'
import { retryableError, sleep } from './util'
import { RetryPolicy } from './retry'
import type {
  CallOptions,
  GroupMembership,
  JoinOptions,
  MemberFence,
  Offsets,
} from './types'

const DEFAULT_SESSION_TIMEOUT_MS = 30_000

export interface GroupConfig {
  sessionTimeoutMs?: number
  heartbeatIntervalMs?: number
  rebalanceTimeoutMs?: number
  instanceId?: string
  clientId?: string
  retry?: RetryPolicy
  signal?: AbortSignal
}

export interface GroupClient {
  joinGroup(group: string, subscription: string[], options?: JoinOptions): Promise<GroupMembership>
  heartbeat(group: string, fence: MemberFence, options?: CallOptions): Promise<void>
  leaveGroup(
    group: string,
    memberId: string,
    instanceId?: string,
    options?: CallOptions,
  ): Promise<void>
  commitOffsets(
    group: string,
    offsets: Offsets,
    fence?: MemberFence,
    options?: CallOptions,
  ): Promise<void>
  fetchOffsets(group: string, streams?: string[], options?: CallOptions): Promise<Offsets>
}

export interface Assignment {
  generation: number
  streams: string[]
}

interface ResolvedConfig {
  sessionTimeoutMs: number
  heartbeatIntervalMs: number
  rebalanceTimeoutMs?: number
  instanceId?: string
  clientId?: string
  retry: RetryPolicy
}

function resolveConfig(config: GroupConfig): ResolvedConfig {
  const sessionTimeoutMs = config.sessionTimeoutMs ?? DEFAULT_SESSION_TIMEOUT_MS
  const resolved: ResolvedConfig = {
    sessionTimeoutMs,
    heartbeatIntervalMs: Math.max(1, config.heartbeatIntervalMs ?? sessionTimeoutMs / 3),
    retry: config.retry ?? new RetryPolicy(Number.MAX_SAFE_INTEGER, 100, 5_000, 2),
  }

  if (config.rebalanceTimeoutMs !== undefined) resolved.rebalanceTimeoutMs = config.rebalanceTimeoutMs
  if (config.instanceId !== undefined) resolved.instanceId = config.instanceId
  if (config.clientId !== undefined) resolved.clientId = config.clientId

  return resolved
}

function joinOptions(
  config: ResolvedConfig,
  memberId: string | undefined,
  signal: AbortSignal | undefined,
): JoinOptions {
  const options: JoinOptions = { sessionTimeoutMs: config.sessionTimeoutMs }

  if (signal !== undefined) options.signal = signal
  if (memberId !== undefined) options.memberId = memberId
  if (config.instanceId !== undefined) options.instanceId = config.instanceId
  if (config.clientId !== undefined) options.clientId = config.clientId
  if (config.rebalanceTimeoutMs !== undefined) options.rebalanceTimeoutMs = config.rebalanceTimeoutMs

  return options
}

class Watch<T> {
  private version = 0
  private waiters: Array<() => void> = []
  private closed = false

  constructor(private current: T) {}

  get value(): T {
    return this.current
  }

  set(value: T): void {
    this.current = value
    this.version += 1
    this.wake()
  }

  close(): void {
    this.closed = true
    this.wake()
  }

  async *changes(): AsyncGenerator<T> {
    let seen = -1
    for (;;) {
      if (this.version !== seen) {
        seen = this.version
        yield this.current
      }

      if (this.closed) return

      await new Promise<void>((resolve) => this.waiters.push(resolve))
    }
  }

  private wake(): void {
    const waiters = this.waiters
    this.waiters = []
    for (const waiter of waiters) waiter()
  }
}

export class GroupMember {
  private readonly controller = new AbortController()
  private readonly watch: Watch<Assignment>
  private readonly heartbeats: Promise<void>
  private currentMemberId: string
  private generation: number
  private failed: ClientError | undefined

  private constructor(
    private readonly client: GroupClient,
    private readonly group: string,
    private readonly subscription: string[],
    private readonly config: ResolvedConfig,
    joined: GroupMembership,
    signal: AbortSignal | undefined,
  ) {
    this.currentMemberId = joined.memberId
    this.generation = joined.generation
    this.watch = new Watch({ generation: joined.generation, streams: joined.assignment })

    if (signal !== undefined) {
      signal.addEventListener('abort', () => this.controller.abort(signal.reason), { once: true })
    }

    this.heartbeats = this.run()
  }

  static async join(
    client: GroupClient,
    group: string,
    subscription: string[],
    config: GroupConfig = {},
  ): Promise<GroupMember> {
    throwIfAborted(config.signal)
    const resolved = resolveConfig(config)

    const joined = await resolved.retry.run(
      () => client.joinGroup(group, subscription, joinOptions(resolved, undefined, config.signal)),
      retryableError,
      config.signal,
    )

    return new GroupMember(client, group, subscription, resolved, joined, config.signal)
  }

  memberId(): string {
    return this.currentMemberId
  }

  assignment(): Assignment {
    return this.watch.value
  }

  assignments(): AsyncIterable<Assignment> {
    return this.watch.changes()
  }

  error(): ClientError | undefined {
    return this.failed
  }

  async commit(offsets: Offsets, options?: CallOptions): Promise<void> {
    if (this.failed !== undefined) throw this.failed
    await this.client.commitOffsets(this.group, offsets, this.fence(), options)
  }

  async fetchOffsets(streams: string[] = [], options?: CallOptions): Promise<Offsets> {
    return this.client.fetchOffsets(this.group, streams, options)
  }

  async leave(options?: CallOptions): Promise<void> {
    this.controller.abort()
    await this.heartbeats
    this.watch.close()

    if (this.failed !== undefined) return

    await this.client.leaveGroup(this.group, this.currentMemberId, this.config.instanceId, options)
  }

  private fence(): MemberFence {
    const fence: MemberFence = { memberId: this.currentMemberId, generation: this.generation }
    if (this.config.instanceId !== undefined) fence.instanceId = this.config.instanceId
    return fence
  }

  private async run(): Promise<void> {
    const signal = this.controller.signal
    let attempt = 0

    for (;;) {
      try {
        await sleep(this.config.heartbeatIntervalMs, signal)
      } catch {
        return
      }

      const fence = this.fence()
      let joined: GroupMembership

      try {
        await this.client.heartbeat(this.group, fence, { signal })
        attempt = 0
        continue
      } catch (error) {
        const err = asClientError(error)
        if (err.kind === 'aborted') return

        try {
          if (err.code === 'rebalance_in_progress' || err.code === 'illegal_generation') {
            joined = await this.rejoin(fence.memberId, signal)
          } else if (err.code === 'unknown_member') {
            joined = await this.rejoin(undefined, signal)
          } else if (err.retryable()) {
            const delay = this.config.retry.delay(attempt)
            if (delay === null) throw err

            attempt += 1
            await sleep(delay, signal)
            continue
          } else {
            throw err
          }
        } catch (failure) {
          const fatal = asClientError(failure)
          if (fatal.kind === 'aborted') return

          this.failed = fatal
          this.watch.close()
          return
        }
      }

      attempt = 0
      this.currentMemberId = joined.memberId
      this.generation = joined.generation
      this.watch.set({ generation: joined.generation, streams: joined.assignment })
    }
  }

  private async rejoin(
    memberId: string | undefined,
    signal: AbortSignal,
  ): Promise<GroupMembership> {
    let attempt = 0

    for (;;) {
      try {
        return await this.client.joinGroup(
          this.group,
          this.subscription,
          joinOptions(this.config, memberId, signal),
        )
      } catch (error) {
        const err = asClientError(error)

        if (err.code === 'unknown_member') {
          memberId = undefined
          continue
        }

        if (err.retryable()) {
          const delay = this.config.retry.delay(attempt)
          if (delay === null) throw err

          attempt += 1
          await sleep(delay, signal)
          continue
        }

        throw err
      }
    }
  }
}
