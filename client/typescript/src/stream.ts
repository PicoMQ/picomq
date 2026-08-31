import { Producer, type ProducerConfig } from './producer'
import type { PicoClient } from './pico/client'
import type {
  AppendAck,
  AppendInput,
  AppendOptions,
  CallOptions,
  Live,
  ProducerAck,
  ProducerRef,
  ReadLimits,
  ReadPage,
  RecordsOptions,
  SseEvent,
  StreamApi,
  StreamInfo,
  StreamRecord,
  SubscribeOptions,
} from './types'

export class Stream {
  constructor(
    protected readonly api: StreamApi,
    readonly name: string,
  ) {}

  create(contentType: string, ttlSeconds?: number, options?: CallOptions): Promise<boolean> {
    return this.api.create(this.name, contentType, ttlSeconds, options)
  }

  head(options?: CallOptions): Promise<StreamInfo | null> {
    return this.api.head(this.name, options)
  }

  append(records: AppendInput[], options?: AppendOptions): Promise<AppendAck> {
    return this.api.append(this.name, records, options)
  }

  read(from: string, live: Live, limits?: ReadLimits, options?: CallOptions): Promise<ReadPage> {
    return this.api.read(this.name, from, live, limits, options)
  }

  subscribe(from: string, options?: SubscribeOptions): AsyncIterable<SseEvent> {
    return this.api.subscribe(this.name, from, options)
  }

  close(options?: CallOptions): Promise<string> {
    return this.api.close(this.name, options)
  }

  delete(options?: CallOptions): Promise<boolean> {
    return this.api.delete(this.name, options)
  }

  async *records(
    from?: string,
    options: RecordsOptions = {},
  ): AsyncGenerator<StreamRecord, void, undefined> {
    let offset = from ?? this.api.beginning()
    const live = options.live ?? false
    const callOptions: CallOptions = {}
    if (options.signal !== undefined) {
      callOptions.signal = options.signal
    }
    for (;;) {
      const page = await this.api.read(
        this.name,
        offset,
        live ? 'long-poll' : 'off',
        options.batch ?? {},
        callOptions,
      )
      yield* page.records
      const advanced = page.next !== offset
      offset = page.next
      if (page.closed) return
      if (!live && page.upToDate) return
      if (!live && !advanced && page.records.length === 0) return
    }
  }
}

export class PicoStream extends Stream {
  constructor(
    private readonly client: PicoClient,
    name: string,
  ) {
    super(client, name)
  }

  appendAs(
    records: AppendInput[],
    producer: ProducerRef,
    options?: CallOptions,
  ): Promise<ProducerAck> {
    return this.client.appendAs(this.name, records, producer, options)
  }

  trim(seq: number, options?: CallOptions): Promise<string> {
    return this.client.trim(this.name, seq, options)
  }

  producer(id: string, config?: ProducerConfig): Producer {
    return new Producer(this.client, this.name, id, config)
  }
}
