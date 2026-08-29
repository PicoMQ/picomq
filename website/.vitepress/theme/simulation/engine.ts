// The simulation engine. Every byte shown in the UI is produced by these
// encoders, which mirror the real Rust codecs field for field:
//
// - metadata commands and log-row batches: picomq/pico-metadata/src/codec.rs
//   (little-endian, version byte 0, type byte, fields in declaration order)
// - record batches: s3stream/s3stream-codec (big-endian, magic 0x22)
// - WAL framing: 24-byte header + CRC-32/ISO-HDLC masked to 31 bits
//
// The state machine follows docs/design/metadata.md: propose -> flusher
// (group commit) -> log row -> tailer -> apply -> published view.

import { Seg, SegWriter, walCrc32, hexU32, segsBytes } from './bytes';

export const CODEC_VERSION = 0;
export const MAGIC_V0 = 0x22;
export const WAL_DATA_MAGIC = 0x87654321;

// Real thresholds (shown in the UI) vs. the compressed ones the sim uses so
// you can trigger everything in a few clicks.
export const REAL = {
  snapshotEvery: 1024,
  snapshotMinIntervalS: 30,
  compactAtObjects: 64,
  groupCommitMax: 256,
  walWindowMs: 5,
  bytesPerStream: 2.2 * 1024,
};
export const SIM = {
  snapshotEvery: 10,
  compactAtObjects: 4,
  flushAtRecords: 3,
  commitAtWalObjects: 2,
};

// ---------------------------------------------------------------------------

export type CommandName =
  | 'RegisterNode'
  | 'CreateStream'
  | 'OpenStream'
  | 'CloseStream'
  | 'PlaceStream'
  | 'PutKv'
  | 'DeleteKv'
  | 'PrepareObject'
  | 'CommitStreamSetObject'
  | 'CompactStreamObject'
  | 'CleanDestroyedObjects'
  | 'TransferStream'
  | 'CompleteTransfer';

export const TYPE_CODES: Record<CommandName, number> = {
  CreateStream: 1,
  OpenStream: 2,
  CloseStream: 4,
  PrepareObject: 6,
  CommitStreamSetObject: 7,
  CompactStreamObject: 8,
  RegisterNode: 10,
  CleanDestroyedObjects: 11,
  PutKv: 12,
  DeleteKv: 14,
  TransferStream: 15,
  CompleteTransfer: 16,
  PlaceStream: 18,
};

export interface Command {
  name: CommandName;
  /** Display-friendly summary of the fields. */
  summary: string;
  /** Field name -> value used to build the encoding. */
  args: Record<string, unknown>;
  segs: Seg[];
}

function body(name: CommandName, build: (w: SegWriter) => void): Seg[] {
  const w = new SegWriter();
  w.u8(`type = ${name}`, TYPE_CODES[name], 'type');
  build(w);
  return w.segs;
}

export const enc = {
  registerNode(nodeId: number, epoch: number, http: string, slots: number): Command {
    return {
      name: 'RegisterNode',
      summary: `node ${nodeId} epoch ${epoch}, ${slots} slots`,
      args: { nodeId, epoch, http, slots },
      segs: body('RegisterNode', (w) => {
        w.i32le('node_id', nodeId);
        w.i64le('node_epoch', epoch);
        w.strLe('http_address', http);
        w.u32le('slots', slots);
        w.u32le('protocol_addresses len', 0, 'len');
      }),
    };
  },
  createStream(nodeId: number, epoch: number): Command {
    return {
      name: 'CreateStream',
      summary: `by node ${nodeId}`,
      args: { nodeId, epoch },
      segs: body('CreateStream', (w) => {
        w.i32le('node_id', nodeId);
        w.i64le('node_epoch', epoch);
      }),
    };
  },
  placeStream(streamId: number): Command {
    return {
      name: 'PlaceStream',
      summary: `stream ${streamId} → least-loaded node`,
      args: { streamId },
      segs: body('PlaceStream', (w) => w.u64le('stream_id', streamId)),
    };
  },
  openStream(nodeId: number, nodeEpoch: number, streamId: number, epoch: number): Command {
    return {
      name: 'OpenStream',
      summary: `stream ${streamId} on node ${nodeId} at epoch ${epoch}`,
      args: { nodeId, nodeEpoch, streamId, epoch },
      segs: body('OpenStream', (w) => {
        w.i32le('node_id', nodeId);
        w.i64le('node_epoch', nodeEpoch);
        w.u64le('stream_id', streamId);
        w.i64le('epoch', epoch);
      }),
    };
  },
  putKv(key: string, value: Uint8Array, display: string): Command {
    return {
      name: 'PutKv',
      summary: `${key} = ${display}`,
      args: { key, display },
      segs: body('PutKv', (w) => {
        w.strLe('key', key);
        w.blobLe('value', value, display);
      }),
    };
  },
  deleteKv(key: string): Command {
    return {
      name: 'DeleteKv',
      summary: key,
      args: { key },
      segs: body('DeleteKv', (w) => w.strLe('key', key)),
    };
  },
  prepareObject(nodeId: number, epoch: number, count: number, nowMs: number): Command {
    return {
      name: 'PrepareObject',
      summary: `reserve ${count} object id${count > 1 ? 's' : ''}`,
      args: { nodeId, epoch, count },
      segs: body('PrepareObject', (w) => {
        w.i32le('node_id', nodeId);
        w.i64le('node_epoch', epoch);
        w.u32le('count', count, 'count');
        w.i64le('ttl_ms', 60_000);
        w.i64le('now_ms', nowMs);
      }),
    };
  },
  commitStreamSetObject(
    nodeId: number,
    epoch: number,
    objectId: number,
    objectSize: number,
    ranges: { streamId: number; epoch: number; start: number; end: number; size: number }[],
    nowMs: number,
  ): Command {
    return {
      name: 'CommitStreamSetObject',
      summary: `object ${objectId} covers ${ranges
        .map((r) => `stream ${r.streamId} [${r.start}, ${r.end})`)
        .join(', ')}`,
      args: { nodeId, objectId, objectSize, ranges },
      segs: body('CommitStreamSetObject', (w) => {
        w.i32le('node_id', nodeId);
        w.i64le('node_epoch', epoch);
        w.i64le('now_ms', nowMs);
        w.u64le('object_id', objectId);
        w.u64le('object_size', objectSize);
        w.u32le('attributes', 0);
        w.u32le('stream_ranges len', ranges.length, 'len');
        for (const range of ranges) {
          w.u64le('  stream_id', range.streamId);
          w.u64le('  epoch', range.epoch);
          w.u64le('  start_offset', range.start, 'offset');
          w.u64le('  end_offset', range.end, 'offset');
          w.u64le('  size', range.size);
        }
        w.u32le('stream_objects len', 0, 'len');
        w.u32le('compacted_object_ids len', 0, 'len');
      }),
    };
  },
  compactStreamObject(
    nodeId: number,
    epoch: number,
    objectId: number,
    objectSize: number,
    streamId: number,
    streamEpoch: number,
    start: number,
    end: number,
    sources: number[],
    nowMs: number,
  ): Command {
    return {
      name: 'CompactStreamObject',
      summary: `stream ${streamId}: ${sources.length} objects → object ${objectId}`,
      args: { nodeId, objectId, streamId, sources },
      segs: body('CompactStreamObject', (w) => {
        w.i32le('node_id', nodeId);
        w.i64le('node_epoch', epoch);
        w.i64le('now_ms', nowMs);
        w.u64le('object_id', objectId);
        w.u64le('object_size', objectSize);
        w.u64le('stream_id', streamId);
        w.u64le('stream_epoch', streamEpoch);
        w.u64le('start_offset', start, 'offset');
        w.u64le('end_offset', end, 'offset');
        w.u32le('attributes', 0);
        w.u32le('source_object_ids len', sources.length, 'len');
        for (const id of sources) w.u64le('  source id', id);
        w.u32le('operations len', sources.length, 'len');
        for (const _ of sources) w.u8('  op = Delete', 0, 'type');
      }),
    };
  },
  cleanDestroyedObjects(ids: number[]): Command {
    return {
      name: 'CleanDestroyedObjects',
      summary: `objects ${ids.join(', ')} removed from storage`,
      args: { ids },
      segs: body('CleanDestroyedObjects', (w) => {
        w.u32le('object_ids len', ids.length, 'len');
        for (const id of ids) w.u64le('  object id', id);
      }),
    };
  },
  transferStream(streamId: number, from: number, to: number): Command {
    return {
      name: 'TransferStream',
      summary: `stream ${streamId}: node ${from} → node ${to}`,
      args: { streamId, from, to },
      segs: body('TransferStream', (w) => {
        w.u64le('stream_id', streamId);
        w.i32le('from_node', from);
        w.i32le('to_node', to);
      }),
    };
  },
  completeTransfer(streamId: number, epoch: number): Command {
    return {
      name: 'CompleteTransfer',
      summary: `stream ${streamId} now at epoch ${epoch}`,
      args: { streamId, epoch },
      segs: body('CompleteTransfer', (w) => {
        w.u64le('stream_id', streamId);
        w.i64le('epoch', epoch);
      }),
    };
  },
};

/** One replicated-log row: version byte + u32 count + command bodies. */
export function encodeBatchRow(commands: Command[]): Seg[] {
  const w = new SegWriter();
  w.u8(`codec version`, CODEC_VERSION, 'version');
  w.u32le('command count', commands.length, 'count');
  for (const c of commands) w.append(c.segs);
  return w.segs;
}

// ---------------------------------------------------------------------------
// Data plane: record batch (BE, magic 0x22) and WAL framing (24-byte header).

export interface EncodedBatch {
  streamId: number;
  epoch: number;
  baseOffset: number;
  count: number;
  payload: Uint8Array;
  segs: Seg[];
  bytes: Uint8Array;
}

export function encodeRecordBatch(
  streamId: number,
  epoch: number,
  baseOffset: number,
  count: number,
  payload: Uint8Array,
  payloadDisplay: string,
): EncodedBatch {
  const w = new SegWriter();
  w.u8('magic', MAGIC_V0, 'magic');
  w.u64be('stream_id', streamId);
  w.u64be('epoch', epoch);
  w.u64be('base_offset', baseOffset, 'offset');
  w.i32be('last_offset_delta (count)', count, 'count');
  w.u32be('payload length', payload.length, 'len');
  w.raw('payload', payload, 'payload', payloadDisplay);
  return { streamId, epoch, baseOffset, count, payload, segs: w.segs, bytes: w.bytes() };
}

export interface WalFrame {
  segs: Seg[];
  bytes: Uint8Array;
  bodyCrc: number;
  headerCrc: number;
}

export function frameWalRecord(offset: number, batch: EncodedBatch): WalFrame {
  const bodyCrc = walCrc32(batch.bytes);
  const head = new SegWriter();
  head.u32be('magic (data)', WAL_DATA_MAGIC, 'magic');
  head.u32be('body length', batch.bytes.length, 'len');
  head.u64be('body offset', offset + 24, 'offset');
  head.u32be(`body CRC = ${hexU32(bodyCrc)}`, bodyCrc, 'crc');
  const headerCrc = walCrc32(head.bytes());
  head.u32be(`header CRC = ${hexU32(headerCrc)}`, headerCrc, 'crc');
  const w = new SegWriter();
  w.append(head.segs);
  w.append(batch.segs);
  return { segs: w.segs, bytes: w.bytes(), bodyCrc, headerCrc };
}

// ---------------------------------------------------------------------------
// Cluster state.

export interface StreamRow {
  id: number;
  name: string;
  epoch: number;
  startOffset: number;
  endOffset: number;
  localEnd: number;
  durableEnd: number;
  state: 'opened' | 'closed';
  nodeId: number;
}

export interface WalBuffered {
  nodeId: number;
  streamId: number;
  batch: EncodedBatch;
}

export interface SealedWal {
  objectId: number;
  nodeId: number;
  key: string;
  size: number;
  segs: Seg[];
  ranges: { streamId: number; epoch: number; start: number; end: number; size: number }[];
}

export interface ObjectRow {
  objectId: number;
  streamId: number;
  start: number;
  end: number;
  size: number;
  kind: 'stream-set' | 'compacted';
}

export interface NodeMemory {
  appliedIndex: number;
  /** Entries retained in the log cache (recently appended batches). */
  logCache: { streamId: number; baseOffset: number; count: number; size: number }[];
  /** Per-map entry counts of the published im::OrdMap view. */
  viewMaps: Record<string, number>;
}

export interface SimNode {
  id: number;
  epoch: number;
  status: 'off' | 'booting' | 'running' | 'dead' | 'restoring';
  slots: number;
  leaseHolder: boolean;
  memory: NodeMemory;
  /** Transient animation flags. */
  glow: '' | 'propose' | 'apply';
}

export interface LogRow {
  idx: number;
  commands: Command[];
  segs: Seg[];
  bytes: Uint8Array;
}

export interface S3Object {
  key: string;
  objectId: number;
  kind: 'wal' | 'stream-set' | 'compacted';
  size: number;
  segs: Seg[];
  note: string;
  streamId: number;
  range: [number, number];
  nodeId?: number;
}

export interface TimelineEvent {
  seq: number;
  at: string;
  category: 'metadata' | 'data' | 'lifecycle' | 'storage';
  text: string;
}

export interface SimState {
  booted: boolean;
  busy: boolean;
  speed: number;
  nodes: SimNode[];
  streams: StreamRow[];
  objects: ObjectRow[];
  destroyedFifo: { objectId: number; seq: number }[];
  cleanerSeq: number;
  kv: { key: string; display: string }[];
  prepared: { objectId: number; owner: number }[];
  log: LogRow[];
  snapshot: { appliedIdx: number; size: number; takenAt: string } | null;
  lease: { holder: string; ttlMs: number } | null;
  s3: S3Object[];
  timeline: TimelineEvent[];
  nextStreamId: number;
  nextObjectId: number;
  nextLogIdx: number;
  rowsSinceSnapshot: number;
  walBuffer: WalBuffered[];
  sealedWal: SealedWal[];
  stage: '' | 'propose' | 'flusher' | 'logrow' | 'tailer' | 'view' | 'wal' | 's3put';
  seq: number;
}

export interface DueTask {
  id: 'flush' | 'commit' | 'snapshot' | 'compact' | 'gc';
  label: string;
  detail: string;
  real: string;
  count: string;
  due: boolean;
  ready: boolean;
}

export function initialState(): SimState {
  return {
    booted: false,
    busy: false,
    speed: 1,
    nodes: [],
    streams: [],
    objects: [],
    destroyedFifo: [],
    cleanerSeq: 0,
    kv: [],
    prepared: [],
    log: [],
    snapshot: null,
    lease: null,
    s3: [],
    timeline: [],
    nextStreamId: 1,
    nextObjectId: 1,
    nextLogIdx: 1,
    rowsSinceSnapshot: 0,
    walBuffer: [],
    sealedWal: [],
    stage: '',
    seq: 0,
  };
}

const te = new TextEncoder();

export class Sim {
  constructor(public s: SimState) {}

  private clock = 0;

  now(): string {
    this.clock += 1;
    const m = Math.floor(this.clock / 60);
    const sec = this.clock % 60;
    return `${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')}`;
  }

  log(category: TimelineEvent['category'], text: string) {
    this.s.timeline.unshift({ seq: ++this.s.seq, at: this.now(), category, text });
    if (this.s.timeline.length > 200) this.s.timeline.pop();
  }

  sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms / this.s.speed));
  }

  private nodeMemory(): NodeMemory {
    return { appliedIndex: 0, logCache: [], viewMaps: {} };
  }

  private refreshViewMaps() {
    const streams = this.s.streams.length;
    const opened = this.s.streams.filter((x) => x.state === 'opened').length;
    const maps: Record<string, number> = {
      streams: streams,
      nodes: this.s.nodes.filter((n) => n.status !== 'off').length,
      opening_by_node: opened,
      placed_by_node: streams,
      stream_set_objects: this.s.objects.filter((o) => o.kind === 'stream-set').length,
      sso_ranges: this.s.objects.filter((o) => o.kind === 'stream-set').length,
      stream_objects: this.s.objects.filter((o) => o.kind === 'compacted').length,
      prepared: this.s.prepared.length,
      mark_destroyed: this.s.destroyedFifo.length,
      kv: this.s.kv.length,
    };
    for (const n of this.s.nodes) {
      if (n.status === 'running') n.memory.viewMaps = { ...maps };
    }
  }

  /** The full metadata write path for a batch of commands, animated. */
  async propose(proposer: number, commands: Command[], apply: () => void) {
    const node = this.s.nodes.find((n) => n.id === proposer);
    if (node) node.glow = 'propose';
    this.s.stage = 'propose';
    this.log(
      'metadata',
      `node ${proposer} proposes ${commands.map((c) => c.name).join(' + ')}: queued for the flusher`,
    );
    await this.sleep(450);

    this.s.stage = 'flusher';
    this.log(
      'metadata',
      `flusher drains the queue: ${commands.length} command${commands.length > 1 ? 's' : ''} packed into one row (group commit, up to ${REAL.groupCommitMax}/row)`,
    );
    await this.sleep(450);

    const segs = encodeBatchRow(commands);
    const bytes = segsBytes(segs);
    const idx = this.s.nextLogIdx++;
    this.s.stage = 'logrow';
    this.s.log.push({ idx, commands, segs, bytes });
    this.s.rowsSinceSnapshot += 1;
    this.log(
      'metadata',
      `INSERT INTO meta_log (idx, payload) VALUES (${idx}, x'...'), ${bytes.length} bytes. Uniqueness on idx is the only coordination`,
    );
    await this.sleep(500);

    this.s.stage = 'tailer';
    this.log('metadata', `every node's tailer fetches row ${idx} and applies it to its in-memory state`);
    await this.sleep(450);

    apply();
    this.s.stage = 'view';
    for (const n of this.s.nodes) {
      if (n.status === 'running') {
        n.memory.appliedIndex = idx;
        n.glow = 'apply';
      }
    }
    this.refreshViewMaps();
    this.log(
      'metadata',
      `view published at applied index ${idx}: an O(1) fork of the persistent maps, readers never lock`,
    );
    await this.sleep(450);

    this.s.stage = '';
    for (const n of this.s.nodes) n.glow = '';

  }

  async boot(nodeCount: number) {
    const s = this.s;
    s.busy = true;
    s.lease = null;
    this.log('lifecycle', `cluster coming up. Postgres and the object store are already there. Nodes are the only moving piece`);
    for (let i = 1; i <= nodeCount; i++) {
      const node: SimNode = {
        id: i,
        epoch: 1,
        status: 'booting',
        slots: 4,
        leaseHolder: false,
        memory: this.nodeMemory(),
        glow: '',
      };
      s.nodes.push(node);
      this.log('lifecycle', `node ${i} starting: connects to Postgres, loads snapshot (none yet), tails the log`);
      await this.sleep(400);
      const cmd = enc.registerNode(i, 1, `http://10.0.0.${i}:4437`, 4);
      await this.propose(i, [cmd], () => {
        node.status = 'running';
      });
    }
    s.nodes[0].leaseHolder = true;
    s.lease = { holder: 'node-1', ttlMs: 10_000 };
    this.log('lifecycle', `node 1 wins the maintenance lease (meta_lease row, TTL 10 s) and runs the expiry and GC ticks`);
    s.booted = true;
    s.busy = false;
  }

  async createStream(name: string) {
    const s = this.s;
    s.busy = true;
    const id = s.nextStreamId++;
    const running = s.nodes.filter((n) => n.status === 'running');
    const counts = new Map(running.map((n) => [n.id, 0]));
    for (const st of s.streams) {
      if (counts.has(st.nodeId)) counts.set(st.nodeId, (counts.get(st.nodeId) ?? 0) + 1);
    }
    const owner = [...counts.entries()].sort((a, b) => a[1] - b[1])[0][0];
    const proposer = running[0].id;

    const create = enc.createStream(proposer, 1);
    const place = enc.placeStream(id);
    const open = enc.openStream(owner, 1, id, 1);
    const registry = enc.putKv(name, te.encode(JSON.stringify({ stream_id: id })), `registry entry → stream ${id}`);

    this.log('metadata', `create "${name}": four commands proposed back-to-back, watch them coalesce`);
    await this.propose(proposer, [create, place, open, registry], () => {
      s.streams.push({
        id,
        name,
        epoch: 1,
        startOffset: 0,
        endOffset: 0,
        localEnd: 0,
        durableEnd: 0,
        state: 'opened',
        nodeId: owner,
      });
      s.kv.push({ key: name, display: `stream ${id}` });
    });
    this.log('lifecycle', `stream ${id} ("${name}") placed on node ${owner} (least-loaded by slots), opened at epoch 1`);
    s.busy = false;
  }

  dueTasks(): DueTask[] {
    const s = this.s;
    const buffered = s.walBuffer.length;
    const sealed = s.sealedWal.length;
    const compactTarget = s.streams.find(
      (st) => s.objects.filter((o) => o.streamId === st.id).length >= SIM.compactAtObjects,
    );
    const compactCount = compactTarget
      ? s.objects.filter((o) => o.streamId === compactTarget.id).length
      : Math.max(0, ...s.streams.map((st) => s.objects.filter((o) => o.streamId === st.id).length));
    return [
      {
        id: 'flush',
        label: 'Flush WAL',
        detail: buffered
          ? `${buffered} encoded record${buffered === 1 ? '' : 's'} in the node buffer`
          : 'nothing buffered',
        real: `real cluster: every ${REAL.walWindowMs} ms window. Ack waits for this PUT`,
        count: `${buffered}/${SIM.flushAtRecords}`,
        due: buffered >= SIM.flushAtRecords,
        ready: buffered > 0,
      },
      {
        id: 'commit',
        label: 'Commit sealed',
        detail: sealed
          ? `${sealed} WAL object${sealed === 1 ? '' : 's'} durable but not in metadata`
          : 'no sealed WAL waiting',
        real: 'real cluster: background upload of sealed log-cache blocks, then Prepare + Commit',
        count: `${sealed}/${SIM.commitAtWalObjects}`,
        due: sealed >= SIM.commitAtWalObjects,
        ready: sealed > 0,
      },
      {
        id: 'snapshot',
        label: 'Snapshot',
        detail: `${s.rowsSinceSnapshot} log rows since the last snapshot`,
        real: `real cluster: ${REAL.snapshotEvery} rows + ${REAL.snapshotMinIntervalS} s`,
        count: `${s.rowsSinceSnapshot}/${SIM.snapshotEvery}`,
        due: s.rowsSinceSnapshot >= SIM.snapshotEvery,
        ready: s.log.length > 0,
      },
      {
        id: 'compact',
        label: 'Compact',
        detail: compactTarget
          ? `${compactTarget.name} has ${compactCount} live committed objects`
          : compactCount
            ? `most a stream holds is ${compactCount} committed objects`
            : 'no committed stream objects yet',
        real: `real cluster: ${REAL.compactAtObjects} live objects per stream`,
        count: `${compactCount}/${SIM.compactAtObjects}`,
        due: Boolean(compactTarget),
        ready: Boolean(compactTarget),
      },
      {
        id: 'gc',
        label: 'Clean objects',
        detail: s.destroyedFifo.length
          ? `${s.destroyedFifo.length} destroyed object${s.destroyedFifo.length === 1 ? '' : 's'} in the FIFO`
          : 'FIFO empty',
        real: 'real cluster: lease holder ticks this in the background',
        count: `${s.destroyedFifo.length}`,
        due: s.destroyedFifo.length > 0,
        ready: s.destroyedFifo.length > 0,
      },
    ];
  }

  async runTask(id: DueTask['id']) {
    if (id === 'flush') await this.flushWal();
    else if (id === 'commit') await this.commitSealed();
    else if (id === 'snapshot') await this.takeSnapshot();
    else if (id === 'compact') {
      const target = this.s.streams.find(
        (st) => this.s.objects.filter((o) => o.streamId === st.id).length >= SIM.compactAtObjects,
      );
      if (target) await this.compact(target.id);
    } else await this.cleanerTick();
  }

  async append(streamId: number, text: string) {
    const s = this.s;
    const stream = s.streams.find((x) => x.id === streamId);
    if (!stream) return;
    const owner = s.nodes.find((n) => n.id === stream.nodeId);
    if (!owner || owner.status !== 'running') return;
    s.busy = true;
    const payload = te.encode(text);
    const base = stream.localEnd;

    const batch = encodeRecordBatch(streamId, stream.epoch, base, 1, payload, JSON.stringify(text));
    this.s.stage = 'wal';
    owner.glow = 'propose';
    owner.memory.logCache.push({ streamId, baseOffset: base, count: 1, size: batch.bytes.length });
    s.walBuffer.push({ nodeId: owner.id, streamId, batch });
    stream.localEnd = base + 1;
    this.log(
      'data',
      `node ${owner.id} encodes the batch once into the WAL buffer and log cache (33-byte header, magic 0x22, ${payload.length}-byte payload). Not durable yet. The producer ack waits for the next WAL PUT. Metadata is not involved.`,
    );
    await this.sleep(350);
    owner.glow = '';
    this.s.stage = '';
    if (s.walBuffer.length >= SIM.flushAtRecords) {
      this.log(
        'data',
        `WAL buffer is at ${s.walBuffer.length} records (sim flush at ${SIM.flushAtRecords}, real window ${REAL.walWindowMs} ms). Flush WAL when you want to watch the durable PUT.`,
      );
    }
    s.busy = false;
  }

  async flushWal() {
    const s = this.s;
    if (!s.walBuffer.length) return;
    s.busy = true;
    const byNode = new Map<number, WalBuffered[]>();
    for (const rec of s.walBuffer) {
      const list = byNode.get(rec.nodeId) ?? [];
      list.push(rec);
      byNode.set(rec.nodeId, list);
    }
    s.walBuffer = [];

    for (const [nodeId, records] of byNode) {
      const owner = s.nodes.find((n) => n.id === nodeId);
      if (!owner || owner.status !== 'running') {
        s.walBuffer.push(...records);
        this.log('data', `node ${nodeId} is not running. Its ${records.length} buffered records stay local`);
        continue;
      }

      const objectId = s.nextObjectId++;
      const w = new SegWriter();
      let offset = 0;
      const ranges = new Map<number, { streamId: number; epoch: number; start: number; end: number; size: number }>();
      for (const rec of records) {
        const frame = frameWalRecord(offset, rec.batch);
        w.append(frame.segs);
        offset += frame.bytes.length;
        const stream = s.streams.find((x) => x.id === rec.streamId);
        const start = rec.batch.baseOffset;
        const end = start + rec.batch.count;
        const prev = ranges.get(rec.streamId);
        if (prev) {
          prev.end = Math.max(prev.end, end);
          prev.size += frame.bytes.length;
        } else {
          ranges.set(rec.streamId, {
            streamId: rec.streamId,
            epoch: stream?.epoch ?? rec.batch.epoch,
            start,
            end,
            size: frame.bytes.length,
          });
        }
      }
      const bytes = w.bytes();
      const rangeList = [...ranges.values()];
      const first = rangeList[0];
      this.s.stage = 's3put';
      owner.glow = 'propose';
      const key = `wal/node-${nodeId}/${String(objectId).padStart(5, '0')}.wal`;
      s.s3.push({
        key,
        objectId,
        kind: 'wal',
        size: bytes.length,
        segs: w.segs,
        note: `${records.length} framed record${records.length === 1 ? '' : 's'} from one WAL window. Durable staging under the node's session prefix. Not yet visible in metadata.`,
        streamId: first.streamId,
        range: [first.start, first.end],
        nodeId,
      });
      s.sealedWal.push({
        objectId,
        nodeId,
        key,
        size: bytes.length,
        segs: w.segs,
        ranges: rangeList,
      });
      for (const range of rangeList) {
        const stream = s.streams.find((x) => x.id === range.streamId);
        if (stream) stream.durableEnd = Math.max(stream.durableEnd, range.end);
      }
      this.log(
        'storage',
        `PUT s3://pico/${key}, ${bytes.length} bytes. WAL staging only. Producers that were waiting on this window are acked here. Stream end offsets in metadata are unchanged.`,
      );
      await this.sleep(700);
      owner.glow = '';
      this.s.stage = '';
      if (s.sealedWal.length >= SIM.commitAtWalObjects) {
        this.log(
          'data',
          `${s.sealedWal.length} sealed WAL object${s.sealedWal.length === 1 ? '' : 's'} waiting. Commit sealed uploads stream-set objects and advances end offsets through metadata.`,
        );
      }
    }
    s.busy = false;
  }

  async commitSealed() {
    const s = this.s;
    if (!s.sealedWal.length) return;
    s.busy = true;
    const batch = [...s.sealedWal];
    s.sealedWal = [];

    for (const sealed of batch) {
      const owner = s.nodes.find((n) => n.id === sealed.nodeId);
      if (!owner || owner.status !== 'running') {
        s.sealedWal.push(sealed);
        this.log(
          'data',
          `node ${sealed.nodeId} is not running. Sealed WAL ${sealed.key} stays durable on S3 until an owner recovers it`,
        );
        continue;
      }

      const objectId = s.nextObjectId++;
      const prep = enc.prepareObject(owner.id, owner.epoch, 1, this.clock * 1000);
      await this.propose(owner.id, [prep], () => {
        s.prepared.push({ objectId, owner: owner.id });
      });

      const w = new SegWriter();
      w.append(
        sealed.segs.map((seg) => ({
          ...seg,
          label: seg.label.startsWith('[wal') ? seg.label : `[from ${sealed.key}] ${seg.label}`,
        })),
      );
      const bytes = w.bytes();
      const first = sealed.ranges[0];
      this.s.stage = 's3put';
      owner.glow = 'propose';
      const key = `stream-set/${String(objectId).padStart(5, '0')}.sso`;
      s.s3.push({
        key,
        objectId,
        kind: 'stream-set',
        size: bytes.length,
        segs: w.segs,
        note: `Stream-set object built from sealed WAL ${sealed.key}. Readers learn about it only after CommitStreamSetObject.`,
        streamId: first.streamId,
        range: [Math.min(...sealed.ranges.map((r) => r.start)), Math.max(...sealed.ranges.map((r) => r.end))],
      });
      this.log(
        'storage',
        `PUT s3://pico/${key}, ${bytes.length} bytes: sealed block rewritten as a stream-set object. Still invisible to readers until metadata commit.`,
      );
      await this.sleep(600);
      owner.glow = '';
      this.s.stage = '';

      const commit = enc.commitStreamSetObject(
        owner.id,
        owner.epoch,
        objectId,
        bytes.length,
        sealed.ranges,
        this.clock * 1000,
      );
      await this.propose(owner.id, [commit], () => {
        s.prepared = s.prepared.filter((p) => p.objectId !== objectId);
        for (const range of sealed.ranges) {
          const stream = s.streams.find((x) => x.id === range.streamId);
          if (stream) stream.endOffset = Math.max(stream.endOffset, range.end);
          s.objects.push({
            objectId,
            streamId: range.streamId,
            start: range.start,
            end: range.end,
            size: range.size,
            kind: 'stream-set',
          });
        }
      });

      s.s3 = s.s3.filter((x) => x.objectId !== sealed.objectId);
      this.log(
        'storage',
        `DELETE s3://pico/${sealed.key}: covered by committed stream-set object ${objectId}`,
      );
      this.log(
        'data',
        `CommitStreamSetObject published object ${objectId}. Stream end offsets in metadata now cover the sealed ranges. Readers can find them via sso_ranges.`,
      );
      await this.sleep(300);
    }
    s.busy = false;
  }

  async compact(streamId: number) {
    const s = this.s;
    const stream = s.streams.find((x) => x.id === streamId);
    if (!stream) return;
    const owner = s.nodes.find((n) => n.id === stream.nodeId);
    if (!owner || owner.status !== 'running') return;
    s.busy = true;
    const sources = s.objects.filter((o) => o.streamId === streamId);
    if (sources.length < 2) {
      s.busy = false;
      return;
    }

    const objectId = s.nextObjectId++;
    const start = Math.min(...sources.map((o) => o.start));
    const end = Math.max(...sources.map((o) => o.end));

    const w = new SegWriter();
    let concatenated = 0;
    for (const src of sources) {
      const obj = s.s3.find((x) => x.objectId === src.objectId);
      if (obj) {
        const bodySegs = obj.segs.slice(5);
        w.append(bodySegs.map((seg) => ({ ...seg, label: `[obj ${src.objectId}] ${seg.label}` })));
        concatenated += bodySegs.reduce((n, seg) => n + seg.bytes.length, 0);
      }
    }
    const size = concatenated;

    this.s.stage = 's3put';
    const key = `stream/${streamId}/${String(objectId).padStart(5, '0')}.obj`;
    s.s3.push({
      key,
      objectId,
      kind: 'compacted',
      size,
      segs: w.segs,
      note: `${sources.length} source objects rewritten as contiguous record batches [${start}, ${end}). WAL framing dropped, ${sources.reduce((n, o) => n + o.size, 0) - size} bytes of framing overhead reclaimed`,
      streamId,
      range: [start, end],
    });
    this.log('storage', `PUT s3://pico/${key}: compacted object ${objectId}, ${size} bytes for offsets [${start}, ${end})`);
    await this.sleep(700);
    this.s.stage = '';

    const cmd = enc.compactStreamObject(
      owner.id,
      owner.epoch,
      objectId,
      size,
      streamId,
      stream.epoch,
      start,
      end,
      sources.map((o) => o.objectId),
      this.clock * 1000,
    );
    await this.propose(owner.id, [cmd], () => {
      s.objects = s.objects.filter((o) => o.streamId !== streamId);
      s.objects.push({ objectId, streamId, start, end, size, kind: 'compacted' });
      for (const src of sources) {
        s.destroyedFifo.push({ objectId: src.objectId, seq: ++s.cleanerSeq });
      }
    });
    this.log(
      'lifecycle',
      `${sources.length} source objects queued in the destroyed FIFO (backlog ${s.destroyedFifo.length}). Clean objects when you want the lease holder to drain it.`,
    );
    s.busy = false;
  }

  async cleanerTick() {
    const s = this.s;
    if (!s.destroyedFifo.length) return;
    s.busy = true;
    const holder = s.nodes.find((n) => n.leaseHolder && n.status === 'running');
    if (!holder) {
      this.log('lifecycle', 'no maintenance lease holder. The destroyed FIFO backlog sits until one is elected');
      s.busy = false;
      return;
    }
    const ids = s.destroyedFifo.map((d) => d.objectId);
    this.log('lifecycle', `lease holder node ${holder.id} drains the FIFO: DELETE ${ids.length} objects from the store`);
    await this.sleep(500);
    for (const id of ids) {
      const obj = s.s3.find((x) => x.objectId === id);
      if (obj) this.log('storage', `DELETE s3://pico/${obj.key}`);
    }
    s.s3 = s.s3.filter((x) => !ids.includes(x.objectId));
    const cmd = enc.cleanDestroyedObjects(ids);
    await this.propose(holder.id, [cmd], () => {
      s.destroyedFifo = [];
    });
    s.busy = false;
  }

  async takeSnapshot() {
    const s = this.s;
    const holderNode = s.nodes.find((n) => n.status === 'running');
    if (!holderNode || !s.log.length) return;
    s.busy = true;
    const appliedIdx = s.log[s.log.length - 1].idx;
    const size =
      64 + s.streams.length * 96 + s.objects.length * 72 + s.kv.length * 48 + s.nodes.length * 64;
    this.log(
      'metadata',
      `${s.rowsSinceSnapshot} rows since the last snapshot (sim threshold ${SIM.snapshotEvery}, real ${REAL.snapshotEvery} rows + ${REAL.snapshotMinIntervalS} s). Node ${holderNode.id} forks the published view (O(1)) and encodes it off the apply path`,
    );
    await this.sleep(700);
    s.snapshot = { appliedIdx, size, takenAt: this.now() };
    const dropped = s.log.length;
    s.log = [];
    s.rowsSinceSnapshot = 0;
    this.log(
      'metadata',
      `snapshot stored at applied_idx ${appliedIdx} (${size} bytes, one row per cluster). ${dropped} log rows truncated. Cold-start replay is now bounded`,
    );
    s.busy = false;
  }

  async killNode(nodeId: number) {
    const s = this.s;
    const node = s.nodes.find((n) => n.id === nodeId);
    if (!node || node.status !== 'running') return;
    s.busy = true;
    node.status = 'dead';
    node.glow = '';
    node.memory = this.nodeMemory();
    this.log('lifecycle', `node ${nodeId} killed. Its in-memory state (view, log cache, gates) is gone. Postgres and S3 still hold everything durable`);

    const lost = s.walBuffer.filter((r) => r.nodeId === nodeId);
    if (lost.length) {
      s.walBuffer = s.walBuffer.filter((r) => r.nodeId !== nodeId);
      this.log(
        'data',
        `${lost.length} unflushed buffered record${lost.length === 1 ? '' : 's'} on node ${nodeId} are gone. They were never acked.`,
      );
    }

    if (node.leaseHolder) {
      node.leaseHolder = false;
      const next = s.nodes.find((n) => n.status === 'running');
      if (next) {
        next.leaseHolder = true;
        s.lease = { holder: `node-${next.id}`, ttlMs: 10_000 };
        this.log('lifecycle', `lease expires after its 10 s TTL. Node ${next.id} wins the next election`);
        await this.sleep(500);
      } else {
        s.lease = null;
      }
    }

    const orphans = s.streams.filter((x) => x.nodeId === nodeId);
    const survivors = s.nodes.filter((n) => n.status === 'running');
    if (!survivors.length) {
      this.log('lifecycle', 'no nodes left running. Streams stay owned by a dead epoch until something restarts');
      s.busy = false;
      return;
    }
    for (const [i, stream] of orphans.entries()) {
      const to = survivors[i % survivors.length];
      const newEpoch = stream.epoch + 1;
      this.log(
        'lifecycle',
        `stream ${stream.id} ("${stream.name}") is orphaned. Real handoff is pending transfer, source drain/close, then CompleteTransfer. Sim collapses that into a force-reassign onto node ${to.id}.`,
      );
      const xfer = enc.transferStream(stream.id, nodeId, to.id);
      await this.propose(to.id, [xfer], () => {
        stream.nodeId = to.id;
      });
      this.log(
        'lifecycle',
        `TransferStream applied. Opening at epoch ${newEpoch} fences anything still in flight from node ${nodeId}.`,
      );
      const open = enc.openStream(to.id, to.epoch, stream.id, newEpoch);
      const done = enc.completeTransfer(stream.id, newEpoch);
      await this.propose(to.id, [open, done], () => {
        stream.epoch = newEpoch;
        stream.localEnd = Math.max(stream.localEnd, stream.durableEnd, stream.endOffset);
      });

      const recoverable = s.sealedWal.filter(
        (w) => w.nodeId === nodeId && w.ranges.some((r) => r.streamId === stream.id && r.end > stream.endOffset),
      );
      if (recoverable.length) {
        for (const sealed of recoverable) {
          sealed.nodeId = to.id;
          this.log(
            'data',
            `WAL recovery: sealed object ${sealed.key} covers past committed end ${stream.endOffset}. Reassigned to node ${to.id}. Commit sealed to publish it.`,
          );
        }
        await this.sleep(400);
      }
    }
    s.busy = false;
  }

  async restartNode(nodeId: number) {
    const s = this.s;
    const node = s.nodes.find((n) => n.id === nodeId);
    if (!node || node.status !== 'dead') return;
    s.busy = true;
    node.status = 'restoring';
    node.epoch += 1;
    this.log('lifecycle', `node ${nodeId} restarting at epoch ${node.epoch}. Cold start: load the snapshot, replay the tail`);
    await this.sleep(500);
    if (s.snapshot) {
      this.log(
        'metadata',
        `node ${nodeId} decodes the snapshot (${s.snapshot.size} bytes, state at applied_idx ${s.snapshot.appliedIdx}). Secondary indexes are rebuilt from primaries, never serialized`,
      );
      await this.sleep(700);
    } else {
      this.log('metadata', `no snapshot yet. Node ${nodeId} replays the log from row 1`);
    }
    for (const row of s.log) {
      node.memory.appliedIndex = row.idx;
      this.log('metadata', `node ${nodeId} replays row ${row.idx}: ${row.commands.map((c) => c.name).join(' + ')}`);
      await this.sleep(220);
    }
    const stranded = s.sealedWal.filter((w) => w.nodeId === nodeId);
    if (stranded.length) {
      this.log(
        'data',
        `node ${nodeId} still has ${stranded.length} sealed WAL object${stranded.length === 1 ? '' : 's'} under its old session. Streams that moved away recover those on open. Leftovers stay until a later owner claims them.`,
      );
      await this.sleep(400);
    }
    const cmd = enc.registerNode(nodeId, node.epoch, `http://10.0.0.${nodeId}:4437`, node.slots);
    await this.propose(nodeId, [cmd], () => {
      node.status = 'running';
    });
    this.refreshViewMaps();
    this.log('lifecycle', `node ${nodeId} is serving again, caught up to applied index ${node.memory.appliedIndex}`);
    s.busy = false;
  }

  reset() {
    Object.assign(this.s, initialState());
    this.clock = 0;
  }
}
