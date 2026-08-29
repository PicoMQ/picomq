<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import HexBytes from './HexBytes.vue';
import { fmtBytes } from './bytes';
import type { Seg } from './bytes';
import { initialState, Sim, REAL, SIM } from './engine';
import type { LogRow, S3Object } from './engine';

const state = reactive(initialState());
const sim = new Sim(state);

const nodeCount = ref(2);
const streamName = ref('/orders/eu');
const payloadText = ref('{"order":1042,"total":18.5}');
const appendStream = ref<number | null>(null);

type Tab = 'memory' | 'postgres' | 's3' | 'bytes';
const tab = ref<Tab>('postgres');
const selectedNode = ref<number>(1);
const inspected = ref<{ title: string; segs: Seg[]; note?: string } | null>(null);

const runningStreams = computed(() => state.streams.filter((s) => s.state === 'opened'));
const currentNode = computed(() => state.nodes.find((n) => n.id === selectedNode.value));
const totalS3Bytes = computed(() => state.s3.reduce((n, o) => n + o.size, 0));

// Edge geometry: measured from the DOM so the wires always meet the cards.
const canvasEl = ref<HTMLElement | null>(null);
const pgEl = ref<HTMLElement | null>(null);
const s3El = ref<HTMLElement | null>(null);
const nodeEls = new Map<number, HTMLElement>();
function setNodeEl(id: number, el: unknown) {
  if (el) nodeEls.set(id, el as HTMLElement);
  else nodeEls.delete(id);
}

interface Edge {
  nodeId: number;
  side: 'pg' | 's3';
  d: string;
}
const edges = ref<Edge[]>([]);
const canvasSize = ref({ w: 0, h: 0 });

function measure() {
  const canvas = canvasEl.value;
  const pg = pgEl.value;
  const s3 = s3El.value;
  if (!canvas || !pg || !s3) return;
  const c = canvas.getBoundingClientRect();
  canvasSize.value = { w: c.width, h: c.height };
  const anchor = (el: HTMLElement, edge: 'left' | 'right') => {
    const r = el.getBoundingClientRect();
    return {
      x: (edge === 'left' ? r.left : r.right) - c.left,
      y: r.top + r.height / 2 - c.top,
    };
  };
  const pgA = anchor(pg, 'right');
  const s3A = anchor(s3, 'left');
  const out: Edge[] = [];
  for (const [nodeId, el] of nodeEls) {
    const left = anchor(el, 'left');
    const right = anchor(el, 'right');
    const bend = (a: { x: number; y: number }, b: { x: number; y: number }) => {
      const mx = (a.x + b.x) / 2;
      return `M ${a.x} ${a.y} C ${mx} ${a.y}, ${mx} ${b.y}, ${b.x} ${b.y}`;
    };
    out.push({ nodeId, side: 'pg', d: bend(pgA, left) });
    out.push({ nodeId, side: 's3', d: bend(right, s3A) });
  }
  edges.value = out;
}

onMounted(() => {
  measure();
  window.addEventListener('resize', measure);
});
onBeforeUnmount(() => window.removeEventListener('resize', measure));
watch(
  () => state.nodes.map((n) => n.id + n.status).join(),
  () => nextTick(measure),
);

function edgeState(edge: Edge): '' | 'to-store' | 'from-store' {
  const node = state.nodes.find((n) => n.id === edge.nodeId);
  if (!node || node.status !== 'running') return '';
  if (edge.side === 'pg') {
    if (state.stage === 'logrow' && node.glow === 'propose') return 'to-store';
    if ((state.stage === 'tailer' || state.stage === 'view') && node.glow !== '') return 'from-store';
  } else {
    if (state.stage === 's3put' && node.glow === 'propose') return 'to-store';
  }
  return '';
}

const MAP_ENTRY_BYTES: Record<string, number> = {
  streams: 96,
  nodes: 120,
  opening_by_node: 24,
  placed_by_node: 24,
  stream_set_objects: 88,
  sso_ranges: 32,
  stream_objects: 72,
  prepared: 24,
  mark_destroyed: 40,
  kv: 64,
};

const memoryRows = computed(() => {
  const node = currentNode.value;
  if (!node) return [];
  return Object.entries(node.memory.viewMaps).map(([name, entries]) => ({
    name,
    entries,
    bytes: entries * (MAP_ENTRY_BYTES[name] ?? 48),
  }));
});

const memoryTotal = computed(() => {
  const maps = memoryRows.value.reduce((n, r) => n + r.bytes, 0);
  const cache = currentNode.value?.memory.logCache.reduce((n, e) => n + e.size, 0) ?? 0;
  return maps + cache;
});

async function boot() {
  await sim.boot(nodeCount.value);
  appendStream.value = null;
}

async function createStream() {
  if (!streamName.value.trim()) return;
  await sim.createStream(streamName.value.trim());
  appendStream.value = state.streams[state.streams.length - 1]?.id ?? null;
  streamName.value = `/orders/${['eu', 'us', 'apac', 'latam'][state.streams.length % 4]}-${state.streams.length}`;
}

async function append() {
  if (appendStream.value == null) return;
  await sim.append(appendStream.value, payloadText.value);
}

function inspectRow(row: LogRow) {
  inspected.value = {
    title: `meta_log row ${row.idx}: ${row.commands.map((c) => c.name).join(' + ')}`,
    segs: row.segs,
    note: `The exact payload BYTEA: codec version byte, little-endian u32 command count, then each command body (type byte, fields in declaration order). Decoding this on any node replays the same state mutation.`,
  };
  tab.value = 'bytes';
}

function inspectObject(obj: S3Object) {
  inspected.value = { title: `s3://pico/${obj.key}`, segs: obj.segs, note: obj.note };
  tab.value = 'bytes';
}

function reset() {
  sim.reset();
  inspected.value = null;
  tab.value = 'postgres';
  selectedNode.value = 1;
  appendStream.value = null;
}

const tasks = computed(() => sim.dueTasks());

const metaStages: [string, string][] = [
  ['propose', 'propose'],
  ['flusher', 'flusher'],
  ['logrow', 'log row'],
  ['tailer', 'tailer'],
  ['view', 'view'],
];
const dataStages: [string, string][] = [
  ['wal', 'encode'],
  ['s3put', 'put object'],
];
</script>

<template>
  <div class="csim">
    <div class="meta-bar">
      <label class="speed">
        speed
        <input v-model.number="state.speed" type="range" min="0.5" max="4" step="0.5" />
        <span class="mono">{{ state.speed }}×</span>
      </label>
      <button v-if="state.booted" class="reset" :disabled="state.busy" @click="reset">reset</button>
    </div>

    <div v-if="state.booted" class="toolbar">
      <div class="action-box">
        <div class="action-kicker">streams</div>
        <div class="op">
          <div class="field">
            <span class="field-label">Name</span>
            <input v-model="streamName" class="txt" :disabled="state.busy" spellcheck="false" />
          </div>
          <button class="btn" :disabled="state.busy" @click="createStream">Create stream</button>
        </div>
        <div class="op-split"></div>
        <div class="op" :class="{ locked: !runningStreams.length }">
          <div class="fields">
            <div class="field">
              <span class="field-label">Stream</span>
              <select v-model.number="appendStream" :disabled="state.busy || !runningStreams.length">
                <option v-if="!runningStreams.length" :value="null">create a stream first</option>
                <option v-for="s in runningStreams" :key="s.id" :value="s.id">{{ s.name }}</option>
              </select>
            </div>
            <div class="field grow">
              <span class="field-label">Record</span>
              <input
                v-model="payloadText"
                class="txt"
                :disabled="state.busy || !runningStreams.length"
                spellcheck="false"
              />
            </div>
          </div>
          <button class="btn" :disabled="state.busy || appendStream == null" @click="append">
            Append record
          </button>
        </div>
      </div>

      <div class="action-box">
        <div class="action-kicker">background</div>
        <p class="action-note">
          Timers in a real cluster. Here they highlight when due. Click to run.
        </p>
        <div class="tasks">
          <button
            v-for="task in tasks"
            :key="task.id"
            class="task"
            :class="{ due: task.due, ready: task.ready && !task.due }"
            :disabled="state.busy || !task.ready"
            :title="task.real"
            @click="sim.runTask(task.id)"
          >
            <span class="task-top">
              <span class="task-label">{{ task.label }}</span>
              <span class="task-count mono">{{ task.count }}</span>
            </span>
            <span class="task-detail">{{ task.detail }}</span>
            <span class="task-real">{{ task.real }}</span>
          </button>
        </div>
      </div>
    </div>

    <!-- Canvas -->
    <div ref="canvasEl" class="canvas">
      <svg
        class="wires"
        :viewBox="`0 0 ${canvasSize.w} ${canvasSize.h}`"
        preserveAspectRatio="none"
        aria-hidden="true"
      >
        <path
          v-for="edge in edges"
          :key="edge.nodeId + edge.side"
          :d="edge.d"
          class="wire"
          :class="edgeState(edge)"
        />
      </svg>

      <div ref="pgEl" class="store" @click="tab = 'postgres'">
        <div class="store-kicker">fixed</div>
        <div class="store-name">Postgres</div>
        <div class="store-rows mono">
          <div><span>meta_log</span><span>{{ state.log.length }} rows</span></div>
          <div>
            <span>meta_snapshot</span
            ><span>{{ state.snapshot ? `idx ${state.snapshot.appliedIdx}` : 'empty' }}</span>
          </div>
          <div><span>meta_lease</span><span>{{ state.lease?.holder ?? 'none' }}</span></div>
        </div>
      </div>

      <div class="nodes">
        <div v-if="!state.nodes.length" class="boot">
          <p class="boot-copy">Postgres and S3 are already waiting.</p>
          <div class="boot-row">
            <label class="field">
              <span class="field-label">Nodes</span>
              <select v-model.number="nodeCount" :disabled="state.busy">
                <option v-for="n in [1, 2, 3, 4]" :key="n" :value="n">{{ n }}</option>
              </select>
            </label>
            <button class="btn" :disabled="state.busy" @click="boot">Boot cluster</button>
          </div>
        </div>
        <div
          v-for="node in state.nodes"
          :key="node.id"
          :ref="(el) => setNodeEl(node.id, el)"
          class="node"
          :class="[node.status, node.glow, { selected: selectedNode === node.id }]"
          @click="selectedNode = node.id; tab = 'memory'"
        >
          <div class="node-head">
            <span class="node-name">node {{ node.id }}</span>
            <span v-if="node.leaseHolder" class="lease" title="maintenance lease holder"></span>
            <span class="node-state">{{ node.status }}</span>
          </div>
          <div class="node-rows mono">
            <div><span>epoch</span><span>{{ node.epoch }}</span></div>
            <div><span>applied</span><span>{{ node.memory.appliedIndex }}</span></div>
            <div>
              <span>streams</span
              ><span>{{
                state.streams.filter((s) => s.nodeId === node.id && s.state === 'opened').length
              }}</span>
            </div>
            <div>
              <span>buf</span
              ><span>{{ state.walBuffer.filter((r) => r.nodeId === node.id).length }}</span>
            </div>
            <div>
              <span>sealed</span
              ><span>{{ state.sealedWal.filter((r) => r.nodeId === node.id).length }}</span>
            </div>
          </div>
          <div class="node-actions">
            <button
              v-if="node.status === 'running'"
              class="linkbtn danger"
              :disabled="state.busy"
              @click.stop="sim.killNode(node.id)"
            >
              kill
            </button>
            <button
              v-if="node.status === 'dead'"
              class="linkbtn"
              :disabled="state.busy"
              @click.stop="sim.restartNode(node.id)"
            >
              restart
            </button>
          </div>
        </div>
      </div>

      <div ref="s3El" class="store" @click="tab = 's3'">
        <div class="store-kicker">fixed</div>
        <div class="store-name">Object store</div>
        <div class="store-rows mono">
          <div><span>objects</span><span>{{ state.s3.length }}</span></div>
          <div><span>bytes</span><span>{{ fmtBytes(totalS3Bytes) }}</span></div>
          <div><span>gc queue</span><span>{{ state.destroyedFifo.length }}</span></div>
        </div>
      </div>
    </div>

    <div class="stagebar mono">
      <span class="stagegroup">
        <template v-for="([key, label], i) in metaStages" :key="key">
          <span class="stage" :class="{ on: state.stage === key }">{{ label }}</span>
          <span v-if="i < metaStages.length - 1" class="sep">·</span>
        </template>
      </span>
      <span class="stagegroup">
        <template v-for="([key, label], i) in dataStages" :key="key">
          <span class="stage" :class="{ on: state.stage === key }">{{ label }}</span>
          <span v-if="i < dataStages.length - 1" class="sep">·</span>
        </template>
      </span>
    </div>

    <!-- Panels -->
    <div class="panels">
      <div class="timeline">
        <div class="kicker">timeline</div>
        <div class="timeline-scroll">
          <div v-if="!state.timeline.length" class="empty">nothing yet</div>
          <div v-for="ev in state.timeline" :key="ev.seq" class="tl-event">
            <span class="tl-dot" :class="ev.category"></span>
            <span class="tl-at mono">{{ ev.at }}</span>
            <span class="tl-text">{{ ev.text }}</span>
          </div>
        </div>
      </div>

      <div class="inspector">
        <div class="tabs">
          <button :class="{ on: tab === 'memory' }" @click="tab = 'memory'">node memory</button>
          <button :class="{ on: tab === 'postgres' }" @click="tab = 'postgres'">postgres</button>
          <button :class="{ on: tab === 's3' }" @click="tab = 's3'">object store</button>
          <button :class="{ on: tab === 'bytes' }" @click="tab = 'bytes'">bytes</button>
        </div>

        <div v-if="tab === 'memory'" class="tabbody">
          <div v-if="!currentNode || currentNode.status === 'off'" class="empty">no node selected</div>
          <template v-else>
            <div class="mem-head">
              <span class="kicker">node {{ currentNode.id }} resident state</span>
              <span class="mono dim">≈ {{ fmtBytes(memoryTotal) }}</span>
            </div>
            <div v-if="currentNode.status === 'dead'" class="empty">
              dead. Everything below was memory-only and is gone. The log, snapshot, and objects
              survive in Postgres and S3.
            </div>
            <template v-else>
              <div class="section">published view (im::OrdMap forks, O(1) to clone)</div>
              <table class="mini">
                <thead>
                  <tr><th>map</th><th>entries</th><th>≈ bytes</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in memoryRows" :key="row.name">
                    <td class="mono">{{ row.name }}</td>
                    <td class="mono num">{{ row.entries }}</td>
                    <td class="mono num">{{ fmtBytes(row.bytes) }}</td>
                  </tr>
                </tbody>
              </table>
              <div class="section">log cache (encoded batches retained for reads)</div>
              <div v-if="!currentNode.memory.logCache.length" class="empty">empty</div>
              <table v-else class="mini">
                <thead>
                  <tr><th>stream</th><th>base offset</th><th>records</th><th>bytes</th></tr>
                </thead>
                <tbody>
                  <tr v-for="(e, i) in currentNode.memory.logCache" :key="i">
                    <td class="mono">{{ e.streamId }}</td>
                    <td class="mono num">{{ e.baseOffset }}</td>
                    <td class="mono num">{{ e.count }}</td>
                    <td class="mono num">{{ fmtBytes(e.size) }}</td>
                  </tr>
                </tbody>
              </table>
              <p class="note">
                Measured on the real thing: about 2.2 KiB resident per stream at one million
                streams.
              </p>
            </template>
          </template>
        </div>

        <div v-if="tab === 'postgres'" class="tabbody">
          <div class="section">meta_log (idx BIGINT PK, payload BYTEA). Click a row to decode it.</div>
          <div v-if="!state.log.length" class="empty">
            {{ state.snapshot ? 'empty: everything below the snapshot was truncated' : 'no rows yet' }}
          </div>
          <table v-else class="mini clickable">
            <thead>
              <tr><th>idx</th><th>payload</th><th>bytes</th></tr>
            </thead>
            <tbody>
              <tr v-for="row in state.log" :key="row.idx" @click="inspectRow(row)">
                <td class="mono num">{{ row.idx }}</td>
                <td>{{ row.commands.map((c) => c.name).join(' + ') }}</td>
                <td class="mono num">{{ row.bytes.length }}</td>
              </tr>
            </tbody>
          </table>
          <div class="section">meta_snapshot (id = 0, applied_idx, payload)</div>
          <div v-if="!state.snapshot" class="empty">
            none yet. Taken every {{ SIM.snapshotEvery }} rows here. The real cadence is
            {{ REAL.snapshotEvery }} rows plus a {{ REAL.snapshotMinIntervalS }} s minimum interval.
          </div>
          <table v-else class="mini">
            <thead>
              <tr><th>applied_idx</th><th>payload</th><th>taken</th></tr>
            </thead>
            <tbody>
              <tr>
                <td class="mono num">{{ state.snapshot.appliedIdx }}</td>
                <td class="mono">{{ fmtBytes(state.snapshot.size) }} (full state, one row per cluster)</td>
                <td class="mono">{{ state.snapshot.takenAt }}</td>
              </tr>
            </tbody>
          </table>
          <div class="section">meta_lease</div>
          <div v-if="!state.lease" class="empty">no holder</div>
          <table v-else class="mini">
            <thead><tr><th>holder</th><th>ttl</th></tr></thead>
            <tbody>
              <tr>
                <td class="mono">{{ state.lease.holder }}</td>
                <td class="mono">{{ state.lease.ttlMs }} ms</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div v-if="tab === 's3'" class="tabbody">
          <div class="section">s3://pico. Click an object for its exact byte layout.</div>
          <div v-if="!state.s3.length" class="empty">no objects. Append, Flush WAL, then Commit sealed.</div>
          <table v-else class="mini clickable">
            <thead>
              <tr><th>key</th><th>kind</th><th>stream</th><th>offsets</th><th>size</th></tr>
            </thead>
            <tbody>
              <tr v-for="obj in state.s3" :key="obj.key" @click="inspectObject(obj)">
                <td class="mono">{{ obj.key }}</td>
                <td>{{ obj.kind }}</td>
                <td class="mono num">{{ obj.streamId }}</td>
                <td class="mono">[{{ obj.range[0] }}, {{ obj.range[1] }})</td>
                <td class="mono num">{{ fmtBytes(obj.size) }}</td>
              </tr>
            </tbody>
          </table>
          <p class="note">
            Record batches are stored verbatim: encoded once at append time, CRC-checked on read.
            There is no recompression on the storage path. Compaction reclaims the per-append WAL
            framing.
          </p>
        </div>

        <div v-if="tab === 'bytes'" class="tabbody">
          <div v-if="!inspected" class="empty">
            click a meta_log row or an S3 object to see its exact bytes
          </div>
          <template v-else>
            <HexBytes :segs="inspected.segs" :title="inspected.title" />
            <p v-if="inspected.note" class="note">{{ inspected.note }}</p>
          </template>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.csim {
  border: 1px solid var(--pico-hairline);
  background: var(--pico-surface-1, #fafafa);
  margin: 24px 0;
  font-size: 13.5px;
  overflow: hidden;
}
.mono {
  font-family: var(--vp-font-family-mono);
  font-size: 12px;
}
.dim {
  color: var(--pico-ink-4);
}
.empty {
  color: var(--pico-ink-5, #8b929a);
  padding: 8px 2px;
  font-size: 13px;
  line-height: 1.5;
}
.note {
  color: var(--pico-ink-4);
  font-size: 12.5px;
  margin: 10px 0 0;
  line-height: 1.55;
}
.kicker,
.section {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.07em;
  color: var(--pico-ink-4);
  font-weight: 600;
}
.section {
  margin: 14px 0 6px;
  font-weight: 500;
  color: var(--pico-ink-5, #8b929a);
  text-transform: none;
  letter-spacing: 0.01em;
  font-size: 12px;
}

.meta-bar {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  gap: 14px;
  padding: 6px 14px;
  background: var(--pico-surface-0, #fff);
  border-bottom: 1px solid var(--pico-hairline);
}
.speed {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  color: var(--pico-ink-5, #8b929a);
}
.reset {
  border: none;
  background: none;
  padding: 0;
  font-size: 11px;
  color: var(--pico-ink-5, #8b929a);
  cursor: pointer;
  text-decoration: underline;
  text-underline-offset: 2px;
}
.reset:disabled {
  opacity: 0.4;
  cursor: default;
}
.toolbar {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 0;
  border-bottom: 1px solid var(--pico-hairline);
  background: var(--pico-surface-0, #fff);
}
@media (max-width: 860px) {
  .toolbar {
    grid-template-columns: 1fr;
  }
}
.action-box {
  padding: 12px 14px 14px;
  min-width: 0;
}
.action-box + .action-box {
  border-left: 1px solid var(--pico-hairline);
}
.action-kicker {
  font-size: 10.5px;
  text-transform: uppercase;
  letter-spacing: 0.07em;
  color: var(--pico-ink-4);
  font-weight: 600;
  margin-bottom: 10px;
}
.action-note {
  margin: 0 0 8px;
  font-size: 12px;
  color: var(--pico-ink-4);
  line-height: 1.45;
}
.op {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 8px;
}
.op.locked {
  opacity: 0.4;
}
.op-split {
  height: 1px;
  background: var(--pico-hairline);
  margin: 12px 0;
}
.fields {
  display: flex;
  gap: 8px;
  width: 100%;
}
.field {
  display: flex;
  flex-direction: column;
  gap: 3px;
  min-width: 0;
}
.field.grow {
  flex: 1;
}
.field-label {
  font-size: 11px;
  color: var(--pico-ink-4);
}
.tasks {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 6px;
}
.task {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 1px;
  text-align: left;
  border: 1px solid var(--pico-ink-6, #c7ccd1);
  background: var(--pico-surface-0, #fff);
  padding: 6px 8px;
  cursor: pointer;
  color: var(--pico-ink-3);
}
.task:disabled {
  cursor: default;
  opacity: 0.45;
}
.task.ready:not(:disabled) {
  border-color: var(--pico-ink-4);
}
.task.due:not(:disabled) {
  border-color: var(--pico-accent, #1f2a37);
  box-shadow: inset 3px 0 0 var(--pico-accent, #1f2a37);
  opacity: 1;
}
.task-top {
  display: flex;
  justify-content: space-between;
  width: 100%;
  gap: 8px;
}
.task-label {
  font-size: 12.5px;
  font-weight: 600;
  color: var(--pico-ink-1);
}
.task-count {
  color: var(--pico-ink-3);
}
.task-detail,
.task-real {
  font-size: 11px;
  color: var(--pico-ink-5, #8b929a);
  line-height: 1.35;
}
.task.due .task-real {
  color: var(--pico-ink-3);
}
.btn {
  border: 1px solid var(--pico-ink-3);
  background: var(--pico-surface-0, #fff);
  padding: 6px 14px;
  font-size: 13px;
  cursor: pointer;
  color: var(--pico-ink-1);
  white-space: nowrap;
}
.btn:hover:not(:disabled) {
  background: var(--pico-accent, #1f2a37);
  border-color: var(--pico-accent, #1f2a37);
  color: #fff;
}
.btn:disabled {
  opacity: 0.35;
  cursor: default;
}
select,
.txt {
  border: 1px solid var(--pico-ink-6, #c7ccd1);
  padding: 5px 8px;
  font-size: 12.5px;
  background: var(--pico-surface-0, #fff);
  color: var(--pico-ink-1);
  font-family: var(--vp-font-family-mono);
  box-sizing: border-box;
  min-width: 120px;
  width: 100%;
}
input[type='range'] {
  width: 72px;
  accent-color: var(--pico-accent, #1f2a37);
}
.boot {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 12px;
  width: 100%;
  min-height: 120px;
  text-align: center;
}
.boot-copy {
  margin: 0;
  color: var(--pico-ink-5, #8b929a);
  font-size: 13px;
}
.boot-row {
  display: flex;
  align-items: flex-end;
  justify-content: center;
  gap: 8px;
}
.boot .field {
  text-align: left;
}
.boot select {
  width: 72px;
  min-width: 72px;
}

/* Canvas */
.canvas {
  position: relative;
  display: grid;
  grid-template-columns: 176px 1fr 176px;
  gap: 28px;
  align-items: center;
  padding: 26px 22px;
  min-height: 190px;
}
.wires {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
  pointer-events: none;
}
.wire {
  fill: none;
  stroke: var(--pico-ink-6, #d4d8dc);
  stroke-width: 1;
}
.wire.to-store,
.wire.from-store {
  stroke: var(--pico-accent, #1f2a37);
  stroke-width: 1.6;
  stroke-dasharray: 5 5;
  animation: flow 0.5s linear infinite;
}
.wire.from-store {
  animation-direction: reverse;
}
@keyframes flow {
  to {
    stroke-dashoffset: -10;
  }
}

.store {
  position: relative;
  border: 1px solid var(--pico-ink-6, #c7ccd1);
  background: var(--pico-surface-2, #f2f3f5);
  padding: 10px 12px 11px;
  cursor: pointer;
}
.store-kicker {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--pico-ink-5, #8b929a);
}
.store-name {
  font-weight: 600;
  color: var(--pico-ink-1);
  margin: 1px 0 7px;
}
.store-rows div,
.node-rows div {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  color: var(--pico-ink-3);
  line-height: 1.75;
}
.store-rows span:last-child,
.node-rows span:last-child {
  color: var(--pico-ink-1);
}

.nodes {
  position: relative;
  display: flex;
  gap: 18px;
  justify-content: center;
  flex-wrap: wrap;
}
.nodes-empty {
  color: var(--pico-ink-5, #8b929a);
  font-size: 13px;
  text-align: center;
  line-height: 1.6;
}
.node {
  position: relative;
  border: 1px solid var(--pico-ink-6, #c7ccd1);
  background: var(--pico-surface-0, #fff);
  padding: 9px 12px 8px;
  min-width: 148px;
  cursor: pointer;
  transition: box-shadow 0.18s, opacity 0.25s;
}
.node.selected {
  border-color: var(--pico-ink-3);
}
.node.propose {
  box-shadow: 0 0 0 3px rgb(31 42 55 / 0.14);
}
.node.apply {
  box-shadow: 0 0 0 3px rgb(26 127 55 / 0.16);
}
.node.dead {
  opacity: 0.45;
  border-style: dashed;
}
.node.booting,
.node.restoring {
  border-style: dashed;
}
.node-head {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 5px;
}
.node-name {
  font-weight: 600;
  color: var(--pico-ink-1);
  font-size: 13px;
}
.lease {
  width: 7px;
  height: 7px;
  background: #1a7f37;
  flex: none;
}
.node-state {
  margin-left: auto;
  font-size: 10.5px;
  color: var(--pico-ink-5, #8b929a);
}
.node-actions {
  margin-top: 5px;
  min-height: 16px;
}
.linkbtn {
  border: none;
  background: none;
  padding: 0;
  font-size: 11.5px;
  color: var(--pico-ink-4);
  cursor: pointer;
  text-decoration: underline;
  text-underline-offset: 2px;
}
.linkbtn.danger {
  color: #b3261e;
}
.linkbtn:disabled {
  opacity: 0.4;
  cursor: default;
}

.stagebar {
  display: flex;
  justify-content: center;
  gap: 34px;
  padding: 0 14px 14px;
  color: var(--pico-ink-5, #a8aeb5);
  font-size: 11px;
}
.stagegroup {
  display: inline-flex;
  gap: 7px;
  align-items: baseline;
}
.stage.on {
  color: var(--pico-accent, #1f2a37);
  font-weight: 700;
}
.sep {
  color: var(--pico-ink-6, #d4d8dc);
}

/* Panels */
.panels {
  display: grid;
  grid-template-columns: minmax(0, 5fr) minmax(0, 7fr);
  border-top: 1px solid var(--pico-hairline);
  background: var(--pico-surface-0, #fff);
}
@media (max-width: 860px) {
  .panels {
    grid-template-columns: 1fr;
  }
  .canvas {
    grid-template-columns: 1fr;
  }
  .wires {
    display: none;
  }
}
.timeline {
  border-right: 1px solid var(--pico-hairline);
  padding: 12px 14px;
  min-height: 300px;
}
.timeline-scroll {
  margin-top: 8px;
  max-height: 420px;
  overflow-y: auto;
}
.tl-event {
  display: flex;
  gap: 8px;
  align-items: baseline;
  padding: 5px 0;
  border-bottom: 1px solid var(--pico-hairline);
  font-size: 12.5px;
  line-height: 1.5;
}
.tl-dot {
  width: 6px;
  height: 6px;
  flex: none;
  align-self: center;
}
.tl-dot.metadata {
  background: #0969da;
}
.tl-dot.data {
  background: #bf3989;
}
.tl-dot.lifecycle {
  background: #1a7f37;
}
.tl-dot.storage {
  background: #9a6700;
}
.tl-at {
  color: var(--pico-ink-6, #b0b6bd);
  flex: none;
}
.tl-text {
  color: var(--pico-ink-2);
}

.inspector {
  padding: 12px 14px;
  min-width: 0;
}
.tabs {
  display: flex;
  gap: 2px;
  border-bottom: 1px solid var(--pico-hairline);
  margin-bottom: 10px;
}
.tabs button {
  border: none;
  background: none;
  padding: 6px 11px;
  font-size: 12.5px;
  color: var(--pico-ink-4);
  cursor: pointer;
  border-bottom: 2px solid transparent;
  margin-bottom: -1px;
}
.tabs button.on {
  color: var(--pico-ink-1);
  border-bottom-color: var(--pico-accent, #1f2a37);
  font-weight: 600;
}
.tabbody {
  max-height: 430px;
  overflow-y: auto;
}
.mem-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 4px;
}
.mini {
  width: 100%;
  border-collapse: collapse;
  font-size: 12.5px;
  margin: 0;
}
.mini th {
  text-align: left;
  font-weight: 600;
  color: var(--pico-ink-4);
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 4px 8px;
  border-bottom: 1px solid var(--pico-hairline);
}
.mini td {
  padding: 4px 8px;
  border-bottom: 1px solid var(--pico-hairline);
  color: var(--pico-ink-2);
}
.mini .num {
  font-variant-numeric: tabular-nums;
}
.mini.clickable tbody tr {
  cursor: pointer;
}
.mini.clickable tbody tr:hover {
  background: var(--pico-surface-2, #f2f3f5);
}
</style>
