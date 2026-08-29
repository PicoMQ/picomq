<script setup lang="ts">
import { computed, ref } from 'vue';
import type { Seg } from './bytes';

const props = defineProps<{ segs: Seg[]; title?: string }>();

const KIND_COLORS: Record<string, string> = {
  magic: '#8250df',
  version: '#8250df',
  type: '#bf3989',
  int: '#0969da',
  offset: '#1a7f37',
  len: '#9a6700',
  str: '#cf222e',
  blob: '#cf222e',
  crc: '#d4691e',
  payload: '#cf222e',
  count: '#9a6700',
};

const hovered = ref<number | null>(null);

interface Cell {
  hex: string;
  segIdx: number;
  color: string;
}

const cells = computed<Cell[]>(() => {
  const out: Cell[] = [];
  props.segs.forEach((seg, segIdx) => {
    for (const b of seg.bytes) {
      out.push({
        hex: b.toString(16).padStart(2, '0'),
        segIdx,
        color: KIND_COLORS[seg.kind] ?? '#57606a',
      });
    }
  });
  return out;
});

const rows = computed(() => {
  const out: { offset: number; cells: Cell[] }[] = [];
  for (let i = 0; i < cells.value.length; i += 16) {
    out.push({ offset: i, cells: cells.value.slice(i, i + 16) });
  }
  return out;
});

const totalBytes = computed(() => cells.value.length);
</script>

<template>
  <div class="hexview">
    <div class="hexview-head">
      <span v-if="title" class="hexview-title">{{ title }}</span>
      <span class="hexview-size">{{ totalBytes }} bytes</span>
    </div>
    <div class="hexview-body">
      <div class="hexgrid">
        <div v-for="row in rows" :key="row.offset" class="hexrow">
          <span class="hexoff">{{ row.offset.toString(16).padStart(4, '0') }}</span>
          <span
            v-for="(cell, i) in row.cells"
            :key="i"
            class="hexbyte"
            :class="{ dim: hovered !== null && hovered !== cell.segIdx }"
            :style="{ color: cell.color }"
            @mouseenter="hovered = cell.segIdx"
            @mouseleave="hovered = null"
            >{{ cell.hex }}</span
          >
        </div>
      </div>
      <div class="hexlegend">
        <div
          v-for="(seg, i) in segs"
          :key="i"
          class="hexfield"
          :class="{ hot: hovered === i }"
          @mouseenter="hovered = i"
          @mouseleave="hovered = null"
        >
          <span class="swatch" :style="{ background: KIND_COLORS[seg.kind] ?? '#57606a' }" />
          <span class="fname">{{ seg.label }}</span>
          <span class="fval">{{ seg.value }}</span>
          <span class="flen">{{ seg.bytes.length }} B</span>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.hexview {
  border: 1px solid var(--pico-hairline);
  background: var(--pico-surface-0, #fff);
  font-size: 12px;
  overflow: hidden;
}
.hexview-head {
  display: flex;
  justify-content: space-between;
  padding: 6px 10px;
  border-bottom: 1px solid var(--pico-hairline);
  background: var(--pico-surface-1, #fafafa);
}
.hexview-title {
  font-weight: 600;
  color: var(--pico-ink-2);
}
.hexview-size {
  color: var(--pico-ink-4);
  font-family: var(--vp-font-family-mono);
}
.hexview-body {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
}
@media (max-width: 720px) {
  .hexview-body {
    grid-template-columns: 1fr;
  }
}
.hexgrid {
  padding: 8px 10px;
  font-family: var(--vp-font-family-mono);
  line-height: 1.7;
  overflow-x: auto;
  border-right: 1px solid var(--pico-hairline);
}
.hexrow {
  white-space: nowrap;
}
.hexoff {
  color: var(--pico-ink-6, #b0b6bd);
  margin-right: 10px;
  user-select: none;
}
.hexbyte {
  margin-right: 5px;
  cursor: default;
  transition: opacity 0.12s;
}
.hexbyte.dim {
  opacity: 0.22;
}
.hexlegend {
  padding: 6px 8px;
  max-height: 300px;
  overflow-y: auto;
}
.hexfield {
  display: flex;
  align-items: baseline;
  gap: 7px;
  padding: 2px 6px;
  cursor: default;
}
.hexfield.hot {
  background: var(--pico-surface-2, #f2f3f5);
}
.swatch {
  width: 8px;
  height: 8px;
  flex: none;
  align-self: center;
}
.fname {
  color: var(--pico-ink-2);
  white-space: pre;
}
.fval {
  color: var(--pico-ink-4);
  font-family: var(--vp-font-family-mono);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  flex: 1;
  min-width: 0;
}
.flen {
  color: var(--pico-ink-6, #b0b6bd);
  font-family: var(--vp-font-family-mono);
  flex: none;
}
</style>
