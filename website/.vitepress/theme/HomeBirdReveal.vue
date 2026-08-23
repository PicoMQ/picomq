<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue';

const active = ref(false);
const mx = ref('50%');
const my = ref('40%');

let reduced = false;
let birdLeft = 0;
let birdTop = 0;
let birdW = 0;
let birdH = 0;

function measure() {
  const root = document.documentElement;
  const styles = getComputedStyle(root);
  const nav = parseFloat(styles.getPropertyValue('--vp-nav-height')) || 64;
  const edge = styles.getPropertyValue('--pico-nav-edge').trim() || '32px';
  const w = styles.getPropertyValue('--pico-bird-w').trim();
  const h = styles.getPropertyValue('--pico-bird-h').trim();

  const probe = document.createElement('div');
  probe.style.cssText = `position:fixed;visibility:hidden;width:${w};height:${h};right:${edge};top:${nav}px;`;
  document.body.appendChild(probe);
  const rect = probe.getBoundingClientRect();
  probe.remove();

  birdLeft = rect.left;
  birdTop = rect.top;
  birdW = rect.width;
  birdH = rect.height;
}

function onMove(event: PointerEvent) {
  if (reduced || birdW <= 0) {
    return;
  }

  const x = event.clientX;
  const y = event.clientY;
  const pad = 48;
  const inside =
    x >= birdLeft - pad &&
    x <= birdLeft + birdW + pad &&
    y >= birdTop - pad &&
    y <= birdTop + birdH + pad;

  if (!inside) {
    active.value = false;
    return;
  }

  active.value = true;
  mx.value = `${((x - birdLeft) / birdW) * 100}%`;
  my.value = `${((y - birdTop) / birdH) * 100}%`;
}

function onLeave() {
  active.value = false;
}

onMounted(() => {
  reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (reduced || window.matchMedia('(max-width: 639px)').matches) {
    return;
  }

  measure();
  window.addEventListener('pointermove', onMove, { passive: true });
  window.addEventListener('resize', measure, { passive: true });
  document.addEventListener('mouseleave', onLeave);
});

onBeforeUnmount(() => {
  window.removeEventListener('pointermove', onMove);
  window.removeEventListener('resize', measure);
  document.removeEventListener('mouseleave', onLeave);
});
</script>

<template>
  <div
    class="pico-bird-color"
    aria-hidden="true"
    :class="{ active }"
    :style="{ '--pico-bird-mx': mx, '--pico-bird-my': my }"
  />
</template>
