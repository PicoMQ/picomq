<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue';

const IMG_ASPECT = 610 / 780;
const FACE_FX = 0.45;
const FACE_FY = 0.42;

const faceX = ref('70%');
const faceY = ref('42%');
const faceBlobX = ref('14rem');
const faceBlobY = ref('17.5rem');

const cursorOn = ref(false);
const mx = ref('70%');
const my = ref('42%');
const blob = ref('12rem');

let reduced = false;
let birdLeft = 0;
let birdTop = 0;
let birdW = 0;
let birdH = 0;
let drawLeft = 0;
let drawTop = 0;
let drawW = 0;
let drawH = 0;

function layoutImage() {
  if (birdW / birdH > IMG_ASPECT) {
    drawH = birdH;
    drawW = birdH * IMG_ASPECT;
    drawLeft = birdW - drawW;
    drawTop = 0;
  } else {
    drawW = birdW;
    drawH = birdW / IMG_ASPECT;
    drawLeft = birdW - drawW;
    drawTop = 0;
  }
}

function placeFace() {
  if (drawW <= 0 || drawH <= 0) {
    return;
  }
  const narrow = window.matchMedia('(max-width: 959px)').matches;
  faceX.value = `${((drawLeft + FACE_FX * drawW) / birdW) * 100}%`;
  faceY.value = `${((drawTop + FACE_FY * drawH) / birdH) * 100}%`;
  faceBlobX.value = narrow ? '9rem' : '14rem';
  faceBlobY.value = narrow ? '12rem' : '17.5rem';
  blob.value = narrow ? '8rem' : '12rem';
}

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
  layoutImage();
  placeFace();
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
    cursorOn.value = false;
    return;
  }

  cursorOn.value = true;
  mx.value = `${((x - birdLeft) / birdW) * 100}%`;
  my.value = `${((y - birdTop) / birdH) * 100}%`;
}

function hideCursor() {
  cursorOn.value = false;
}

onMounted(() => {
  reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (window.matchMedia('(max-width: 639px)').matches) {
    return;
  }

  measure();
  if (reduced) {
    return;
  }

  window.addEventListener('pointermove', onMove, { passive: true });
  window.addEventListener('resize', measure, { passive: true });
  document.addEventListener('mouseleave', hideCursor);
});

onBeforeUnmount(() => {
  window.removeEventListener('pointermove', onMove);
  window.removeEventListener('resize', measure);
  document.removeEventListener('mouseleave', hideCursor);
});
</script>

<template>
  <div
    class="pico-bird-color pico-bird-color--face active"
    aria-hidden="true"
    :style="{
      '--pico-bird-mx': faceX,
      '--pico-bird-my': faceY,
      '--pico-bird-blob-x': faceBlobX,
      '--pico-bird-blob-y': faceBlobY,
    }"
  />
  <div
    class="pico-bird-color pico-bird-color--cursor"
    aria-hidden="true"
    :class="{ active: cursorOn }"
    :style="{
      '--pico-bird-mx': mx,
      '--pico-bird-my': my,
      '--pico-bird-blob-x': blob,
      '--pico-bird-blob-y': blob,
    }"
  />
</template>
