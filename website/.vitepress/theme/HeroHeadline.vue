<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue';

const WORDS = [
  'Object Storage',
  'Amazon S3',
  'Cloudflare R2',
  'Google Cloud Storage',
  'MinIO',
  'RustFS',
  'Tigris',
] as const;

const rotor = ref<HTMLElement | null>(null);
let timer: ReturnType<typeof setInterval> | undefined;
let flip: ReturnType<typeof setTimeout> | undefined;
let index = 0;

function widestWidth(el: HTMLElement) {
  const probe = el.cloneNode() as HTMLElement;
  probe.style.cssText =
    'position:absolute;visibility:hidden;white-space:nowrap;';
  el.parentElement?.appendChild(probe);

  let max = 0;
  for (const word of WORDS) {
    probe.textContent = word;
    max = Math.max(max, probe.offsetWidth);
  }

  probe.remove();
  el.style.minWidth = `${Math.ceil(max)}px`;
}

function swap(word: string) {
  const el = rotor.value;
  if (!el) {
    return;
  }

  el.classList.add('flip-out');
  if (flip) {
    clearTimeout(flip);
  }
  flip = window.setTimeout(() => {
    el.textContent = word;
    el.classList.remove('flip-out');
    el.classList.add('flip-in');
    void el.offsetWidth;
    el.classList.remove('flip-in');
  }, 220);
}

function show(next: number) {
  index = next % WORDS.length;
  swap(WORDS[index]);
}

onMounted(() => {
  const el = rotor.value;
  if (!el || window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    return;
  }

  widestWidth(el);
  if (document.fonts?.ready) {
    void document.fonts.ready.then(() => {
      if (rotor.value) {
        widestWidth(rotor.value);
      }
    });
  }

  timer = setInterval(() => show(index + 1), 2600);
});

onBeforeUnmount(() => {
  if (timer) {
    clearInterval(timer);
  }
  if (flip) {
    clearTimeout(flip);
  }
});
</script>

<template>
  <h1 class="heading">
    <span class="text">
      Durable streams on<br />
      <span ref="rotor" class="pico-hero-rotor">object storage</span>
    </span>
  </h1>
  <p class="tagline">
    PicoMQ is durable, real-time streams over HTTP and Kafka,<br />
    built on S3-compatible object storage.
  </p>
  <p class="pico-hero-tags">
    <span>Open source</span>
    <span>Built with Rust</span>
  </p>
</template>
