<script setup lang="ts">
import { computed, ref } from 'vue';

const features = [
  {
    name: 'Unlimited streams',
    body: 'Create a stream per use case instead of packing every record of a kind into one topic. Each stream is independently addressable, bottomless, and can scale from idle to high throughput.',
  },
  {
    name: 'Zero-disk architecture',
    body: 'The WAL and stream data live on S3-compatible object storage. Inherit object-store durability and economics without cross-AZ traffic.',
  },
  {
    name: 'Decoupled layers',
    body: 'Storage, metadata, and protocol are separate layers. The data and metadata planes can evolve independently. A new protocol is a facade on the same engine without changing how bytes are stored.',
  },
  {
    name: 'High throughput',
    body: 'The write path is the battle-tested S3Stream storage engine. Throughput up to 100 MiB/s per stream. Experiment with the transparent benchmark suite.',
  },
  {
    name: 'Easy deployment',
    body: 'One process per node: storage engine, WAL, SQL metadata log, HTTP and Kafka frontends. Run locally with SQLite and a file bucket, or deploy to AWS, GCS, Fly.io and more as a single node or a multi-node cluster.',
  },
] as const;

const active = ref(0);
const current = computed(() => features[active.value]);

function select(index: number) {
  active.value = index;
}
</script>

<template>
  <section class="engine pico-wrap" aria-label="The architecture">
    <p class="engine__eyebrow">the architecture</p>

    <div class="engine__plate">
      <div class="engine__copy">
        <h3>{{ current.name }}</h3>
        <p class="engine__body">{{ current.body }}</p>
      </div>

      <div class="engine__tabs" role="tablist" aria-label="Engine features">
        <button
          v-for="(feature, index) in features"
          :key="feature.name"
          class="engine__tab"
          type="button"
          role="tab"
          :class="{ active: index === active }"
          :aria-selected="index === active"
          @click="select(index)"
        >
          <span class="engine__tab-name">{{ feature.name }}</span>
        </button>
      </div>
    </div>
  </section>
</template>

<style scoped>
.engine {
  position: relative;
  z-index: 1;
  margin: 0 auto 5rem;
}

.engine__eyebrow {
  margin: 0 0 1.25rem;
  font-family: var(--pico-font-serif);
  font-size: 1.5rem;
  font-weight: 400;
  letter-spacing: -0.005em;
  color: var(--pico-ink-1);
}

.engine__plate {
  border: 1px solid var(--pico-ink-6);
  background: var(--pico-surface-0);
}

.engine__copy {
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
  min-width: 0;
  padding: 2rem 1.5rem 1.75rem;
}

.engine__copy h3 {
  margin: 0;
  font-family: var(--pico-font-serif);
  font-size: 1.6rem;
  font-weight: 400;
  line-height: 1.15;
  letter-spacing: 0;
  color: var(--pico-ink-1);
}

.engine__body {
  margin: 0;
  max-width: 36rem;
  min-height: 4.8rem;
  font-size: 1rem;
  line-height: 1.62;
  color: var(--pico-ink-3);
}

.engine__tabs {
  display: grid;
  grid-template-columns: 1fr;
  gap: 1px;
  border-top: 1px solid var(--pico-ink-6);
  background: var(--pico-ink-6);
}

.engine__tab {
  display: grid;
  align-content: center;
  min-width: 0;
  padding: 0.95rem 1rem;
  border: 0;
  border-radius: 0;
  background: var(--pico-surface-0);
  color: var(--pico-ink-3);
  text-align: left;
  cursor: pointer;
}

.engine__tab:hover {
  background: var(--pico-surface-1);
  color: var(--pico-ink-1);
}

.engine__tab.active {
  background: var(--pico-ink-1);
  color: var(--pico-surface-0);
}

.engine__tab-name {
  min-width: 0;
  font-family: var(--vp-font-family-mono);
  font-size: 0.8125rem;
  font-weight: 400;
  line-height: 1.25;
}

@media (min-width: 860px) {
  .engine__copy {
    padding: 2.25rem 2rem;
  }

  .engine__tabs {
    grid-template-columns: repeat(5, minmax(0, 1fr));
  }
}
</style>
