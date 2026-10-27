<script setup lang="ts">
import IconBulb from '~icons/tabler/bulb'
import IconCode from '~icons/tabler/code'
import IconTopology from '~icons/tabler/topology-star-3'

const props = defineProps<{ part: number; minutes?: string }>()

const parts = [
  { n: 1, title: 'Architecture', color: 'var(--blue)', icon: IconTopology },
  { n: 2, title: 'Code walkthrough', color: 'var(--orange)', icon: IconCode },
  { n: 3, title: 'Reflections', color: 'var(--green)', icon: IconBulb },
]
const current = parts[props.part - 1] ?? parts[0]
</script>

<template>
  <div class="slidev-layout divider-layout">
    <div
      v-motion
      class="kicker"
      :style="{ color: current.color }"
      :initial="{ opacity: 0, x: -30 }"
      :enter="{ opacity: 1, x: 0, transition: { duration: 450 } }"
    >
      PART {{ current.n }} OF 3<span v-if="minutes"> · {{ minutes }}</span>
    </div>
    <div
      v-motion
      class="title vt-part"
      :style="{ borderColor: current.color }"
      :initial="{ opacity: 0, y: 24 }"
      :enter="{ opacity: 1, y: 0, transition: { delay: 120, duration: 500 } }"
    >
      <slot />
    </div>
    <div class="track">
      <template v-for="(p, i) in parts" :key="p.n">
        <div
          v-motion
          class="chip"
          :class="{ on: p.n === current.n, done: p.n < current.n }"
          :style="{ '--c': p.color }"
          :initial="{ opacity: 0, y: 16 }"
          :enter="{ opacity: 1, y: 0, transition: { delay: 300 + i * 110, duration: 400 } }"
        >
          <component :is="p.icon" class="icon" />
          <span>{{ p.n }} · {{ p.title }}</span>
        </div>
        <svg v-if="i < parts.length - 1" class="link" viewBox="0 0 60 12" aria-hidden="true">
          <line x1="2" y1="6" x2="50" y2="6" />
          <path d="M48 2 L56 6 L48 10" />
        </svg>
      </template>
    </div>
  </div>
</template>

<style scoped>
.divider-layout {
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 1.1rem;
  padding: 2.5rem 4rem;
}
.kicker {
  font-size: 0.85rem;
  font-weight: 700;
  letter-spacing: 0.14em;
}
.title {
  border-left: 3px solid;
  padding-left: 1.2rem;
}
.title :deep(h1) {
  font-size: 2.9rem;
  color: var(--text);
  margin: 0;
}
.title :deep(p) {
  color: var(--text-2);
  font-size: 1.1rem;
  margin: 0.5rem 0 0;
}
.track {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  margin-top: 1.6rem;
}
.chip {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  border: 1.5px solid var(--divider);
  border-radius: 10px;
  padding: 0.45rem 0.8rem;
  color: var(--muted);
  font-size: 0.9rem;
  transition: border-color 0.3s, color 0.3s;
}
.chip.done {
  color: var(--text-2);
}
.chip.on {
  border-color: var(--c);
  color: var(--c);
  font-weight: 700;
}
.link {
  width: 44px;
  height: 12px;
  fill: none;
  stroke: var(--divider);
  stroke-width: 1.5;
  stroke-linecap: round;
  stroke-linejoin: round;
}
.link line {
  stroke-dasharray: 5 6;
  animation: march 1.2s linear infinite;
}
</style>
