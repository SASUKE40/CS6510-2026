<script setup lang="ts">
import { onSlideEnter, onSlideLeave } from '@slidev/client'
import { computed, onMounted, ref, watch } from 'vue'

interface Run {
  label: string
  note: string
  highlight?: boolean
  values: Record<Metric, [number, number]>
}
type Metric = 'tx' | 'scans' | 'p50'

const metrics: { id: Metric; title: string; unit: string; better: string }[] = [
  { id: 'tx', title: 'Completed tx / s', unit: '', better: 'higher is better' },
  { id: 'scans', title: 'Scans / s', unit: '', better: 'higher is better' },
  { id: 'p50', title: 'Complete p50 (ms)', unit: ' ms', better: 'lower is better' },
]

// [default 10 × 60 s, stress 100 × 120 s], submitted runs from comparison/data/performance.json.
const runs: Run[] = [
  {
    label: 'A1 Monolith',
    note: 'one process, one SQLite',
    values: { tx: [255.4, 194.4], scans: [5326.7, 5397.3], p50: [1.6, 15.7] },
  },
  {
    label: 'A2 Layered',
    note: 'layers in one process',
    values: { tx: [254.3, 192.3], scans: [5274.0, 5358.4], p50: [1.7, 16.1] },
  },
  {
    label: 'A3 Pipeline',
    note: 'five filters, bounded channels',
    values: { tx: [246.2, 198.1], scans: [5129.8, 5599.6], p50: [1.6, 14.0] },
  },
  {
    label: 'A4 Service-based',
    note: 'four services, one shared SQLite',
    values: { tx: [252.2, 185.9], scans: [5200.3, 5042.6], p50: [2.3, 2.4] },
  },
  {
    label: 'A5 Microservices',
    note: 'database per service, HTTP saga',
    values: { tx: [258.2, 166.9], scans: [5488.0, 4092.0], p50: [4.7, 32.3] },
  },
  {
    label: 'A6 Event-based',
    note: 'Kafka between services',
    highlight: true,
    values: { tx: [220.7, 231.7], scans: [4273.6, 7238.7], p50: [9.4, 23.3] },
  },
]

const metric = ref<Metric>('tx')
const current = computed(() => metrics.find((m) => m.id === metric.value)!)
const max = computed(() => Math.max(...runs.flatMap((r) => r.values[metric.value])))

const grown = ref(false)
const shown = ref<number[][]>(runs.map(() => [0, 0]))
let raf = 0

function animateTo(target: number[][]) {
  cancelAnimationFrame(raf)
  const from = shown.value.map((r) => [...r])
  const start = performance.now()
  const step = (t: number) => {
    const k = Math.min((t - start) / 700, 1)
    const e = 1 - (1 - k) ** 3
    shown.value = from.map((r, i) => r.map((v, j) => v + (target[i][j] - v) * e))
    if (k < 1) raf = requestAnimationFrame(step)
  }
  raf = requestAnimationFrame(step)
}

const target = () => runs.map((r) => [...r.values[metric.value]])

function play() {
  grown.value = false
  shown.value = runs.map(() => [0, 0])
  requestAnimationFrame(() => {
    grown.value = true
    animateTo(target())
  })
}

watch(metric, () => animateTo(target()))
onMounted(play)
onSlideEnter(play)
onSlideLeave(() => {
  grown.value = false
})

function width(v: number) {
  if (!grown.value) return '0%'
  return `${Math.max((v / max.value) * 100, 0.8)}%`
}
function format(v: number) {
  return v >= 100 ? Math.round(v).toLocaleString('en-US') : v.toFixed(1)
}
</script>

<template>
  <div class="chart" @keydown.stop>
    <div class="toolbar">
      <button
        v-for="m in metrics"
        :key="m.id"
        class="ctl"
        :class="{ active: m.id === metric }"
        @click="metric = m.id"
      >
        {{ m.title }}
      </button>
      <span class="hint">{{ current.better }}</span>
      <span class="grow" />
      <span class="legend"><i class="sw default" /> default (10 stations)</span>
      <span class="legend"><i class="sw stress" /> stress (100 stations)</span>
    </div>
    <div v-for="(run, i) in runs" :key="run.label" class="run" :class="{ highlight: run.highlight }">
      <div class="label">
        <div class="name">{{ run.label }}</div>
        <div class="note">{{ run.note }}</div>
      </div>
      <div class="bars">
        <div class="bar default" :style="{ width: width(run.values[metric][0]), transitionDelay: `${i * 70}ms` }">
          <span>{{ format(shown[i][0]) }}{{ current.unit }}</span>
        </div>
        <div class="bar stress" :style="{ width: width(run.values[metric][1]), transitionDelay: `${i * 70 + 35}ms` }">
          <span>{{ format(shown[i][1]) }}{{ current.unit }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.chart {
  font-size: 0.8rem;
}
.toolbar {
  display: flex;
  gap: 0.4rem;
  align-items: center;
  margin-bottom: 0.8rem;
}
.grow {
  flex: 1;
}
.hint {
  color: var(--muted);
  margin-left: 0.4rem;
}
.legend {
  margin-left: 0.7rem;
  color: var(--text-2);
}
.sw {
  display: inline-block;
  width: 12px;
  height: 12px;
  border-radius: 3px;
  vertical-align: middle;
}
.run {
  display: grid;
  grid-template-columns: 13.5rem 1fr;
  gap: 0.7rem;
  align-items: center;
  padding: 0.3rem 0.6rem;
  margin-bottom: 0.2rem;
  line-height: 1.3;
  border: 1.5px solid transparent;
  border-radius: 10px;
}
.run.highlight {
  border-color: var(--orange);
}
.name {
  font-weight: 700;
}
.run.highlight .name {
  color: var(--orange);
}
.note {
  font-size: 0.7rem;
  color: var(--muted);
}
.bars {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding-right: 4.8rem;
}
.bar {
  height: 14px;
  border-radius: 4px;
  position: relative;
  transition: width 0.8s cubic-bezier(0.2, 0.7, 0.2, 1);
}
.bar span {
  position: absolute;
  left: calc(100% + 8px);
  top: -1px;
  white-space: nowrap;
  font-family: var(--mono);
  font-size: 0.7rem;
  color: var(--text);
}
.default,
.sw.default {
  background: transparent;
  box-shadow: inset 0 0 0 1.75px var(--blue);
  background-image: linear-gradient(90deg, rgba(47, 128, 237, 0.55), rgba(47, 128, 237, 0.25));
}
.stress,
.sw.stress {
  box-shadow: inset 0 0 0 1.75px var(--orange);
  background-image: linear-gradient(90deg, rgba(255, 121, 0, 0.55), rgba(255, 121, 0, 0.25));
}
</style>
