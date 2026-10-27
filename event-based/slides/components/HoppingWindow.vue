<script setup lang="ts">
import { computed, ref } from 'vue'

const MAX = 4000
const SKUS = 12

function mulberry32(seed: number) {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

const weights = Array.from({ length: SKUS }, (_, k) => 1 / (k + 1))
const totalWeight = weights.reduce((a, b) => a + b, 0)
const random = mulberry32(6510)
const stream: string[] = [''].concat(
  Array.from({ length: MAX }, () => {
    let r = random() * totalWeight
    let k = 0
    while (r > weights[k] && k < SKUS - 1) r -= weights[k++]
    return `SKU-${String(k + 1).padStart(6, '0')}`
  }),
)

const size = ref(1000)
const slide = ref(500)
const cursor = ref(1499)
const log = ref('')

const end = computed(() => Math.floor(cursor.value / slide.value) * slide.value)
const start = computed(() => (end.value > 0 ? Math.max(end.value - size.value + 1, 1) : 0))
const trimmed = computed(() => Math.max(end.value - size.value, 0))

const top = computed(() => {
  if (end.value === 0) return []
  const counts = new Map<string, number>()
  for (let s = start.value; s <= end.value; s++) counts.set(stream[s], (counts.get(stream[s]) ?? 0) + 1)
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 5)
    .map(([sku, n], i) => ({ rank: i + 1, sku, n }))
})

const pct = (n: number) => `${(n / MAX) * 100}%`
const ticks = computed(() => {
  const out: number[] = []
  for (let s = slide.value; s <= MAX; s += slide.value) out.push(s)
  return out
})

function deliver(kind: 'next' | 'duplicate' | 'gap') {
  const c = cursor.value
  if (kind === 'next') {
    if (c >= MAX) return
    const before = end.value
    cursor.value = c + 1
    log.value =
      end.value > before
        ? `seq ${c + 1} accepted · boundary crossed → publish [${start.value}, ${end.value}], trim ≤ ${trimmed.value}`
        : `seq ${c + 1} accepted · cursor = ${c + 1} · no new hop`
  } else if (kind === 'duplicate') {
    log.value = `seq ${Math.max(c - 2, 1)} ≤ cursor ${c} → skipped (already counted)`
  } else {
    log.value = `expected ${c + 1}, got ${c + 2} → batch stops · cursor stays ${c} · relay re-sends ${c + 1}`
  }
}

function onSlider() {
  log.value = ''
}

const logKind = computed(() => {
  if (log.value.includes('boundary')) return 'publish'
  if (log.value.includes('skipped')) return 'skip'
  if (log.value.includes('stops')) return 'gap'
  return ''
})
</script>

<template>
  <div class="hw" @keydown.stop>
    <div class="controls">
      <label>
        last sequence (cursor)
        <input v-model.number="cursor" type="range" min="0" :max="MAX" step="1" @input="onSlider" />
        <b class="mono">{{ cursor }}</b>
      </label>
      <label>
        size
        <select v-model.number="size">
          <option :value="500">500</option>
          <option :value="1000">1000</option>
          <option :value="2000">2000</option>
        </select>
      </label>
      <label>
        slide
        <select v-model.number="slide">
          <option :value="250">250</option>
          <option :value="500">500</option>
          <option :value="1000">1000</option>
        </select>
      </label>
    </div>

    <div class="track">
      <div class="seg trimmed" :style="{ left: 0, width: pct(trimmed) }" />
      <div class="seg window" :style="{ left: pct(start - 1), width: pct(end - start + 1) }" v-if="end > 0" />
      <div class="seg pending" :style="{ left: pct(end), width: pct(cursor - end) }" />
      <div v-for="t in ticks" :key="t" class="tick" :style="{ left: pct(t) }">
        <span>{{ t }}</span>
      </div>
      <div class="cursor" :style="{ left: pct(cursor) }" />
    </div>

    <div class="legend">
      <span><i class="sw trimmed" /> deleted (≤ {{ trimmed }})</span>
      <span><i class="sw window" /> published window
        <b class="mono">{{ end > 0 ? `[${start}, ${end}]` : 'none yet' }}</b></span>
      <span><i class="sw pending" /> stored, not yet published ({{ cursor > end ? `${end + 1}…${cursor}` : 'none' }})</span>
    </div>

    <div class="bottom">
      <div class="top5">
        <div class="top5-head"><span>rank</span><span>sku</span><span>scanCount</span></div>
        <TransitionGroup name="rank" tag="div" class="top5-body">
          <div v-for="row in top" :key="row.sku" class="top5-row">
            <span class="rank">{{ row.rank }}</span>
            <span class="mono">{{ row.sku }}</span>
            <span class="count">
              <i class="meter" :style="{ width: `${(row.n / (top[0]?.n || 1)) * 100}%` }" />
              <b class="mono">{{ row.n }}</b>
            </span>
          </div>
        </TransitionGroup>
        <div v-if="top.length === 0" class="muted empty">no window published yet</div>
      </div>
      <div class="ingest">
        <div class="buttons">
          <button class="ctl primary" @click="deliver('next')">Deliver next event</button>
          <button class="ctl" @click="deliver('duplicate')">Deliver duplicate</button>
          <button class="ctl" @click="deliver('gap')">Deliver with gap</button>
        </div>
        <Transition name="log" mode="out-in">
          <div :key="log" class="log mono" :class="logKind">
            {{ log || 'Ingest rule: skip seq ≤ cursor · stop at seq ≠ cursor + 1' }}
          </div>
        </Transition>
      </div>
    </div>
  </div>
</template>

<style scoped>
.hw {
  font-size: 0.9rem;
}
.muted {
  color: var(--muted);
}
.controls {
  display: flex;
  gap: 1.5rem;
  align-items: center;
  margin-bottom: 0.9rem;
  color: var(--text-2);
}
.controls label {
  display: flex;
  gap: 0.45rem;
  align-items: center;
}
.controls b {
  color: var(--orange);
  min-width: 2.6rem;
}
.controls input[type='range'] {
  width: 260px;
  accent-color: var(--orange);
}
select {
  background: #000;
  color: var(--text);
  border: 1.25px solid var(--divider);
  border-radius: 6px;
  padding: 0.05rem 0.3rem;
}
.track {
  position: relative;
  height: 34px;
  border: 1.5px solid var(--divider);
  border-radius: 8px;
  margin: 0 0 1.5rem;
}
.seg {
  position: absolute;
  top: 3px;
  bottom: 3px;
  border-radius: 4px;
  transition: left 0.18s ease-out, width 0.18s ease-out;
}
.trimmed,
.sw.trimmed {
  background: repeating-linear-gradient(45deg, transparent, transparent 4px, #484848 4px, #484848 6px);
}
.window,
.sw.window {
  background: rgba(47, 128, 237, 0.35);
  box-shadow: inset 0 0 0 1.5px var(--blue);
}
.pending,
.sw.pending {
  box-shadow: inset 0 0 0 1.5px var(--line);
  background: repeating-linear-gradient(90deg, transparent, transparent 6px, rgba(189, 189, 189, 0.18) 6px, rgba(189, 189, 189, 0.18) 8px);
}
.tick {
  position: absolute;
  top: 0;
  bottom: -14px;
  border-left: 1px solid var(--divider);
}
.tick span {
  position: absolute;
  top: 38px;
  transform: translateX(-50%);
  font-size: 0.6rem;
  color: var(--muted);
}
.cursor {
  position: absolute;
  top: -7px;
  bottom: -7px;
  width: 3px;
  margin-left: -1px;
  background: var(--orange);
  border-radius: 2px;
  transition: left 0.18s ease-out;
}
.cursor::after {
  content: '';
  position: absolute;
  top: -6px;
  left: -4px;
  width: 11px;
  height: 11px;
  border-radius: 50%;
  background: var(--orange);
  animation: pulse-ring 1.6s ease-out infinite;
}
.legend {
  display: flex;
  gap: 1.2rem;
  margin-bottom: 0.8rem;
  color: var(--text-2);
}
.legend b {
  color: var(--blue);
}
.sw {
  display: inline-block;
  width: 14px;
  height: 12px;
  border-radius: 3px;
  vertical-align: middle;
}
.bottom {
  display: grid;
  grid-template-columns: 1fr 1.4fr;
  gap: 1.1rem;
  align-items: start;
}
.top5 {
  border: 1.5px solid var(--divider);
  border-radius: 10px;
  padding: 0.35rem 0.6rem;
  font-size: 0.76rem;
}
.top5-head,
.top5-row {
  display: grid;
  grid-template-columns: 2.4rem 6.6rem 1fr;
  align-items: center;
  gap: 0.4rem;
}
.top5-head {
  color: var(--text-2);
  font-size: 0.62rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  border-bottom: 1.25px solid var(--line);
  padding-bottom: 0.2rem;
  margin-bottom: 0.15rem;
}
.top5-body {
  position: relative;
}
.top5-row {
  padding: 0.12rem 0;
  border-bottom: 1px solid #1f1f1f;
}
.rank {
  color: var(--orange);
  font-weight: 700;
}
.count {
  display: flex;
  align-items: center;
  gap: 0.4rem;
}
.meter {
  display: block;
  height: 6px;
  border-radius: 3px;
  background: var(--blue);
  transition: width 0.35s cubic-bezier(0.2, 0.7, 0.2, 1);
}
.empty {
  padding: 0.4rem 0;
}
.rank-move,
.rank-enter-active,
.rank-leave-active {
  transition: transform 0.4s cubic-bezier(0.2, 0.7, 0.2, 1), opacity 0.3s;
}
.rank-enter-from,
.rank-leave-to {
  opacity: 0;
  transform: translateX(-16px);
}
.rank-leave-active {
  position: absolute;
  width: 100%;
}
.buttons {
  display: flex;
  gap: 0.4rem;
  margin-bottom: 0.55rem;
}
.log {
  font-size: 0.72rem;
  padding: 0.45rem 0.7rem;
  border: 1.5px solid var(--divider);
  border-left: 3px solid var(--orange);
  border-radius: 10px;
  min-height: 2.6rem;
  color: var(--text-2);
}
.log.publish {
  border-left-color: var(--blue);
  color: var(--text);
}
.log.skip {
  border-left-color: var(--green);
}
.log.gap {
  border-left-color: var(--red);
}
.log-enter-active,
.log-leave-active {
  transition: opacity 0.2s, transform 0.2s;
}
.log-enter-from {
  opacity: 0;
  transform: translateY(6px);
}
.log-leave-to {
  opacity: 0;
}
</style>
