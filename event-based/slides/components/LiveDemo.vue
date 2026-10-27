<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { runStep, steps } from './demo-steps.js'
import recorded from '../assets/demo-recorded.json'

interface Result {
  id: string
  method: string
  path: string
  status: number
  ms: number
  body: unknown
}

const BASE = '/api'
const mode = ref<'checking' | 'live' | 'recorded'>('checking')
const results = ref<Record<string, Result>>({})
const selected = ref<string>('')
const running = ref('')
const ctx = { tx: '' }

const recordedById = Object.fromEntries((recorded.results as Result[]).map((r) => [r.id, r]))
const shown = computed(() => results.value[selected.value])

async function detect() {
  mode.value = 'checking'
  if (!import.meta.env.DEV) {
    mode.value = 'recorded'
    return
  }
  try {
    const response = await fetch(`${BASE}/health`, { signal: AbortSignal.timeout(1000) })
    mode.value = response.ok ? 'live' : 'recorded'
  } catch {
    mode.value = 'recorded'
  }
}

async function run(index: number) {
  const step = steps[index]
  running.value = step.id
  try {
    if (mode.value === 'live') {
      if (step.id !== 'start' && !ctx.tx) await run(0)
      results.value[step.id] = { id: step.id, ...(await runStep(step, ctx, BASE)) }
    } else {
      await new Promise((resolve) => setTimeout(resolve, 150))
      results.value[step.id] = recordedById[step.id]
    }
    selected.value = step.id
  } catch (error) {
    mode.value = 'recorded'
    results.value[step.id] = { ...recordedById[step.id] }
    selected.value = step.id
  } finally {
    running.value = ''
  }
}

async function runAll() {
  results.value = {}
  ctx.tx = ''
  for (let i = 0; i < steps.length; i++) await run(i)
}

function statusClass(status: number) {
  return status >= 200 && status < 300 ? 'ok' : 'bad'
}

function pretty(body: unknown) {
  const text = JSON.stringify(body, null, 2)
  return text.length > 1600 ? `${text.slice(0, 1600)}\n…` : text
}

onMounted(detect)
</script>

<template>
  <div class="demo" @keydown.stop>
    <div class="toolbar">
      <span class="mode" :class="mode">
        {{ mode === 'live' ? '● LIVE · gateway :8080 via /api proxy' : mode === 'recorded' ? '● RECORDED · ' + recorded.recordedAt.slice(0, 16).replace('T', ' ') + ' UTC' : '… checking gateway' }}
      </span>
      <button class="ctl" @click="detect">Check again</button>
      <span class="grow" />
      <button class="ctl primary" :disabled="!!running" @click="runAll">Run all ▶</button>
    </div>
    <div class="body">
      <ol class="steps">
        <li v-for="(step, i) in steps" :key="step.id" :class="{ active: selected === step.id, done: !!results[step.id] }">
          <button class="ctl" :disabled="!!running" @click="run(i)">{{ step.title }}</button>
          <span v-if="running === step.id" class="spinner" />
          <Transition name="pop">
            <span v-if="running !== step.id && results[step.id]" class="result">
              <span class="status" :class="statusClass(results[step.id].status)">{{ results[step.id].status }}</span>
              <span class="ms">{{ results[step.id].ms.toFixed(1) }} ms</span>
            </span>
          </Transition>
        </li>
      </ol>
      <div class="panel">
        <Transition name="swap" mode="out-in">
          <div v-if="shown" :key="shown.id">
            <div class="request"><b>{{ shown.method }}</b> {{ shown.path }}</div>
            <pre>{{ pretty(shown.body) }}</pre>
          </div>
          <div v-else class="muted placeholder">Press a step, or “Run all”.</div>
        </Transition>
      </div>
    </div>
  </div>
</template>

<style scoped>
.demo {
  font-size: 0.8rem;
}
.toolbar {
  display: flex;
  gap: 0.5rem;
  align-items: center;
  margin-bottom: 0.7rem;
}
.grow {
  flex: 1;
}
.mode {
  font-family: var(--mono);
  font-size: 0.72rem;
  font-weight: 700;
  border: 1.5px solid currentColor;
  border-radius: 8px;
  padding: 0.12rem 0.55rem;
}
.mode.live {
  color: var(--green);
  animation: pulse-green 1.8s ease-out infinite;
}
.mode.recorded {
  color: var(--orange);
}
.mode.checking {
  color: var(--muted);
}
@keyframes pulse-green {
  0% { box-shadow: 0 0 0 0 rgba(22, 200, 62, 0.45); }
  100% { box-shadow: 0 0 0 9px rgba(22, 200, 62, 0); }
}
.body {
  display: grid;
  grid-template-columns: 16rem 1fr;
  gap: 0.9rem;
}
.steps {
  list-style: none;
  padding: 0;
  margin: 0;
  border-left: 1.5px solid var(--divider);
}
.steps li {
  position: relative;
  display: flex;
  gap: 0.45rem;
  align-items: center;
  padding: 0.14rem 0 0.14rem 0.8rem;
  margin: 0 !important;
}
.steps li::before {
  content: '';
  position: absolute;
  left: -5px;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #000;
  border: 1.5px solid var(--divider);
  transition: background 0.3s, border-color 0.3s;
}
.steps li.done::before {
  background: var(--green);
  border-color: var(--green);
}
.steps li.active::before {
  background: var(--orange);
  border-color: var(--orange);
}
.steps li.active .ctl {
  border-color: var(--orange);
  color: var(--orange);
}
.steps li .ctl {
  width: 9.4rem;
  text-align: left;
  font-size: 0.76rem;
  padding: 0.1rem 0.55rem;
}
.result {
  display: inline-flex;
  gap: 0.4rem;
  align-items: baseline;
}
.status {
  font-family: var(--mono);
  font-weight: 700;
}
.status.ok {
  color: var(--green);
}
.status.bad {
  color: var(--red);
}
.ms {
  font-family: var(--mono);
  font-size: 0.7rem;
  color: var(--text-2);
}
.spinner {
  width: 12px;
  height: 12px;
  border: 2px solid var(--divider);
  border-top-color: var(--orange);
  border-radius: 50%;
  animation: spin 0.7s linear infinite;
}
@keyframes spin {
  to { transform: rotate(360deg); }
}
.panel {
  border: 1.5px solid var(--divider);
  border-radius: 12px;
  padding: 0.5rem 0.7rem;
  min-height: 300px;
}
.request {
  font-family: var(--mono);
  font-size: 0.74rem;
  margin-bottom: 0.35rem;
  color: var(--text-2);
}
.request b {
  color: var(--blue);
}
pre {
  font-family: var(--mono);
  font-size: 0.72rem;
  line-height: 1.5;
  max-height: 290px;
  overflow: auto;
  margin: 0;
  color: var(--text);
  background: transparent;
  padding: 0.2rem 0;
}
.muted {
  color: var(--muted);
}
.placeholder {
  padding: 2rem;
  text-align: center;
}
.pop-enter-active {
  transition: opacity 0.3s, transform 0.3s cubic-bezier(0.2, 0.7, 0.2, 1.4);
}
.pop-enter-from {
  opacity: 0;
  transform: scale(0.6);
}
.swap-enter-active,
.swap-leave-active {
  transition: opacity 0.18s, transform 0.18s;
}
.swap-enter-from {
  opacity: 0;
  transform: translateY(6px);
}
.swap-leave-to {
  opacity: 0;
}
</style>
