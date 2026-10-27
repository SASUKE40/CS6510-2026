<script setup lang="ts">
import { computed, ref } from 'vue'
import IconArrowBack from '~icons/tabler/arrow-back-up'
import IconDatabase from '~icons/tabler/database'
import IconUser from '~icons/tabler/user'
import { backward, forward, scenarios, type MessageKind, type SagaState } from './saga-scenarios'

const scenarioId = ref(scenarios[0].id)
const index = ref(0)
const redelivered = ref(false)

const scenario = computed(() => scenarios.find((s) => s.id === scenarioId.value) ?? scenarios[0])
const step = computed(() => scenario.value.steps[index.value])
const visited = computed(() => new Set(scenario.value.steps.slice(0, index.value + 1).map((s) => s.saga)))
const atEnd = computed(() => index.value === scenario.value.steps.length - 1)

function choose(id: string) {
  scenarioId.value = id
  index.value = 0
  redelivered.value = false
}

function move(delta: number) {
  const next = index.value + delta
  if (next < 0 || next >= scenario.value.steps.length) return
  index.value = next
  redelivered.value = false
}

function stateClass(state: SagaState) {
  if (step.value.saga === state) return 'state current'
  if (visited.value.has(state)) return 'state visited'
  return 'state'
}

function clientClass(client: string) {
  if (client.startsWith('200')) return 'ok'
  if (client.startsWith('4') || client.startsWith('5')) return 'bad'
  return ''
}

function kindLabel(kind: MessageKind) {
  switch (kind) {
    case 'http':
      return 'HTTP'
    case 'cmd':
      return 'COMMAND'
    case 'evt':
      return 'REPLY'
    case 'rej':
      return 'REJECTION'
    case 'local':
      return 'LOCAL'
    default: {
      const unknown: never = kind
      return unknown
    }
  }
}

const cards = computed(() => [
  { label: 'saga', db: 'payment.sqlite', value: step.value.saga, cls: 'saga' },
  { label: 'basket', db: 'basket.sqlite', value: step.value.basket, cls: '' },
  { label: 'reservation', db: 'inventory.sqlite', value: step.value.reservation, cls: '' },
])
</script>

<template>
  <div class="stepper" @keydown.stop>
    <div class="toolbar">
      <button
        v-for="s in scenarios"
        :key="s.id"
        class="ctl"
        :class="{ active: s.id === scenarioId }"
        @click="choose(s.id)"
      >
        {{ s.title }}
      </button>
      <span class="grow" />
      <button class="ctl" :disabled="index === 0" @click="move(-1)">◀ Back</button>
      <span class="counter mono">{{ index + 1 }} / {{ scenario.steps.length }}</span>
      <button class="ctl primary" :disabled="atEnd" @click="move(1)">Next ▶</button>
      <button class="ctl redeliver" @click="redelivered = true">
        <IconArrowBack class="inline-icon" /> Redeliver
      </button>
    </div>

    <div class="progress">
      <div class="bar" :style="{ width: `${((index + 1) / scenario.steps.length) * 100}%` }" />
    </div>

    <div class="machine">
      <div class="row">
        <span class="row-label c-green">forward</span>
        <template v-for="(s, i) in forward" :key="s">
          <span :class="stateClass(s)">{{ s }}</span>
          <svg v-if="i < forward.length - 1" class="arrow" viewBox="0 0 28 12"><path d="M2 6 H22 M18 2 L24 6 L18 10" /></svg>
        </template>
      </div>
      <div class="row">
        <span class="row-label c-red">rollback</span>
        <template v-for="(s, i) in backward" :key="s">
          <span :class="stateClass(s)">{{ s }}</span>
          <svg v-if="i < backward.length - 1" class="arrow" viewBox="0 0 28 12"><path d="M2 6 H22 M18 2 L24 6 L18 10" /></svg>
        </template>
      </div>
    </div>

    <div class="cards">
      <div v-for="c in cards" :key="c.label" class="card" :class="c.cls">
        <div class="label"><IconDatabase class="inline-icon" /> {{ c.label }} · {{ c.db }}</div>
        <Transition name="flip" mode="out-in">
          <div :key="c.value" class="value">{{ c.value }}</div>
        </Transition>
      </div>
      <div class="card client" :class="clientClass(step.client)">
        <div class="label"><IconUser class="inline-icon" /> client</div>
        <Transition name="flip" mode="out-in">
          <div :key="step.client" class="value">{{ step.client }}</div>
        </Transition>
      </div>
    </div>

    <Transition name="msg" mode="out-in">
      <div :key="`${scenarioId}-${index}`" class="message" :class="step.kind">
        <div class="msg-head">
          <span class="kind">{{ kindLabel(step.kind) }}</span>
          <span class="actor">{{ step.actor }}</span>
          <span class="topic mono">{{ step.topic }}</span>
          <code class="payload">{{ step.message }}</code>
        </div>
        <div class="note">{{ step.note }}</div>
      </div>
    </Transition>
    <Transition name="msg">
      <div v-if="redelivered" class="again">
        <b class="c-orange">Redelivered:</b> {{ step.redeliver }}
      </div>
    </Transition>
  </div>
</template>

<style scoped>
.stepper {
  font-size: 0.92rem;
}
.toolbar {
  display: flex;
  gap: 0.35rem;
  align-items: center;
  margin-bottom: 0.5rem;
}
.grow {
  flex: 1;
}
.counter {
  min-width: 3rem;
  text-align: center;
  color: var(--text-2);
  font-size: 0.8rem;
}
.ctl.redeliver {
  border-color: var(--orange);
  color: var(--orange);
}
.inline-icon {
  display: inline-block;
  vertical-align: -0.15em;
}
.progress {
  height: 2px;
  background: var(--divider);
  border-radius: 2px;
  margin-bottom: 0.7rem;
  overflow: hidden;
}
.bar {
  height: 100%;
  background: var(--purple);
  transition: width 0.45s cubic-bezier(0.2, 0.7, 0.2, 1);
}
.machine {
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
  margin-bottom: 0.7rem;
}
.row {
  display: flex;
  align-items: center;
  gap: 0.3rem;
}
.row-label {
  width: 4.6rem;
  font-size: 0.66rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}
.state {
  font-family: var(--mono);
  font-size: 0.72rem;
  padding: 0.2rem 0.55rem;
  border-radius: 6px;
  border: 1.25px dashed var(--divider);
  color: var(--muted);
  transition: all 0.35s cubic-bezier(0.2, 0.7, 0.2, 1);
}
.state.visited {
  border-style: solid;
  border-color: var(--line);
  color: var(--text-2);
}
.state.current {
  border: 1.75px solid var(--purple);
  color: var(--purple);
  font-weight: 700;
  transform: scale(1.08);
  box-shadow: 0 0 0 4px rgba(165, 106, 244, 0.15);
}
.arrow {
  width: 22px;
  height: 10px;
  fill: none;
  stroke: var(--divider);
  stroke-width: 1.6;
  stroke-linecap: round;
  stroke-linejoin: round;
}
.cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 0.55rem;
  margin-bottom: 0.7rem;
}
.card {
  border: 1.5px solid var(--divider);
  border-radius: 10px;
  padding: 0.4rem 0.6rem;
  transition: border-color 0.3s;
}
.card.saga {
  border-color: var(--purple);
}
.card.client.ok {
  border-color: var(--green);
}
.card.client.bad {
  border-color: var(--red);
}
.label {
  font-size: 0.64rem;
  color: var(--text-2);
  letter-spacing: 0.02em;
}
.value {
  font-family: var(--mono);
  font-weight: 700;
  font-size: 0.8rem;
  margin-top: 0.15rem;
}
.card.saga .value { color: var(--purple); }
.card.client.ok .value { color: var(--green); }
.card.client.bad .value { color: var(--red); }
.message {
  border: 1.5px solid var(--divider);
  border-left: 4px solid var(--line);
  border-radius: 10px;
  padding: 0.45rem 0.75rem;
}
.message.cmd { border-left-color: var(--blue); }
.message.evt { border-left-color: var(--orange); }
.message.rej { border-left-color: var(--red); }
.message.local { border-left-color: var(--purple); }
.msg-head {
  display: flex;
  gap: 0.7rem;
  align-items: center;
}
.kind {
  font-size: 0.62rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  color: var(--text-2);
  min-width: 4.6rem;
}
.message.cmd .kind { color: var(--blue); }
.message.evt .kind { color: var(--orange); }
.message.rej .kind { color: var(--red); }
.message.local .kind { color: var(--purple); }
.actor {
  font-weight: 700;
  min-width: 4.4rem;
}
.topic {
  font-size: 0.72rem;
  color: var(--text-2);
  min-width: 8.5rem;
}
.payload {
  font-size: 0.78rem;
}
.note {
  margin-top: 0.3rem;
  color: var(--text-2);
}
.again {
  margin-top: 0.45rem;
  padding: 0.35rem 0.75rem;
  border: 1.5px dashed var(--orange);
  border-radius: 10px;
}
.flip-enter-active,
.flip-leave-active {
  transition: opacity 0.2s, transform 0.2s;
}
.flip-enter-from {
  opacity: 0;
  transform: translateY(8px);
}
.flip-leave-to {
  opacity: 0;
  transform: translateY(-8px);
}
.msg-enter-active,
.msg-leave-active {
  transition: opacity 0.3s, transform 0.3s cubic-bezier(0.2, 0.7, 0.2, 1);
}
.msg-enter-from {
  opacity: 0;
  transform: translateX(-18px);
}
.msg-leave-to {
  opacity: 0;
  transform: translateX(18px);
}
</style>
