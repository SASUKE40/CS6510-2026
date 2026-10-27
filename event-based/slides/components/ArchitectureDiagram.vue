<script setup lang="ts">
import { useIsSlideActive, useSlideContext } from '@slidev/client'
import { computed, onUnmounted, ref, watchEffect } from 'vue'
import IconBasket from '~icons/tabler/basket'
import IconChart from '~icons/tabler/chart-bar'
import IconCard from '~icons/tabler/credit-card'
import IconDatabase from '~icons/tabler/database'
import IconPackages from '~icons/tabler/packages'
import IconRoute from '~icons/tabler/route'
import IconStack from '~icons/tabler/stack-2'

type Kind = 'cmd' | 'reply' | 'ok' | 'feed'
type Point = [number, number]

const { $clicks } = useSlideContext()
const active = useIsSlideActive()

const CARD_TOP = 210
const CARD_BOTTOM = 420
const BUS_Y = 465
const TOPIC_TOP = 530

const services = [
  { id: 'inventory', x: 140, port: ':8081', icon: IconPackages, roles: ['stock and reservations', 'publishes the catalog'] },
  { id: 'payment', x: 480, port: ':8083', icon: IconCard, roles: ['saga orchestrator', 'payment row = pivot'] },
  { id: 'basket', x: 820, port: ':8082', icon: IconBasket, roles: ['baskets, catalog replica', 'scan outbox + relay'] },
  { id: 'analytics', x: 1160, port: ':8084', icon: IconChart, roles: ['hopping window', 'popular items'] },
] as const
type ServiceId = (typeof services)[number]['id']

const routes: Record<ServiceId, string> = {
  inventory: '/items · /inventory',
  payment: '/transactions/{id}/complete',
  basket: '/transactions/…',
  analytics: '/analytics/*',
}

const topics = [
  { id: 'catalog', group: 'inventory.', name: 'catalog', kind: 'feed', lines: ['CatalogPublished', 'compacted'] },
  { id: 'invCmd', group: 'inventory.', name: 'commands', kind: 'cmd', lines: ['Reserve · Cancel', ''] },
  { id: 'invEvt', group: 'inventory.', name: 'events', kind: 'reply', lines: ['StockReserved', 'Rejected · Cancelled'] },
  { id: 'basketCmd', group: 'basket.', name: 'commands', kind: 'cmd', lines: ['Checkout · Release', 'Complete'] },
  { id: 'basketEvt', group: 'basket.', name: 'events', kind: 'reply', lines: ['Locked · Released', 'Completed · Rejected'] },
  { id: 'scans', group: 'basket.', name: 'scans', kind: 'feed', lines: ['ItemScanned', 'outbox relay'] },
].map((t, i) => ({ ...t, x: 140 + 224.8 * i }))
type TopicId = (typeof topics)[number]['id']

const kindLabel: Record<string, string> = { cmd: 'COMMANDS', reply: 'REPLIES', feed: 'DATA FEED' }

const serviceX = (id: ServiceId) => services.find((s) => s.id === id)!.x + 150
const topicX = (id: TopicId) => topics.find((t) => t.id === id)!.x + 98

interface Hop {
  label: string
  kind: Kind
  produce: Point[]
  consume: Point[]
  topic?: TopicId
}

function viaTopic(from: ServiceId, topic: TopicId, to: ServiceId, label: string, kind: Kind): Hop {
  const fx = serviceX(from)
  const tx = topicX(topic)
  const rx = serviceX(to)
  return {
    label,
    kind,
    topic,
    produce: [[fx, CARD_BOTTOM], [fx, BUS_Y], [tx, BUS_Y], [tx, TOPIC_TOP]],
    consume: [[tx, TOPIC_TOP], [tx, BUS_Y], [rx, BUS_Y], [rx, CARD_BOTTOM]],
  }
}

const sagaHops: Hop[] = [
  viaTopic('payment', 'basketCmd', 'basket', 'Checkout', 'cmd'),
  viaTopic('basket', 'basketEvt', 'payment', 'BasketLocked', 'reply'),
  viaTopic('payment', 'invCmd', 'inventory', 'Reserve', 'cmd'),
  viaTopic('inventory', 'invEvt', 'payment', 'StockReserved', 'reply'),
  {
    label: '200 receipt',
    kind: 'ok',
    produce: [[serviceX('payment'), CARD_TOP], [serviceX('payment'), 168], [800, 168], [800, 130]],
    consume: [],
  },
  viaTopic('payment', 'basketCmd', 'basket', 'Complete', 'cmd'),
  viaTopic('basket', 'basketEvt', 'payment', 'BasketCompleted', 'reply'),
]

const feedHops: Hop[] = [
  viaTopic('inventory', 'catalog', 'basket', 'CatalogPublished', 'feed'),
  viaTopic('basket', 'scans', 'analytics', 'ItemScanned', 'feed'),
]

const MOVE = 0.75
const DWELL = 0.3
const GAP = 0.25

function length(points: Point[]) {
  let total = 0
  for (let i = 1; i < points.length; i++)
    total += Math.abs(points[i][0] - points[i - 1][0]) + Math.abs(points[i][1] - points[i - 1][1])
  return total
}

/** Leading part of an orthogonal polyline, cut at fraction `f` of its length. */
function cut(points: Point[], f: number): Point[] {
  if (points.length === 0) return []
  let left = length(points) * Math.min(Math.max(f, 0), 1)
  const out: Point[] = [points[0]]
  for (let i = 1; i < points.length; i++) {
    const [ax, ay] = points[i - 1]
    const [bx, by] = points[i]
    const seg = Math.abs(bx - ax) + Math.abs(by - ay)
    if (left >= seg) {
      out.push(points[i])
      left -= seg
      continue
    }
    const r = seg === 0 ? 0 : left / seg
    out.push([ax + (bx - ax) * r, ay + (by - ay) * r])
    break
  }
  return out
}

function hopDuration(hop: Hop) {
  return hop.consume.length ? MOVE + DWELL + MOVE : MOVE + DWELL
}

interface Frame {
  hop: Hop
  trail: Point[]
  head: Point
  atTopic: boolean
}

function frameAt(hops: Hop[], t: number, rest: number): Frame | null {
  const cycle = hops.reduce((sum, h) => sum + hopDuration(h) + GAP, 0) + rest
  let local = t % cycle
  for (const hop of hops) {
    const d = hopDuration(hop)
    if (local < d) {
      if (local < MOVE) {
        const trail = cut(hop.produce, local / MOVE)
        return { hop, trail, head: trail[trail.length - 1], atTopic: false }
      }
      if (local < MOVE + DWELL || hop.consume.length === 0) {
        const end = hop.produce[hop.produce.length - 1]
        return { hop, trail: hop.produce, head: end, atTopic: !!hop.topic }
      }
      const trail = cut(hop.consume, (local - MOVE - DWELL) / MOVE)
      return { hop, trail, head: trail[trail.length - 1], atTopic: false }
    }
    local -= d + GAP
  }
  return null
}

const stage = computed(() => Math.min($clicks.value, 3))
const now = ref(0)
let raf = 0
let origin = 0

function tick(ts: number) {
  if (!origin) origin = ts
  now.value = (ts - origin) / 1000
  raf = requestAnimationFrame(tick)
}

watchEffect(() => {
  const running = active.value && stage.value >= 2
  if (running && !raf) {
    origin = 0
    raf = requestAnimationFrame(tick)
  } else if (!running && raf) {
    cancelAnimationFrame(raf)
    raf = 0
    now.value = 0
  }
})
onUnmounted(() => cancelAnimationFrame(raf))

const saga = computed(() => (stage.value >= 2 ? frameAt(sagaHops, now.value, 1.2) : null))
const feed = computed(() => (stage.value >= 3 ? frameAt(feedHops, now.value + 0.6, 0.4) : null))

const hotTopics = computed(() => {
  const out = new Set<string>()
  for (const f of [saga.value, feed.value]) if (f?.hop.topic && f.atTopic) out.add(f.hop.topic)
  return out
})

const colorOf: Record<Kind, string> = {
  cmd: 'var(--blue)',
  reply: 'var(--orange)',
  ok: 'var(--green)',
  feed: 'var(--text)',
}

const pointsAttr = (points: Point[]) => points.map((p) => p.join(',')).join(' ')
const chipWidth = (label: string) => label.length * 11.4 + 24

const captions = [
  'The gateway routes public HTTP by path. It has no logic, no database, and no broker.',
  'Services never call each other. Kafka is the only path: 6 topics, 1 partition each.',
  'Saga: payment sends commands and waits for replies. Each pulse is one Kafka record.',
  'Data feeds: the compacted catalog goes to basket. The scan outbox goes to analytics.',
]
</script>

<template>
  <svg class="arch" viewBox="0 0 1600 790" role="img" aria-label="Architecture: a gateway routes HTTP to four services. Services exchange records only through six Kafka topics.">
    <defs>
      <marker v-for="(c, k) in { blue: '#2F80ED', orange: '#FF7900', white: '#BDBDBD', green: '#16C83E' }" :id="`ad-${k}`" :key="k" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M1 1 L9 5 L1 9" fill="none" :stroke="c" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" />
      </marker>
    </defs>

    <!-- client cue -->
    <circle cx="150" cy="85" r="16" class="stroke-neutral" fill="none" stroke-width="2.5" />
    <text x="150" y="134" class="t-body" text-anchor="middle">Java load client</text>
    <text x="150" y="158" class="t-small" text-anchor="middle">10–100 stations</text>
    <path d="M176 85 H552" class="wire neutral dashed" marker-end="url(#ad-white)" />
    <text x="364" y="70" class="t-small" text-anchor="middle">HTTP :8080</text>

    <!-- gateway -->
    <rect x="560" y="40" width="480" height="90" rx="12" class="box blue" />
    <IconRoute x="578" y="61" width="48" height="48" class="ico c-blue" />
    <text x="640" y="80" class="t-head blue">checkout-gateway</text>
    <text x="640" y="110" class="t-body">routes by path · no logic, no broker</text>

    <!-- stage caption -->
    <foreignObject x="1080" y="30" width="490" height="130">
      <div xmlns="http://www.w3.org/1999/xhtml" class="caption">
        <Transition name="cap" mode="out-in">
          <div :key="stage" class="cap-body">
            <span class="cap-n">{{ stage + 1 }} / 4</span>
            {{ captions[stage] }}
          </div>
        </Transition>
      </div>
    </foreignObject>

    <!-- HTTP routes -->
    <path d="M800 130 V168" class="wire blue" />
    <path d="M290 168 H1310" class="wire blue" />
    <circle cx="800" cy="168" r="5" class="dot-blue" />
    <template v-for="s in services" :key="`r-${s.id}`">
      <path :d="`M${s.x + 150} 168 V${CARD_TOP - 4}`" class="wire blue" marker-end="url(#ad-blue)" />
      <text :x="s.x + 162" y="195" class="t-route">{{ routes[s.id] }}</text>
    </template>

    <!-- services -->
    <g v-for="s in services" :key="s.id">
      <rect :x="s.x" :y="CARD_TOP" width="300" height="210" rx="12" class="box blue" />
      <component :is="s.icon" :x="s.x + 20" :y="CARD_TOP + 20" width="44" height="44" class="ico c-blue" />
      <text :x="s.x + 76" :y="CARD_TOP + 46" class="t-head blue">{{ s.id }}</text>
      <text :x="s.x + 76" :y="CARD_TOP + 70" class="t-small">{{ s.port }}</text>
      <text :x="s.x + 22" :y="CARD_TOP + 112" class="t-body">{{ s.roles[0] }}</text>
      <text :x="s.x + 22" :y="CARD_TOP + 138" class="t-body">{{ s.roles[1] }}</text>
      <line :x1="s.x + 20" :x2="s.x + 280" :y1="CARD_TOP + 158" :y2="CARD_TOP + 158" class="hair" />
      <IconDatabase :x="s.x + 20" :y="CARD_TOP + 170" width="26" height="26" class="ico c-text2" />
      <text :x="s.x + 54" :y="CARD_TOP + 190" class="t-mono">{{ s.id }}.sqlite</text>
    </g>

    <!-- Kafka layer -->
    <g class="layer" :class="{ shown: stage >= 1 }">
      <path :d="`M${serviceX('inventory')} ${BUS_Y} H${topicX('scans')}`" class="wire orange faint" />
      <path :d="`M${topicX('catalog')} ${BUS_Y} H${serviceX('inventory')}`" class="wire orange faint" />
      <template v-for="s in services" :key="`k-${s.id}`">
        <path :d="`M${s.x + 150} ${CARD_BOTTOM} V${BUS_Y}`" class="wire orange faint" />
        <circle :cx="s.x + 150" :cy="BUS_Y" r="5" class="dot-orange" />
      </template>
      <template v-for="t in topics" :key="`s-${t.id}`">
        <path :d="`M${t.x + 98} ${BUS_Y} V${TOPIC_TOP}`" class="wire orange faint" />
        <circle :cx="t.x + 98" :cy="BUS_Y" r="5" class="dot-orange" />
      </template>

      <rect x="100" y="500" width="1400" height="275" rx="20" class="box orange" />
      <g v-for="t in topics" :key="t.id">
        <rect
          :x="t.x" :y="TOPIC_TOP" width="196" height="130" rx="10"
          class="box topic"
          :style="hotTopics.has(t.id) ? { stroke: t.kind === 'cmd' ? 'var(--blue)' : t.kind === 'reply' ? 'var(--orange)' : 'var(--text)' } : {}"
        />
        <text :x="t.x + 16" :y="TOPIC_TOP + 26" class="t-kind" :class="t.kind">{{ kindLabel[t.kind] }}</text>
        <text :x="t.x + 16" :y="TOPIC_TOP + 52" class="t-small">{{ t.group }}</text>
        <text :x="t.x + 16" :y="TOPIC_TOP + 77" class="t-topic">{{ t.name }}</text>
        <text :x="t.x + 16" :y="TOPIC_TOP + 101" class="t-msg">{{ t.lines[0] }}</text>
        <text :x="t.x + 16" :y="TOPIC_TOP + 121" class="t-msg">{{ t.lines[1] }}</text>
      </g>
      <line x1="140" x2="1460" y1="682" y2="682" class="hair" />
      <IconStack x="140" y="696" width="44" height="44" class="ico c-orange" />
      <text x="200" y="724" class="t-head orange">Apache Kafka 4.3 · the only path between services</text>
      <text x="200" y="756" class="t-body">KRaft in Docker · topics checkout.* · 1 partition each · at-least-once delivery</text>
    </g>

    <!-- moving records -->
    <template v-for="(f, i) in [saga, feed]" :key="i">
      <g v-if="f">
        <polyline
          :points="pointsAttr(f.trail)"
          class="trail"
          :class="{ dashed: f.hop.kind === 'feed' }"
          :style="{ stroke: colorOf[f.hop.kind] }"
        />
        <circle :cx="f.head[0]" :cy="f.head[1]" r="9" :style="{ fill: colorOf[f.hop.kind] }" />
        <circle :cx="f.head[0]" :cy="f.head[1]" r="16" class="halo" :style="{ stroke: colorOf[f.hop.kind] }" />
        <g :transform="`translate(${f.head[0] + 18}, ${f.hop.kind === 'ok' ? f.head[1] - 44 : CARD_BOTTOM + 6})`">
          <rect :width="chipWidth(f.hop.label)" height="32" rx="8" class="chip" :style="{ stroke: colorOf[f.hop.kind] }" />
          <text x="11" y="22" class="t-chip" :style="{ fill: colorOf[f.hop.kind] }">{{ f.hop.label }}</text>
        </g>
      </g>
    </template>
  </svg>
</template>

<style scoped>
.arch {
  width: 100%;
  font-family: var(--sans);
}
.box {
  fill: #000;
  stroke-width: 1.75;
  transition: stroke 0.25s;
}
.box.blue { stroke: var(--blue); }
.box.orange { stroke: var(--orange); }
.box.topic { stroke: var(--divider); }
.stroke-neutral { stroke: var(--line); }
.hair {
  stroke: var(--divider);
  stroke-width: 1.25;
}
.ico {
  stroke-width: 1.6;
}
.c-text2 { color: var(--text-2); }
.t-head {
  font-size: 28px;
  font-weight: 700;
}
.t-head.blue { fill: var(--blue); }
.t-head.orange { fill: var(--orange); }
.t-body {
  fill: var(--text);
  font-size: 21px;
}
.t-small {
  fill: var(--text-2);
  font-size: 19px;
}
.t-route {
  fill: var(--text-2);
  font-size: 18px;
  font-family: var(--mono);
}
.t-mono {
  fill: var(--text-2);
  font-size: 19px;
  font-family: var(--mono);
}
.t-kind {
  font-size: 15px;
  font-weight: 700;
  letter-spacing: 0.08em;
}
.t-kind.cmd { fill: var(--blue); }
.t-kind.reply { fill: var(--orange); }
.t-kind.feed { fill: var(--text-2); }
.t-topic {
  fill: var(--text);
  font-size: 24px;
  font-weight: 700;
}
.t-msg {
  fill: var(--text-2);
  font-size: 16px;
}
.t-chip {
  font-size: 19px;
  font-weight: 700;
  font-family: var(--mono);
}
.wire {
  fill: none;
  stroke-width: 2.5;
  stroke-linecap: round;
  stroke-linejoin: round;
}
.wire.blue { stroke: var(--blue); }
.wire.orange { stroke: var(--orange); }
.wire.neutral { stroke: var(--line); }
.wire.faint {
  stroke-opacity: 0.45;
}
.wire.dashed {
  stroke-dasharray: 8 9;
  animation: march 1.4s linear infinite;
}
.dot-blue { fill: var(--blue); }
.dot-orange { fill: var(--orange); }
.layer {
  opacity: 0;
  transform: translateY(26px);
  transition: opacity 0.6s cubic-bezier(0.2, 0.7, 0.2, 1), transform 0.6s cubic-bezier(0.2, 0.7, 0.2, 1);
}
.layer.shown {
  opacity: 1;
  transform: none;
}
.trail {
  fill: none;
  stroke-width: 4;
  stroke-linecap: round;
  stroke-linejoin: round;
}
.trail.dashed {
  stroke-dasharray: 8 9;
}
.halo {
  fill: none;
  stroke-width: 2;
  opacity: 0.5;
}
.chip {
  fill: #000;
  stroke-width: 1.5;
}
.caption {
  font-family: var(--sans);
  color: var(--text);
  font-size: 21px;
  line-height: 1.5;
  border-left: 3px solid var(--orange);
  padding: 4px 0 4px 16px;
  height: 100%;
  box-sizing: border-box;
}
.cap-n {
  display: block;
  color: var(--orange);
  font-weight: 700;
  font-size: 16px;
  letter-spacing: 0.1em;
  margin-bottom: 4px;
}
.cap-enter-active,
.cap-leave-active {
  transition: opacity 0.3s, transform 0.3s;
}
.cap-enter-from {
  opacity: 0;
  transform: translateY(10px);
}
.cap-leave-to {
  opacity: 0;
  transform: translateY(-10px);
}
</style>
