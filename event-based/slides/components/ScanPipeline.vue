<script setup lang="ts">
import IconDatabase from '~icons/tabler/database'
import IconInbox from '~icons/tabler/inbox'
import IconSend from '~icons/tabler/send'
import IconStack from '~icons/tabler/stack-2'
import IconStackPop from '~icons/tabler/stack-pop'

const W = 270
const GAP = 62.5
const TOP = 40
const H = 118
const MID = TOP + H / 2

const nodes = [
  { title: 'scan commit', lines: ['line + outbox row', 'one transaction'], color: 'blue', icon: IconInbox },
  { title: 'relay task', lines: ['sends in order', 'rewinds on failure'], color: 'blue', icon: IconSend },
  { title: 'basket.scans', lines: ['1 partition', 'seq = window clock'], color: 'orange', icon: IconStack },
  { title: 'consumer', lines: ['batches up to 5,000', 'dup: skip · gap: wait'], color: 'blue', icon: IconStackPop },
  { title: 'SQLite', lines: ['scans · cursor', 'snapshot · trim'], color: 'neutral', icon: IconDatabase },
].map((n, i) => ({ ...n, x: i * (W + GAP) }))

const owners = [
  { label: 'basket', from: 0, to: 1, color: 'blue' },
  { label: 'Kafka', from: 2, to: 2, color: 'orange' },
  { label: 'analytics', from: 3, to: 4, color: 'blue' },
]
</script>

<template>
  <svg class="pipe" viewBox="-4 0 1608 170" role="img" aria-label="Scan pipeline: basket commits the scan and an outbox row, a relay publishes to basket.scans, the analytics consumer stores scans, cursor, and snapshot in SQLite.">
    <defs>
      <marker id="sp-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto">
        <path d="M1 1 L9 5 L1 9" fill="none" stroke="#FF7900" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" />
      </marker>
      <path id="sp-track" :d="`M${W} ${MID} H${1600 - W}`" />
    </defs>

    <g v-for="o in owners" :key="o.label">
      <path
        :d="`M${nodes[o.from].x + 8} 26 V20 H${nodes[o.to].x + W - 8} V26`"
        :class="['bracket', o.color]"
      />
      <text :x="(nodes[o.from].x + nodes[o.to].x + W) / 2" y="13" text-anchor="middle" :class="['owner', o.color]">{{ o.label }}</text>
    </g>

    <template v-for="(n, i) in nodes" :key="`c-${i}`">
      <path
        v-if="i < nodes.length - 1"
        :d="`M${n.x + W + 6} ${MID} H${n.x + W + GAP - 10}`"
        class="wire"
        marker-end="url(#sp-arrow)"
      />
    </template>

    <circle v-for="k in 5" :key="`d-${k}`" r="7" class="record">
      <animateMotion dur="3.5s" :begin="`${-(k - 1) * 0.7}s`" repeatCount="indefinite">
        <mpath href="#sp-track" />
      </animateMotion>
    </circle>

    <g v-for="n in nodes" :key="n.title">
      <rect :x="n.x" :y="TOP" :width="W" :height="H" rx="12" :class="['box', n.color]" />
      <component :is="n.icon" :x="n.x + 18" :y="TOP + 18" width="38" height="38" :class="['ico', n.color]" />
      <text :x="n.x + 68" :y="TOP + 44" :class="['title', n.color]">{{ n.title }}</text>
      <text :x="n.x + 20" :y="TOP + 80" class="sub">{{ n.lines[0] }}</text>
      <text :x="n.x + 20" :y="TOP + 104" class="sub">{{ n.lines[1] }}</text>
    </g>
  </svg>
</template>

<style scoped>
.pipe {
  width: 100%;
  font-family: var(--sans);
}
.box {
  fill: #000;
  stroke-width: 1.75;
}
.box.blue { stroke: var(--blue); }
.box.orange { stroke: var(--orange); }
.box.neutral { stroke: var(--line); }
.ico { stroke-width: 1.6; }
.ico.blue { color: var(--blue); }
.ico.orange { color: var(--orange); }
.ico.neutral { color: var(--text); }
.title {
  font-size: 25px;
  font-weight: 700;
}
.title.blue { fill: var(--blue); }
.title.orange { fill: var(--orange); }
.title.neutral { fill: var(--text); }
.sub {
  fill: var(--text-2);
  font-size: 19px;
}
.owner {
  font-size: 15px;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}
.owner.blue { fill: var(--blue); }
.owner.orange { fill: var(--orange); }
.bracket {
  fill: none;
  stroke-width: 1.25;
}
.bracket.blue { stroke: rgba(47, 128, 237, 0.6); }
.bracket.orange { stroke: rgba(255, 121, 0, 0.7); }
.wire {
  fill: none;
  stroke: var(--orange);
  stroke-width: 2.5;
  stroke-dasharray: 8 9;
  stroke-linecap: round;
  animation: march 1.2s linear infinite;
}
.record {
  fill: var(--orange);
}
</style>
