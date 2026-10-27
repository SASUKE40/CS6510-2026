<script setup lang="ts">
import IconBarcode from '~icons/tabler/barcode'
import IconRoute from '~icons/tabler/route'
import IconServer from '~icons/tabler/server-2'
import IconStack from '~icons/tabler/stack-2'

const hops = [
  { path: 'cf-http-1', fill: 'blue-f', from: 0.02, to: 0.3 },
  { path: 'cf-http-2', fill: 'blue-f', from: 0.32, to: 0.58 },
  { path: 'cf-down', fill: 'orange-f', from: 0.6, to: 0.78 },
  { path: 'cf-up', fill: 'orange-f', from: 0.8, to: 0.98 },
]
</script>

<template>
  <svg class="cover-flow" viewBox="0 0 960 200" role="img" aria-label="Station 7 sends HTTP to the gateway. The gateway routes to four services. The services exchange events through Kafka.">
    <defs>
      <marker id="cf-arrow-blue" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
        <path d="M1 1 L9 5 L1 9" fill="none" stroke="#2F80ED" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" />
      </marker>
      <marker id="cf-arrow-orange" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
        <path d="M1 1 L9 5 L1 9" fill="none" stroke="#FF7900" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" />
      </marker>
    </defs>

    <!-- connectors -->
    <path id="cf-http-1" d="M232 52 H318" class="wire blue dashed" marker-end="url(#cf-arrow-blue)" />
    <path id="cf-http-2" d="M552 52 H638" class="wire blue" marker-end="url(#cf-arrow-blue)" />
    <path id="cf-down" d="M770 86 V128" class="wire orange" marker-end="url(#cf-arrow-orange)" />
    <path id="cf-up" d="M830 128 V86" class="wire orange" marker-end="url(#cf-arrow-orange)" />

    <!-- nodes -->
    <g class="node">
      <rect x="20" y="18" width="212" height="68" rx="10" class="box neutral" />
      <IconBarcode x="36" y="34" width="36" height="36" class="ico neutral" />
      <text x="84" y="47" class="label">Station 7</text>
      <text x="84" y="72" class="sub">tx-42 · 3 scans</text>
    </g>
    <g class="node">
      <rect x="320" y="18" width="232" height="68" rx="10" class="box blue" />
      <IconRoute x="336" y="34" width="36" height="36" class="ico blue" />
      <text x="384" y="47" class="label blue-t">gateway</text>
      <text x="384" y="72" class="sub">routes HTTP only</text>
    </g>
    <g class="node">
      <rect x="640" y="18" width="300" height="68" rx="10" class="box blue" />
      <IconServer x="656" y="34" width="36" height="36" class="ico blue" />
      <text x="704" y="47" class="label blue-t">4 services</text>
      <text x="704" y="72" class="sub">one private SQLite each</text>
    </g>
    <g class="node">
      <rect x="640" y="130" width="300" height="60" rx="10" class="box orange" />
      <IconStack x="656" y="142" width="36" height="36" class="ico orange" />
      <text x="704" y="158" class="label orange-t">Apache Kafka</text>
      <text x="704" y="180" class="sub">the only path between services</text>
    </g>
    <text x="275" y="40" class="tag" text-anchor="middle">HTTP</text>
    <text x="760" y="112" class="tag" text-anchor="end">command</text>
    <text x="840" y="112" class="tag">reply</text>

    <!-- moving records -->
    <circle v-for="hop in hops" :key="hop.path" r="5" :class="['dot', hop.fill]" opacity="0">
      <animateMotion
        dur="2.8s"
        repeatCount="indefinite"
        calcMode="linear"
        keyPoints="0;0;1;1"
        :keyTimes="`0;${hop.from};${hop.to};1`"
      >
        <mpath :href="`#${hop.path}`" />
      </animateMotion>
      <animate
        attributeName="opacity"
        dur="2.8s"
        repeatCount="indefinite"
        calcMode="discrete"
        values="0;1;0"
        :keyTimes="`0;${hop.from};${hop.to}`"
      />
    </circle>
  </svg>
</template>

<style scoped>
.cover-flow {
  width: 100%;
  max-width: 820px;
  font-family: var(--sans);
}
.box {
  fill: #000;
  stroke-width: 1.75;
}
.box.neutral { stroke: var(--line); }
.box.blue { stroke: var(--blue); }
.box.orange { stroke: var(--orange); }
.ico {
  stroke-width: 1.6;
}
.ico.neutral { color: var(--text); }
.ico.blue { color: var(--blue); }
.ico.orange { color: var(--orange); }
.label {
  fill: var(--text);
  font-size: 20px;
  font-weight: 700;
}
.blue-t { fill: var(--blue); }
.orange-t { fill: var(--orange); }
.sub {
  fill: var(--text-2);
  font-size: 15px;
}
.tag {
  fill: var(--muted);
  font-size: 13px;
}
.wire {
  fill: none;
  stroke-width: 2.2;
  stroke-linecap: round;
}
.wire.blue { stroke: var(--blue); }
.wire.orange { stroke: var(--orange); }
.wire.dashed {
  stroke-dasharray: 8 9;
  animation: march 1.4s linear infinite;
}
.dot.blue-f { fill: var(--blue); }
.dot.orange-f { fill: var(--orange); }
</style>
