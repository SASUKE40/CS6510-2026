<script setup lang="ts">
import { useIsSlideActive, useSlideContext } from '@slidev/client'
import { computed, ref, watch } from 'vue'

const { $page, $nav } = useSlideContext()
const active = useIsSlideActive()
const grown = ref(false)

watch(
  active,
  (on) => {
    grown.value = false
    if (on) requestAnimationFrame(() => requestAnimationFrame(() => (grown.value = true)))
  },
  { immediate: true },
)

const width = computed(() => {
  const page = grown.value || !active.value ? $page.value : $page.value - 1
  return `${Math.min(page / $nav.value.total, 1) * 100}%`
})
</script>

<template>
  <div v-if="$page > 1" class="footer">
    <div class="rail">
      <div class="fill" :style="{ width }" />
    </div>
    <div class="meta">CS6510 · Event-based self-checkout · {{ $page }} / {{ $nav.total }}</div>
  </div>
</template>

<style scoped>
.footer {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
  pointer-events: none;
}
.rail {
  height: 2px;
  background: #1a1a1a;
}
.fill {
  height: 100%;
  background: var(--orange);
  transition: width 0.6s cubic-bezier(0.2, 0.7, 0.2, 1);
}
.meta {
  position: absolute;
  right: 0.9rem;
  bottom: 0.45rem;
  font-size: 0.62rem;
  color: var(--muted);
  font-family: var(--sans);
}
</style>
