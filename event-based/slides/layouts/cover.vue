<template>
  <div class="slidev-layout cover-layout">
    <div
      v-motion
      class="body"
      :initial="{ opacity: 0, y: 30 }"
      :enter="{ opacity: 1, y: 0, transition: { duration: 600 } }"
    >
      <slot />
    </div>
    <div
      v-motion
      class="flow"
      :initial="{ opacity: 0 }"
      :enter="{ opacity: 1, transition: { delay: 350, duration: 700 } }"
    >
      <CoverFlow />
    </div>
  </div>
</template>

<style scoped>
.cover-layout {
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 1.4rem;
  padding: 2.4rem 3.4rem;
}
.body :deep(h1) {
  font-size: 2.7rem;
  margin-bottom: 0.4rem;
}
.body :deep(p) {
  margin: 0.2rem 0;
}
</style>
