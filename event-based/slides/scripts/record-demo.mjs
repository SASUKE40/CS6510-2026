// Runs the live-demo steps against a running gateway and saves the responses
// as the offline fallback for LiveDemo.vue.
// Usage: ./event-based/run.sh --reset, then `pnpm record-demo` in event-based/slides.
import { writeFile } from 'node:fs/promises'
import { runStep, steps } from '../components/demo-steps.js'

const base = process.env.CHECKOUT_GATEWAY ?? 'http://localhost:8080'
const ctx = { tx: '' }
const results = []
for (const step of steps) {
  const result = await runStep(step, ctx, base)
  results.push({ id: step.id, ...result, ms: Math.round(result.ms * 100) / 100 })
  console.log(`${result.status} ${result.method} ${result.path} ${result.ms.toFixed(1)} ms`)
}
const out = new URL('../assets/demo-recorded.json', import.meta.url)
await writeFile(out, `${JSON.stringify({ recordedAt: new Date().toISOString(), base, results }, null, 2)}\n`)
console.log(`wrote ${out.pathname}`)
