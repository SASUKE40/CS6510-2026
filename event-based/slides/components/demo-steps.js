export const STATION = 'station-7'
const BURST = 600
const BURST_PARALLEL = 25
const BURST_SKUS = [5, 5, 7, 5, 11, 7, 5, 13, 5, 7, 17, 5].map(
  (n) => `SKU-${String(n).padStart(6, '0')}`,
)

const json = (body) => ({ 'content-type': 'application/json', body: JSON.stringify(body) })

export const steps = [
  { id: 'start', title: 'Start tx', method: 'POST', path: () => '/transactions', ...json({ stationId: STATION }) },
  { id: 'scan-1', title: 'Scan SKU-000002', method: 'POST', path: (tx) => `/transactions/${tx}/items`, ...json({ sku: 'SKU-000002' }) },
  { id: 'scan-2', title: 'Scan SKU-000003', method: 'POST', path: (tx) => `/transactions/${tx}/items`, ...json({ sku: 'SKU-000003' }) },
  { id: 'scan-3', title: 'Scan SKU-000002', method: 'POST', path: (tx) => `/transactions/${tx}/items`, ...json({ sku: 'SKU-000002' }) },
  { id: 'complete', title: 'Complete (saga)', method: 'POST', path: (tx) => `/transactions/${tx}/complete` },
  { id: 'status', title: 'Basket status', method: 'GET', path: (tx) => `/transactions/${tx}` },
  { id: 'burst', title: `Burst: ${BURST} scans`, method: 'POST', path: () => '/transactions/{burst}/items', burst: BURST },
  { id: 'popular', title: 'Popular items', method: 'GET', path: () => '/analytics/popular-items?limit=5' },
  { id: 'low', title: 'Low stock', method: 'GET', path: () => '/inventory/low-stock?threshold=10000' },
]

async function call(base, method, path, step, fetchImpl) {
  const response = await fetchImpl(base + path, {
    method,
    headers: step?.body ? { 'content-type': step['content-type'] } : undefined,
    body: step?.body,
  })
  const text = await response.text()
  let body = text
  try {
    body = JSON.parse(text)
  } catch {}
  return { status: response.status, body }
}

/** Runs one step against `base` and returns { method, path, status, ms, body }. Updates ctx.tx. */
export async function runStep(step, ctx, base, fetchImpl = fetch) {
  const began = performance.now()
  if (step.burst) {
    const started = await call(base, 'POST', '/transactions', json({ stationId: `${STATION}-burst` }), fetchImpl)
    const tx = started.body.transactionId
    let failed = 0
    for (let i = 0; i < step.burst; i += BURST_PARALLEL) {
      const batch = []
      for (let j = i; j < Math.min(i + BURST_PARALLEL, step.burst); j++) {
        const sku = BURST_SKUS[j % BURST_SKUS.length]
        batch.push(call(base, 'POST', `/transactions/${tx}/items`, json({ sku }), fetchImpl))
      }
      failed += (await Promise.all(batch)).filter((r) => r.status !== 200).length
    }
    const ms = performance.now() - began
    return {
      method: 'POST',
      path: `/transactions/${tx}/items × ${step.burst}`,
      status: failed ? 207 : 200,
      ms,
      body: { transactionId: tx, scans: step.burst, failed, scansPerSecond: Math.round(step.burst / (ms / 1000)) },
    }
  }
  const path = step.path(ctx.tx)
  const result = await call(base, step.method, path, step, fetchImpl)
  if (step.id === 'start' && result.status === 201) ctx.tx = result.body.transactionId
  return { method: step.method, path, status: result.status, ms: performance.now() - began, body: result.body }
}
