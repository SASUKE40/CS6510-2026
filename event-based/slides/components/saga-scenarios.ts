export type SagaState =
  | 'STARTED'
  | 'BASKET_LOCKED'
  | 'STOCK_RESERVED'
  | 'COMPLETED'
  | 'COMPENSATING'
  | 'RELEASING'
  | 'ABORTED'

export type MessageKind = 'http' | 'cmd' | 'evt' | 'rej' | 'local'

export interface Step {
  actor: 'client' | 'payment' | 'basket' | 'inventory' | 'recovery'
  saga: SagaState
  basket: string
  reservation: string
  client: string
  topic: string
  kind: MessageKind
  message: string
  note: string
  redeliver: string
}

export interface Scenario {
  id: string
  title: string
  steps: Step[]
}

const request: Step = {
  actor: 'client',
  saga: 'STARTED',
  basket: 'OPEN',
  reservation: '—',
  client: 'waiting',
  topic: 'gateway :8080',
  kind: 'http',
  message: 'POST /transactions/tx-42/complete',
  note: 'payment writes saga 42 as STARTED and registers a waiter.',
  redeliver: 'A second HTTP request starts saga 43. Only one saga can lock the basket. The other gets TRANSACTION_NOT_OPEN.',
}

const checkout: Step = {
  ...request,
  actor: 'payment',
  topic: 'basket.commands',
  kind: 'cmd',
  message: 'Checkout {sagaId: saga-42}',
  note: 'payment publishes Checkout, keyed by the transaction id.',
  redeliver: 'basket sees the basket held by saga-42 and replays the same BasketLocked.',
}

const locked: Step = {
  ...checkout,
  actor: 'basket',
  basket: 'CHECKING_OUT (saga-42)',
  topic: 'basket.events',
  kind: 'evt',
  message: 'BasketLocked {lines, prices, total}',
  note: 'basket moves OPEN to CHECKING_OUT in one local transaction. Scans now get 409.',
  redeliver: "payment: UPDATE ... WHERE state='STARTED' changes 0 rows. No second Reserve.",
}

const reserve: Step = {
  ...locked,
  actor: 'payment',
  saga: 'BASKET_LOCKED',
  topic: 'inventory.commands',
  kind: 'cmd',
  message: 'Reserve {lines}',
  note: 'payment stores the basket and moves to BASKET_LOCKED. Then it sends Reserve.',
  redeliver: 'inventory finds reservation saga-42 RESERVED and replays StockReserved.',
}

const happy: Step[] = [
  request,
  checkout,
  locked,
  reserve,
  {
    ...reserve,
    actor: 'inventory',
    reservation: 'RESERVED',
    topic: 'inventory.events',
    kind: 'evt',
    message: 'StockReserved',
    note: 'inventory decrements every line or none, in one local transaction.',
    redeliver: "payment: UPDATE ... WHERE state='BASKET_LOCKED' changes 0 rows. No second charge.",
  },
  {
    actor: 'payment',
    saga: 'STOCK_RESERVED',
    basket: 'CHECKING_OUT (saga-42)',
    reservation: 'RESERVED',
    client: '200 receipt',
    topic: 'basket.commands',
    kind: 'cmd',
    message: 'Complete {completedAt}',
    note: 'Pivot: state change and payment row commit together. The client gets the receipt now.',
    redeliver: 'basket: the basket is already COMPLETED by saga-42. The command is a no-op.',
  },
  {
    actor: 'basket',
    saga: 'STOCK_RESERVED',
    basket: 'COMPLETED',
    reservation: 'RESERVED',
    client: '200 receipt',
    topic: 'basket.events',
    kind: 'evt',
    message: 'BasketCompleted',
    note: 'basket moves CHECKING_OUT to COMPLETED.',
    redeliver: "payment: UPDATE ... WHERE state='STOCK_RESERVED' changes 0 rows.",
  },
  {
    actor: 'payment',
    saga: 'COMPLETED',
    basket: 'COMPLETED',
    reservation: 'RESERVED',
    client: '200 receipt',
    topic: '—',
    kind: 'local',
    message: 'saga COMPLETED',
    note: 'The saga is finished. Recovery ignores it.',
    redeliver: 'Any copy of an old reply matches no (state, event) pair. payment ignores it.',
  },
]

const rejected: Step[] = [
  request,
  checkout,
  locked,
  reserve,
  {
    ...reserve,
    actor: 'inventory',
    reservation: 'REJECTED (tombstone)',
    topic: 'inventory.events',
    kind: 'rej',
    message: 'StockRejected {INSUFFICIENT_STOCK, kind: CONFLICT}',
    note: 'One line is short. inventory changes no stock and stores a REJECTED row.',
    redeliver: 'inventory finds the REJECTED row and sends the same StockRejected again.',
  },
  {
    actor: 'payment',
    saga: 'RELEASING',
    basket: 'CHECKING_OUT (saga-42)',
    reservation: 'REJECTED (tombstone)',
    client: 'waiting',
    topic: 'basket.commands',
    kind: 'cmd',
    message: 'Release',
    note: 'payment records the failure and moves to RELEASING.',
    redeliver: 'basket: saga-42 no longer holds the basket. It replies BasketReleased {released: false}.',
  },
  {
    actor: 'basket',
    saga: 'RELEASING',
    basket: 'OPEN',
    reservation: 'REJECTED (tombstone)',
    client: 'waiting',
    topic: 'basket.events',
    kind: 'evt',
    message: 'BasketReleased',
    note: 'basket moves back to OPEN and stores saga-42 in released_sagas.',
    redeliver: "payment: UPDATE ... WHERE state='RELEASING' changes 0 rows.",
  },
  {
    actor: 'payment',
    saga: 'ABORTED',
    basket: 'OPEN',
    reservation: 'REJECTED (tombstone)',
    client: '409 INSUFFICIENT_STOCK',
    topic: '—',
    kind: 'local',
    message: 'saga ABORTED',
    note: 'The client gets the same error as in Assignment 5.',
    redeliver: 'A late Checkout from saga-42 finds the tombstone: SAGA_RELEASED.',
  },
]

const empty: Step[] = [
  request,
  checkout,
  {
    ...checkout,
    actor: 'basket',
    topic: 'basket.events',
    kind: 'rej',
    message: 'CheckoutRejected {EMPTY_BASKET}',
    note: 'The basket has no items. basket changes nothing.',
    redeliver: 'basket checks again and sends the same CheckoutRejected.',
  },
  {
    ...checkout,
    actor: 'payment',
    saga: 'ABORTED',
    client: '409 EMPTY_BASKET',
    topic: '—',
    kind: 'local',
    message: 'saga ABORTED',
    note: 'No stock and no basket changed. The client gets 409.',
    redeliver: "payment: UPDATE ... WHERE state='STARTED' changes 0 rows.",
  },
]

const down: Step[] = [
  request,
  checkout,
  locked,
  { ...reserve, note: 'inventory is down. Reserve waits in Kafka.' },
  {
    ...reserve,
    actor: 'client',
    client: '503 after 10 s',
    topic: '—',
    kind: 'local',
    message: 'timeout: waiter removed',
    note: 'The request ends. The saga stays BASKET_LOCKED in payment.sqlite.',
    redeliver: 'Nothing to redeliver: no record was consumed.',
  },
  {
    ...reserve,
    actor: 'recovery',
    saga: 'COMPENSATING',
    client: '503 after 10 s',
    topic: 'inventory.commands',
    kind: 'cmd',
    message: 'Cancel',
    note: 'Recovery finds an idle saga with no waiter. It moves to COMPENSATING and sends Cancel behind Reserve.',
    redeliver: 'Recovery sends Cancel again each pass. inventory applies it once.',
  },
  {
    ...reserve,
    actor: 'inventory',
    saga: 'COMPENSATING',
    reservation: 'RESERVED',
    client: '503 after 10 s',
    topic: 'inventory.events',
    kind: 'evt',
    message: 'StockReserved (late)',
    note: 'inventory is back. It applies the late Reserve first: same key, same order.',
    redeliver: "payment: (COMPENSATING, StockReserved) matches no step. It is stale and ignored.",
  },
  {
    ...reserve,
    actor: 'inventory',
    saga: 'COMPENSATING',
    reservation: 'CANCELLED (restocked)',
    client: '503 after 10 s',
    topic: 'inventory.events',
    kind: 'evt',
    message: 'ReservationCancelled {restocked: true}',
    note: 'The Cancel queued behind it puts the stock back.',
    redeliver: 'inventory finds CANCELLED and replies {restocked: false}. No double restock.',
  },
  {
    ...reserve,
    actor: 'payment',
    saga: 'RELEASING',
    reservation: 'CANCELLED (restocked)',
    client: '503 after 10 s',
    topic: 'basket.commands',
    kind: 'cmd',
    message: 'Release',
    note: 'payment moves COMPENSATING to RELEASING.',
    redeliver: 'basket replies BasketReleased {released: false}.',
  },
  {
    ...reserve,
    actor: 'basket',
    saga: 'RELEASING',
    basket: 'OPEN',
    reservation: 'CANCELLED (restocked)',
    client: '503 after 10 s',
    topic: 'basket.events',
    kind: 'evt',
    message: 'BasketReleased',
    note: 'The basket is OPEN again. The station can try once more.',
    redeliver: "payment: UPDATE ... WHERE state='RELEASING' changes 0 rows.",
  },
  {
    ...reserve,
    actor: 'payment',
    saga: 'ABORTED',
    basket: 'OPEN',
    reservation: 'CANCELLED (restocked)',
    client: '503 after 10 s',
    topic: '—',
    kind: 'local',
    message: 'saga ABORTED',
    note: 'Stock, basket, and saga agree again. No unit is lost.',
    redeliver: 'A late Reserve for saga-42 would find the CANCELLED row and be refused.',
  },
]

export const scenarios: Scenario[] = [
  { id: 'happy', title: 'Happy path', steps: happy },
  { id: 'rejected', title: 'Stock short', steps: rejected },
  { id: 'empty', title: 'Empty basket', steps: empty },
  { id: 'down', title: 'Inventory down', steps: down },
]

export const forward: SagaState[] = ['STARTED', 'BASKET_LOCKED', 'STOCK_RESERVED', 'COMPLETED']
export const backward: SagaState[] = ['COMPENSATING', 'RELEASING', 'ABORTED']
