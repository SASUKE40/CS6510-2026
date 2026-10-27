-- Private to payment-service. No other service opens this file.
-- The saga log: one row per completion attempt. Each state change is a
-- compare-and-set from the expected state, so the event consumer and the
-- recovery pass never overwrite each other. The basket (lines and prices) is
-- kept from BASKET_LOCKED on; the receipt is derived from it. A rejection
-- (failure code, kind, and message) is kept until the client is answered.
CREATE TABLE IF NOT EXISTS sagas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('STARTED','BASKET_LOCKED','STOCK_RESERVED','COMPLETED',
        'RELEASING','COMPENSATING','ABORTED')),
    basket_json TEXT, failure TEXT, failure_kind TEXT, failure_message TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sagas_unfinished ON sagas(id)
    WHERE state NOT IN ('COMPLETED','ABORTED');
-- Simulated charges; payment always succeeds once stock is reserved.
CREATE TABLE IF NOT EXISTS payments (
    saga_id INTEGER PRIMARY KEY REFERENCES sagas(id),
    transaction_id TEXT NOT NULL, amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),
    paid_at TEXT NOT NULL
);
