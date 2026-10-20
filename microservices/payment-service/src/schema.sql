-- Private to payment-service. No other service opens this file.
-- The saga log: one row per completion attempt, written before each step so
-- a restarted orchestrator knows whether to roll forward or back. The basket
-- (lines and prices) is kept from the pivot on; the receipt is derived from it.
CREATE TABLE IF NOT EXISTS sagas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('STARTED','BASKET_LOCKED','STOCK_RESERVED','COMPLETED','ABORTED')),
    basket_json TEXT, failure TEXT,
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
