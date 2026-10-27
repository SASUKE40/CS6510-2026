-- Private to basket-service. No other service opens this file.
-- Read-only replica of inventory's catalog (SKUs, names, prices never change
-- after seeding), refreshed from the latest CatalogPublished event at startup.
CREATE TABLE IF NOT EXISTS catalog (
    sku TEXT PRIMARY KEY, name TEXT NOT NULL, price_cents INTEGER NOT NULL CHECK(price_cents >= 0)
);
-- CHECKING_OUT means a payment saga (checkout_id) holds the basket; scans
-- are refused until the saga completes or releases it.
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK(status IN ('OPEN','CHECKING_OUT','COMPLETED')),
    item_count INTEGER NOT NULL DEFAULT 0, total_cents INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL, completed_at TEXT, checkout_id TEXT
);
-- Sagas that released (or never held) a basket. A checkout request from one
-- of them that arrives late, e.g. a timed-out duplicate, must not re-lock it.
CREATE TABLE IF NOT EXISTS released_sagas (
    saga_id TEXT PRIMARY KEY, transaction_id INTEGER NOT NULL REFERENCES transactions(id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS lines (
    transaction_id INTEGER NOT NULL REFERENCES transactions(id),
    sku TEXT NOT NULL REFERENCES catalog(sku), quantity INTEGER NOT NULL CHECK(quantity > 0),
    PRIMARY KEY(transaction_id, sku)
);
-- Transactional outbox: one event per accepted scan, written in the same
-- local transaction as the basket line, then relayed in order to Kafka.
-- The gap-free, commit-ordered sequence is the global scan number analytics
-- windows are based on.
CREATE TABLE IF NOT EXISTS scan_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, sku TEXT NOT NULL, name TEXT NOT NULL
);
