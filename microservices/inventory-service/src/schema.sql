-- Private to inventory-service. No other service opens this file.
CREATE TABLE IF NOT EXISTS settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    catalog_size INTEGER NOT NULL, initial_stock INTEGER NOT NULL, threshold INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS items (
    sku TEXT PRIMARY KEY, name TEXT NOT NULL, price_cents INTEGER NOT NULL CHECK(price_cents >= 0),
    initial_stock INTEGER NOT NULL, stock INTEGER NOT NULL CHECK(stock >= 0)
);
-- Latest time each SKU reached each stock level; drives low-stock triggeredAt.
CREATE TABLE IF NOT EXISTS stock_changes (
    sku TEXT NOT NULL REFERENCES items(sku), stock INTEGER NOT NULL, changed_at TEXT NOT NULL,
    PRIMARY KEY(sku, stock)
);
-- One row per checkout saga. RESERVED rows are sales; CANCELLED rows were
-- restocked, or are tombstones that make a late reserve call a no-op.
CREATE TABLE IF NOT EXISTS reservations (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('RESERVED','CANCELLED')),
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reservation_lines (
    reservation_id TEXT NOT NULL REFERENCES reservations(id),
    sku TEXT NOT NULL REFERENCES items(sku), quantity INTEGER NOT NULL CHECK(quantity > 0),
    PRIMARY KEY(reservation_id, sku)
);
