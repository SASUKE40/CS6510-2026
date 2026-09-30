PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    catalog_size INTEGER NOT NULL, initial_stock INTEGER NOT NULL,
    threshold INTEGER NOT NULL, window_size INTEGER NOT NULL, slide_interval INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS items (
    sku TEXT PRIMARY KEY, name TEXT NOT NULL, price_cents INTEGER NOT NULL CHECK(price_cents >= 0),
    initial_stock INTEGER NOT NULL, stock INTEGER NOT NULL CHECK(stock >= 0)
);
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK(status IN ('OPEN','COMPLETED')),
    item_count INTEGER NOT NULL DEFAULT 0, total_cents INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL, completed_at TEXT
);
CREATE TABLE IF NOT EXISTS lines (
    transaction_id INTEGER NOT NULL REFERENCES transactions(id),
    sku TEXT NOT NULL REFERENCES items(sku), quantity INTEGER NOT NULL CHECK(quantity > 0),
    PRIMARY KEY(transaction_id, sku)
);
CREATE TABLE IF NOT EXISTS scans (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, sku TEXT NOT NULL REFERENCES items(sku)
);
CREATE TABLE IF NOT EXISTS stock_changes (
    sku TEXT NOT NULL REFERENCES items(sku), stock INTEGER NOT NULL, changed_at TEXT NOT NULL,
    PRIMARY KEY(sku, stock)
);
CREATE TABLE IF NOT EXISTS popularity (
    id INTEGER PRIMARY KEY CHECK(id = 1), snapshot TEXT NOT NULL
);
