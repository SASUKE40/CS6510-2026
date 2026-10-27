-- Private to analytics-service. No other service opens this file.
CREATE TABLE IF NOT EXISTS settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    window_size INTEGER NOT NULL, slide_interval INTEGER NOT NULL
);
-- Local copy of the scan events still needed by a current or future window,
-- keyed by basket's global scan sequence.
CREATE TABLE IF NOT EXISTS scans (
    sequence INTEGER PRIMARY KEY, sku TEXT NOT NULL, name TEXT NOT NULL
);
-- Last event consumed from basket's feed; advanced in the same transaction
-- as the inserts, so a crash or replay never counts a scan twice.
CREATE TABLE IF NOT EXISTS cursor (
    id INTEGER PRIMARY KEY CHECK (id = 1), last_sequence INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS popularity (
    id INTEGER PRIMARY KEY CHECK (id = 1), snapshot TEXT NOT NULL
);
