//! The only layer that knows SQL or rusqlite. Write closures are atomic units of work.
use crate::{
    config::Config,
    domain::*,
    error::{Error, Result},
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::sync::Mutex;

pub struct Database {
    connection: Mutex<Connection>,
    config: Config,
}
impl Database {
    pub fn open(
        path: &str,
        config: Config,
        reset: bool,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        if config.catalog_size <= 0
            || config.initial_stock < 0
            || config.threshold < 0
            || config.window_size <= 0
            || config.slide_interval <= 0
        {
            return Err(
                "Catalog/window/slide must be positive; stock/threshold must be nonnegative".into(),
            );
        }
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )?;
        let db = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if reset {
            db.execute_batch("DROP TABLE IF EXISTS lines; DROP TABLE IF EXISTS scans;
                DROP TABLE IF EXISTS stock_changes; DROP TABLE IF EXISTS transactions;
                DROP TABLE IF EXISTS items; DROP TABLE IF EXISTS settings; DROP TABLE IF EXISTS popularity;")?;
        }
        db.execute_batch(include_str!("schema.sql"))?;
        let existing = db.query_row("SELECT catalog_size, initial_stock, threshold, window_size, slide_interval FROM settings WHERE id=1", [], |r| {
            Ok(Config { catalog_size: r.get(0)?, initial_stock: r.get(1)?, threshold: r.get(2)?, window_size: r.get(3)?, slide_interval: r.get(4)? })
        }).optional()?;
        if let Some(existing) = existing {
            if existing != config {
                return Err(
                    "Database configuration differs; use the original settings or --reset".into(),
                );
            }
        } else {
            db.execute(
                "INSERT INTO settings VALUES (1, ?1, ?2, ?3, ?4, ?5)",
                params![
                    config.catalog_size,
                    config.initial_stock,
                    config.threshold,
                    config.window_size,
                    config.slide_interval
                ],
            )?;
            let timestamp = now();
            for i in 1..=config.catalog_size {
                let sku = format!("SKU-{i:06}");
                db.execute(
                    "INSERT INTO items VALUES (?1, ?2, ?3, ?4, ?4)",
                    params![
                        sku,
                        format!("Item {i}"),
                        50 + (i % 47) * 35,
                        config.initial_stock
                    ],
                )?;
                db.execute(
                    "INSERT INTO stock_changes VALUES (?1, ?2, ?3)",
                    params![sku, config.initial_stock, timestamp],
                )?;
            }
            let snapshot = json!({"windowSize": config.window_size, "slideInterval": config.slide_interval,
                "windowStart": 0, "windowEnd": 0, "computedAt": timestamp, "items": []});
            db.execute(
                "INSERT INTO popularity VALUES (1, ?1)",
                [snapshot.to_string()],
            )?;
        }
        db.commit()?;
        Ok(Self {
            connection: Mutex::new(connection),
            config,
        })
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn read<T>(&self, f: impl FnOnce(&Repository<'_>) -> Result<T>) -> Result<T> {
        let db = self.connection.lock().map_err(Error::internal)?;
        f(&Repository { connection: &db })
    }

    pub(crate) fn write<T>(&self, f: impl FnOnce(&Repository<'_>) -> Result<T>) -> Result<T> {
        let mut connection = self.connection.lock().map_err(Error::internal)?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = f(&Repository { connection: &tx })?;
        tx.commit()?;
        Ok(result)
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::internal(error)
    }
}

// A repository is borrowed only for the duration of a read or atomic write.
// It cannot leak the connection, commit independently, or outlive the lock.
pub(crate) struct Repository<'a> {
    connection: &'a Connection,
}
impl Repository<'_> {
    pub fn catalog(&self) -> Result<Vec<Item>> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT sku, name, price_cents FROM items ORDER BY sku")?;
        Ok(stmt
            .query_map([], |r| {
                Ok(Item {
                    sku: r.get(0)?,
                    name: r.get(1)?,
                    cents: r.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }
    pub fn item(&self, sku: &str) -> Result<Option<Item>> {
        Ok(self
            .connection
            .query_row(
                "SELECT sku, name, price_cents FROM items WHERE sku=?1",
                [sku],
                |r| {
                    Ok(Item {
                        sku: r.get(0)?,
                        name: r.get(1)?,
                        cents: r.get(2)?,
                    })
                },
            )
            .optional()?)
    }
    pub fn transaction(&self, id: i64) -> Result<Option<Basket>> {
        Ok(self.connection.query_row("SELECT station_id, status, item_count, total_cents, started_at FROM transactions WHERE id=?1", [id], |r|
            Ok(Basket { id, station: r.get(0)?, status: r.get(1)?, count: r.get(2)?, cents: r.get(3)?, started_at: r.get(4)? })).optional()?)
    }
    pub fn insert_transaction(&self, station: &str, timestamp: &str) -> Result<i64> {
        self.connection.execute(
            "INSERT INTO transactions(station_id, started_at) VALUES (?1, ?2)",
            params![station, timestamp],
        )?;
        Ok(self.connection.last_insert_rowid())
    }
    pub fn add_unit(&self, id: i64, item: &Item) -> Result<()> {
        self.connection.execute("INSERT INTO lines VALUES (?1, ?2, 1) ON CONFLICT(transaction_id, sku) DO UPDATE SET quantity=quantity+1", params![id, item.sku])?;
        self.connection.execute("UPDATE transactions SET item_count=item_count+1, total_cents=total_cents+?1 WHERE id=?2", params![item.cents, id])?;
        Ok(())
    }
    pub fn lines(&self, id: i64) -> Result<Vec<Line>> {
        let mut stmt = self.connection.prepare_cached("SELECT l.sku, i.name, i.price_cents, l.quantity FROM lines l JOIN items i ON i.sku=l.sku WHERE l.transaction_id=?1 ORDER BY l.sku")?;
        Ok(stmt
            .query_map([id], |r| {
                Ok(Line {
                    item: Item {
                        sku: r.get(0)?,
                        name: r.get(1)?,
                        cents: r.get(2)?,
                    },
                    quantity: r.get(3)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }
    pub fn decrement(&self, sku: &str, quantity: i64, timestamp: &str) -> Result<bool> {
        let changed = self.connection.execute(
            "UPDATE items SET stock=stock-?1 WHERE sku=?2 AND stock>=?1",
            params![quantity, sku],
        )?;
        if changed == 1 {
            self.connection.execute(
                "INSERT INTO stock_changes SELECT sku, stock, ?1 FROM items WHERE sku=?2",
                params![timestamp, sku],
            )?;
        }
        Ok(changed == 1)
    }
    pub fn finalize(&self, id: i64, timestamp: &str) -> Result<()> {
        self.connection.execute(
            "UPDATE transactions SET status='COMPLETED', completed_at=?1 WHERE id=?2",
            params![timestamp, id],
        )?;
        Ok(())
    }
    pub fn insert_scan(&self, sku: &str) -> Result<i64> {
        self.connection
            .execute("INSERT INTO scans(sku) VALUES (?1)", [sku])?;
        Ok(self.connection.last_insert_rowid())
    }
    pub fn remove_scans_through(&self, sequence: i64) -> Result<()> {
        self.connection
            .execute("DELETE FROM scans WHERE sequence<=?1", [sequence])?;
        Ok(())
    }
    pub fn scan_counts(&self) -> Result<Vec<ScanCount>> {
        let mut stmt = self.connection.prepare_cached("SELECT s.sku, i.name, COUNT(*) FROM scans s JOIN items i ON i.sku=s.sku GROUP BY s.sku")?;
        Ok(stmt
            .query_map([], |r| {
                Ok(ScanCount {
                    sku: r.get(0)?,
                    name: r.get(1)?,
                    count: r.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }
    pub fn save_popularity(&self, snapshot: &Value) -> Result<()> {
        self.connection.execute(
            "UPDATE popularity SET snapshot=?1 WHERE id=1",
            [snapshot.to_string()],
        )?;
        Ok(())
    }
    pub fn popularity(&self) -> Result<Value> {
        let raw: String =
            self.connection
                .query_row("SELECT snapshot FROM popularity WHERE id=1", [], |r| {
                    r.get(0)
                })?;
        serde_json::from_str(&raw).map_err(Error::internal)
    }
    pub fn low_stock(&self, threshold: i64) -> Result<Vec<StockAlert>> {
        let mut stmt = self.connection.prepare_cached("SELECT i.sku, i.name, i.stock,
            (SELECT changed_at FROM stock_changes s WHERE s.sku=i.sku AND s.stock<?1 ORDER BY s.stock DESC LIMIT 1)
            FROM items i WHERE i.stock<?1 ORDER BY i.sku")?;
        Ok(stmt
            .query_map([threshold], |r| {
                Ok(StockAlert {
                    sku: r.get(0)?,
                    name: r.get(1)?,
                    stock: r.get(2)?,
                    triggered_at: r.get(3)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }
}
