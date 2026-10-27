//! basket.sqlite: baskets, their lines, the catalog replica, and the scan
//! outbox. Basket state changes requested by a payment saga are conditional
//! on the saga id that holds the basket, so retries and stale sagas are safe.
use rusqlite::{Connection, OptionalExtension, params};
use service_kit::{Error, Result, now, sqlite::Database};

pub struct CatalogItem {
    pub sku: String,
    pub name: String,
    pub cents: i64,
}
pub struct Line {
    pub item: CatalogItem,
    pub quantity: i64,
}
pub struct Basket {
    pub id: i64,
    pub station: String,
    pub status: String,
    pub count: i64,
    pub cents: i64,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub checkout_id: Option<String>,
}
impl Basket {
    fn held_by(&self, saga: &str) -> bool {
        self.checkout_id.as_deref() == Some(saga)
    }
}
pub struct ScanEvent {
    pub sequence: i64,
    pub sku: String,
    pub name: String,
}

pub struct Store {
    db: Database,
}

impl Store {
    /// Opens (creating if needed) the basket database. With `reset`, drops
    /// every basket table first.
    pub fn open(path: &str, reset: bool) -> Result<Self> {
        let db = Database::open(path)?;
        db.write(|c| {
            if reset {
                c.execute_batch(
                    "DROP TABLE IF EXISTS lines; DROP TABLE IF EXISTS released_sagas;
                     DROP TABLE IF EXISTS transactions; DROP TABLE IF EXISTS scan_events;
                     DROP TABLE IF EXISTS catalog;",
                )?;
            }
            Ok(c.execute_batch(include_str!("schema.sql"))?)
        })?;
        Ok(Self { db })
    }

    pub fn catalog_size(&self) -> Result<i64> {
        self.db
            .read(|c| Ok(c.query_row("SELECT COUNT(*) FROM catalog", [], |r| r.get(0))?))
    }

    pub fn replace_catalog(&self, items: &[CatalogItem]) -> Result<()> {
        self.db.write(|c| {
            let mut upsert = c.prepare_cached(
                "INSERT INTO catalog VALUES (?1, ?2, ?3)
                 ON CONFLICT(sku) DO UPDATE SET name=excluded.name, price_cents=excluded.price_cents",
            )?;
            for item in items {
                upsert.execute(params![item.sku, item.name, item.cents])?;
            }
            Ok(())
        })
    }

    pub fn create(&self, station: &str) -> Result<Basket> {
        self.db.write(|c| {
            c.prepare_cached("INSERT INTO transactions(station_id, started_at) VALUES (?1, ?2)")?
                .execute(params![station, now()])?;
            basket(c, c.last_insert_rowid())
        })
    }

    pub fn get(&self, id: i64) -> Result<Basket> {
        self.db.read(|c| basket(c, id))
    }

    /// Adds one unit and appends its scan event in one local transaction, so
    /// an event exists exactly when a scan is accepted.
    pub fn scan(&self, id: i64, sku: &str) -> Result<(Basket, CatalogItem)> {
        self.db.write(|c| {
            if basket(c, id)?.status != "OPEN" {
                return Err(not_open());
            }
            let item = c
                .prepare_cached("SELECT sku, name, price_cents FROM catalog WHERE sku=?1")?
                .query_row([sku], |r| {
                    Ok(CatalogItem {
                        sku: r.get(0)?,
                        name: r.get(1)?,
                        cents: r.get(2)?,
                    })
                })
                .optional()?
                .ok_or_else(|| Error::not_found("UNKNOWN_SKU", "No such SKU"))?;
            c.prepare_cached(
                "INSERT INTO lines VALUES (?1, ?2, 1)
                 ON CONFLICT(transaction_id, sku) DO UPDATE SET quantity=quantity+1",
            )?
            .execute(params![id, item.sku])?;
            c.prepare_cached(
                "UPDATE transactions SET item_count=item_count+1, total_cents=total_cents+?1 WHERE id=?2",
            )?
            .execute(params![item.cents, id])?;
            c.prepare_cached("INSERT INTO scan_events(sku, name) VALUES (?1, ?2)")?
                .execute(params![item.sku, item.name])?;
            Ok((basket(c, id)?, item))
        })
    }

    /// The newest outbox sequence, or 0.
    pub fn last_sequence(&self) -> Result<i64> {
        self.db.read(|c| {
            Ok(c.query_row(
                "SELECT COALESCE(MAX(sequence), 0) FROM scan_events",
                [],
                |r| r.get(0),
            )?)
        })
    }

    /// Saga step: `OPEN -> CHECKING_OUT` held by `saga`. Replaying the same
    /// saga returns the same basket; a saga that already released is refused.
    pub fn checkout(&self, id: i64, saga: &str) -> Result<(Basket, Vec<Line>)> {
        self.db.write(|c| {
            let current = basket(c, id)?;
            if !(current.status == "CHECKING_OUT" && current.held_by(saga)) {
                if c.prepare_cached("SELECT 1 FROM released_sagas WHERE saga_id=?1")?
                    .exists([saga])?
                {
                    return Err(Error::conflict(
                        "SAGA_RELEASED",
                        "This checkout attempt was already released",
                    ));
                }
                if current.status != "OPEN" {
                    return Err(not_open());
                }
                if current.count == 0 {
                    return Err(Error::conflict(
                        "EMPTY_BASKET",
                        "Cannot complete an empty transaction",
                    ));
                }
                c.prepare_cached(
                    "UPDATE transactions SET status='CHECKING_OUT', checkout_id=?2 WHERE id=?1",
                )?
                .execute(params![id, saga])?;
            }
            Ok((basket(c, id)?, lines(c, id)?))
        })
    }

    /// Compensation: `CHECKING_OUT -> OPEN` if `saga` holds the basket, and
    /// a tombstone so a late checkout from `saga` cannot re-lock it.
    /// Returns whether it released.
    pub fn release(&self, id: i64, saga: &str) -> Result<bool> {
        self.db.write(|c| {
            let current = basket(c, id)?;
            if current.status == "COMPLETED" && current.held_by(saga) {
                return Err(Error::conflict(
                    "TRANSACTION_COMPLETED",
                    "A completed transaction cannot be released",
                ));
            }
            c.prepare_cached("INSERT OR IGNORE INTO released_sagas VALUES (?1, ?2)")?
                .execute(params![saga, id])?;
            if !current.held_by(saga) {
                return Ok(false);
            }
            c.prepare_cached(
                "UPDATE transactions SET status='OPEN', checkout_id=NULL WHERE id=?1",
            )?
            .execute([id])?;
            Ok(true)
        })
    }

    /// Saga step: `CHECKING_OUT -> COMPLETED`. Replaying is a no-op.
    pub fn complete(&self, id: i64, saga: &str, completed_at: &str) -> Result<Basket> {
        self.db.write(|c| {
            let current = basket(c, id)?;
            if !current.held_by(saga) {
                return Err(not_open());
            }
            if current.status == "CHECKING_OUT" {
                c.prepare_cached(
                    "UPDATE transactions SET status='COMPLETED', completed_at=?2 WHERE id=?1",
                )?
                .execute(params![id, completed_at])?;
            }
            basket(c, id)
        })
    }

    /// Outbox feed: events with sequence greater than `after`, oldest first.
    pub fn events(&self, after: i64, limit: i64) -> Result<Vec<ScanEvent>> {
        self.db.read(|c| {
            let mut stmt = c.prepare_cached(
                "SELECT sequence, sku, name FROM scan_events WHERE sequence>?1 ORDER BY sequence LIMIT ?2",
            )?;
            Ok(stmt
                .query_map([after, limit], |r| {
                    Ok(ScanEvent {
                        sequence: r.get(0)?,
                        sku: r.get(1)?,
                        name: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?)
        })
    }
}

fn not_open() -> Error {
    Error::conflict("TRANSACTION_NOT_OPEN", "Transaction is not open")
}

fn basket(c: &Connection, id: i64) -> Result<Basket> {
    c.prepare_cached(
        "SELECT station_id, status, item_count, total_cents, started_at, completed_at, checkout_id
         FROM transactions WHERE id=?1",
    )?
    .query_row([id], |r| {
        Ok(Basket {
            id,
            station: r.get(0)?,
            status: r.get(1)?,
            count: r.get(2)?,
            cents: r.get(3)?,
            started_at: r.get(4)?,
            completed_at: r.get(5)?,
            checkout_id: r.get(6)?,
        })
    })
    .optional()?
    .ok_or_else(|| Error::missing("No such transaction"))
}

fn lines(c: &Connection, id: i64) -> Result<Vec<Line>> {
    let mut stmt = c.prepare_cached(
        "SELECT l.sku, k.name, k.price_cents, l.quantity FROM lines l JOIN catalog k ON k.sku=l.sku
         WHERE l.transaction_id=?1 ORDER BY l.sku",
    )?;
    Ok(stmt
        .query_map([id], |r| {
            Ok(Line {
                item: CatalogItem {
                    sku: r.get(0)?,
                    name: r.get(1)?,
                    cents: r.get(2)?,
                },
                quantity: r.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}
