//! inventory.sqlite: the only place stock is stored. Every stock change is a
//! local transaction, so the `stock >= qty` guard needs no distributed lock.
use rusqlite::{Connection, OptionalExtension, params};
use service_kit::{Error, Result, now, sqlite::Database};
use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub catalog_size: i64,
    pub initial_stock: i64,
    pub threshold: i64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            catalog_size: 2000,
            initial_stock: 10000,
            threshold: 50,
        }
    }
}

pub struct Item {
    pub sku: String,
    pub name: String,
    pub cents: i64,
}
pub struct StockAlert {
    pub sku: String,
    pub name: String,
    pub stock: i64,
    pub triggered_at: String,
}

/// Result of a reserve call; `replayed` is true when the id was already reserved.
pub struct Reservation {
    pub replayed: bool,
}

pub struct Store {
    db: Database,
    settings: Settings,
}

impl Store {
    /// Opens (creating and seeding if needed) the inventory database. With
    /// `reset`, drops every inventory table first.
    pub fn open(path: &str, settings: &Settings, reset: bool) -> Result<Self> {
        if settings.catalog_size <= 0 || settings.initial_stock < 0 || settings.threshold < 0 {
            return Err(Error::bad(
                "catalog size must be positive; stock and threshold must be nonnegative",
            ));
        }
        let db = Database::open(path)?;
        db.write(|c| {
            if reset {
                c.execute_batch(
                    "DROP TABLE IF EXISTS reservation_lines; DROP TABLE IF EXISTS reservations;
                     DROP TABLE IF EXISTS stock_changes; DROP TABLE IF EXISTS items;
                     DROP TABLE IF EXISTS settings;",
                )?;
            }
            c.execute_batch(include_str!("schema.sql"))?;
            let existing = c
                .query_row(
                    "SELECT catalog_size, initial_stock, threshold FROM settings WHERE id=1",
                    [],
                    |r| {
                        Ok(Settings {
                            catalog_size: r.get(0)?,
                            initial_stock: r.get(1)?,
                            threshold: r.get(2)?,
                        })
                    },
                )
                .optional()?;
            match existing {
                Some(existing) if &existing != settings => Err(Error::bad(format!(
                    "{path} was seeded with {existing:?}; use those settings or --reset=true"
                ))),
                Some(_) => Ok(()),
                None => seed(c, settings),
            }
        })?;
        Ok(Self {
            db,
            settings: settings.clone(),
        })
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    pub fn items(&self) -> Result<Vec<Item>> {
        self.db.read(|c| {
            let mut stmt =
                c.prepare_cached("SELECT sku, name, price_cents FROM items ORDER BY sku")?;
            Ok(stmt
                .query_map([], |r| {
                    Ok(Item {
                        sku: r.get(0)?,
                        name: r.get(1)?,
                        cents: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?)
        })
    }

    /// Items strictly below `threshold`, with the time each last crossed it.
    pub fn low_stock(&self, threshold: i64) -> Result<Vec<StockAlert>> {
        self.db.read(|c| {
            let mut stmt = c.prepare_cached(
                "SELECT i.sku, i.name, i.stock,
                    (SELECT changed_at FROM stock_changes s WHERE s.sku=i.sku AND s.stock<?1 ORDER BY s.stock DESC LIMIT 1)
                 FROM items i WHERE i.stock<?1 ORDER BY i.sku",
            )?;
            Ok(stmt
                .query_map([threshold], |r| {
                    Ok(StockAlert {
                        sku: r.get(0)?,
                        name: r.get(1)?,
                        stock: r.get(2)?,
                        triggered_at: r.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?)
        })
    }

    /// Decrements every line or none. Idempotent per `id`: replaying a
    /// reserved id changes nothing, and a cancelled id is refused.
    pub fn reserve(&self, id: &str, lines: &[(String, i64)]) -> Result<Reservation> {
        if lines.is_empty() || lines.iter().any(|(_, q)| *q <= 0) {
            return Err(Error::bad(
                "a reservation needs lines with positive quantities",
            ));
        }
        if lines
            .iter()
            .map(|(sku, _)| sku)
            .collect::<HashSet<_>>()
            .len()
            != lines.len()
        {
            return Err(Error::bad("each SKU may appear once per reservation"));
        }
        self.db.write(|c| {
            match status(c, id)?.as_deref() {
                Some("RESERVED") => return Ok(Reservation { replayed: true }),
                Some(_) => {
                    return Err(Error::conflict(
                        "RESERVATION_CANCELLED",
                        "This reservation was already cancelled",
                    ));
                }
                None => {}
            }
            let timestamp = now();
            c.prepare_cached("INSERT INTO reservations VALUES (?1, 'RESERVED', ?2, ?2)")?
                .execute(params![id, timestamp])?;
            for (sku, quantity) in lines {
                let changed = c
                    .prepare_cached("UPDATE items SET stock=stock-?1 WHERE sku=?2 AND stock>=?1")?
                    .execute(params![quantity, sku])?;
                if changed == 0 {
                    let known = c
                        .prepare_cached("SELECT 1 FROM items WHERE sku=?1")?
                        .exists([sku])?;
                    // Returning an error rolls back the decrements of earlier lines.
                    return Err(if known {
                        Error::conflict(
                            "INSUFFICIENT_STOCK",
                            "Insufficient stock to complete the whole basket",
                        )
                    } else {
                        Error::not_found("UNKNOWN_SKU", "No such SKU")
                    });
                }
                record_level(c, sku, &timestamp)?;
                c.prepare_cached("INSERT INTO reservation_lines VALUES (?1, ?2, ?3)")?
                    .execute(params![id, sku, quantity])?;
            }
            Ok(Reservation { replayed: false })
        })
    }

    /// Restocks a reservation. Idempotent; an unknown id becomes a tombstone
    /// so a reserve call that arrives later is refused. Returns whether stock
    /// was put back.
    pub fn cancel(&self, id: &str) -> Result<bool> {
        self.db.write(|c| {
            let timestamp = now();
            match status(c, id)?.as_deref() {
                Some("CANCELLED") => Ok(false),
                Some(_) => {
                    let lines: Vec<(String, i64)> = c
                        .prepare_cached(
                            "SELECT sku, quantity FROM reservation_lines WHERE reservation_id=?1",
                        )?
                        .query_map([id], |r| Ok((r.get(0)?, r.get(1)?)))?
                        .collect::<rusqlite::Result<_>>()?;
                    for (sku, quantity) in &lines {
                        c.prepare_cached("UPDATE items SET stock=stock+?1 WHERE sku=?2")?
                            .execute(params![quantity, sku])?;
                        record_level(c, sku, &timestamp)?;
                    }
                    c.prepare_cached(
                        "UPDATE reservations SET status='CANCELLED', updated_at=?2 WHERE id=?1",
                    )?
                    .execute(params![id, timestamp])?;
                    Ok(true)
                }
                None => {
                    c.prepare_cached("INSERT INTO reservations VALUES (?1, 'CANCELLED', ?2, ?2)")?
                        .execute(params![id, timestamp])?;
                    Ok(false)
                }
            }
        })
    }
}

fn status(c: &Connection, id: &str) -> Result<Option<String>> {
    Ok(
        c.prepare_cached("SELECT status FROM reservations WHERE id=?1")?
            .query_row([id], |r| r.get(0))
            .optional()?,
    )
}

fn record_level(c: &Connection, sku: &str, timestamp: &str) -> Result<()> {
    c.prepare_cached(
        "INSERT OR REPLACE INTO stock_changes SELECT sku, stock, ?1 FROM items WHERE sku=?2",
    )?
    .execute(params![timestamp, sku])?;
    Ok(())
}

fn seed(c: &Connection, settings: &Settings) -> Result<()> {
    c.execute(
        "INSERT INTO settings VALUES (1, ?1, ?2, ?3)",
        params![
            settings.catalog_size,
            settings.initial_stock,
            settings.threshold
        ],
    )?;
    let timestamp = now();
    let mut item = c.prepare("INSERT INTO items VALUES (?1, ?2, ?3, ?4, ?4)")?;
    let mut change = c.prepare("INSERT INTO stock_changes VALUES (?1, ?2, ?3)")?;
    for i in 1..=settings.catalog_size {
        let sku = format!("SKU-{i:06}");
        item.execute(params![
            sku,
            format!("Item {i}"),
            50 + (i % 47) * 35,
            settings.initial_stock
        ])?;
        change.execute(params![sku, settings.initial_stock, timestamp])?;
    }
    Ok(())
}
