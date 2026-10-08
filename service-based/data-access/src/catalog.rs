//! Inventory context: `items` and `stock_changes`.
use crate::{Db, Item, Result, StockAlert};
use rusqlite::{OptionalExtension, params};

fn item(r: &rusqlite::Row<'_>) -> rusqlite::Result<Item> {
    Ok(Item {
        sku: r.get(0)?,
        name: r.get(1)?,
        cents: r.get(2)?,
    })
}

pub fn list(db: &Db<'_>) -> Result<Vec<Item>> {
    let mut stmt = db
        .connection
        .prepare_cached("SELECT sku, name, price_cents FROM items ORDER BY sku")?;
    Ok(stmt.query_map([], item)?.collect::<rusqlite::Result<_>>()?)
}

pub fn find(db: &Db<'_>, sku: &str) -> Result<Option<Item>> {
    Ok(db
        .connection
        .prepare_cached("SELECT sku, name, price_cents FROM items WHERE sku=?1")?
        .query_row([sku], item)
        .optional()?)
}

/// Items strictly below `threshold`, with the time each first crossed it.
pub fn low_stock(db: &Db<'_>, threshold: i64) -> Result<Vec<StockAlert>> {
    let mut stmt = db.connection.prepare_cached(
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
}

/// Guarded check-and-decrement in one statement: returns `false`, changing
/// nothing, when fewer than `quantity` units remain. Callers run it inside a
/// write unit of work so a later shortage rolls back earlier decrements.
pub fn decrement(db: &Db<'_>, sku: &str, quantity: i64, timestamp: &str) -> Result<bool> {
    let changed = db
        .connection
        .prepare_cached("UPDATE items SET stock=stock-?1 WHERE sku=?2 AND stock>=?1")?
        .execute(params![quantity, sku])?;
    if changed == 1 {
        db.connection
            .prepare_cached(
                "INSERT INTO stock_changes SELECT sku, stock, ?1 FROM items WHERE sku=?2",
            )?
            .execute(params![timestamp, sku])?;
    }
    Ok(changed == 1)
}
