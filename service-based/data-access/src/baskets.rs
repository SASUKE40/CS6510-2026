//! Basket context: `transactions` and `lines`.
use crate::{Basket, Db, Item, Line, Result};
use rusqlite::{OptionalExtension, params};

pub fn create(db: &Db<'_>, station: &str, timestamp: &str) -> Result<i64> {
    db.connection
        .prepare_cached("INSERT INTO transactions(station_id, started_at) VALUES (?1, ?2)")?
        .execute(params![station, timestamp])?;
    Ok(db.connection.last_insert_rowid())
}

pub fn get(db: &Db<'_>, id: i64) -> Result<Option<Basket>> {
    Ok(db
        .connection
        .prepare_cached(
            "SELECT station_id, status, item_count, total_cents, started_at FROM transactions WHERE id=?1",
        )?
        .query_row([id], |r| {
            Ok(Basket {
                id,
                station: r.get(0)?,
                status: r.get(1)?,
                count: r.get(2)?,
                cents: r.get(3)?,
                started_at: r.get(4)?,
            })
        })
        .optional()?)
}

pub fn add_unit(db: &Db<'_>, id: i64, item: &Item) -> Result<()> {
    db.connection
        .prepare_cached(
            "INSERT INTO lines VALUES (?1, ?2, 1)
             ON CONFLICT(transaction_id, sku) DO UPDATE SET quantity=quantity+1",
        )?
        .execute(params![id, item.sku])?;
    db.connection
        .prepare_cached(
            "UPDATE transactions SET item_count=item_count+1, total_cents=total_cents+?1 WHERE id=?2",
        )?
        .execute(params![item.cents, id])?;
    Ok(())
}

pub fn lines(db: &Db<'_>, id: i64) -> Result<Vec<Line>> {
    let mut stmt = db.connection.prepare_cached(
        "SELECT l.sku, i.name, i.price_cents, l.quantity FROM lines l JOIN items i ON i.sku=l.sku
         WHERE l.transaction_id=?1 ORDER BY l.sku",
    )?;
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
        .collect::<rusqlite::Result<_>>()?)
}

pub fn mark_completed(db: &Db<'_>, id: i64, timestamp: &str) -> Result<()> {
    db.connection
        .prepare_cached("UPDATE transactions SET status='COMPLETED', completed_at=?1 WHERE id=?2")?
        .execute(params![timestamp, id])?;
    Ok(())
}
