//! The scan log that couples basket (producer) and analytics (consumer)
//! through the database instead of a direct call.
use crate::{Db, Result};

/// Appends one accepted scan. The database assigns the global, gap-free,
/// commit-ordered sequence number.
pub fn append(db: &Db<'_>, sku: &str) -> Result<i64> {
    db.connection
        .prepare_cached("INSERT INTO scans(sku) VALUES (?1)")?
        .execute([sku])?;
    Ok(db.connection.last_insert_rowid())
}

/// Sequence number of the most recent committed scan, or 0.
pub fn latest_sequence(db: &Db<'_>) -> Result<i64> {
    Ok(db
        .connection
        .prepare_cached("SELECT COALESCE(MAX(sequence), 0) FROM scans")?
        .query_row([], |r| r.get(0))?)
}

/// Per-SKU scan counts in the inclusive range, most scanned first, ties by
/// SKU, with item names.
pub fn ranked_counts(db: &Db<'_>, start: i64, end: i64) -> Result<Vec<(String, String, i64)>> {
    let mut stmt = db.connection.prepare_cached(
        "SELECT s.sku, i.name, COUNT(*) AS n FROM scans s JOIN items i ON i.sku=s.sku
         WHERE s.sequence BETWEEN ?1 AND ?2 GROUP BY s.sku ORDER BY n DESC, s.sku",
    )?;
    Ok(stmt
        .query_map([start, end], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?)
}

/// Deletes scans that can no longer fall inside any future window.
pub fn trim_through(db: &Db<'_>, sequence: i64) -> Result<()> {
    db.connection
        .prepare_cached("DELETE FROM scans WHERE sequence<=?1")?
        .execute([sequence])?;
    Ok(())
}
