//! Schema ownership: creation, seeding, and reset of the shared database.
//! Used by the `db-admin` deployable, never by a running domain service.
use crate::{
    domain::now,
    store::{SCHEMA_VERSION, Settings, connect, read_settings},
};
use rusqlite::{OpenFlags, TransactionBehavior, params};
use serde_json::json;

pub enum Outcome {
    Seeded,
    AlreadyInitialized,
}

/// Creates the schema and seeds the catalog if needed. With `reset`, drops
/// every application table first. Stop all services before resetting.
pub fn initialize(
    path: &str,
    settings: &Settings,
    reset: bool,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    if settings.catalog_size <= 0
        || settings.initial_stock < 0
        || settings.threshold < 0
        || settings.window_size <= 0
        || settings.slide_interval <= 0
    {
        return Err(
            "Catalog/window/slide must be positive; stock/threshold must be nonnegative".into(),
        );
    }
    let mut connection = connect(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )?;
    let db = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if reset {
        db.execute_batch(
            "DROP TABLE IF EXISTS lines; DROP TABLE IF EXISTS scans;
             DROP TABLE IF EXISTS stock_changes; DROP TABLE IF EXISTS transactions;
             DROP TABLE IF EXISTS items; DROP TABLE IF EXISTS settings; DROP TABLE IF EXISTS popularity;",
        )?;
    }
    db.execute_batch(include_str!("schema.sql"))?;
    let outcome = if let Some(existing) = read_settings(&db)? {
        if &existing != settings {
            return Err("Database settings differ; use the original settings or --reset".into());
        }
        Outcome::AlreadyInitialized
    } else {
        db.execute(
            "INSERT INTO settings VALUES (1, ?1, ?2, ?3, ?4, ?5)",
            params![
                settings.catalog_size,
                settings.initial_stock,
                settings.threshold,
                settings.window_size,
                settings.slide_interval
            ],
        )?;
        let timestamp = now();
        let mut item = db.prepare("INSERT INTO items VALUES (?1, ?2, ?3, ?4, ?4)")?;
        let mut change = db.prepare("INSERT INTO stock_changes VALUES (?1, ?2, ?3)")?;
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
        drop((item, change));
        let snapshot = json!({"windowSize": settings.window_size, "slideInterval": settings.slide_interval,
            "windowStart": 0, "windowEnd": 0, "computedAt": timestamp, "items": []});
        db.execute(
            "INSERT INTO popularity VALUES (1, ?1)",
            [snapshot.to_string()],
        )?;
        Outcome::Seeded
    };
    db.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    db.commit()?;
    Ok(outcome)
}
