//! Connection management shared by every service process.
use crate::error::{Error, Result};
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use std::{sync::Mutex, time::Duration};

/// Bumped whenever `schema.sql` changes; services refuse to open a database
/// that `db-admin` initialized with a different version.
pub const SCHEMA_VERSION: i64 = 1;

/// System-wide settings stored once in the database, so every service
/// applies the same threshold and window parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub catalog_size: i64,
    pub initial_stock: i64,
    pub threshold: i64,
    pub window_size: i64,
    pub slide_interval: i64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            catalog_size: 2000,
            initial_stock: 10000,
            threshold: 50,
            window_size: 1000,
            slide_interval: 500,
        }
    }
}

/// One service's connection to the shared database. Writes inside a process
/// are serialized by the mutex; writes across processes by SQLite's lock.
pub struct Store {
    connection: Mutex<Connection>,
    settings: Settings,
}

/// A borrowed handle for repository calls. It exists only inside
/// [`Store::read`] or [`Store::write`], so a service can neither leak the
/// connection nor commit part of a unit of work.
pub struct Db<'a> {
    pub(crate) connection: &'a Connection,
}

impl Store {
    /// Opens an existing database initialized by `db-admin`. Never creates
    /// or migrates the schema.
    pub fn open(path: &str) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let connection = connect(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(|e| format!("cannot open {path}: {e}; run checkout-db-admin first"))?;
        let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version != SCHEMA_VERSION {
            return Err(format!(
                "{path} has schema version {version}, expected {SCHEMA_VERSION}; run checkout-db-admin"
            )
            .into());
        }
        let settings = read_settings(&connection)?.ok_or("settings row is missing")?;
        Ok(Self {
            connection: Mutex::new(connection),
            settings,
        })
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Runs `f` in a read transaction so multi-statement reads see one snapshot.
    pub fn read<T>(&self, f: impl FnOnce(&Db<'_>) -> Result<T>) -> Result<T> {
        let mut connection = self.connection.lock().map_err(Error::internal)?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let result = f(&Db { connection: &tx })?;
        tx.commit()?;
        Ok(result)
    }

    /// Runs `f` as one atomic unit of work (`BEGIN IMMEDIATE`), committing
    /// only if it succeeds.
    pub fn write<T>(&self, f: impl FnOnce(&Db<'_>) -> Result<T>) -> Result<T> {
        let mut connection = self.connection.lock().map_err(Error::internal)?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = f(&Db { connection: &tx })?;
        tx.commit()?;
        Ok(result)
    }
}

pub(crate) fn connect(path: &str, flags: OpenFlags) -> rusqlite::Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        flags | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI,
    )?;
    connection.busy_handler(Some(wait_for_writer))?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
    )?;
    Ok(connection)
}

pub(crate) fn read_settings(connection: &Connection) -> rusqlite::Result<Option<Settings>> {
    use rusqlite::OptionalExtension;
    connection
        .query_row(
            "SELECT catalog_size, initial_stock, threshold, window_size, slide_interval FROM settings WHERE id=1",
            [],
            |r| {
                Ok(Settings {
                    catalog_size: r.get(0)?,
                    initial_stock: r.get(1)?,
                    threshold: r.get(2)?,
                    window_size: r.get(3)?,
                    slide_interval: r.get(4)?,
                })
            },
        )
        .optional()
}

// Several processes now write the same file. SQLite's default busy handler
// backs off up to 100 ms per retry, which lets one process starve another and
// inflates tail latency. Polling briefly keeps the lock handoff close to FIFO.
const BUSY_POLL: Duration = Duration::from_micros(200);
const BUSY_LIMIT: Duration = Duration::from_secs(10);
fn wait_for_writer(attempt: i32) -> bool {
    if BUSY_POLL * attempt as u32 >= BUSY_LIMIT {
        return false;
    }
    std::thread::sleep(BUSY_POLL);
    true
}
