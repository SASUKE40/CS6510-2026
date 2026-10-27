//! Connection handling for a service's private SQLite file. Each service
//! owns its schema and SQL; this module only provides units of work.
use crate::error::{Error, Result};
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use std::{
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

/// Most units of work one commit may cover: `CHECKOUT_MAX_BATCH`, default 1.
/// At 1 every unit commits and fsyncs on its own; larger values let queued
/// writers share one fsync.
fn max_batch() -> usize {
    static MAX_BATCH: OnceLock<usize> = OnceLock::new();
    *MAX_BATCH.get_or_init(|| {
        std::env::var("CHECKOUT_MAX_BATCH")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(1)
    })
}

/// One writer and one reader connection. WAL lets reads proceed while a
/// write is in progress, so feed and lookup queries never queue behind scans.
///
/// Writes can be group-committed: each unit of work runs in its own savepoint
/// inside a shared transaction, and the last writer in line, or the one that
/// fills the batch to [`max_batch`], commits for the whole batch. No caller
/// returns before the commit covering its work, and readers only see
/// committed data.
pub struct Database {
    writer: Mutex<Writer>,
    queued: AtomicUsize,
    reader: Mutex<Connection>,
}

struct Writer {
    connection: Connection,
    batch: Option<Batch>,
}

struct Batch {
    size: usize,
    outcome: Arc<Outcome>,
}

#[derive(Default)]
struct Outcome {
    result: Mutex<Option<Result<()>>>,
    done: Condvar,
}

impl Outcome {
    fn finish(&self, result: Result<()>) {
        if let Ok(mut slot) = self.result.lock() {
            *slot = Some(result);
        }
        self.done.notify_all();
    }

    fn wait(&self) -> Result<()> {
        let slot = self.result.lock().map_err(Error::internal)?;
        let slot = self
            .done
            .wait_while(slot, |slot| slot.is_none())
            .map_err(Error::internal)?;
        slot.clone()
            .unwrap_or_else(|| Err(Error::internal("batch outcome missing")))
    }
}

impl Database {
    /// Opens the file, creating it if needed.
    pub fn open(path: &str) -> Result<Self> {
        let writer = connect(path)?;
        let reader = connect(path)?;
        reader.pragma_update(None, "query_only", true)?;
        Ok(Self {
            writer: Mutex::new(Writer {
                connection: writer,
                batch: None,
            }),
            queued: AtomicUsize::new(0),
            reader: Mutex::new(reader),
        })
    }

    /// Runs `f` in a read transaction so multi-statement reads see one snapshot.
    pub fn read<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let mut connection = self.reader.lock().map_err(Error::internal)?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let result = f(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    /// Runs `f` as one atomic unit of work and returns once it is durable.
    /// If `f` fails, only its own changes are rolled back.
    pub fn write<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        self.queued.fetch_add(1, Ordering::SeqCst);
        let locked = self.writer.lock();
        self.queued.fetch_sub(1, Ordering::SeqCst);
        let mut writer = locked.map_err(Error::internal)?;
        let Writer { connection, batch } = &mut *writer;

        if batch.is_none() {
            connection.execute_batch("BEGIN IMMEDIATE")?;
            *batch = Some(Batch {
                size: 0,
                outcome: Arc::default(),
            });
        }
        let result = unit(connection, f);
        let Some(mut current) = batch.take() else {
            return Err(Error::internal("write batch missing"));
        };
        let outcome = Arc::clone(&current.outcome);

        if connection.is_autocommit() {
            // SQLite aborted the whole transaction, discarding the batch.
            drop(writer);
            outcome.finish(Err(Error::unavailable("write batch was rolled back")));
        } else {
            current.size += 1;
            if self.queued.load(Ordering::SeqCst) == 0 || current.size >= max_batch() {
                let committed = connection.execute_batch("COMMIT").map_err(Error::from);
                if committed.is_err() && !connection.is_autocommit() {
                    let _ = connection.execute_batch("ROLLBACK");
                }
                drop(writer);
                outcome.finish(committed);
            } else {
                *batch = Some(current);
                drop(writer);
            }
        }
        outcome.wait()?;
        result
    }
}

fn unit<T>(connection: &Connection, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    connection.execute_batch("SAVEPOINT unit")?;
    match f(connection) {
        Ok(value) => {
            connection.execute_batch("RELEASE unit")?;
            Ok(value)
        }
        Err(error) => {
            if !connection.is_autocommit() {
                connection.execute_batch("ROLLBACK TO unit; RELEASE unit")?;
            }
            Err(error)
        }
    }
}

fn connect(path: &str) -> rusqlite::Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    connection.busy_timeout(Duration::from_secs(10))?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
    )?;
    Ok(connection)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_writes_commit_only_successful_units() {
        let path = std::env::temp_dir().join(format!("group-commit-{}.db", std::process::id()));
        let path = path.to_str().unwrap().to_owned();
        let db = Arc::new(Database::open(&path).unwrap());
        db.write(|c| Ok(c.execute_batch("CREATE TABLE t(thread INTEGER, n INTEGER)")?))
            .unwrap();

        let threads: Vec<_> = (0..16)
            .map(|thread| {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    let mut kept = 0;
                    for n in 0..200 {
                        let result = db.write(|c| {
                            c.execute("INSERT INTO t VALUES (?1, ?2)", (thread, n))?;
                            if n % 10 == 0 {
                                return Err(Error::bad("rejected"));
                            }
                            Ok(())
                        });
                        kept += usize::from(result.is_ok());
                    }
                    kept
                })
            })
            .collect();
        let kept: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
        assert_eq!(kept, 16 * 180);

        let rows: i64 = db
            .read(|c| Ok(c.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))?))
            .unwrap();
        let rejected: i64 = db
            .read(|c| Ok(c.query_row("SELECT COUNT(*) FROM t WHERE n % 10 = 0", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!((rows, rejected), (kept as i64, 0));

        drop(db);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }
}
