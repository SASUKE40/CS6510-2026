//! payment.sqlite: the saga log and the payment records. Every state change
//! is conditional on the state the caller expects and reports whether it
//! happened, so a duplicate or stale event changes nothing.
use rusqlite::{Connection, params};
use serde_json::Value;
use service_kit::{Error, Kind, Result, now, sqlite::Database};

#[derive(Clone, Debug)]
pub struct Saga {
    pub id: i64,
    pub transaction_id: String,
    pub state: String,
    pub basket: Option<Value>,
    pub failure: Option<Error>,
    pub paid_at: Option<String>,
}

pub struct Store {
    db: Database,
}

impl Store {
    /// Opens (creating if needed) the payment database. With `reset`, drops
    /// every payment table first.
    pub fn open(path: &str, reset: bool) -> Result<Self> {
        let db = Database::open(path)?;
        db.write(|c| {
            if reset {
                c.execute_batch("DROP TABLE IF EXISTS payments; DROP TABLE IF EXISTS sagas;")?;
            }
            Ok(c.execute_batch(include_str!("schema.sql"))?)
        })?;
        Ok(Self { db })
    }

    pub fn start(&self, transaction_id: &str) -> Result<i64> {
        self.db.write(|c| {
            c.prepare_cached(
                "INSERT INTO sagas(transaction_id, state, created_at, updated_at) VALUES (?1, 'STARTED', ?2, ?2)",
            )?
            .execute(params![transaction_id, now()])?;
            Ok(c.last_insert_rowid())
        })
    }

    /// STARTED -> BASKET_LOCKED, keeping the locked basket.
    pub fn locked(&self, id: i64, basket: &Value) -> Result<bool> {
        self.db.write(|c| {
            Ok(c.prepare_cached(
                "UPDATE sagas SET state='BASKET_LOCKED', basket_json=?2, updated_at=?3
                 WHERE id=?1 AND state='STARTED'",
            )?
            .execute(params![id, basket.to_string(), now()])?
                == 1)
        })
    }

    /// The pivot, BASKET_LOCKED -> STOCK_RESERVED: stock is reserved and the
    /// charge is recorded in one local transaction. From here the saga only
    /// rolls forward.
    pub fn reserved(&self, id: i64, amount_cents: i64, paid_at: &str) -> Result<bool> {
        self.db.write(|c| {
            if !transition(c, id, "BASKET_LOCKED", "STOCK_RESERVED", None)? {
                return Ok(false);
            }
            c.prepare_cached(
                "INSERT INTO payments SELECT id, transaction_id, ?2, ?3 FROM sagas WHERE id=?1",
            )?
            .execute(params![id, amount_cents, paid_at])?;
            Ok(true)
        })
    }

    /// STOCK_RESERVED -> COMPLETED.
    pub fn completed(&self, id: i64) -> Result<bool> {
        self.db
            .write(|c| transition(c, id, "STOCK_RESERVED", "COMPLETED", None))
    }

    /// `from` -> RELEASING (the basket must be unlocked), recording `failure`
    /// if given.
    pub fn releasing(&self, id: i64, from: &str, failure: Option<&Error>) -> Result<bool> {
        self.db
            .write(|c| transition(c, id, from, "RELEASING", failure))
    }

    /// STARTED or BASKET_LOCKED -> COMPENSATING: recovery gives up on a
    /// stalled saga and undoes whatever it may have done.
    pub fn compensating(&self, id: i64, from: &str) -> Result<bool> {
        let failure = Error::new(
            Kind::Unavailable,
            "RECOVERED",
            "The checkout did not finish and was undone",
        );
        self.db
            .write(|c| transition(c, id, from, "COMPENSATING", Some(&failure)))
    }

    /// `from` -> ABORTED, recording `failure` if given.
    pub fn aborted(&self, id: i64, from: &str, failure: Option<&Error>) -> Result<bool> {
        self.db
            .write(|c| transition(c, id, from, "ABORTED", failure))
    }

    /// Marks a saga as just retried, so recovery waits a grace period again.
    pub fn touch(&self, id: i64) -> Result<()> {
        self.db.write(|c| {
            c.prepare_cached("UPDATE sagas SET updated_at=?2 WHERE id=?1")?
                .execute(params![id, now()])?;
            Ok(())
        })
    }

    pub fn get(&self, id: i64) -> Result<Saga> {
        self.db.read(|c| {
            sagas(c, "s.id=?1", params![id])?
                .pop()
                .ok_or_else(|| Error::missing("No such saga"))
        })
    }

    /// Sagas not yet COMPLETED or ABORTED whose last step is older than
    /// `before` (an RFC 3339 timestamp), oldest first.
    pub fn unfinished(&self, before: &str) -> Result<Vec<Saga>> {
        self.db.read(|c| {
            sagas(
                c,
                "s.state NOT IN ('COMPLETED','ABORTED') AND s.updated_at<?1",
                params![before],
            )
        })
    }
}

fn transition(
    c: &Connection,
    id: i64,
    from: &str,
    to: &str,
    failure: Option<&Error>,
) -> Result<bool> {
    let changed = match failure {
        Some(error) => c
            .prepare_cached(
                "UPDATE sagas SET state=?3, failure=?4, failure_kind=?5, failure_message=?6,
                 updated_at=?7 WHERE id=?1 AND state=?2",
            )?
            .execute(params![
                id,
                from,
                to,
                error.code,
                error.kind.as_str(),
                error.message,
                now()
            ])?,
        None => c
            .prepare_cached("UPDATE sagas SET state=?3, updated_at=?4 WHERE id=?1 AND state=?2")?
            .execute(params![id, from, to, now()])?,
    };
    Ok(changed == 1)
}

fn sagas(c: &Connection, filter: &str, args: &[&dyn rusqlite::ToSql]) -> Result<Vec<Saga>> {
    let mut stmt = c.prepare_cached(&format!(
        "SELECT s.id, s.transaction_id, s.state, s.basket_json, s.failure, s.failure_kind,
            s.failure_message, p.paid_at
         FROM sagas s LEFT JOIN payments p ON p.saga_id=s.id WHERE {filter} ORDER BY s.id"
    ))?;
    let rows = stmt
        .query_map(args, |r| {
            let failure = match r.get::<_, Option<String>>(4)? {
                Some(code) => Some(Error::new(
                    Kind::parse(&r.get::<_, Option<String>>(5)?.unwrap_or_default()),
                    code,
                    r.get::<_, Option<String>>(6)?.unwrap_or_default(),
                )),
                None => None,
            };
            Ok(Saga {
                id: r.get(0)?,
                transaction_id: r.get(1)?,
                state: r.get(2)?,
                basket: r
                    .get::<_, Option<String>>(3)?
                    .and_then(|s| serde_json::from_str(&s).ok()),
                failure,
                paid_at: r.get(7)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}
