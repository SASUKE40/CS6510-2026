//! payment.sqlite: the saga log and the payment records.
use rusqlite::{Connection, params};
use serde_json::Value;
use service_kit::{Error, Result, now, sqlite::Database};

#[derive(Clone, Debug)]
pub struct Saga {
    pub id: i64,
    pub transaction_id: String,
    pub state: String,
    pub basket: Option<Value>,
    pub failure: Option<String>,
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

    pub fn locked(&self, id: i64) -> Result<()> {
        self.set_state(id, "BASKET_LOCKED")
    }

    /// The pivot: stock is reserved and the charge is recorded in one local
    /// transaction, with the basket needed to finish. From here the saga
    /// only rolls forward.
    pub fn reserved(
        &self,
        id: i64,
        basket: &Value,
        amount_cents: i64,
        paid_at: &str,
    ) -> Result<()> {
        self.db.write(|c| {
            c.prepare_cached(
                "UPDATE sagas SET state='STOCK_RESERVED', basket_json=?2, updated_at=?3 WHERE id=?1",
            )?
            .execute(params![id, basket.to_string(), paid_at])?;
            c.prepare_cached(
                "INSERT INTO payments SELECT id, transaction_id, ?2, ?3 FROM sagas WHERE id=?1",
            )?
            .execute(params![id, amount_cents, paid_at])?;
            Ok(())
        })
    }

    pub fn completed(&self, id: i64) -> Result<()> {
        self.set_state(id, "COMPLETED")
    }

    pub fn aborted(&self, id: i64, failure: &str) -> Result<()> {
        self.db.write(|c| {
            c.prepare_cached(
                "UPDATE sagas SET state='ABORTED', failure=?2, updated_at=?3 WHERE id=?1",
            )?
            .execute(params![id, failure, now()])?;
            Ok(())
        })
    }

    fn set_state(&self, id: i64, state: &str) -> Result<()> {
        self.db.write(|c| {
            c.prepare_cached("UPDATE sagas SET state=?2, updated_at=?3 WHERE id=?1")?
                .execute(params![id, state, now()])?;
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

fn sagas(c: &Connection, filter: &str, args: &[&dyn rusqlite::ToSql]) -> Result<Vec<Saga>> {
    let mut stmt = c.prepare_cached(&format!(
        "SELECT s.id, s.transaction_id, s.state, s.basket_json, s.failure, p.paid_at
         FROM sagas s LEFT JOIN payments p ON p.saga_id=s.id WHERE {filter} ORDER BY s.id"
    ))?;
    let rows = stmt
        .query_map(args, |r| {
            Ok(Saga {
                id: r.get(0)?,
                transaction_id: r.get(1)?,
                state: r.get(2)?,
                basket: r
                    .get::<_, Option<String>>(3)?
                    .and_then(|s| serde_json::from_str(&s).ok()),
                failure: r.get(4)?,
                paid_at: r.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}
