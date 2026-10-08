//! Analytics context: the latest published popular-items snapshot.
use crate::{Db, Error, Result};
use serde_json::Value;

pub fn load(db: &Db<'_>) -> Result<Value> {
    let raw: String = db
        .connection
        .prepare_cached("SELECT snapshot FROM popularity WHERE id=1")?
        .query_row([], |r| r.get(0))?;
    serde_json::from_str(&raw).map_err(Error::internal)
}

pub fn save(db: &Db<'_>, snapshot: &Value) -> Result<()> {
    db.connection
        .prepare_cached("UPDATE popularity SET snapshot=?1 WHERE id=1")?
        .execute([snapshot.to_string()])?;
    Ok(())
}
