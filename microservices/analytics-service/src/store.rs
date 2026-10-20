//! analytics.sqlite: consumed scan events, the feed cursor, and the latest
//! published popular-items snapshot.
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use service_kit::{Error, Result, now, sqlite::Database};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub window_size: i64,
    pub slide_interval: i64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            window_size: 1000,
            slide_interval: 500,
        }
    }
}

pub struct ScanEvent {
    pub sequence: i64,
    pub sku: String,
    pub name: String,
}

pub struct Store {
    db: Database,
    settings: Settings,
}

impl Store {
    /// Opens (creating if needed) the analytics database. With `reset`,
    /// drops every analytics table first.
    pub fn open(path: &str, settings: &Settings, reset: bool) -> Result<Self> {
        if settings.window_size <= 0 || settings.slide_interval <= 0 {
            return Err(Error::bad(
                "window size and slide interval must be positive",
            ));
        }
        let db = Database::open(path)?;
        db.write(|c| {
            if reset {
                c.execute_batch(
                    "DROP TABLE IF EXISTS scans; DROP TABLE IF EXISTS cursor;
                     DROP TABLE IF EXISTS popularity; DROP TABLE IF EXISTS settings;",
                )?;
            }
            c.execute_batch(include_str!("schema.sql"))?;
            let existing = c
                .query_row(
                    "SELECT window_size, slide_interval FROM settings WHERE id=1",
                    [],
                    |r| {
                        Ok(Settings {
                            window_size: r.get(0)?,
                            slide_interval: r.get(1)?,
                        })
                    },
                )
                .optional()?;
            match existing {
                Some(existing) if &existing != settings => Err(Error::bad(format!(
                    "{path} was created with {existing:?}; use those settings or --reset=true"
                ))),
                Some(_) => Ok(()),
                None => {
                    c.execute(
                        "INSERT INTO settings VALUES (1, ?1, ?2)",
                        params![settings.window_size, settings.slide_interval],
                    )?;
                    c.execute("INSERT INTO cursor VALUES (1, 0)", [])?;
                    let empty = json!({"windowSize": settings.window_size,
                        "slideInterval": settings.slide_interval, "windowStart": 0,
                        "windowEnd": 0, "computedAt": now(), "items": []});
                    c.execute("INSERT INTO popularity VALUES (1, ?1)", [empty.to_string()])?;
                    Ok(())
                }
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

    pub fn cursor(&self) -> Result<i64> {
        self.db.read(cursor)
    }

    pub fn snapshot(&self) -> Result<Value> {
        self.db.read(snapshot)
    }

    pub fn retained(&self) -> Result<i64> {
        self.db
            .read(|c| Ok(c.query_row("SELECT COUNT(*) FROM scans", [], |r| r.get(0))?))
    }

    /// Stores a batch of feed events, advances the cursor, and publishes the
    /// newest completed hop if a slide boundary was crossed, all in one local
    /// transaction. Already-consumed events are skipped; a gap is an error.
    /// Returns the new cursor and the new snapshot if one was published.
    pub fn ingest(&self, events: &[ScanEvent]) -> Result<(i64, Option<Value>)> {
        let Settings {
            window_size: size,
            slide_interval: slide,
        } = self.settings;
        self.db.write(|c| {
            let mut last = cursor(c)?;
            let mut insert = c.prepare_cached("INSERT INTO scans VALUES (?1, ?2, ?3)")?;
            for event in events {
                if event.sequence <= last {
                    continue;
                }
                if event.sequence != last + 1 {
                    return Err(Error::internal(format!(
                        "scan feed gap: expected {}, got {}",
                        last + 1,
                        event.sequence
                    )));
                }
                insert.execute(params![event.sequence, event.sku, event.name])?;
                last = event.sequence;
            }
            c.prepare_cached("UPDATE cursor SET last_sequence=?1 WHERE id=1")?
                .execute([last])?;
            let end = last / slide * slide;
            if end <= snapshot(c)?["windowEnd"].as_i64().unwrap_or(0) {
                return Ok((last, None));
            }
            let start = (end - size + 1).max(1);
            let mut ranked = c.prepare_cached(
                "SELECT sku, MIN(name), COUNT(*) AS n FROM scans WHERE sequence BETWEEN ?1 AND ?2
                 GROUP BY sku ORDER BY n DESC, sku",
            )?;
            let items: Vec<_> = ranked
                .query_map([start, end], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
                .into_iter()
                .enumerate()
                .map(|(index, (sku, name, count))| {
                    json!({"sku": sku, "name": name, "scanCount": count, "rank": index + 1})
                })
                .collect();
            let published = json!({"windowSize": size, "slideInterval": slide,
                "windowStart": start, "windowEnd": end, "computedAt": now(), "items": items});
            c.prepare_cached("UPDATE popularity SET snapshot=?1 WHERE id=1")?
                .execute([published.to_string()])?;
            // No future window can reach back past end - size.
            c.prepare_cached("DELETE FROM scans WHERE sequence<=?1")?
                .execute([end - size])?;
            Ok((last, Some(published)))
        })
    }
}

fn cursor(c: &Connection) -> Result<i64> {
    Ok(
        c.prepare_cached("SELECT last_sequence FROM cursor WHERE id=1")?
            .query_row([], |r| r.get(0))?,
    )
}

fn snapshot(c: &Connection) -> Result<Value> {
    let raw: String = c
        .prepare_cached("SELECT snapshot FROM popularity WHERE id=1")?
        .query_row([], |r| r.get(0))?;
    serde_json::from_str(&raw).map_err(Error::internal)
}
