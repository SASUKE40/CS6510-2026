//! Checkout rules and atomic application workflows; no SQL or HTTP types.
use crate::{
    analytics::Analytics,
    database::{Database, Repository},
    domain::{Basket, money, now},
    error::{Error, Result},
};
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Transactions {
    database: Arc<Database>,
    analytics: Analytics,
}
impl Transactions {
    pub fn new(database: Arc<Database>, analytics: Analytics) -> Self {
        Self {
            database,
            analytics,
        }
    }
    pub fn start(&self, station: &str) -> Result<Value> {
        if station.trim().is_empty() {
            return Err(Error::bad("stationId must not be blank"));
        }
        self.database.write(|repo| {
            let id = repo.insert_transaction(station, &now())?;
            Ok(basket(repo, id)?.summary())
        })
    }
    pub fn status(&self, id: &str) -> Result<Value> {
        let id = parse_id(id)?;
        self.database.read(|repo| Ok(basket(repo, id)?.summary()))
    }
    pub fn scan(&self, id: &str, sku: &str) -> Result<Value> {
        let id = parse_id(id)?;
        self.database.write(|repo| {
            require_open(&basket(repo, id)?)?;
            let item = repo.item(sku)?.ok_or_else(|| Error::not_found("UNKNOWN_SKU", "No such SKU"))?;
            repo.add_unit(id, &item)?;
            self.analytics.record_scan(repo, sku)?;
            let updated = basket(repo, id)?;
            Ok(json!({"transactionId": format!("tx-{id}"), "sku": item.sku, "name": item.name,
                "unitPrice": money(item.cents), "itemCount": updated.count, "runningTotal": money(updated.cents)}))
        })
    }
    pub fn complete(&self, id: &str) -> Result<Value> {
        let id = parse_id(id)?;
        self.database.write(|repo| {
            let current = basket(repo, id)?;
            require_open(&current)?;
            if current.count == 0 { return Err(Error::conflict("EMPTY_BASKET", "Cannot complete an empty transaction")); }
            let lines = repo.lines(id)?;
            let completed_at = now();
            for line in &lines {
                if !repo.decrement(&line.item.sku, line.quantity, &completed_at)? {
                    return Err(Error::conflict("INSUFFICIENT_STOCK", "Insufficient stock to complete the whole basket"));
                }
            }
            repo.finalize(id, &completed_at)?;
            let lines: Vec<_> = lines.into_iter().map(|line| json!({"sku": line.item.sku, "name": line.item.name,
                "unitPrice": money(line.item.cents), "quantity": line.quantity})).collect();
            Ok(json!({"transactionId": format!("tx-{id}"), "stationId": current.station, "itemCount": current.count,
                "totalAmount": money(current.cents), "startedAt": current.started_at, "completedAt": completed_at, "lines": lines}))
        })
    }
}
fn parse_id(id: &str) -> Result<i64> {
    id.strip_prefix("tx-")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| n > 0)
        .ok_or_else(|| Error::missing("No such transaction"))
}
fn basket(repo: &Repository<'_>, id: i64) -> Result<Basket> {
    repo.transaction(id)?
        .ok_or_else(|| Error::missing("No such transaction"))
}
fn require_open(basket: &Basket) -> Result<()> {
    if basket.status != "OPEN" {
        Err(Error::conflict(
            "TRANSACTION_NOT_OPEN",
            "Transaction is not open",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    #[test]
    fn failed_unit_of_work_rolls_back_basket_and_analytics_together() {
        let db = Arc::new(
            Database::open(
                ":memory:",
                Config {
                    window_size: 1,
                    slide_interval: 1,
                    ..Config::default()
                },
                false,
            )
            .unwrap(),
        );
        let analytics = Analytics::new(db.clone());
        let service = Transactions::new(db.clone(), analytics.clone());
        let started = service.start("unit-test").unwrap();
        let id = started["transactionId"].as_str().unwrap();
        let result: Result<()> = db.write(|repo| {
            let item = repo.item("SKU-000001")?.unwrap();
            repo.add_unit(parse_id(id)?, &item)?;
            analytics.record_scan(repo, &item.sku)?;
            Err(Error::conflict("TEST_FAILURE", "Failure before commit"))
        });
        assert!(result.is_err());
        assert_eq!(service.status(id).unwrap()["itemCount"], 0);
        assert_eq!(analytics.popular(10).unwrap()["windowEnd"], 0);
        // AUTOINCREMENT and the ranking must recover without a skipped scan.
        service.scan(id, "SKU-000001").unwrap();
        assert_eq!(analytics.popular(10).unwrap()["windowEnd"], 1);
        assert_eq!(service.complete(id).unwrap()["itemCount"], 1);
    }
}
