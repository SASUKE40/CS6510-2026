//! Catalog and low-stock application services.
use crate::{
    database::Database,
    domain::{money, now},
    error::{Error, Result},
};
use serde_json::{Value, json};
use std::sync::Arc;
#[derive(Clone)]
pub(crate) struct Inventory {
    database: Arc<Database>,
}
impl Inventory {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }
    pub fn catalog(&self) -> Result<Value> {
        self.database.read(|repo| {
            let items: Vec<_> = repo
                .catalog()?
                .into_iter()
                .map(|i| json!({"sku": i.sku, "name": i.name, "price": money(i.cents)}))
                .collect();
            Ok(json!({"items": items}))
        })
    }
    pub fn low_stock(&self, threshold: Option<i64>) -> Result<Value> {
        let threshold = threshold.unwrap_or(self.database.config().threshold);
        if threshold < 0 {
            return Err(Error::bad("threshold must be nonnegative"));
        }
        self.database.read(|repo| {
            let alerts: Vec<_> = repo
                .low_stock(threshold)?
                .into_iter()
                .map(|a| {
                    json!({"sku": a.sku, "name": a.name,
                "currentStock": a.stock, "threshold": threshold, "triggeredAt": a.triggered_at})
                })
                .collect();
            Ok(json!({"threshold": threshold, "generatedAt": now(), "alerts": alerts}))
        })
    }
}
