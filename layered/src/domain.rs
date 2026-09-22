//! Plain values shared by services and persistence; no HTTP or SQLite types.
use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};

pub(crate) fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
pub(crate) fn money(cents: i64) -> f64 {
    cents as f64 / 100.0
}
pub(crate) struct Item {
    pub sku: String,
    pub name: String,
    pub cents: i64,
}
pub(crate) struct Line {
    pub item: Item,
    pub quantity: i64,
}
pub(crate) struct ScanCount {
    pub sku: String,
    pub name: String,
    pub count: i64,
}
pub(crate) struct StockAlert {
    pub sku: String,
    pub name: String,
    pub stock: i64,
    pub triggered_at: String,
}
pub(crate) struct Basket {
    pub id: i64,
    pub station: String,
    pub status: String,
    pub count: i64,
    pub cents: i64,
    pub started_at: String,
}
impl Basket {
    pub fn summary(&self) -> Value {
        json!({"transactionId": format!("tx-{}", self.id), "stationId": self.station,
            "status": self.status, "itemCount": self.count, "runningTotal": money(self.cents), "startedAt": self.started_at})
    }
}
