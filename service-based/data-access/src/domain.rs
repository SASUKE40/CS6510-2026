//! Plain records shared by services and persistence; no HTTP or SQLite types.
use chrono::{SecondsFormat, Utc};

pub fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
pub fn money(cents: i64) -> f64 {
    cents as f64 / 100.0
}
pub struct Item {
    pub sku: String,
    pub name: String,
    pub cents: i64,
}
pub struct Line {
    pub item: Item,
    pub quantity: i64,
}
pub struct StockAlert {
    pub sku: String,
    pub name: String,
    pub stock: i64,
    pub triggered_at: String,
}
pub struct Basket {
    pub id: i64,
    pub station: String,
    pub status: String,
    pub count: i64,
    pub cents: i64,
    pub started_at: String,
}
impl Basket {
    pub fn is_open(&self) -> bool {
        self.status == "OPEN"
    }
}
