//! Composition root for the layered server.
mod analytics;
mod api;
mod config;
mod database;
mod domain;
mod error;
mod inventory;
mod transactions;

pub use config::Config;
pub use database::Database;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Services {
    transactions: transactions::Transactions,
    analytics: analytics::Analytics,
    inventory: inventory::Inventory,
}

pub fn router(database: Database) -> axum::Router {
    let database = Arc::new(database);
    let analytics = analytics::Analytics::new(database.clone());
    api::router(Services {
        transactions: transactions::Transactions::new(database.clone(), analytics.clone()),
        analytics,
        inventory: inventory::Inventory::new(database),
    })
}
