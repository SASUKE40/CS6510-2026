//! Composition root: the layered checkout server with popular-items analytics
//! running as a pipes-and-filters pipeline.
mod analytics;
mod api;
mod config;
mod database;
mod domain;
mod error;
mod inventory;
mod transactions;

pub use analytics::Pipeline;
pub use config::Config;
pub use database::Database;
use std::sync::Arc;

pub const PIPELINE_STAGES: &str = analytics::STAGES;

#[derive(Clone)]
pub(crate) struct Services {
    transactions: transactions::Transactions,
    analytics: analytics::Analytics,
    inventory: inventory::Inventory,
}

/// Starts the analytics filters and returns the HTTP router with the handle
/// that drains them on shutdown.
pub fn app(database: Database) -> Result<(axum::Router, Pipeline), Box<dyn std::error::Error>> {
    let database = Arc::new(database);
    let (analytics, pipeline) = analytics::start(database.clone())?;
    let router = api::router(Services {
        transactions: transactions::Transactions::new(database.clone(), analytics.clone()),
        analytics,
        inventory: inventory::Inventory::new(database),
    });
    Ok((router, pipeline))
}

/// Like [`app`], for callers that never shut down explicitly: the filters
/// exit once the router and all of its clones are dropped.
pub fn router(database: Database) -> axum::Router {
    app(database)
        .expect("analytics state could not be loaded")
        .0
}
