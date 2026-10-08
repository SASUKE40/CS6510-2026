//! Shared data-access library for the service-based checkout system.
//!
//! Every service links this crate and opens its own connection to the one
//! centralized SQLite database. It is the only crate that knows SQL or
//! rusqlite. Services couple only through the tables, never through calls to
//! each other. Repositories are grouped by bounded context so each service
//! uses only the modules it needs:
//!
//! | module         | tables                    | written by        | read by                        |
//! | -------------- | ------------------------- | ----------------- | ------------------------------ |
//! | [`catalog`]    | `items`, `stock_changes`  | payment (stock)   | inventory, basket, payment, analytics |
//! | [`baskets`]    | `transactions`, `lines`   | basket, payment   | basket, payment                |
//! | [`scans`]      | `scans`                   | basket, analytics | analytics                      |
//! | [`popularity`] | `popularity`              | analytics         | analytics                      |
//!
//! Schema creation, seeding, and reset belong to [`admin`] (the `db-admin`
//! tool), so no domain service owns the schema.
pub mod admin;
pub mod baskets;
pub mod catalog;
mod domain;
mod error;
pub mod popularity;
pub mod scans;
mod store;

pub use domain::{Basket, Item, Line, StockAlert, money, now};
pub use error::{Error, Kind, Result};
pub use store::{Db, SCHEMA_VERSION, Settings, Store};
