//! Inventory service: the catalog, stock levels, low-stock alerts, and stock
//! reservations for checkout sagas. Owns `inventory.sqlite`.
//!
//! Public routes (through the gateway): `GET /items`, `GET /inventory/low-stock`.
//! Events (no other service calls this one):
//! - publishes `CatalogPublished` (SKU, name, price in cents) to the catalog
//!   topic at startup, for basket's replica;
//! - consumes `Reserve` (all-or-nothing stock decrement) and `Cancel`
//!   (compensation; restocks or tombstones) from its command topic and
//!   answers each, keyed by saga id, on its event topic.
pub mod store;

use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    routing::get,
};
use serde::Deserialize;
use serde_json::{Value, json};
use service_kit::{
    ApiResult, Bus, Delivery, Error, Outgoing, Result, Topic, blocking, bus, finish, money, now,
    query,
};
use std::sync::Arc;
pub use store::{Reservation, Settings, Store};

pub const NAME: &str = "inventory-service";

pub fn router(store: Arc<Store>) -> Router {
    finish(
        NAME,
        Router::new()
            .route("/items", get(items))
            .route("/inventory/low-stock", get(low_stock))
            .with_state(store),
    )
}

/// Publishes the full catalog so replicas can (re)build from the latest one.
pub async fn publish_catalog(store: &Arc<Store>, bus: &Bus) -> Result<usize> {
    let store = store.clone();
    let items = blocking(move || store.items()).await?;
    let count = items.len();
    let items: Vec<_> = items
        .into_iter()
        .map(|i| json!({"sku": i.sku, "name": i.name, "priceCents": i.cents}))
        .collect();
    bus.publish(
        Topic::Catalog,
        "catalog",
        json!({"type": "CatalogPublished", "items": items}),
    )
    .await?;
    Ok(count)
}

/// Consumes inventory commands until aborted.
pub async fn spawn_consumer(store: Arc<Store>, bus: Bus) -> Result<tokio::task::JoinHandle<()>> {
    let consumer = bus.subscribe(NAME, &[Topic::InventoryCommands])?;
    let polled = consumer.clone();
    Ok(bus::start(NAME, &consumer, async move {
        bus::run(bus.clone(), polled, NAME, move |delivery| {
            handle(store.clone(), delivery)
        })
        .await
    })
    .await)
}

/// Applies one command and returns its reply. Business refusals become
/// `StockRejected` replies; only infrastructure failures are errors.
pub async fn handle(store: Arc<Store>, command: Delivery) -> Result<Vec<Outgoing>> {
    let saga = command.value["sagaId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let reply = match command.kind() {
        "Reserve" => {
            let lines: Vec<_> = command.value["lines"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|l| {
                    (
                        l["sku"].as_str().unwrap_or_default().to_owned(),
                        l["quantity"].as_i64().unwrap_or_default(),
                    )
                })
                .collect();
            let id = saga.clone();
            let outcome = match blocking(move || store.reserve(&id, &lines)).await {
                Ok(outcome) => outcome,
                Err(error) if error.is_rejection() => Reservation::Rejected(error),
                Err(error) => return Err(error),
            };
            match outcome {
                Reservation::Reserved { replayed } => {
                    json!({"type": "StockReserved", "sagaId": saga, "replayed": replayed})
                }
                Reservation::Rejected(error) => with_type("StockRejected", &saga, &error),
            }
        }
        "Cancel" => {
            let id = saga.clone();
            let restocked = blocking(move || store.cancel(&id)).await?;
            json!({"type": "ReservationCancelled", "sagaId": saga, "restocked": restocked})
        }
        other => {
            eprintln!("{NAME}: ignoring unknown command {other:?}");
            return Ok(vec![]);
        }
    };
    Ok(vec![Outgoing::new(Topic::InventoryEvents, saga, reply)])
}

fn with_type(kind: &str, saga: &str, error: &Error) -> Value {
    let mut event = error.to_event();
    event["type"] = json!(kind);
    event["sagaId"] = json!(saga);
    event
}

async fn items(State(store): State<Arc<Store>>) -> ApiResult<Json<Value>> {
    blocking(move || {
        let items: Vec<_> = store
            .items()?
            .into_iter()
            .map(|i| json!({"sku": i.sku, "name": i.name, "price": money(i.cents)}))
            .collect();
        Ok(Json(json!({"items": items})))
    })
    .await
}

#[derive(Deserialize)]
struct LowStockQuery {
    threshold: Option<i64>,
}
async fn low_stock(
    State(store): State<Arc<Store>>,
    input: std::result::Result<Query<LowStockQuery>, QueryRejection>,
) -> ApiResult<Json<Value>> {
    let threshold = query(input)?
        .threshold
        .unwrap_or(store.settings().threshold);
    if threshold < 0 {
        return Err(Error::bad("threshold must be nonnegative"));
    }
    blocking(move || {
        let alerts: Vec<_> = store
            .low_stock(threshold)?
            .into_iter()
            .map(|a| {
                json!({"sku": a.sku, "name": a.name, "currentStock": a.stock,
                    "threshold": threshold, "triggeredAt": a.triggered_at})
            })
            .collect();
        Ok(Json(
            json!({"threshold": threshold, "generatedAt": now(), "alerts": alerts}),
        ))
    })
    .await
}
