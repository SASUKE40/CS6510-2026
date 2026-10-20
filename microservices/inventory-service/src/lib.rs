//! Inventory service: the catalog, stock levels, low-stock alerts, and stock
//! reservations for checkout sagas. Owns `inventory.sqlite`.
//!
//! Public routes (through the gateway): `GET /items`, `GET /inventory/low-stock`.
//! Internal routes (service to service only):
//! - `GET /internal/catalog`: SKU, name, and price in cents, for replicas.
//! - `POST /internal/reservations`: all-or-nothing stock decrement, idempotent per id.
//! - `POST /internal/reservations/{id}/cancel`: compensation; restocks or tombstones.
pub mod store;

use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::JsonRejection, rejection::QueryRejection},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use service_kit::{ApiResult, Error, blocking, body, finish, money, now, query};
use std::sync::Arc;
pub use store::{Settings, Store};

pub const NAME: &str = "inventory-service";

pub fn router(store: Arc<Store>) -> Router {
    finish(
        NAME,
        Router::new()
            .route("/items", get(items))
            .route("/inventory/low-stock", get(low_stock))
            .route("/internal/catalog", get(catalog))
            .route("/internal/reservations", post(reserve))
            .route("/internal/reservations/{id}/cancel", post(cancel))
            .with_state(store),
    )
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

async fn catalog(State(store): State<Arc<Store>>) -> ApiResult<Json<Value>> {
    blocking(move || {
        let items: Vec<_> = store
            .items()?
            .into_iter()
            .map(|i| json!({"sku": i.sku, "name": i.name, "priceCents": i.cents}))
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
    input: Result<Query<LowStockQuery>, QueryRejection>,
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

#[derive(Deserialize)]
struct ReserveLine {
    sku: String,
    quantity: i64,
}
#[derive(Deserialize)]
struct Reserve {
    id: String,
    lines: Vec<ReserveLine>,
}
async fn reserve(
    State(store): State<Arc<Store>>,
    input: Result<Json<Reserve>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let request = body(input)?;
    let lines: Vec<_> = request
        .lines
        .into_iter()
        .map(|l| (l.sku, l.quantity))
        .collect();
    blocking(move || {
        let outcome = store.reserve(&request.id, &lines)?;
        Ok(Json(
            json!({"id": request.id, "status": "RESERVED", "replayed": outcome.replayed}),
        ))
    })
    .await
}

async fn cancel(State(store): State<Arc<Store>>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    blocking(move || {
        let restocked = store.cancel(&id)?;
        Ok(Json(
            json!({"id": id, "status": "CANCELLED", "restocked": restocked}),
        ))
    })
    .await
}
