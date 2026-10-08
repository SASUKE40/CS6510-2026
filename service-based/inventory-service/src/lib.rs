//! Inventory service: the product catalog and low-stock alerts.
//! Routes: `GET /items`, `GET /inventory/low-stock`.
use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    routing::get,
};
use checkout_data::{Error, Store, catalog, money, now};
use serde::Deserialize;
use serde_json::{Value, json};
use service_kit::{ApiResult, blocking, finish, query};
use std::sync::Arc;

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

async fn items(State(store): State<Arc<Store>>) -> ApiResult<Json<Value>> {
    blocking(move || {
        let items: Vec<_> = store
            .read(catalog::list)?
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
    input: Result<Query<LowStockQuery>, QueryRejection>,
) -> ApiResult<Json<Value>> {
    let threshold = query(input)?
        .threshold
        .unwrap_or(store.settings().threshold);
    if threshold < 0 {
        return Err(Error::bad("threshold must be nonnegative").into());
    }
    blocking(move || {
        let alerts: Vec<_> = store
            .read(|db| catalog::low_stock(db, threshold))?
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
