//! Basket service: starts transactions, scans items into open baskets, and
//! reports basket state. Each accepted scan is also appended to the shared
//! scan log, which is how the analytics service learns about it.
//! Routes: `POST /transactions`, `GET /transactions/{id}`,
//! `POST /transactions/{id}/items`.
use axum::{
    Json, Router,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{get, post},
};
use checkout_data::{Basket, Db, Error, Result, Store, baskets, catalog, money, now, scans};
use serde::Deserialize;
use serde_json::{Value, json};
use service_kit::{ApiResult, blocking, body, finish};
use std::sync::Arc;

pub const NAME: &str = "basket-service";

pub fn router(store: Arc<Store>) -> Router {
    finish(
        NAME,
        Router::new()
            .route("/transactions", post(start))
            .route("/transactions/{id}", get(status))
            .route("/transactions/{id}/items", post(scan))
            .with_state(store),
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Start {
    station_id: String,
}
async fn start(
    State(store): State<Arc<Store>>,
    input: std::result::Result<Json<Start>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let station = body(input)?.station_id;
    if station.trim().is_empty() {
        return Err(Error::bad("stationId must not be blank").into());
    }
    let summary = blocking(move || {
        store.write(|db| {
            let id = baskets::create(db, &station, &now())?;
            Ok(summary(&basket(db, id)?))
        })
    })
    .await?;
    Ok((StatusCode::CREATED, Json(summary)))
}

async fn status(State(store): State<Arc<Store>>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let id = parse_id(&id)?;
    blocking(move || store.read(|db| Ok(Json(summary(&basket(db, id)?))))).await
}

#[derive(Deserialize)]
struct Scan {
    sku: String,
}
async fn scan(
    State(store): State<Arc<Store>>,
    Path(id): Path<String>,
    input: std::result::Result<Json<Scan>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let sku = body(input)?.sku;
    let id = parse_id(&id)?;
    blocking(move || {
        store.write(|db| {
            if !basket(db, id)?.is_open() {
                return Err(Error::conflict("TRANSACTION_NOT_OPEN", "Transaction is not open"));
            }
            let item = catalog::find(db, &sku)?
                .ok_or_else(|| Error::not_found("UNKNOWN_SKU", "No such SKU"))?;
            baskets::add_unit(db, id, &item)?;
            // Same unit of work as the basket change: a scan is counted
            // exactly when it is accepted.
            scans::append(db, &item.sku)?;
            let updated = basket(db, id)?;
            Ok(Json(json!({"transactionId": format!("tx-{id}"), "sku": item.sku, "name": item.name,
                "unitPrice": money(item.cents), "itemCount": updated.count, "runningTotal": money(updated.cents)})))
        })
    })
    .await
}

fn parse_id(id: &str) -> Result<i64> {
    id.strip_prefix("tx-")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| n > 0)
        .ok_or_else(|| Error::missing("No such transaction"))
}
fn basket(db: &Db<'_>, id: i64) -> Result<Basket> {
    baskets::get(db, id)?.ok_or_else(|| Error::missing("No such transaction"))
}
fn summary(basket: &Basket) -> Value {
    json!({"transactionId": format!("tx-{}", basket.id), "stationId": basket.station,
        "status": basket.status, "itemCount": basket.count, "runningTotal": money(basket.cents),
        "startedAt": basket.started_at})
}
