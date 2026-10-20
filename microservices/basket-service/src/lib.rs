//! Basket service: starts transactions, scans items into open baskets, and
//! reports basket state. Owns `basket.sqlite`.
//!
//! Public routes: `POST /transactions`, `GET /transactions/{id}`,
//! `POST /transactions/{id}/items`.
//! Internal routes, called by the payment saga orchestrator:
//! `POST /internal/baskets/{id}/checkout|release|complete` (body `{"sagaId"}`).
//! Internal route consumed by analytics: `GET /internal/scan-events?after=&limit=`.
pub mod store;

use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::JsonRejection, rejection::QueryRejection},
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use service_kit::{
    ApiResult, Error, HttpClient, Result, blocking, body, finish, money, parse_transaction_id,
    query,
};
use std::{sync::Arc, time::Duration};
pub use store::Store;
use store::{Basket, CatalogItem};

pub const NAME: &str = "basket-service";
const MAX_EVENTS: i64 = 10_000;

/// Pulls inventory's catalog into the local replica. Waits up to `wait` for
/// inventory on first start; with a replica already present, an unreachable
/// inventory is only a warning.
pub async fn sync_catalog(
    store: &Arc<Store>,
    client: &HttpClient,
    inventory: &str,
    wait: Duration,
) -> Result<usize> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let reply = match client.get(&format!("{inventory}/internal/catalog")).await {
            Ok(reply) => reply.ok(),
            Err(error) => Err(error),
        };
        match reply {
            Ok(body) => {
                let items: Vec<_> = body["items"]
                    .as_array()
                    .ok_or_else(|| Error::internal("catalog reply has no items"))?
                    .iter()
                    .map(|i| CatalogItem {
                        sku: i["sku"].as_str().unwrap_or_default().to_owned(),
                        name: i["name"].as_str().unwrap_or_default().to_owned(),
                        cents: i["priceCents"].as_i64().unwrap_or_default(),
                    })
                    .collect();
                let count = items.len();
                let store = store.clone();
                blocking(move || store.replace_catalog(&items)).await?;
                return Ok(count);
            }
            Err(error) => {
                let store_ = store.clone();
                if blocking(move || store_.catalog_size()).await? > 0 {
                    eprintln!("{NAME}: catalog refresh failed, using local replica: {error}");
                    return Ok(0);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(error);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

pub fn router(store: Arc<Store>) -> Router {
    finish(
        NAME,
        Router::new()
            .route("/transactions", post(start))
            .route("/transactions/{id}", get(status))
            .route("/transactions/{id}/items", post(scan))
            .route("/internal/baskets/{id}/checkout", post(checkout))
            .route("/internal/baskets/{id}/release", post(release))
            .route("/internal/baskets/{id}/complete", post(complete))
            .route("/internal/scan-events", get(events))
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
        return Err(Error::bad("stationId must not be blank"));
    }
    let basket = blocking(move || store.create(&station)).await?;
    Ok((StatusCode::CREATED, Json(summary(&basket))))
}

async fn status(State(store): State<Arc<Store>>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let id = parse_transaction_id(&id)?;
    blocking(move || Ok(Json(summary(&store.get(id)?)))).await
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
    let id = parse_transaction_id(&id)?;
    let (basket, item) = blocking(move || store.scan(id, &sku)).await?;
    Ok(Json(
        json!({"transactionId": format!("tx-{id}"), "sku": item.sku, "name": item.name,
            "unitPrice": money(item.cents), "itemCount": basket.count, "runningTotal": money(basket.cents)}),
    ))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SagaStep {
    saga_id: String,
    completed_at: Option<String>,
}

async fn checkout(
    State(store): State<Arc<Store>>,
    Path(id): Path<String>,
    input: std::result::Result<Json<SagaStep>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let saga = body(input)?.saga_id;
    let id = parse_transaction_id(&id)?;
    let (basket, lines) = blocking(move || store.checkout(id, &saga)).await?;
    let lines: Vec<_> = lines
        .into_iter()
        .map(|l| {
            json!({"sku": l.item.sku, "name": l.item.name,
                "unitPriceCents": l.item.cents, "quantity": l.quantity})
        })
        .collect();
    Ok(Json(json!({"transactionId": format!("tx-{}", basket.id),
        "stationId": basket.station, "itemCount": basket.count, "totalCents": basket.cents,
        "startedAt": basket.started_at, "lines": lines})))
}

async fn release(
    State(store): State<Arc<Store>>,
    Path(id): Path<String>,
    input: std::result::Result<Json<SagaStep>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let saga = body(input)?.saga_id;
    let id = parse_transaction_id(&id)?;
    let released = blocking(move || store.release(id, &saga)).await?;
    Ok(Json(json!({"released": released})))
}

async fn complete(
    State(store): State<Arc<Store>>,
    Path(id): Path<String>,
    input: std::result::Result<Json<SagaStep>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let step = body(input)?;
    let completed_at = step
        .completed_at
        .ok_or_else(|| Error::bad("completedAt is required"))?;
    let id = parse_transaction_id(&id)?;
    let basket = blocking(move || store.complete(id, &step.saga_id, &completed_at)).await?;
    Ok(Json(summary(&basket)))
}

#[derive(Deserialize)]
struct EventsQuery {
    after: Option<i64>,
    limit: Option<i64>,
}
async fn events(
    State(store): State<Arc<Store>>,
    input: std::result::Result<Query<EventsQuery>, QueryRejection>,
) -> ApiResult<Json<Value>> {
    let request = query(input)?;
    let after = request.after.unwrap_or(0).max(0);
    let limit = request.limit.unwrap_or(5_000).clamp(1, MAX_EVENTS);
    blocking(move || {
        let events: Vec<_> = store
            .events(after, limit)?
            .into_iter()
            .map(|e| json!({"sequence": e.sequence, "sku": e.sku, "name": e.name}))
            .collect();
        Ok(Json(json!({"events": events})))
    })
    .await
}

/// The contract's basket view. A basket held by a saga is still `OPEN` to
/// clients; the spec has no in-between state.
fn summary(basket: &Basket) -> Value {
    let status = if basket.status == "CHECKING_OUT" {
        "OPEN"
    } else {
        &basket.status
    };
    json!({"transactionId": format!("tx-{}", basket.id), "stationId": basket.station,
        "status": status, "itemCount": basket.count, "runningTotal": money(basket.cents),
        "startedAt": basket.started_at})
}
