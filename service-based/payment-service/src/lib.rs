//! Payment service: completes a basket. Payment always succeeds in this
//! simulation, so completion means decrementing stock for every line
//! all-or-nothing, marking the basket COMPLETED, and returning the receipt.
//! Route: `POST /transactions/{id}/complete`.
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use checkout_data::{Error, Store, baskets, catalog, money, now};
use serde_json::{Value, json};
use service_kit::{ApiResult, blocking, finish};
use std::sync::Arc;

pub const NAME: &str = "payment-service";

pub fn router(store: Arc<Store>) -> Router {
    finish(
        NAME,
        Router::new()
            .route("/transactions/{id}/complete", post(complete))
            .with_state(store),
    )
}

async fn complete(
    State(store): State<Arc<Store>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let id = id
        .strip_prefix("tx-")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|&n| n > 0)
        .ok_or_else(|| Error::missing("No such transaction"))?;
    // One database transaction covers the status check, every guarded stock
    // decrement, and the status change. The shared database's write lock
    // serializes this against concurrent scans and completions, even those
    // issued by other service processes.
    blocking(move || {
        store.write(|db| {
            let basket =
                baskets::get(db, id)?.ok_or_else(|| Error::missing("No such transaction"))?;
            if !basket.is_open() {
                return Err(Error::conflict(
                    "TRANSACTION_NOT_OPEN",
                    "Transaction is not open",
                ));
            }
            if basket.count == 0 {
                return Err(Error::conflict(
                    "EMPTY_BASKET",
                    "Cannot complete an empty transaction",
                ));
            }
            let lines = baskets::lines(db, id)?;
            let completed_at = now();
            for line in &lines {
                if !catalog::decrement(db, &line.item.sku, line.quantity, &completed_at)? {
                    return Err(Error::conflict(
                        "INSUFFICIENT_STOCK",
                        "Insufficient stock to complete the whole basket",
                    ));
                }
            }
            baskets::mark_completed(db, id, &completed_at)?;
            let lines: Vec<_> = lines
                .into_iter()
                .map(|line| {
                    json!({"sku": line.item.sku, "name": line.item.name,
                        "unitPrice": money(line.item.cents), "quantity": line.quantity})
                })
                .collect();
            Ok(Json(
                json!({"transactionId": format!("tx-{id}"), "stationId": basket.station,
                "itemCount": basket.count, "totalAmount": money(basket.cents),
                "startedAt": basket.started_at, "completedAt": completed_at, "lines": lines}),
            ))
        })
    })
    .await
}
