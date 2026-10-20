//! Payment service: completes a basket as an orchestrated saga. Owns
//! `payment.sqlite` (the saga log and payments) and calls the basket and
//! inventory services' internal APIs. Route: `POST /transactions/{id}/complete`.
//!
//! ```text
//! STARTED --basket checkout--> BASKET_LOCKED --inventory reserve--> STOCK_RESERVED
//!    |                              |          (+ payment recorded)      |
//!    +-------- compensate: cancel reservation, release basket -----+     +--basket complete--> COMPLETED
//!                                   v
//!                                ABORTED
//! ```
//!
//! Every step is idempotent and keyed by the saga id, so transport failures
//! are retried, and a restarted orchestrator finishes what it started:
//! sagas at STOCK_RESERVED roll forward, earlier ones roll back.
pub mod store;

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};
use service_kit::{
    ApiResult, Error, HttpClient, Kind, Result, blocking, finish, money, now, parse_transaction_id,
};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
pub use store::{Saga, Store};

pub const NAME: &str = "payment-service";
const ATTEMPTS: u32 = 3;
/// Periodic recovery leaves recent sagas alone; their requests may still be running.
const RECOVERY_GRACE: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct Upstreams {
    pub basket: String,
    pub inventory: String,
}

pub struct Payment {
    store: Arc<Store>,
    client: HttpClient,
    upstreams: Upstreams,
    active: Mutex<HashSet<i64>>,
}

/// Marks a saga as driven by a live request so recovery skips it.
struct Active<'a>(&'a Mutex<HashSet<i64>>, i64);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        if let Ok(mut set) = self.0.lock() {
            set.remove(&self.1);
        }
    }
}

fn is_rejection(error: &Error) -> bool {
    matches!(error.kind, Kind::Invalid | Kind::NotFound | Kind::Conflict)
}

impl Payment {
    pub fn new(store: Arc<Store>, upstreams: Upstreams, client: HttpClient) -> Self {
        Self {
            store,
            client,
            upstreams,
            active: Mutex::new(HashSet::new()),
        }
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    fn activate(&self, saga: i64) -> Option<Active<'_>> {
        let mut set = self.active.lock().ok()?;
        set.insert(saga).then(|| Active(&self.active, saga))
    }

    async fn db<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let store = self.store.clone();
        blocking(move || f(&store)).await
    }

    /// POSTs to an idempotent internal endpoint, retrying only when the
    /// outcome is unknown (transport failure or timeout).
    async fn call(&self, url: &str, body: Value) -> Result<Value> {
        self.call_counted(url, body).await.0
    }

    /// Like [`Self::call`], also returning how many requests were sent. With
    /// more than one, an earlier request may still execute late.
    async fn call_counted(&self, url: &str, body: Value) -> (Result<Value>, u32) {
        let mut attempt = 1;
        loop {
            match self.client.post(url, &body).await {
                Ok(reply) => return (reply.ok(), attempt),
                Err(error) if attempt >= ATTEMPTS => return (Err(error), attempt),
                Err(error) => {
                    eprintln!("{NAME}: retrying {url}: {error}");
                    tokio::time::sleep(Duration::from_millis(25 * u64::from(attempt))).await;
                    attempt += 1;
                }
            }
        }
    }

    fn basket_url(&self, tx: &str, step: &str) -> String {
        format!("{}/internal/baskets/{tx}/{step}", self.upstreams.basket)
    }

    /// Runs the whole saga for one completion request.
    pub async fn complete(&self, transaction: &str) -> Result<Value> {
        let tx = format!("tx-{}", parse_transaction_id(transaction)?);
        let saga = self
            .db({
                let tx = tx.clone();
                move |s| s.start(&tx)
            })
            .await?;
        let _active = self.activate(saga);
        let saga_id = format!("saga-{saga}");

        // Step 1: lock the basket so no scan can change it mid-checkout.
        let basket = match self
            .call(
                &self.basket_url(&tx, "checkout"),
                json!({"sagaId": saga_id}),
            )
            .await
        {
            Ok(basket) => basket,
            Err(error) if is_rejection(&error) => {
                let code = error.code.clone();
                self.db(move |s| s.aborted(saga, &code)).await?;
                return Err(error);
            }
            Err(error) => {
                eprintln!("{NAME}: saga {saga}: basket checkout failed: {error}");
                self.roll_back(saga, &tx, "BASKET_UNAVAILABLE").await;
                return Err(Error::unavailable("The basket service is unavailable"));
            }
        };
        self.db(move |s| s.locked(saga)).await?;

        // Step 2: reserve every line's stock, all or nothing, in inventory.
        let lines: Vec<_> = basket["lines"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|l| json!({"sku": l["sku"], "quantity": l["quantity"]}))
            .collect();
        let reserve = format!("{}/internal/reservations", self.upstreams.inventory);
        match self
            .call_counted(&reserve, json!({"id": saga_id, "lines": lines}))
            .await
        {
            (Ok(_), _) => {}
            (Err(error), 1) if is_rejection(&error) => {
                // The only request was refused, so nothing was reserved and
                // nothing can be later; just undo the basket lock.
                match self
                    .call(&self.basket_url(&tx, "release"), json!({"sagaId": saga_id}))
                    .await
                {
                    Ok(_) => {
                        let code = error.code.clone();
                        self.db(move |s| s.aborted(saga, &code)).await?
                    }
                    Err(release) => {
                        eprintln!(
                            "{NAME}: saga {saga}: release failed, left for recovery: {release}"
                        )
                    }
                }
                return Err(error);
            }
            (Err(error), _) if is_rejection(&error) => {
                // A timed-out earlier request might still reserve: tombstone it.
                let code = error.code.clone();
                self.roll_back(saga, &tx, &code).await;
                return Err(error);
            }
            (Err(error), _) => {
                eprintln!("{NAME}: saga {saga}: reservation outcome unknown: {error}");
                self.roll_back(saga, &tx, "INVENTORY_UNAVAILABLE").await;
                return Err(Error::unavailable("The inventory service is unavailable"));
            }
        }

        // Step 3 (pivot): record the payment. Payment never fails here.
        let paid_at = now();
        let amount = basket["totalCents"].as_i64().unwrap_or_default();
        self.db({
            let (basket, paid_at) = (basket.clone(), paid_at.clone());
            move |s| s.reserved(saga, &basket, amount, &paid_at)
        })
        .await?;

        // Step 4: mark the basket completed. If basket is unreachable now,
        // the sale stands and recovery finishes this step later.
        self.finish(saga, &tx, &paid_at).await;
        Ok(receipt(&basket, &paid_at))
    }

    async fn finish(&self, saga: i64, tx: &str, paid_at: &str) -> bool {
        let body = json!({"sagaId": format!("saga-{saga}"), "completedAt": paid_at});
        match self.call(&self.basket_url(tx, "complete"), body).await {
            Ok(_) => match self.db(move |s| s.completed(saga)).await {
                Ok(()) => true,
                Err(error) => {
                    eprintln!("{NAME}: saga {saga}: {error}");
                    false
                }
            },
            Err(error) => {
                eprintln!(
                    "{NAME}: saga {saga}: basket complete failed, left for recovery: {error}"
                );
                false
            }
        }
    }

    /// Compensation when a request may still execute late: cancel the
    /// reservation (a tombstone if it never happened) and release the basket
    /// (also tombstoned). On failure the saga stays unfinished for recovery.
    async fn roll_back(&self, saga: i64, tx: &str, reason: &str) -> bool {
        let saga_id = format!("saga-{saga}");
        let cancel = format!(
            "{}/internal/reservations/{saga_id}/cancel",
            self.upstreams.inventory
        );
        let undone = match self.call(&cancel, json!({})).await {
            Ok(_) => {
                match self
                    .call(&self.basket_url(tx, "release"), json!({"sagaId": saga_id}))
                    .await
                {
                    Ok(_) => true,
                    Err(error) if error.kind == Kind::NotFound => true,
                    Err(error) => {
                        eprintln!("{NAME}: saga {saga}: release failed: {error}");
                        false
                    }
                }
            }
            Err(error) => {
                eprintln!("{NAME}: saga {saga}: cancel failed: {error}");
                false
            }
        };
        let reason = reason.to_owned();
        undone && self.db(move |s| s.aborted(saga, &reason)).await.is_ok()
    }

    /// Settles unfinished sagas: STOCK_RESERVED rolls forward, earlier states
    /// roll back. With `all`, includes recent sagas (use only when no request
    /// can be in flight, i.e. at startup). Returns how many were settled.
    pub async fn recover(&self, all: bool) -> Result<usize> {
        let before = if all {
            "9999".to_owned()
        } else {
            (Utc::now() - RECOVERY_GRACE).to_rfc3339_opts(SecondsFormat::Millis, true)
        };
        let sagas = self.db(move |s| s.unfinished(&before)).await?;
        let mut settled = 0;
        for saga in sagas {
            let Some(_active) = self.activate(saga.id) else {
                continue;
            };
            let done = match (saga.state.as_str(), &saga.paid_at) {
                ("STOCK_RESERVED", Some(paid_at)) => {
                    self.finish(saga.id, &saga.transaction_id, paid_at).await
                }
                _ => {
                    self.roll_back(saga.id, &saga.transaction_id, "RECOVERED")
                        .await
                }
            };
            if done {
                println!("{NAME}: recovered saga {} from {}", saga.id, saga.state);
                settled += 1;
            }
        }
        Ok(settled)
    }
}

fn receipt(basket: &Value, completed_at: &str) -> Value {
    let lines: Vec<_> = basket["lines"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|l| {
            json!({"sku": l["sku"], "name": l["name"],
                "unitPrice": money(l["unitPriceCents"].as_i64().unwrap_or_default()),
                "quantity": l["quantity"]})
        })
        .collect();
    json!({"transactionId": basket["transactionId"], "stationId": basket["stationId"],
        "itemCount": basket["itemCount"],
        "totalAmount": money(basket["totalCents"].as_i64().unwrap_or_default()),
        "startedAt": basket["startedAt"], "completedAt": completed_at, "lines": lines})
}

/// Retries unfinished sagas in the background (e.g. after a downstream outage).
pub fn spawn_recovery(payment: Arc<Payment>, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(error) = payment.recover(false).await {
                eprintln!("{NAME}: recovery pass failed: {error}");
            }
        }
    })
}

pub fn router(payment: Arc<Payment>) -> Router {
    finish(
        NAME,
        Router::new()
            .route("/transactions/{id}/complete", post(complete))
            .with_state(payment),
    )
}

async fn complete(
    State(payment): State<Arc<Payment>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    payment.complete(&id).await.map(Json)
}
